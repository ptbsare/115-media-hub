"""本地存储位置管理。

跨盘复制的大文件暂存需要落在大容量磁盘（机械硬盘/NAS），而 Docker 容器默认
只有 config/strm/logs 三个卷。本模块把"容器内可写的本地目录"抽象成「存储位置」：
用户在设置页维护位置列表（label + 容器内路径），复制引擎按配置选择暂存落点。

设计约定：
- 位置 id 由内容生成（路径归一化后的短哈希），同路径重复添加会被去重；
- 保存位置时校验目录存在、可写、可读出剩余空间，不合格拒绝保存；
- 容器内路径保持中立（/app/storage、/app/storage2...），盘型信息放 label；
- 默认位置指向 config 卷所在目录（/app/config），无需额外挂载即可工作。
"""

import hashlib
import logging
import os
import re
import shutil
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# 跨盘复制默认暂存子目录名
CROSS_COPY_TEMP_SUBDIR = "cross_copy_tmp"

# 位置 id 字符集（防路径穿越/怪异字符进配置）
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_SAFE_SUBDIR_RE = re.compile(r"^[A-Za-z0-9._\-/]{1,128}$")


def _normalize_container_path(raw: Any) -> str:
    """归一化容器内路径：去空白、统一正斜杠、去尾斜杠、拒绝相对路径。"""
    path = str(raw or "").strip()
    if not path:
        return ""
    path = path.replace("\\", "/")
    # 折叠重复斜杠（保留开头的 // 语义不存在于容器路径，直接折叠）
    path = re.sub(r"/{2,}", "/", path)
    if not path.startswith("/"):
        return ""
    while len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return path


def _normalize_subdir(raw: Any) -> str:
    """归一化子目录：仅允许相对路径片段，拒绝 ../ 穿越与绝对路径。"""
    subdir = str(raw or "").strip().replace("\\", "/")
    if not subdir:
        return ""
    if subdir.startswith("/"):
        return ""
    if not _SAFE_SUBDIR_RE.match(subdir):
        return ""
    segments = [seg for seg in subdir.split("/") if seg not in ("", ".")]
    if any(seg == ".." for seg in segments):
        return ""
    return "/".join(segments)


def _make_storage_id(path: str) -> str:
    """由归一化路径生成稳定 id（同路径 → 同 id，天然去重）。"""
    digest = hashlib.md5(path.encode("utf-8")).hexdigest()[:10]
    return f"st-{digest}"


def normalize_local_storage_entry(raw: Any) -> Optional[Dict[str, Any]]:
    """归一化单条存储位置；不合格返回 None。"""
    item = raw if isinstance(raw, dict) else {}
    path = _normalize_container_path(item.get("path", ""))
    if not path:
        return None
    label = str(item.get("label", "") or "").strip()[:60]
    entry_id = str(item.get("id", "") or "").strip()
    if not _SAFE_ID_RE.match(entry_id):
        entry_id = _make_storage_id(path)
    return {
        "id": entry_id,
        "label": label,
        "path": path,
        "enabled": bool(item.get("enabled", True)),
    }


def normalize_local_storages(value: Any) -> List[Dict[str, Any]]:
    """归一化位置列表：按 id 去重，保序；附带 config 卷默认位置。"""
    raw_items: List[Any] = []
    if isinstance(value, list):
        raw_items = list(value)
    elif isinstance(value, dict):
        # 兼容旧版单对象写法
        raw_items = [value]

    normalized: List[Dict[str, Any]] = []
    seen: set = set()
    for raw in raw_items:
        entry = normalize_local_storage_entry(raw)
        if entry is None or entry["id"] in seen:
            continue
        seen.add(entry["id"])
        normalized.append(entry)
    return normalized


def default_storage_path() -> str:
    """默认位置：config 卷所在目录（/app/config，容器内无需额外挂载）。"""
    return _normalize_container_path(os.environ.get("MEDIA_HUB_CONFIG_DIR", "/app/config"))


def ensure_default_storage(storages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """确保默认位置（config 卷）存在，避免配置为空时无暂存落点。

    按归一化路径判重（id 允许被外部改写，不可作为唯一键）。
    """
    default_path = default_storage_path()
    if not default_path:
        return storages
    for entry in storages:
        if _normalize_container_path(entry.get("path", "")) == default_path:
            return storages
    return [{"id": _make_storage_id(default_path), "label": "默认（config 卷）", "path": default_path, "enabled": True}] + storages


def probe_storage_path(path: str) -> Dict[str, Any]:
    """探测目录可用性：存在性、可写、剩余空间。

    返回 {ok, path, exists, writable, free_bytes, total_bytes, error}，
    不做任何创建/修改操作。
    """
    result: Dict[str, Any] = {
        "ok": False,
        "path": path,
        "exists": False,
        "writable": False,
        "free_bytes": 0,
        "total_bytes": 0,
        "error": "",
    }
    if not path:
        result["error"] = "路径不能为空"
        return result
    try:
        result["exists"] = os.path.isdir(path)
        if not result["exists"]:
            result["error"] = "目录不存在（需先在宿主机挂载进容器）"
            return result
        probe_name = f".wb_storage_probe_{os.getpid()}_{threading.get_ident()}"
        probe_file = os.path.join(path, probe_name)
        try:
            with open(probe_file, "wb") as fh:
                fh.write(b"ok")
            result["writable"] = True
        except OSError as exc:
            result["error"] = f"目录不可写：{exc}"
            return result
        finally:
            try:
                if os.path.exists(probe_file):
                    os.remove(probe_file)
            except OSError:
                pass
        usage = shutil.disk_usage(path)
        result["free_bytes"] = int(usage.free)
        result["total_bytes"] = int(usage.total)
        result["ok"] = True
        return result
    except OSError as exc:
        result["error"] = f"目录不可用：{exc}"
        return result


def list_storage_volumes(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """返回带运行时状态的位置列表（供设置页/任务中心展示）。"""
    storages = ensure_default_storage(normalize_local_storages(cfg.get("local_storages", [])))
    volumes: List[Dict[str, Any]] = []
    for entry in storages:
        probe = probe_storage_path(entry["path"])
        volumes.append(
            {
                **entry,
                "exists": probe["exists"],
                "writable": probe["writable"],
                "free_bytes": probe["free_bytes"],
                "total_bytes": probe["total_bytes"],
                "error": probe["error"],
                "ok": probe["ok"],
            }
        )
    return volumes


def get_storage_entry(cfg: Dict[str, Any], storage_id: str) -> Optional[Dict[str, Any]]:
    """按 id 查位置（未配置/已禁用返回 None）。"""
    key = str(storage_id or "").strip()
    if not key:
        return None
    storages = ensure_default_storage(normalize_local_storages(cfg.get("local_storages", [])))
    for entry in storages:
        if entry["id"] == key and entry.get("enabled", True):
            return entry
    return None


def resolve_storage_dir(
    cfg: Dict[str, Any],
    storage_id: str,
    subdir: str = "",
    *,
    create: bool = True,
) -> str:
    """把「位置 + 子目录」合成为容器内绝对路径。

    storage_id 为空时用默认位置；位置不存在时抛 ValueError。
    """
    entry = get_storage_entry(cfg, storage_id) if storage_id else None
    if entry is None:
        if storage_id:
            raise ValueError(f"存储位置不存在或已禁用：{storage_id}")
        entry = {
            "path": default_storage_path() or "/app/config",
        }
    base = entry["path"]
    clean_subdir = _normalize_subdir(subdir)
    target = os.path.join(base, clean_subdir) if clean_subdir else base
    if create:
        os.makedirs(target, exist_ok=True)
    return target


def resolve_cross_copy_temp_dir(cfg: Dict[str, Any], *, create: bool = False) -> str:
    """解析跨盘复制临时目录：位置 + 子目录 合成（缺省 /app/config/cross_copy_tmp）。"""
    storage_id = str(cfg.get("cross_copy_temp_storage", "") or "").strip()
    subdir = str(cfg.get("cross_copy_temp_subdir", "") or "").strip() or CROSS_COPY_TEMP_SUBDIR
    return resolve_storage_dir(cfg, storage_id, subdir, create=create)


def ensure_free_space(dir_path: str, required_bytes: int) -> None:
    """预检磁盘空间：不足时抛 ValueError，避免下到一半磁盘满。"""
    if required_bytes <= 0:
        return
    try:
        usage = shutil.disk_usage(dir_path)
    except OSError as exc:
        raise ValueError(f"无法读取暂存目录磁盘空间：{dir_path}（{exc}）") from exc
    if int(usage.free) < int(required_bytes):
        raise ValueError(
            f"暂存位置空间不足：需要 {_human_bytes(required_bytes)}，"
            f"可用 {_human_bytes(int(usage.free))}（{dir_path}）"
        )


def _human_bytes(num: int) -> str:
    value = float(num or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"
