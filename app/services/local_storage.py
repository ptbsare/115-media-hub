"""本地暂存目录管理。

跨盘复制的大文件暂存需要落在大容量磁盘（机械硬盘/NAS），而 Docker 容器默认
只有 config/strm/logs 三个卷。本模块只回答一个问题：**跨盘复制的临时文件放
哪个容器内目录**。

设计约定（2026-10-21 简化，原「位置列表」模型下线）：
- 一个配置项 `cross_copy_temp_dir`（容器内绝对路径），空 = 用默认目录；
- 默认目录是 config 卷下的 `cross_copy_tmp`（/app/config/cross_copy_tmp），
  config 卷是 compose 默认挂载的，不映射任何大盘也能工作；
- 想换成大盘：先在 compose 里把宿主机目录挂进容器，再把这里改成容器内路径，
  保存前会校验目录存在、可写、可读出剩余空间，不合格拒绝保存；
- 旧配置 `local_storages` + `cross_copy_temp_storage` + `cross_copy_temp_subdir`
  只在读取时一次性迁移（resolve_temp_dir），保存新值后由 core 归一化清掉。
"""

import logging
import os
import re
import shutil
import threading
from typing import Any, Dict

logger = logging.getLogger(__name__)

# 跨盘复制默认暂存子目录名（位于 config 卷下）
CROSS_COPY_TEMP_SUBDIR = "cross_copy_tmp"


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


def default_storage_path() -> str:
    """config 卷所在目录（compose 默认挂载，无需额外映射）。"""
    return _normalize_container_path(os.environ.get("MEDIA_HUB_CONFIG_DIR", "/app/config"))


def default_temp_dir() -> str:
    """默认暂存目录：config 卷下的 cross_copy_tmp。"""
    base = default_storage_path() or "/app/config"
    return os.path.join(base, CROSS_COPY_TEMP_SUBDIR)


def resolve_temp_dir(cfg: Dict[str, Any]) -> str:
    """解析当前暂存目录：新字段优先，其次迁移旧配置，最后默认值。

    迁移只读不写：旧配置（位置 id + 子目录）在此合成出实际路径，保存新值
    后由 core 的配置归一化清理旧字段。
    """
    path = _normalize_container_path(cfg.get("cross_copy_temp_dir", ""))
    if path:
        return path

    # 兼容旧版：cross_copy_temp_storage（位置 id）+ cross_copy_temp_subdir
    storage_id = str(cfg.get("cross_copy_temp_storage", "") or "").strip()
    if storage_id:
        subdir = str(cfg.get("cross_copy_temp_subdir", "") or "").strip() or CROSS_COPY_TEMP_SUBDIR
        raw_items = cfg.get("local_storages", [])
        items = raw_items if isinstance(raw_items, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("id", "") or "").strip() != storage_id:
                continue
            base = _normalize_container_path(item.get("path", ""))
            if base:
                return os.path.join(base, subdir)
    return default_temp_dir()


def resolve_cross_copy_temp_dir(cfg: Dict[str, Any], *, create: bool = False) -> str:
    """跨盘复制暂存目录解析入口（复制引擎调用）。

    create=True 时顺带建目录，避免任务执行时才发现路径不存在。
    """
    target = resolve_temp_dir(cfg)
    if create:
        os.makedirs(target, exist_ok=True)
    return target


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
