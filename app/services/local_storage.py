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
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# 跨盘复制默认暂存子目录名（位于 config 卷下）
CROSS_COPY_TEMP_SUBDIR = "cross_copy_tmp"

# 容器挂载表（Linux）。读取失败说明不在 Linux 容器里，退化为「只能手填」。
PROC_MOUNTS_PATH = "/proc/mounts"

# 这些文件系统不是「能存大文件的地方」，不作为候选目录列出。
_SYSTEM_FS = frozenset({
    "proc", "sysfs", "devpts", "devtmpfs", "cgroup", "cgroup2", "mqueue", "tmpfs",
    "ramfs", "overlay", "squashfs", "securityfs", "debugfs", "tracefs", "pstore",
    "configfs", "fusectl", "bpf", "autofs", "nsfs", "hugetlbfs", "selinuxfs",
    "binfmt_misc", "efivarfs", "fuse.lxcfs",
})

# 根文件系统是这几种时，说明 `/` 是容器的可写层（不是挂在真实磁盘上）。
_CONTAINER_ROOT_FS = frozenset({"overlay", "aufs", "fuse.overlayfs"})

# 这些挂载点前缀一律跳过（系统目录 + 容器注入的单文件）。
_SYSTEM_MOUNT_PREFIXES = (
    "/proc", "/sys", "/dev", "/run", "/etc/hostname", "/etc/hosts",
    "/etc/resolv.conf", "/etc/hostname", "/var/run",
)

# 应用自己的卷：可以选，但不是给暂存准备的，列表里加一句说明。
_APP_VOLUME_NOTES = {
    "/app/config": "config 卷：应用配置和数据库所在盘，默认暂存目录就在这下面",
    "/app/logs": "logs 卷：应用日志所在盘",
    "/app/strm": "strm 卷：生成的 STRM 播放文件所在盘",
}


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


def _unescape_mount_field(value: str) -> str:
    """还原挂载表里的八进制转义（空格是 \\040、制表符是 \\011）。"""
    if "\\" not in value:
        return value

    def _repl(match: "re.Match[str]") -> str:
        try:
            return chr(int(match.group(1), 8))
        except ValueError:
            return match.group(0)

    return re.sub(r"\\([0-7]{3})", _repl, value)


def read_mount_points(mounts_path: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
    """读容器挂载表，返回能存东西的挂载点；读不到返回 None（环境不支持枚举）。

    只留「目录型 + 非系统文件系统」的挂载：/proc /sys /dev 这些系统挂载、
    tmpfs 内存盘、以及 /etc/hosts 这种单文件挂载全部剔掉。返回 None 和返回
    空列表含义不同——None 表示压根判断不了（比如在 macOS 上直接跑），空列表
    表示能读但一个可用的都没有。
    """
    target_path = mounts_path or PROC_MOUNTS_PATH
    try:
        with open(target_path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except OSError:
        return None

    points: List[Dict[str, str]] = []
    seen = set()
    for line in lines:
        parts = line.split()
        if len(parts) < 4:
            continue
        device = parts[0]
        mount_point = _normalize_container_path(_unescape_mount_field(parts[1]))
        fs_type = parts[2]
        if not mount_point:
            continue
        # `/` 只保留「容器可写层」这一种，用来判断其它目录是不是落在临时层；
        # 裸机上 / 是真实磁盘，也保留（此时 is_container_layer=False）。
        is_container_root = mount_point == "/" and fs_type in _CONTAINER_ROOT_FS
        if fs_type in _SYSTEM_FS and not is_container_root:
            continue
        if mount_point != "/" and any(
            mount_point == p or mount_point.startswith(p + "/") for p in _SYSTEM_MOUNT_PREFIXES
        ):
            continue
        if mount_point in seen:
            continue
        # /etc/hosts 这类是「单文件挂载」，isdir 过滤掉
        if not os.path.isdir(mount_point):
            continue
        seen.add(mount_point)
        points.append({
            "path": mount_point,
            "fs_type": fs_type,
            "device": device,
            "is_container_layer": is_container_root,
        })

    # 层级浅的排前面（/app/config 在 /app/xxx/yyy 之前），同层按名字
    points.sort(key=lambda item: (item["path"].count("/"), item["path"]))
    return points


def _longest_mount_for(path: str, mount_points: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """path 落在哪个挂载点里（取最长的那个，处理嵌套挂载）。"""
    best: Optional[Dict[str, str]] = None
    for item in mount_points:
        mount_path = item["path"]
        if path == mount_path or path.startswith(mount_path.rstrip("/") + "/"):
            if best is None or len(mount_path) > len(best["path"]):
                best = item
    return best


def detect_storage_tier(path: str, mount_points: Optional[List[Dict[str, str]]] = None) -> str:
    """目标目录落在哪一层。

    - ``mount``：在真实挂载进来的磁盘里（宿主机盘/NAS），数据持久，推荐；
    - ``container_layer``：在容器自己的可写层里，容器重建就没了，只适合临时试；
    - ``unknown``：读不到挂载表，判断不了。
    """
    if not path:
        return "unknown"
    if mount_points is None:
        mount_points = read_mount_points()
        if mount_points is None:
            return "unknown"
    hit = _longest_mount_for(path, mount_points)
    if not hit:
        return "container_layer"
    return "container_layer" if hit.get("is_container_layer") else "mount"


def storage_tier_warning(tier: str) -> str:
    """给用户看的风险提示；没问题时返回空串。"""
    if tier == "container_layer":
        return (
            "这个目录在容器的临时层里，没有挂载宿主机磁盘：容器重建后临时文件会丢，"
            "而且写满会拖垮容器本身。长期用建议先挂一块盘进容器再选。"
        )
    return ""


def _resolve_existing_ancestor(path: str) -> str:
    """向上找到第一个真实存在的目录（用来看能不能在它下面建目录）。"""
    current = path
    while True:
        parent = os.path.dirname(current)
        if parent == current:
            return current if os.path.isdir(current) else ""
        if os.path.isdir(parent):
            return parent
        current = parent


def probe_storage_path(path: str, *, create_missing: bool = False) -> Dict[str, Any]:
    """探测目录可用性：存在性、可写、剩余空间。

    返回 {ok, path, exists, writable, free_bytes, total_bytes, error, created, tier}，
    除 ``create_missing=True`` 时会按规则建目录外，不做任何修改。

    ``create_missing`` 只在「上层已经有挂载进来的磁盘」时才建目录。这条限制
    是防呆的关键：用户填了宿主机路径（比如 /mnt/hdd，容器里根本不存在），若
    无脑 makedirs 会在容器可写层造出同名目录，文件全落进容器里，看起来成功、
    实际容器一重建就丢。所以建目录前先确认锚点是真实挂载点。
    """
    result: Dict[str, Any] = {
        "ok": False,
        "path": path,
        "exists": False,
        "writable": False,
        "free_bytes": 0,
        "total_bytes": 0,
        "error": "",
        "created": False,
        "tier": "unknown",
    }
    if not path:
        result["error"] = "路径不能为空"
        return result

    mounts = read_mount_points()
    result["tier"] = detect_storage_tier(path, mounts) if mounts is not None else "unknown"

    if not os.path.isdir(path):
        if not create_missing:
            result["error"] = "目录不存在（需先在宿主机挂载进容器）"
            return result

        anchor = _resolve_existing_ancestor(path)
        anchor_mount = _longest_mount_for(anchor, mounts) if mounts is not None else None
        if mounts is not None and (not anchor_mount or anchor_mount.get("is_container_layer")):
            result["error"] = (
                f"目录不存在，而且上层 {anchor or '/'} 不是挂载进来的磁盘。"
                "要放到新盘上，请先在 compose 里把宿主机目录挂载进容器（例如 /mnt/hdd → /app/storage）；"
                "直接在容器里建目录只会落在临时层，容器重建后文件就没了。"
            )
            return result
        try:
            os.makedirs(path, exist_ok=True)
            result["created"] = True
        except OSError as exc:
            result["error"] = f"无法创建目录：{exc}"
            return result

    result["exists"] = True
    try:
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


def list_storage_candidates(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """列出容器里可选的暂存目录，给设置页做「点一下选中」的列表。

    挂载点是唯一可靠的判断依据：只要 compose 里多挂一块盘，它就会自动出现
    在列表里，用户不用记容器内路径，也就不用猜、不会填错。
    """
    cfg = cfg if isinstance(cfg, dict) else {}
    default_dir = default_temp_dir()
    current_dir = resolve_temp_dir(cfg)
    mounts = read_mount_points()
    supported = mounts is not None

    candidates: List[Dict[str, Any]] = []
    for item in (mounts or []):
        mount_path = item["path"]
        # `/` 只是用来判断层级的参照，不作为候选（它就是容器临时层本身）
        if mount_path == "/":
            continue
        suggested = _normalize_container_path(os.path.join(mount_path, CROSS_COPY_TEMP_SUBDIR)) or mount_path
        probe = probe_storage_path(mount_path)
        candidates.append({
            "path": suggested,
            "mount_point": mount_path,
            "fs_type": item["fs_type"],
            "tier": "mount",
            "ok": bool(probe.get("ok")),
            "writable": bool(probe.get("writable")),
            "free_bytes": int(probe.get("free_bytes") or 0),
            "total_bytes": int(probe.get("total_bytes") or 0),
            "error": probe.get("error") or "",
            "is_default": suggested == default_dir,
            "note": _APP_VOLUME_NOTES.get(mount_path, ""),
        })

    # 当前用的目录不在候选里（老配置或手填的其它路径）时补一条，让用户看得到现状
    if current_dir and not any(item["path"] == current_dir for item in candidates):
        probe = probe_storage_path(current_dir)
        candidates.insert(0, {
            "path": current_dir,
            "mount_point": "",
            "fs_type": "",
            "tier": detect_storage_tier(current_dir, mounts) if supported else "unknown",
            "ok": bool(probe.get("ok")),
            "writable": bool(probe.get("writable")),
            "free_bytes": int(probe.get("free_bytes") or 0),
            "total_bytes": int(probe.get("total_bytes") or 0),
            "error": probe.get("error") or "",
            "is_default": current_dir == default_dir,
            "note": "当前正在使用的目录",
        })

    def _sort_key(item: Dict[str, Any]) -> tuple:
        if item["is_default"]:
            group = 0
        elif item.get("note") == "当前正在使用的目录":
            group = 1
        else:
            group = 2
        return (group, -int(item.get("free_bytes") or 0))

    candidates.sort(key=_sort_key)

    container_layer: Dict[str, int] = {}
    try:
        usage = shutil.disk_usage("/")
        container_layer = {"free_bytes": int(usage.free), "total_bytes": int(usage.total)}
    except OSError:
        pass

    return {
        "supported": supported,
        "candidates": candidates,
        "current": current_dir,
        "default_dir": default_dir,
        "tier": (detect_storage_tier(current_dir, mounts) if supported else "unknown"),
        "container_layer": container_layer,
    }


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
