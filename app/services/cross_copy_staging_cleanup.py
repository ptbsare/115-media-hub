"""跨盘补源中转目录（夸克）残留自动清理。

补源链路把缺的集转存到 ``云下载/跨盘中转/<订阅任务名>``，复制成功后由
``cross_copy_jobs`` 按 file_id 删掉源副本；**被取消或失败的任务会保留副本**，
为的是重试能续传。问题是用户如果不重试，这些副本就一直占着夸克空间（夸克免费
账号空间很小，一个整季包就能塞满）。

这里做的是兜底清理：按「文件年龄 + 活跃任务」两个条件判断，只删确实过期且没有
任何任务在引用的文件。安全红线（与订阅磁力中转清理同一套口径）：

- 只在 ``CROSS_COPY_STAGING_ROOT`` 之下动，别的地方一律不碰；
- 只删**文件**（按 file_id），只删**确认空**的子目录，任务目录与中转根永不删；
- 读不到目录 / 解析不出时间戳 → 当作「有内容、不过期」，宁可不删；
- 正在跑（pending/running）的任务引用的文件与其所在文件夹子树整体跳过。

删除走夸克的「移到回收站」，所以文件不会立刻从配额里消失——回收站被系统回收或
用户手动清空之后空间才真正回来。这一点在设置项提示里也写明了，不假装即清即释放。
"""

import asyncio
import logging
import math
import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from ..core import get_config
from ..db import db_connection, ensure_db
from ..providers.registry import get_or_none as _get_provider_or_none
from .cross_copy_jobs import ACTIVE_STATUSES
from .subscription_cross_copy import CROSS_COPY_STAGING_ROOT

logger = logging.getLogger(__name__)

# 扫描上限：中转目录结构最多三层（任务目录 / 分享文件夹 / 文件），给到 6 层足够，
# 同时防住异常深的目录把一轮清理拖成几分钟。
STAGING_CLEANUP_MAX_DEPTH = 6
STAGING_CLEANUP_MAX_DIRS = 300
STAGING_CLEANUP_MAX_ENTRIES = 3000
# 单次删除批量：夸克 file/delete 一次收几十个 id 没问题，太大反而不好排查
STAGING_CLEANUP_DELETE_BATCH = 100


def staging_cleanup_interval_seconds() -> int:
    return max(
        300,
        int(os.environ.get("CROSS_COPY_STAGING_CLEANUP_INTERVAL_SECONDS", 1800) or 1800),
    )


def _entry_id(entry: Dict[str, Any]) -> str:
    return str(entry.get("id", "") or entry.get("fid", "") or "").strip()


def _entry_is_dir(entry: Dict[str, Any]) -> bool:
    return bool(entry.get("is_dir", False))


def _normalize_numeric_timestamp(value: float) -> Optional[float]:
    """数字时间戳统一成「秒」。夸克给的是毫秒（13 位），115 给秒（10 位）。"""
    if not math.isfinite(value) or value <= 0:
        return None
    return value / 1000.0 if value > 1e11 else value


def parse_staging_timestamp(value: Any) -> Optional[float]:
    """把网盘条目里的修改时间解析成 epoch 秒；解析不出来返回 None。

    不能复用 `parse_resource_datetime_to_timestamp`：那个只认 ISO 字符串，
    纯数字（夸克的 updated_at 就是毫秒数字串）会返回 0.0，导致「永不过期」。
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return _normalize_numeric_timestamp(float(value))
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _normalize_numeric_timestamp(float(text))
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    try:
        return float(parsed.timestamp())
    except (OverflowError, OSError, ValueError):
        return None


def staging_entry_age_seconds(entry: Dict[str, Any], now: Optional[float] = None) -> Optional[float]:
    """条目已存在多少秒。时间戳解析不出来返回 None（调用方据此「永不删」）。"""
    stamp = parse_staging_timestamp((entry or {}).get("modified_at"))
    if stamp is None:
        return None
    current = time.time() if now is None else float(now)
    # 时钟偏差导致的「未来时间」按刚创建处理，不要得出负数年龄
    return max(0.0, current - stamp)


def _collect_active_protection() -> Tuple[Set[str], Set[str]]:
    """取活跃任务引用的条目：``(文件 id 集合, 文件夹 id 集合)``。

    只有 pending/running 会引用中转副本；completed 的源文件复制成功时已删，
    failed/cancelled 是「留着等重试」的那批，正是要按年龄回收的对象。
    """
    ensure_db()
    file_ids: Set[str] = set()
    dir_ids: Set[str] = set()
    try:
        with db_connection() as conn:
            cursor = conn.execute(
                "SELECT source_file_id, source_is_dir FROM cross_copy_jobs "
                f"WHERE status IN ({','.join(['?'] * len(ACTIVE_STATUSES))})",
                tuple(sorted(ACTIVE_STATUSES)),
            )
            for row in cursor.fetchall():
                entry_id = str(row[0] or "").strip()
                if not entry_id:
                    continue
                try:
                    is_dir = bool(int(row[1] or 0))
                except (TypeError, ValueError):
                    is_dir = False
                (dir_ids if is_dir else file_ids).add(entry_id)
    except Exception as exc:  # noqa: BLE001 - 读不到活跃集合时宁可不清理
        logger.warning("跨盘中转清理读取活跃任务失败，本轮跳过：%s", exc)
        raise
    return file_ids, dir_ids


def _collect_staging_tree(
    provider: Any,
    cookie: str,
    task_cid: str,
    protected_dirs: Set[str],
) -> Dict[str, Any]:
    """BFS 收集一个任务目录下的文件与目录，并标记「受保护」的子树。"""
    files: List[Dict[str, Any]] = []
    dirs: List[Dict[str, Any]] = []
    queue: List[Tuple[str, str, int, bool]] = [(str(task_cid or "").strip(), "", 0, False)]
    scanned_entries = 0
    truncated = False
    while queue:
        if len(dirs) >= STAGING_CLEANUP_MAX_DIRS:
            truncated = True
            break
        cid, parent_cid, depth, inherited_protected = queue.pop(0)
        if not cid:
            continue
        protected = bool(inherited_protected or cid in protected_dirs)
        node = {
            "cid": cid,
            "parent_cid": parent_cid,
            "depth": depth,
            "protected": protected,
            "unreadable": False,
        }
        dirs.append(node)
        try:
            entries = provider.list_entries(cookie, cid) or []
        except Exception as exc:  # noqa: BLE001 - 读不到就当它有内容，绝不瞎删
            node["unreadable"] = True
            logger.info("跨盘中转清理读取目录失败（%s）：%s", cid, exc)
            continue
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            scanned_entries += 1
            if scanned_entries > STAGING_CLEANUP_MAX_ENTRIES:
                truncated = True
                break
            entry_id = _entry_id(entry)
            if not entry_id:
                continue
            if _entry_is_dir(entry):
                if depth + 1 <= STAGING_CLEANUP_MAX_DEPTH:
                    queue.append((entry_id, cid, depth + 1, protected))
                continue
            files.append(
                {
                    "id": entry_id,
                    "name": str(entry.get("name", "") or "").strip(),
                    "parent_cid": str(entry.get("parent_id", "") or cid).strip() or cid,
                    "size": int(entry.get("size", 0) or 0),
                    "modified_at": entry.get("modified_at", ""),
                    "protected": protected,
                }
            )
    return {
        "files": files,
        "dirs": dirs,
        "scanned_entries": scanned_entries,
        "truncated": truncated,
    }


def _delete_in_batches(provider: Any, cookie: str, items: Iterable[Dict[str, Any]]) -> Set[str]:
    """按 file_id 分批删除，返回真的删掉的 id 集合。"""
    grouped: Dict[str, List[str]] = {}
    for item in items:
        grouped.setdefault(str(item.get("parent_cid", "") or "").strip(), []).append(
            str(item.get("id", "") or "").strip()
        )
    deleted: Set[str] = set()
    for parent_cid, ids in grouped.items():
        clean_ids = [entry_id for entry_id in ids if entry_id]
        for start in range(0, len(clean_ids), STAGING_CLEANUP_DELETE_BATCH):
            batch = clean_ids[start : start + STAGING_CLEANUP_DELETE_BATCH]
            try:
                provider.delete_entries(cookie, batch, parent_cid)
            except Exception as exc:  # noqa: BLE001 - 单批失败不影响其余批次
                logger.warning("跨盘中转清理删除文件失败（%s）：%s", parent_cid or "-", exc)
                continue
            deleted.update(batch)
    return deleted


def _clean_one_task_dir(
    provider: Any,
    cookie: str,
    task_cid: str,
    retention_seconds: float,
    active_file_ids: Set[str],
    active_dir_ids: Set[str],
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "scanned_files": 0,
        "expired_deleted": 0,
        "empty_dirs_deleted": 0,
        "kept_files": 0,
        "truncated": False,
    }
    tree = _collect_staging_tree(provider, cookie, task_cid, active_dir_ids)
    stats["scanned_files"] = len(tree["files"])
    stats["truncated"] = bool(tree["truncated"])

    now = time.time()
    expired: List[Dict[str, Any]] = []
    for item in tree["files"]:
        if item.get("protected") or str(item.get("id", "")) in active_file_ids:
            continue
        age = staging_entry_age_seconds(item, now=now)
        if age is None:
            continue  # 时间戳认不出来：永不删（可能是别的工具塞进来的东西）
        if age > retention_seconds:
            expired.append(item)

    if expired:
        # 算过期集合与真正开删之间有新任务注册引用同一文件：删前再查一次活跃集合做差集
        fresh_file_ids, fresh_dir_ids = _collect_active_protection()
        active_file_ids = active_file_ids | fresh_file_ids
        if fresh_dir_ids - active_dir_ids:
            # 新增了活跃的文件夹任务：本轮放弃删除，下一轮按新的子树重算
            stats["kept_files"] = len(tree["files"])
            stats["deferred_new_task"] = True
            return stats
        deletable = [item for item in expired if str(item.get("id", "")) not in active_file_ids]
        deleted = _delete_in_batches(provider, cookie, deletable)
        stats["expired_deleted"] = len(deleted)
    else:
        deleted = set()

    remaining_parents = {
        str(item.get("parent_cid", "") or "")
        for item in tree["files"]
        if str(item.get("id", "")) not in deleted
    }
    stats["kept_files"] = len(tree["files"]) - len(deleted)

    # 倒序（深 → 浅）删确认空的子目录；任务目录本身与读不到的一律留着
    alive: Set[str] = set(remaining_parents)
    for node in sorted(tree["dirs"], key=lambda item: item["depth"], reverse=True):
        cid = str(node.get("cid", "") or "")
        if not cid or cid == str(task_cid or ""):
            alive.add(cid)
            continue
        if node.get("protected") or node.get("unreadable") or cid in alive:
            alive.add(cid)
            parent_cid = str(node.get("parent_cid", "") or "")
            if parent_cid:
                alive.add(parent_cid)
            continue
        try:
            provider.delete_entries(cookie, [cid], str(node.get("parent_cid", "") or task_cid))
        except Exception as exc:  # noqa: BLE001 - 删空目录失败不影响文件清理结果
            logger.warning("跨盘中转清理删除空目录失败（%s）：%s", cid, exc)
            alive.add(cid)
            parent_cid = str(node.get("parent_cid", "") or "")
            if parent_cid:
                alive.add(parent_cid)
            continue
        stats["empty_dirs_deleted"] += 1
    return stats


def run_cross_copy_staging_cleanup_once() -> Dict[str, Any]:
    """跑一轮清理。返回统计（也给测试和排查用）。"""
    cfg = get_config()
    try:
        retention_days = int(cfg.get("cross_copy_staging_retention_days", 7) or 0)
    except (TypeError, ValueError):
        retention_days = 0
    if retention_days <= 0:
        return {"skipped": "disabled", "retention_days": retention_days}

    provider = _get_provider_or_none("quark")
    if not provider or not getattr(provider, "supports_delete", False):
        return {"skipped": "provider_unsupported"}
    cookie = str(provider.get_cookie(cfg) or "").strip()
    if not cookie:
        return {"skipped": "no_cookie"}
    resolver = getattr(provider, "resolve_folder_id_by_path", None)
    if not callable(resolver):
        return {"skipped": "provider_unsupported"}
    try:
        root_cid = str(resolver(cookie, CROSS_COPY_STAGING_ROOT) or "").strip()
    except Exception as exc:  # noqa: BLE001 - 目录不存在/接口异常都当「没有中转目录」
        logger.info("跨盘中转清理定位中转根失败：%s", exc)
        return {"skipped": "staging_root_missing"}
    # "0" 是网盘根目录，绝不能当成中转目录去扫（扫下去就是把整盘当垃圾）
    if not root_cid or root_cid == "0":
        return {"skipped": "staging_root_missing"}

    try:
        entries = provider.list_entries(cookie, root_cid) or []
    except Exception as exc:  # noqa: BLE001 - 列不出根就当这轮没得清
        logger.warning("跨盘中转清理读取中转根失败：%s", exc)
        return {"skipped": "staging_root_unreadable"}

    try:
        active_file_ids, active_dir_ids = _collect_active_protection()
    except Exception:  # noqa: BLE001 - 拿不到活跃集合绝不能删东西
        return {"skipped": "active_lookup_failed"}

    retention_seconds = retention_days * 86400.0
    stats: Dict[str, Any] = {
        "retention_days": retention_days,
        "task_dirs": 0,
        "scanned_files": 0,
        "expired_deleted": 0,
        "empty_dirs_deleted": 0,
        "kept_files": 0,
        "truncated": 0,
        "deferred": 0,
    }
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict) or not _entry_is_dir(entry):
            continue  # 根下的散文件不动
        task_cid = _entry_id(entry)
        if not task_cid:
            continue
        stats["task_dirs"] += 1
        try:
            task_stats = _clean_one_task_dir(
                provider,
                cookie,
                task_cid,
                retention_seconds,
                active_file_ids,
                active_dir_ids,
            )
        except Exception as exc:  # noqa: BLE001 - 单个任务目录出错不影响其它目录
            logger.warning("跨盘中转清理任务目录失败（%s）：%s", task_cid, exc)
            continue
        for key in ("scanned_files", "expired_deleted", "empty_dirs_deleted", "kept_files"):
            stats[key] += int(task_stats.get(key, 0) or 0)
        if task_stats.get("truncated"):
            stats["truncated"] += 1
        if task_stats.get("deferred_new_task"):
            stats["deferred"] += 1

    if stats["expired_deleted"] or stats["empty_dirs_deleted"]:
        logger.info("跨盘补源中转目录清理完成：%s", stats)
    return stats


async def cross_copy_staging_cleanup_watcher() -> None:
    """定时清理夸克中转目录里的过期副本（容器长期运行、没人点按钮时的兜底）。"""
    await asyncio.sleep(120)
    while True:
        try:
            await asyncio.to_thread(run_cross_copy_staging_cleanup_once)
        except Exception as exc:  # noqa: BLE001 - 定时任务不能把循环打死
            logger.warning("跨盘补源中转目录定期清理失败：%s", exc)
        await asyncio.sleep(staging_cleanup_interval_seconds())
