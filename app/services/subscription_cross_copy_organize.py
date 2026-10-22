"""跨盘补源「入库后整理」：补进来的集按订阅任务的开关原地改名。

订阅导入会把新集改成「片名 (年份) - S01E42.mp4」，而补源复制的是分享里的原条目，
文件名是 ``S01E42.mp4``。同一部剧出现两种命名，刮削器虽然认 ``SxxEyy``，但对不齐
用户已有的命名习惯。这里复用订阅任务自己的「入库后整理」开关（``organize_on_import``
+ ``organize_options``），不新造开关。

为什么不在复制任务的收口里直接改：
- 改名要提交刮削任务并等结果（``submit_scraper_job(...).result(...)`` 阻塞，最长 300s），
  塞进 ``cross_copy_jobs`` 的收口会占死复制并发名额（默认同时只跑 2 个）；
- 一个订阅轮次常常注册 3~5 条任务（每集一条），逐条整理等于 N 次建计划 + N 个刮削
  任务，同一目录并发改名容易互相踩。

所以拆成两步：复制成功时只打 ``organize_state='pending'`` 标记（在 cross_copy_jobs
里一行 UPDATE 完成），由本模块的定时器按「同一目标目录 + 同一订阅任务」成组，每组
只提交一次改名任务。

边界：
- 只处理 ``origin='subscription'`` 的单文件成功任务；手动复制永远不动；
- 整包文件夹补源不整理（它的落点本身就是分享文件夹，且结果里没有逐文件清单，
  递归改名会动到目标里原有的文件）；
- 开关关 / 电影 / 没绑 TMDB → 标 ``skipped``，文件保持原名，不报错。
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..core import (
    get_config,
    normalize_mount_provider,
    normalize_relative_path,
    reset_subscription_log_context,
    set_subscription_log_context,
    write_subscription_log,
)
from ..db import db_connection, ensure_db, now_text, sqlite_row_to_dict
from ..providers.registry import get_or_none as _get_provider_or_none

logger = logging.getLogger(__name__)

# 改名任务的等待上限：与订阅导入里那份保持一致，别让一个卡住的刮削任务拖死一轮整理
SUBSCRIPTION_ORGANIZE_JOB_WAIT_SECONDS = max(
    30,
    int(os.environ.get("SUBSCRIPTION_ORGANIZE_JOB_WAIT_SECONDS", 300) or 300),
)
# 防抖：同一轮补源会连续完成好几条任务，等它们都落库再一次性整理
ORGANIZE_DEBOUNCE_SECONDS = max(0, int(os.environ.get("CROSS_COPY_ORGANIZE_DEBOUNCE_SECONDS", 90) or 90))
ORGANIZE_INTERVAL_SECONDS = max(30, int(os.environ.get("CROSS_COPY_ORGANIZE_INTERVAL_SECONDS", 120) or 120))
# 一轮最多处理几组（每组最长要等 300s，太多会把 tick 拖得很长）
ORGANIZE_MAX_GROUPS_PER_TICK = max(1, int(os.environ.get("CROSS_COPY_ORGANIZE_MAX_GROUPS", 5) or 5))
ORGANIZE_PENDING_LIMIT = 200
# running 超过这个时间视为进程中断，重置回 pending 重试
ORGANIZE_RUNNING_TIMEOUT_SECONDS = 15 * 60

ORGANIZE_STATE_PENDING = "pending"
ORGANIZE_STATE_RUNNING = "running"
ORGANIZE_STATE_DONE = "done"
ORGANIZE_STATE_SKIPPED = "skipped"
ORGANIZE_STATE_FAILED = "failed"


def organize_enabled(task: Dict[str, Any]) -> bool:
    """订阅任务是否需要「入库后整理」。

    三个条件与订阅导入那边一致：电视剧 + 开关打开 + 绑定过 TMDB。任一不满足都
    不整理（电影本来就不改名，没 TMDB 认不出集数和片名）。
    """
    if not isinstance(task, dict):
        return False
    if str(task.get("media_type", "movie") or "movie").strip().lower() != "tv":
        return False
    if not bool(task.get("organize_on_import", False)):
        return False
    return max(0, int(task.get("tmdb_id", 0) or 0)) > 0


def find_subscription_task(cfg: Dict[str, Any], task_name: str) -> Dict[str, Any]:
    """按任务名反查订阅任务（补源任务里只存了 ``origin_ref`` = 任务名）。"""
    wanted = str(task_name or "").strip()
    if not wanted:
        return {}
    tasks = cfg.get("subscription_tasks") if isinstance(cfg.get("subscription_tasks"), list) else []
    for task in tasks:
        if isinstance(task, dict) and str(task.get("name", "") or "").strip() == wanted:
            return task
    return {}


def _organize_cutoff_text() -> str:
    """防抖窗口的截止时间：``finished_at`` 是 ISO 秒级字符串，直接字符串比较即可。"""
    cutoff = datetime.now() - timedelta(seconds=ORGANIZE_DEBOUNCE_SECONDS)
    return cutoff.isoformat(timespec="seconds")


def list_organize_jobs(cutoff_text: str, limit: int = ORGANIZE_PENDING_LIMIT) -> List[Dict[str, Any]]:
    ensure_db()
    try:
        with db_connection() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM cross_copy_jobs
                WHERE organize_state = ?
                  AND finished_at != ''
                  AND finished_at <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (ORGANIZE_STATE_PENDING, str(cutoff_text or ""), int(limit)),
            )
            return [sqlite_row_to_dict(row) for row in cursor.fetchall()]
    except Exception as exc:  # noqa: BLE001 - 定时任务不能因为一次查询失败就死掉
        logger.warning("跨盘补源整理：读取待整理任务失败：%s", exc)
        return []


def group_jobs(jobs: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], List[Dict[str, Any]]]:
    """按「目标网盘 + 目标目录 + 订阅任务」成组：一组只提交一次改名任务。"""
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for job in jobs if isinstance(jobs, list) else []:
        if not isinstance(job, dict):
            continue
        key = (
            str(job.get("target_provider", "") or "").strip().lower(),
            str(job.get("target_dir_id", "") or "").strip(),
            str(job.get("origin_ref", "") or "").strip(),
        )
        groups.setdefault(key, []).append(job)
    return groups


def _reset_stale_running() -> int:
    """把卡在 running 的任务放回 pending（进程重启/go 线程被打断时会留下）。"""
    ensure_db()
    cutoff = (datetime.now() - timedelta(seconds=ORGANIZE_RUNNING_TIMEOUT_SECONDS)).isoformat(
        timespec="seconds"
    )
    try:
        with db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE cross_copy_jobs
                SET organize_state = ?, organize_detail = ?, updated_at = ?
                WHERE organize_state = ? AND updated_at <= ?
                """,
                (ORGANIZE_STATE_PENDING, "上次整理被中断，重新排队", now_text(), ORGANIZE_STATE_RUNNING, cutoff),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
    except Exception as exc:  # noqa: BLE001
        logger.warning("跨盘补源整理：重置中断任务失败：%s", exc)
        return 0


def _claim_jobs(job_ids: List[int]) -> List[int]:
    """CAS 抢占：只有仍然是 pending 的才能改成 running，避免两轮重复整理。"""
    ensure_db()
    claimed: List[int] = []
    try:
        with db_connection() as conn:
            for job_id in job_ids:
                cursor = conn.execute(
                    """
                    UPDATE cross_copy_jobs
                    SET organize_state = ?, updated_at = ?
                    WHERE id = ? AND organize_state = ?
                    """,
                    (ORGANIZE_STATE_RUNNING, now_text(), int(job_id), ORGANIZE_STATE_PENDING),
                )
                if int(cursor.rowcount or 0) > 0:
                    claimed.append(int(job_id))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("跨盘补源整理：抢占任务失败：%s", exc)
    return claimed


def _finish_jobs(job_ids: List[int], state: str, detail: str) -> None:
    if not job_ids:
        return
    ensure_db()
    normalized_detail = str(detail or "").strip()[:200]
    placeholders = ",".join(["?"] * len(job_ids))
    try:
        with db_connection() as conn:
            conn.execute(
                f"""
                UPDATE cross_copy_jobs
                SET organize_state = ?, organize_detail = ?, updated_at = ?
                WHERE id IN ({placeholders}) AND organize_state = ?
                """,
                (str(state), normalized_detail, now_text(), *[int(item) for item in job_ids], ORGANIZE_STATE_RUNNING),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("跨盘补源整理：写回整理结果失败：%s", exc)


async def locate_subscription_imported_files(
    provider_meta: Any,
    cookie: str,
    target_cid: str,
    target_savepath: str,
    imported_entries: Any,
) -> List[Dict[str, Any]]:
    """在目标目录里按「文件名 + 大小」定位本次新入库的文件。

    转存 / 复制 / 上传都可能换掉条目 id（115 的上传结果根本不返回目标 file_id），
    所以不能沿用源条目 id，必须回目标目录重新取一条，再交给刮削引擎原地改名。
    """
    wanted: List[Tuple[str, int]] = []
    for entry in imported_entries if isinstance(imported_entries, list) else []:
        if not isinstance(entry, dict) or bool(entry.get("is_dir", False)):
            continue
        name = str(entry.get("name", "") or "").strip()
        if not name:
            continue
        try:
            size = max(0, int(entry.get("size", 0) or 0))
        except (TypeError, ValueError):
            size = 0
        wanted.append((name, size))
    if not wanted or not provider_meta:
        return []
    try:
        listed = await asyncio.to_thread(provider_meta.list_entries, cookie, target_cid)
    except Exception:
        return []
    by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for item in listed if isinstance(listed, list) else []:
        if not isinstance(item, dict) or bool(item.get("is_dir", False)):
            continue
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        try:
            size = max(0, int(item.get("size", 0) or 0))
        except (TypeError, ValueError):
            size = 0
        by_key[(name, size)] = item
    normalized_parent = normalize_relative_path(str(target_savepath or "").strip())
    located: List[Dict[str, Any]] = []
    for name, size in wanted:
        item = by_key.get((name, size))
        if not item:
            continue
        fid = str(item.get("fid", "") or item.get("id", "") or "").strip()
        if not fid:
            continue
        located.append(
            {
                "id": fid,
                "cid": fid,
                "name": name,
                "is_dir": False,
                "size": size,
                "parent_id": str(target_cid or "").strip() or "0",
                "parent_path": normalized_parent,
            }
        )
    return located


async def organize_files_for_subscription_task(
    *,
    task: Dict[str, Any],
    provider: str,
    cookie: str,
    provider_meta: Any,
    target_cid: str,
    target_savepath: str,
    imported_entries: Any,
) -> Dict[str, Any]:
    """跑一次「入库后整理」，返回结构化结果（**不写订阅日志**，日志由调用方写）。

    返回 ``{"ok": bool, "silent": bool, "reason": str, "level": str,
    "job_id": int, "succeeded": int, "failed": int, "status": str}``。
    ``silent=True`` 表示「按开关就不该整理」，调用方不必写日志（免得每部电影都刷一条）。
    """
    if not organize_enabled(task):
        return {"ok": False, "silent": True, "reason": "not_applicable"}
    normalized_cid = str(target_cid or "").strip()
    normalized_savepath = normalize_relative_path(str(target_savepath or "").strip())
    if not normalized_cid or not normalized_savepath:
        return {"ok": False, "silent": True, "reason": "target_dir_missing"}
    target_files = await locate_subscription_imported_files(
        provider_meta,
        cookie,
        normalized_cid,
        normalized_savepath,
        imported_entries,
    )
    if not target_files:
        return {
            "ok": False,
            "reason": "未在目标目录找到本次新入库的文件，保留原名",
            "level": "info",
        }
    payload = {
        "provider": normalize_mount_provider(provider) or "115",
        "base_cid": normalized_cid,
        "base_path": normalized_savepath,
        "entries": target_files,
        "tmdb": {
            "tmdb_id": max(0, int(task.get("tmdb_id", 0) or 0)),
            "tmdb_media_type": str(task.get("tmdb_media_type", "") or "tv").strip() or "tv",
            "tmdb_title": str(task.get("tmdb_title", "") or "").strip(),
            "tmdb_year": str(task.get("tmdb_year", "") or "").strip(),
            "tmdb_season_episode_map": task.get("tmdb_season_episode_map", {}),
        },
        "options": dict(task.get("organize_options", {}) or {}),
    }
    try:
        # 延迟 import：测试靠 patch scraper 模块属性生效，模块级 import 会绑死旧函数
        from .scraper import (
            build_scraper_rename_plan,
            create_scraper_job_from_plan,
            get_scraper_jobs_state,
            submit_scraper_job,
        )

        plan = await asyncio.to_thread(build_scraper_rename_plan, payload)
    except Exception as exc:  # noqa: BLE001 - 整理失败只保留原名，不影响入库
        return {"ok": False, "reason": f"生成改名计划失败（{str(exc)[:120]}），保留原名", "level": "warn"}
    actions = [
        action
        for action in (plan.get("actions") if isinstance(plan.get("actions"), list) else [])
        if isinstance(action, dict) and action.get("ready") and not action.get("issue")
    ]
    if not actions:
        return {"ok": False, "reason": "没有可执行的改名动作，保留原名", "level": "info"}
    executable_plan = {**plan, "actions": actions, "ready_count": len(actions)}
    job_id = 0
    try:
        created = await asyncio.to_thread(create_scraper_job_from_plan, {"plan": executable_plan})
        job_id = max(0, int((created or {}).get("job_id", 0) or 0))
        if job_id <= 0:
            raise RuntimeError("创建改名任务失败")
        await asyncio.to_thread(
            lambda: submit_scraper_job(job_id).result(timeout=SUBSCRIPTION_ORGANIZE_JOB_WAIT_SECONDS)
        )
        state = get_scraper_jobs_state(job_id=job_id)
        jobs = state.get("jobs") if isinstance(state, dict) else []
        actual = jobs[0] if isinstance(jobs, list) and jobs else {}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"执行改名失败（{str(exc)[:120]}），保留原名", "level": "warn"}
    status = str((actual or {}).get("status", "") or "").strip()
    succeeded = max(0, int((actual or {}).get("succeeded_actions", 0) or 0))
    failed = max(0, int((actual or {}).get("failed_actions", 0) or 0))
    if status == "completed":
        return {"ok": True, "job_id": job_id, "succeeded": succeeded, "failed": failed, "status": status}
    if status == "partial":
        return {"ok": True, "job_id": job_id, "succeeded": succeeded, "failed": failed, "status": status}
    detail = str((actual or {}).get("status_detail", "") or "改名未完成").strip()
    return {"ok": False, "job_id": job_id, "reason": f"改名未完成（{detail[:120]}），保留原名", "level": "warn"}


def _log_context(task_name: str, provider: str) -> Any:
    """订阅日志靠 contextvar 补任务名，定时器里得手动包一层。"""
    return set_subscription_log_context(
        {
            "task_name": str(task_name or "").strip(),
            "provider": str(provider or "").strip(),
            "media_type": "tv",
            "trigger": "cross_copy",
        }
    )


async def _organize_group(
    key: Tuple[str, str, str],
    jobs: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    provider_name, target_cid, task_name = key
    job_ids = [int(job_id) for job_id in (int(job.get("id", 0) or 0) for job in jobs) if job_id > 0]
    claimed = _claim_jobs(job_ids)
    if not claimed:
        return
    stats["groups"] += 1
    claimed_set = set(claimed)
    claimed_jobs = [job for job in jobs if int(job.get("id", 0) or 0) in claimed_set]
    token = _log_context(task_name, provider_name)
    try:
        cfg = get_config()
        task = find_subscription_task(cfg, task_name)
        if not task:
            _finish_jobs(claimed, ORGANIZE_STATE_SKIPPED, "订阅任务已不存在，保留原名")
            stats["skipped"] += len(claimed)
            return
        if not organize_enabled(task):
            _finish_jobs(claimed, ORGANIZE_STATE_SKIPPED, "未开启入库后整理或缺少 TMDB，按原名保留")
            stats["skipped"] += len(claimed)
            return
        provider_meta = _get_provider_or_none(provider_name)
        cookie = str(getattr(provider_meta, "get_cookie", lambda _cfg: "")(cfg) or "").strip()
        if not provider_meta or not cookie:
            _finish_jobs(claimed, ORGANIZE_STATE_SKIPPED, "目标网盘未配置或未启用，保留原名")
            stats["skipped"] += len(claimed)
            await write_subscription_log("跨盘补源入库后整理：目标网盘未配置，保留原名", "warn")
            return
        target_savepath = str(claimed_jobs[0].get("target_savepath", "") or "").strip()
        imported_entries = [
            {
                "name": str(job.get("filename", "") or "").strip(),
                "size": max(0, int(job.get("file_size", 0) or 0)),
                "is_dir": False,
            }
            for job in claimed_jobs
        ]
        result = await organize_files_for_subscription_task(
            task=task,
            provider=provider_name,
            cookie=cookie,
            provider_meta=provider_meta,
            target_cid=target_cid,
            target_savepath=target_savepath,
            imported_entries=imported_entries,
        )
        if result.get("silent"):
            _finish_jobs(claimed, ORGANIZE_STATE_SKIPPED, "按开关不需要整理")
            stats["skipped"] += len(claimed)
            return
        if result.get("ok"):
            detail = f"已原地改名 {int(result.get('succeeded', 0) or 0)} 个文件（任务 #{result.get('job_id', 0)}）"
            _finish_jobs(claimed, ORGANIZE_STATE_DONE, detail)
            stats["done"] += len(claimed)
            await write_subscription_log(f"跨盘补源{detail}", "success")
            return
        reason = str(result.get("reason", "") or "整理未完成").strip()
        _finish_jobs(claimed, ORGANIZE_STATE_FAILED, reason)
        stats["failed"] += len(claimed)
        await write_subscription_log(f"跨盘补源入库后整理：{reason}", str(result.get("level", "warn") or "warn"))
    except Exception as exc:  # noqa: BLE001 - 一组出错不能把整个定时器打死
        logger.exception("跨盘补源整理分组失败（%s）", task_name)
        _finish_jobs(claimed, ORGANIZE_STATE_FAILED, f"整理异常：{str(exc)[:120]}")
        stats["failed"] += len(claimed)
    finally:
        reset_subscription_log_context(token)


async def run_pending_organize_once() -> Dict[str, Any]:
    """跑一轮「待整理」任务。返回统计，方便排查与测试。"""
    stats: Dict[str, Any] = {"groups": 0, "done": 0, "skipped": 0, "failed": 0, "reset": 0}
    stats["reset"] = _reset_stale_running()
    jobs = list_organize_jobs(_organize_cutoff_text())
    if not jobs:
        return stats
    groups = group_jobs(jobs)
    for key, group in list(groups.items())[:ORGANIZE_MAX_GROUPS_PER_TICK]:
        await _organize_group(key, group, stats)
    if stats["done"] or stats["failed"]:
        logger.info("跨盘补源入库后整理：%s", stats)
    return stats


async def cross_copy_organize_watcher() -> None:
    """定时把补源复制成功的文件按订阅开关原地改名。"""
    await asyncio.sleep(60)
    while True:
        try:
            await run_pending_organize_once()
        except Exception as exc:  # noqa: BLE001 - 定时任务不能把循环打死
            logger.warning("跨盘补源入库后整理失败：%s", exc)
        await asyncio.sleep(ORGANIZE_INTERVAL_SECONDS)
