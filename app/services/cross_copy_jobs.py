"""跨盘复制任务（#9 任务接入）。

独立任务表 cross_copy_jobs（与 resource_jobs / scraper_jobs 同模式），
复用任务中心的展示 / 取消 / 重试框架，不破坏资源导入链路。

生命周期：pending → running → completed / failed / cancelled / skipped
- 排队：全局同时只跑 MAX_CONCURRENT_JOBS 个任务，其余 pending 排 FIFO 队列
- 取消：内存取消集合 + DB 状态双保险；取消后不自动重试（显式 retry 才重来）
- 重试：整任务重来（断点续传由各 provider 自行处理，当前三家均为 False）
- 启动恢复：把残留的 running/pending 任务重新入队（防重启丢任务）
"""

import asyncio
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from ..background import submit_background
from ..core import get_config, safe_json_dumps, safe_json_loads
from ..db import db_connection, ensure_db, now_text, sqlite_row_to_dict
from ..providers.registry import get_or_none as _get_provider_or_none
from ..providers.registry import label_for as _provider_label
from . import cross_cloud_copy as xcc
from . import cross_cloud_copy_folder as xccf
from .notify import (
    _send_notify_content,
    build_notify_runtime_config,
)

logger = logging.getLogger(__name__)

# 取消请求集合（与 resource_jobs 的取消机制同模式）
_cancel_requested: set = set()
_cancel_lock = threading.Lock()

# 单个任务的文件级进度（内存，任务结束即弃）
_job_progress: Dict[int, Dict[str, Any]] = {}
_progress_lock = threading.Lock()
# 文件夹任务当前处理到第几个文件（「文件 3/12」），随进度一起写进 status_detail
_job_detail: Dict[int, str] = {}
# 进度写库节流：内存每块都更新，DB 最多 ~1 秒一次（2.6GB 会有数千次回调）
_PROGRESS_DB_MIN_INTERVAL = 0.8
_last_db_progress: Dict[int, float] = {}

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_SKIPPED = "skipped"

ACTIVE_STATUSES = {STATUS_PENDING, STATUS_RUNNING}

# 全局并发上限：同时执行的跨盘复制任务数。
# 单个任务内部已经按 Range 分段并发下载，2 个并行基本能跑满带宽；
# 再多只会互相抢带宽，还会把 115 秒传 / 夸克取链打出 405 与风控。
DEFAULT_MAX_CONCURRENT = 2
MAX_CONCURRENT_HARD_LIMIT = 8


def _resolve_max_concurrent() -> int:
    """读 CROSS_COPY_MAX_CONCURRENT（1~8），没填或填了非法值都回落默认 2。"""
    raw = str(os.environ.get("CROSS_COPY_MAX_CONCURRENT", "") or "").strip()
    if not raw:
        return DEFAULT_MAX_CONCURRENT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_MAX_CONCURRENT
    if value < 1 or value > MAX_CONCURRENT_HARD_LIMIT:
        return DEFAULT_MAX_CONCURRENT
    return value


MAX_CONCURRENT_JOBS = _resolve_max_concurrent()

# 排队状态（进程内）：_queued 是按入队顺序等待的任务，_active 是已经占住名额的。
# 用 threading.Lock 而不是 asyncio 原语：submit 可能从线程池里调用
# （订阅补源走 asyncio.to_thread），锁必须能跨线程用。
_queued: List[int] = []
_active: set = set()
_sched_lock = threading.Lock()

# 防重复执行（同一个 job 不会被两个协程同时跑）
_running: set = set()
_running_lock = threading.Lock()


def _normalize_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    return status if status in {
        STATUS_PENDING,
        STATUS_RUNNING,
        STATUS_COMPLETED,
        STATUS_FAILED,
        STATUS_CANCELLED,
        STATUS_SKIPPED,
    } else STATUS_PENDING


def create_cross_copy_job(
    *,
    title: str,
    source_provider: str,
    target_provider: str,
    source_file_id: str,
    target_dir_id: str = "",
    target_savepath: str = "",
    source_path: str = "",
    target_path: str = "",
    filename: str = "",
    file_size: int = 0,
    source_mode: str = "direct",
    source_share_url: str = "",
    origin: str = "manual",
    origin_ref: str = "",
    source_is_dir: bool = False,
) -> int:
    """创建跨盘复制任务，返回 job_id。

    source_path / target_path 是「网盘内路径」（不带网盘名），用于任务卡片展示
    "源文件在哪、会被放到哪"；传空则由 target_savepath + filename 兜底拼。

    source_is_dir 为真表示 source_file_id 指向一个文件夹：任务内部会列出
    整个文件夹再逐个文件复制（一个任务 = 一个文件夹）。
    """
    ensure_db()
    now = now_text()
    resolved_target_path = str(target_path or "").strip() or _join_pan_path(target_savepath, filename)
    params = (
        str(title or "").strip()[:200],
        str(source_provider or "").strip(),
        str(target_provider or "").strip(),
        str(source_file_id or "").strip(),
        str(source_mode or "direct").strip() or "direct",
        str(target_dir_id or "").strip(),
        str(target_savepath or "").strip()[:500],
        str(source_path or "").strip()[:1000],
        resolved_target_path[:1000],
        str(filename or "").strip()[:300],
        int(file_size or 0),
        STATUS_PENDING,
        "排队中",
        str(origin or "manual").strip(),
        str(origin_ref or "").strip()[:200],
        str(source_share_url or "").strip()[:1000],
        1 if source_is_dir else 0,
        now,
        now,
    )
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO cross_copy_jobs(
                title, source_provider, target_provider, source_file_id, source_mode,
                target_dir_id, target_savepath, source_path, target_path, filename, file_size,
                status, status_detail, origin, origin_ref, source_share_url, source_is_dir,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        job_id = int(cursor.lastrowid)
        conn.commit()
    return job_id


def _attach_progress(job: Dict[str, Any]) -> Dict[str, Any]:
    """把内存里的实时进度（含已下载字节与速度）挂到任务字典上。"""
    try:
        job_id = int(job.get("id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    with _progress_lock:
        job["progress"] = dict(_job_progress.get(job_id, {})) if job_id else {}
    return job


def get_cross_copy_job(job_id: int) -> Optional[Dict[str, Any]]:
    ensure_db()
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cross_copy_jobs WHERE id = ?", (int(job_id),))
        row = cursor.fetchone()
    if not row:
        return None
    job = sqlite_row_to_dict(row)
    job["result"] = safe_json_loads(job.get("result_json"), {})
    return _attach_progress(job)


def list_cross_copy_jobs(limit: int = 50, status: str = "") -> List[Dict[str, Any]]:
    ensure_db()
    clauses = []
    params: List[Any] = []
    if status:
        clauses.append("status = ?")
        params.append(_normalize_status(status))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM cross_copy_jobs {where} ORDER BY id DESC LIMIT ?",
            params,
        )
        rows = cursor.fetchall()
    jobs = []
    for row in rows:
        job = sqlite_row_to_dict(row)
        job["result"] = safe_json_loads(job.get("result_json"), {})
        jobs.append(_attach_progress(job))
    return jobs


def update_cross_copy_job(job_id: int, **fields: Any) -> None:
    if not fields:
        return
    ensure_db()
    payload = dict(fields)
    if "result" in payload and isinstance(payload["result"], dict):
        payload["result_json"] = safe_json_dumps(payload.pop("result"))
    if "status" in payload:
        payload["status"] = _normalize_status(payload["status"])
    payload["updated_at"] = now_text()
    sets = ", ".join(f"{key} = ?" for key in payload.keys())
    values = list(payload.values()) + [int(job_id)]
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE cross_copy_jobs SET {sets} WHERE id = ?", values)
        conn.commit()


def cancel_cross_copy_job(job_id: int) -> Dict[str, Any]:
    """请求取消；已在运行的任务在下个检查点退出，未开始的直接标记 cancelled。"""
    job_id = int(job_id)
    job = get_cross_copy_job(job_id)
    if not job:
        return {"ok": False, "msg": "任务不存在"}
    status = _normalize_status(job.get("status"))
    if status not in ACTIVE_STATUSES:
        return {"ok": False, "msg": f"任务已结束（{status}），无需取消"}
    with _cancel_lock:
        _cancel_requested.add(job_id)
    if status == STATUS_PENDING:
        # 还在排队：直接从队列摘掉，别让空出来的名额把它捡起来
        _discard_from_queue(job_id)
        _finalize_cancelled(job_id, "已取消（尚未开始）")
    return {"ok": True, "msg": "已请求取消"}


def _finalize_cancelled(job_id: int, detail: str) -> None:
    update_cross_copy_job(
        job_id,
        status=STATUS_CANCELLED,
        status_detail=detail,
        finished_at=now_text(),
    )
    _clear_job_progress(job_id)


def retry_cross_copy_job(job_id: int) -> Dict[str, Any]:
    """重试：整任务重来（清空结果与错误，回到 pending）。"""
    job_id = int(job_id)
    job = get_cross_copy_job(job_id)
    if not job:
        return {"ok": False, "msg": "任务不存在"}
    status = _normalize_status(job.get("status"))
    if status in ACTIVE_STATUSES:
        return {"ok": False, "msg": "任务进行中，无法重试"}
    with _cancel_lock:
        _cancel_requested.discard(job_id)
    update_cross_copy_job(
        job_id,
        status=STATUS_PENDING,
        status_detail="等待重试",
        error_message="",
        result={},
        progress_percent=0,
        phase="",
        retry_count=int(job.get("retry_count", 0) or 0) + 1,
        started_at="",
        finished_at="",
    )
    submit_cross_copy_job(job_id)
    return {"ok": True, "msg": "已重新入队"}


def submit_cross_copy_job(job_id: int) -> None:
    """把任务放进等待队列并触发调度。

    这里只负责「排队 + 叫一声调度器」，真正能不能开跑由全局并发上限决定；
    队列里的任务不占名额，所以粘 20 个文件不会同时起 20 个下载线程。
    """
    job_id = int(job_id)
    with _sched_lock:
        if job_id in _active or job_id in _queued:
            return
        _queued.append(job_id)
    _schedule()


def _schedule() -> None:
    """有空闲名额就按 FIFO 拉起排队任务。

    名额在「派发」这一刻就记进 _active，任务结束时由 run_cross_copy_job 的
    finally 归还。派发本身失败（后台运行器起不来）也必须把名额还回去，
    否则一个卡住的派发就能把整个队列永久堵死。
    """
    while True:
        with _sched_lock:
            if not _queued or len(_active) >= MAX_CONCURRENT_JOBS:
                break
            job_id = _queued.pop(0)
            _active.add(job_id)
        try:
            submit_background(run_cross_copy_job, job_id, label=f"cross-copy-job-{job_id}")
        except Exception as exc:  # noqa: BLE001 - 起不来也要还名额，别拖死队列
            logger.exception("cross-copy job %s submit failed", job_id)
            _release_slot(job_id)
            _fail_job(job_id, f"任务启动失败：{exc}")
    _refresh_queue_details()


def _release_slot(job_id: int) -> None:
    """归还并发名额。必须在 finally 里调用，漏一次就少一个坑位。"""
    with _sched_lock:
        _active.discard(int(job_id))


def _refresh_queue_details() -> None:
    """把「前面还有几个任务」写回排队中的任务，让用户看到真实位置。"""
    with _sched_lock:
        waiting = list(_queued)
        ahead_base = len(_active)
    try:
        for index, job_id in enumerate(waiting):
            update_cross_copy_job(job_id, status_detail=_queue_waiting_detail(ahead_base + index))
    except Exception as exc:  # noqa: BLE001 - 文案写不进去不影响调度
        logger.info("cross-copy queue detail refresh skipped: %s", exc)


def _queue_waiting_detail(ahead: int) -> str:
    return f"排队中：前面还有 {max(0, int(ahead))} 个任务"


def _discard_from_queue(job_id: int) -> None:
    """把任务从等待队列里摘掉（取消用），别让后面的空闲名额把它捡起来跑。"""
    job_id = int(job_id)
    with _sched_lock:
        try:
            _queued.remove(job_id)
        except ValueError:
            pass


def _clear_job_progress(job_id: int) -> None:
    with _progress_lock:
        _job_progress.pop(int(job_id), None)
        _last_db_progress.pop(int(job_id), None)
        _job_detail.pop(int(job_id), None)


def format_transfer_size(value: Any) -> str:
    """字节数转人话（1.24GB / 860MB），任务卡片与状态明细共用。"""
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        size = 0.0
    if size <= 0:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024.0
        unit += 1
    if unit == 0:
        return f"{int(size)}B"
    return f"{size:.2f}{units[unit]}" if size < 100 else f"{size:.1f}{units[unit]}"


def _join_pan_path(*parts: Any) -> str:
    """把网盘目录与文件名拼成「/目录/文件」形式的展示路径。"""
    segments: List[str] = []
    for part in parts:
        text = str(part or "").strip().strip("/")
        if text:
            segments.append(text)
    return "/" + "/".join(segments) if segments else ""


def _report_progress(job_id: int, sent: int, total: int, phase: str, detail: str = "") -> None:
    """记录传输进度：已完成字节、总字节、实时速度（滑动平均）。

    速度只在同一阶段内累计；阶段切换（下载 → 上传）或进度回退时重新起算，
    避免把下载速度带进上传阶段。

    detail 用于文件夹任务的「文件 3/12」这类过程文案，会一起写进 status_detail。
    """
    job_id = int(job_id)
    now = time.monotonic()
    sent = max(0, int(sent or 0))
    total = int(total or 0)
    phase = str(phase or "").strip() or "download"
    percent = 0
    if total > 0:
        percent = max(0, min(100, int(sent * 100 / total)))
    phase_label = {
        "download": "下载",
        "upload": "上传",
    }.get(phase, phase)

    with _progress_lock:
        if detail:
            _job_detail[job_id] = str(detail or "").strip()
        detail_text = str(_job_detail.get(job_id, "") or "").strip()
        prev = _job_progress.get(job_id) or {}
        try:
            prev_ts = float(prev.get("updated_ts") or 0)
            prev_sent = int(prev.get("sent") or 0)
            prev_speed = float(prev.get("speed") or 0)
        except (TypeError, ValueError):
            prev_ts, prev_sent, prev_speed = 0.0, 0, 0.0
        speed = 0.0
        # 用「有没有上一帧」判断，而不是 prev_ts > 0：monotonic 的原点不固定
        # （容器/短命进程里可能从 0 附近开始），拿 0 当哨兵会把合法的时间戳判成缺失。
        if prev and now > prev_ts and str(prev.get("phase") or "") == phase and sent >= prev_sent:
            instant = (sent - prev_sent) / (now - prev_ts)
            speed = prev_speed * 0.6 + instant * 0.4 if prev_speed > 0 else instant
        _job_progress[job_id] = {
            "phase": phase,
            "sent": sent,
            "total": total,
            "percent": percent,
            "speed": int(round(speed)),
            "updated_ts": now,
        }
        last_persist = float(_last_db_progress.get(job_id) or 0)
        finished_transfer = total > 0 and sent >= total
        should_persist = finished_transfer or (now - last_persist) >= _PROGRESS_DB_MIN_INTERVAL
        if should_persist:
            _last_db_progress[job_id] = now

    if not should_persist:
        return

    parts = [f"{phase_label} {percent}%"]
    if detail_text:
        parts.insert(0, detail_text)
    if total > 0:
        parts.append(f"{format_transfer_size(sent)} / {format_transfer_size(total)}")
    if speed > 0:
        parts.append(f"{format_transfer_size(speed)}/s")
    update_cross_copy_job(
        job_id,
        progress_percent=percent,
        phase=phase,
        status_detail=" · ".join(parts),
    )


def _report_file_detail(job_id: int, detail: str) -> None:
    """记录文件夹任务的过程文案（「文件 3/12」），并（受节流）立刻写进任务状态。

    文件夹任务每个文件只产生几次字节进度回调，若只靠进度回调带出文案，
    小文件会一直看不到「第几个」；这里单独落一次状态，写库频率与进度同节流。
    """
    job_id = int(job_id)
    text = str(detail or "").strip()
    if not text:
        return
    now = time.monotonic()
    with _progress_lock:
        _job_detail[job_id] = text
        entry = _job_progress.get(job_id)
        if isinstance(entry, dict):
            entry["detail"] = text
        last_persist = float(_last_db_progress.get(job_id) or 0)
        should_persist = (now - last_persist) >= _PROGRESS_DB_MIN_INTERVAL
        if should_persist:
            _last_db_progress[job_id] = now
    if should_persist:
        update_cross_copy_job(job_id, status_detail=text)


def _is_cancel_requested(job_id: int) -> bool:
    with _cancel_lock:
        return int(job_id) in _cancel_requested


async def run_cross_copy_job(job_id: int) -> None:
    """执行单个跨盘复制任务（进度/取消/通知/状态收口）。"""
    job_id = int(job_id)
    with _running_lock:
        if job_id in _running:
            return
        _running.add(job_id)
    try:
        job = get_cross_copy_job(job_id)
        if not job:
            return
        status = _normalize_status(job.get("status"))
        if status not in ACTIVE_STATUSES:
            return
        if _is_cancel_requested(job_id):
            _finalize_cancelled(job_id, "已取消（尚未开始）")
            return

        update_cross_copy_job(job_id, status=STATUS_RUNNING, status_detail="准备中", started_at=now_text())
        cfg = get_config()
        source_name = str(job.get("source_provider", "") or "").strip()
        target_name = str(job.get("target_provider", "") or "").strip()
        source_provider = _get_provider_or_none(source_name)
        target_provider = _get_provider_or_none(target_name)
        if source_provider is None or target_provider is None:
            raise xcc.CrossCloudCopyError("复制源或目标网盘未注册")
        if not getattr(target_provider, "supports_upload", False):
            raise xcc.CrossCloudCopyError(
                f"{getattr(target_provider, 'label', target_name) or '目标网盘'} 暂不支持上传"
            )
        if source_name == target_name:
            raise xcc.CrossCloudCopyError("复制源与目标不能是同一个网盘")

        source_cookie = source_provider.get_cookie(cfg)
        target_cookie = target_provider.get_cookie(cfg)
        if not source_cookie:
            raise xcc.CrossCloudCopyError(f"请先在参数配置中填写{source_provider.label}认证信息")
        if not target_cookie:
            raise xcc.CrossCloudCopyError(f"请先在参数配置中填写{target_provider.label}认证信息")

        # 订阅补源这类调用只给了「保存到哪个路径」，没给目录 id。这里必须先把路径
        # 换成真实目录 id，否则上传会落到根目录（看起来成功，其实存错地方）。
        job = _ensure_target_dir_id(job, target_provider, target_cookie)

        update_cross_copy_job(job_id, status_detail="开始复制")
        if not str(job.get("target_path", "") or "").strip():
            early_target_path = _join_pan_path(job.get("target_savepath"), job.get("filename"))
            if early_target_path:
                job["target_path"] = early_target_path
                update_cross_copy_job(job_id, target_path=early_target_path)

        # 源是文件夹：整个文件夹一个任务，内部逐个文件复制
        if _job_source_is_dir(job):
            folder_result = await asyncio.to_thread(
                xccf.copy_folder_across_clouds,
                source_provider=source_provider,
                source_cookie=source_cookie,
                source_folder_id=str(job.get("source_file_id", "") or ""),
                target_provider=target_provider,
                target_cookie=target_cookie,
                target_dir_id=str(job.get("target_dir_id", "") or ""),
                target_folder_name=str(job.get("filename", "") or ""),
                temp_dir=xcc.resolve_temp_dir(cfg, create=True),
                progress_cb=lambda sent, total, phase: _report_progress(job_id, sent, total, phase),
                cancel_check=lambda: _is_cancel_requested(job_id),
                check_existing=True,
                cfg=cfg,
                status_cb=lambda detail: _report_file_detail(job_id, detail),
            )
            _finish_folder_job(job_id, job, cfg, folder_result)
            return

        result = await asyncio.to_thread(
            xcc.copy_file_across_clouds,
            source_provider=source_provider,
            source_cookie=source_cookie,
            source_file_id=str(job.get("source_file_id", "") or ""),
            target_provider=target_provider,
            target_cookie=target_cookie,
            target_dir_id=str(job.get("target_dir_id", "") or ""),
            filename=str(job.get("filename", "") or ""),
            size=int(job.get("file_size", 0) or 0),
            temp_dir=xcc.resolve_temp_dir(cfg, create=True),
            progress_cb=lambda sent, total, phase: _report_progress(job_id, sent, total, phase),
            cancel_check=lambda: _is_cancel_requested(job_id),
            check_existing=True,
            cfg=cfg,
        )
        result_status = str(getattr(result, "status", "") or "uploaded")

        # 复制成功后按配置清理「订阅补源自己转存的中转副本」。
        # 只认 origin=subscription：手动复制的源文件是用户自己网盘里的原片，
        # 绝不能删（删了就是数据丢失）。文件夹任务一律不删源（这次只做复制）。
        job_origin = str(job.get("origin", "") or "").strip().lower()
        delete_source_after = bool(cfg.get("cross_copy_delete_source_after", True))
        if (
            job_origin == "subscription"
            and not _job_source_is_dir(job)
            and result_status in ("rapid", "uploaded")
            and delete_source_after
        ):
            try:
                _delete_source_file(source_provider, source_cookie, str(job.get("source_file_id", "") or ""))
            except Exception as exc:
                logger.info("cross-copy source cleanup failed (job=%s): %s", job_id, exc)

        final_status = STATUS_COMPLETED
        if result_status == "skipped":
            final_status = STATUS_SKIPPED
        final_name = str(getattr(result, "name", "") or "").strip() or str(job.get("filename", "") or "").strip()
        final_target_path = _join_pan_path(job.get("target_savepath"), final_name) or str(
            job.get("target_path", "") or ""
        )
        update_cross_copy_job(
            job_id,
            status=final_status,
            status_detail=str(getattr(result, "message", "") or ("已完成" if final_status == STATUS_COMPLETED else "已跳过")),
            progress_percent=100,
            phase="done",
            finished_at=now_text(),
            filename=final_name or str(job.get("filename", "") or ""),
            target_path=final_target_path,
            result=result.to_dict() if hasattr(result, "to_dict") else {"status": result_status},
        )
        _clear_job_progress(job_id)
        _push_cross_copy_notification(cfg, job, ok=True, message=str(getattr(result, "message", "") or ""))
    except xcc.CrossCloudCopyCancelled:
        _finalize_cancelled(job_id, "已取消")
    except xcc.CrossCloudCopyError as exc:
        _fail_job(job_id, str(exc))
    except Exception as exc:  # noqa: BLE001 - 兜底：任何异常都不能杀死任务循环
        logger.exception("cross-copy job %s crashed", job_id)
        _fail_job(job_id, f"复制失败：{exc}")
    finally:
        with _cancel_lock:
            _cancel_requested.discard(job_id)
        with _running_lock:
            _running.discard(job_id)
        # 无论成功/失败/取消/异常，都必须还名额并拉起下一个排队的任务
        _release_slot(job_id)
        _schedule()


def _ensure_target_dir_id(
    job: Dict[str, Any],
    target_provider,
    target_cookie: str,
) -> Dict[str, Any]:
    """补齐目标目录 id。

    两条入口给的字段不一样：界面上建任务的是「目录 id」，订阅补源给的是
    「保存到哪个路径」。后者如果不换成目录 id，上传会落到根目录——文件确实
    传上去了，但存错地方，用户很难发现。这里就地补一次并写回任务记录。

    解析失败不阻断：抛明确错误比悄悄传到根目录好。
    """
    existing = str(job.get("target_dir_id", "") or "").strip()
    if existing:
        return job
    savepath = str(job.get("target_savepath", "") or "").strip()
    if not savepath:
        return job
    resolver = getattr(target_provider, "ensure_folder_id_by_path", None)
    if not callable(resolver):
        raise xcc.CrossCloudCopyError(
            f"{getattr(target_provider, 'label', '目标网盘') or '目标网盘'} 无法按路径定位保存目录"
        )
    try:
        resolved = str(resolver(target_cookie, savepath) or "").strip()
    except Exception as exc:  # noqa: BLE001 - 统一翻成人话
        raise xcc.CrossCloudCopyError(f"在目标网盘创建保存目录失败：{exc}") from exc
    if not resolved:
        raise xcc.CrossCloudCopyError(f"在目标网盘找不到保存目录：{savepath}")
    job["target_dir_id"] = resolved
    try:
        job_id = int(job.get("id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if job_id:
        update_cross_copy_job(job_id, target_dir_id=resolved)
    return job


def _job_source_is_dir(job: Dict[str, Any]) -> bool:
    """任务源是文件夹？（老任务没有这列时为 0，走单文件逻辑）"""
    try:
        return bool(int(job.get("source_is_dir", 0) or 0))
    except (TypeError, ValueError):
        return False


def _finish_folder_job(job_id: int, job: Dict[str, Any], cfg: Dict[str, Any], result) -> None:
    """文件夹任务收口：写结果明细、按「全部失败/部分失败」定状态。

    文件夹任务一律不删源：这次只做复制，源文件夹里的东西不能动。
    """
    result_status = str(getattr(result, "status", "") or "completed")
    folder_name = str(getattr(result, "folder_name", "") or "").strip() or str(
        job.get("filename", "") or ""
    ).strip()
    total_bytes = int(getattr(result, "total_bytes", 0) or 0)
    message = str(getattr(result, "message", "") or "")
    target_path = _join_pan_path(job.get("target_savepath"), folder_name) or str(
        job.get("target_path", "") or ""
    )
    payload = result.to_dict() if hasattr(result, "to_dict") else {"status": result_status}
    fields: Dict[str, Any] = {
        "result": payload,
        "filename": folder_name or str(job.get("filename", "") or ""),
        "target_path": target_path,
    }
    if total_bytes > 0:
        fields["file_size"] = total_bytes
    update_cross_copy_job(job_id, **fields)

    if result_status == "failed":
        _fail_job(job_id, message or "整个文件夹都没复制成功")
        return

    update_cross_copy_job(
        job_id,
        status=STATUS_COMPLETED,
        status_detail=message or "复制完成",
        progress_percent=100,
        phase="done",
        error_message="",
        finished_at=now_text(),
    )
    _clear_job_progress(job_id)
    _push_cross_copy_notification(cfg, job, ok=True, message=message)


def _delete_source_file(source_provider, source_cookie: str, source_file_id: str) -> None:
    """删除源网盘中本任务转存的文件（仅按 file_id 精确删，不碰其他文件）。"""
    fid = str(source_file_id or "").strip()
    if not fid or not getattr(source_provider, "supports_delete", False):
        return
    source_provider.delete_entries(source_cookie, [fid])


def _fail_job(job_id: int, message: str) -> None:
    update_cross_copy_job(
        job_id,
        status=STATUS_FAILED,
        status_detail=message[:200],
        error_message=message[:2000],
        finished_at=now_text(),
    )
    _clear_job_progress(job_id)
    try:
        cfg = get_config()
        job = get_cross_copy_job(job_id) or {}
        _push_cross_copy_notification(cfg, job, ok=False, message=message)
    except Exception:  # noqa: BLE001 - 通知失败不影响任务收口
        logger.info("cross-copy failure notify skipped (job=%s)", job_id)


def _push_cross_copy_notification(cfg: Dict[str, Any], job: Dict[str, Any], *, ok: bool, message: str) -> None:
    """跨盘复制完成/失败通知（受 cross_copy_notify 开关控制）。"""
    if not bool(cfg.get("cross_copy_notify", True)):
        return
    runtime_cfg = build_notify_runtime_config(cfg)
    if not bool(runtime_cfg.get("enabled", False)):
        return
    # 通知里要给人看的网盘名，不能直接拼内部代号（否则会显示 tianyi 这种英文）
    source_label = _provider_label(str(job.get("source_provider", "") or ""), default="源网盘")
    target_label = _provider_label(str(job.get("target_provider", "") or ""), default="目标网盘")
    title = str(job.get("title", "") or job.get("filename", "") or "文件")
    if ok:
        content = "\n".join(
            [
                "### 跨盘复制完成",
                f"> 文件：{title}",
                f"> 链路：{source_label} → {target_label}",
                f"> 结果：{message or '成功'}",
            ]
        )
    else:
        content = "\n".join(
            [
                "### 跨盘复制失败",
                f"> 文件：{title}",
                f"> 链路：{source_label} → {target_label}",
                f"> 原因：{message or '未知错误'}",
                "> 建议：到任务中心查看详情并重试",
            ]
        )
    try:
        _send_notify_content(runtime_cfg, content, timeout=15)
    except Exception as exc:  # noqa: BLE001 - 通知失败只记录
        logger.info("cross-copy notify failed (job=%s): %s", job.get("id"), exc)


def requeue_cross_copy_jobs_on_startup() -> Dict[str, int]:
    """启动恢复：重启前残留的 pending/running 任务重新入队。

    走的是和新建任务同一条排队路径（submit → _schedule），
    重启后一次涌入几十个任务也不会同时开跑。
    """
    ensure_db()
    requeued = 0
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM cross_copy_jobs WHERE status IN (?, ?)",
            (STATUS_PENDING, STATUS_RUNNING),
        )
        rows = cursor.fetchall()
        for row in rows:
            job_id = int(row[0])
            update_cross_copy_job(
                job_id,
                status=STATUS_PENDING,
                status_detail="服务重启，等待重新执行",
            )
            requeued += 1
        conn.commit()
    for job_id in _stale_job_ids():
        submit_cross_copy_job(job_id)
    return {"requeued": requeued}


def cleanup_temp_files_on_startup() -> Dict[str, int]:
    """扫掉异常退出留下的半截临时文件，返回清掉的数量。

    正常完成/失败的任务都在 ``finally`` 里自己删临时文件，只有容器被杀、进程
    崩溃这类硬中断才会留下 ``xcopy-*.part``。这些文件没人认领，不清理就会一直
    占着磁盘，所以启动时扫一遍、长期运行时再按间隔扫一遍。

    清理失败不影响主流程：临时文件占的是磁盘，不该把服务拉不起来。
    """
    removed = 0
    try:
        cfg = get_config()
        temp_dir = xcc.resolve_temp_dir(cfg)
        if temp_dir:
            removed = int(xcc.cleanup_orphan_temp_files(temp_dir) or 0)
            if removed:
                logger.info("cross-copy orphan temp files cleaned: %s in %s", removed, temp_dir)
    except Exception as exc:  # noqa: BLE001 - 清理失败不阻断启动
        logger.warning("cross-copy orphan temp cleanup skipped: %s", exc)
    return {"removed": removed}


# 容器长期运行时的兜底扫描间隔。孤儿文件本来就不多，30 分钟一次足够，
# 又不会把磁盘 IO 花在空扫描上。
TEMP_CLEANUP_INTERVAL_SECONDS = 30 * 60


async def cross_copy_temp_cleanup_watcher() -> None:
    """定时清理半截临时文件（容器长期运行、没有重启机会时的兜底）。"""
    await asyncio.sleep(60)
    while True:
        try:
            await asyncio.to_thread(cleanup_temp_files_on_startup)
        except Exception as exc:  # noqa: BLE001 - 定时任务不能把循环打死
            logger.warning("跨盘复制临时文件定期清理失败：%s", exc)
        await asyncio.sleep(TEMP_CLEANUP_INTERVAL_SECONDS)


def _stale_job_ids() -> List[int]:
    """待恢复的任务按 job id 升序排队（先建的先跑）。"""
    jobs = list_cross_copy_jobs(limit=100, status=STATUS_PENDING)
    return sorted(int(job["id"]) for job in jobs)
