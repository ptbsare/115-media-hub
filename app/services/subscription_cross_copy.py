"""订阅跨盘补源（#10）。

订阅任务（provider ≠ quark）开启 cross_copy_mode 后，把候选中的夸克分享
自动注册为跨盘复制任务，省去手动用 OpenList 跨盘复制的步骤。

流程（save_first，绕开分享直下的限速/次数/验证风险）：
    夸克分享链接 → 转存到夸克专属中转目录 → 列目录匹配拿 fid
    → 创建 cross_copy_jobs（origin=subscription）→ 后台复制到任务网盘

去重：同一订阅任务（origin_ref）+ 同一分享链接已有未完成任务时跳过，
避免每轮订阅重复注册；复制成功后按配置删除中转文件。
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from ..core import get_config
from ..db import db_connection, ensure_db
from ..providers.registry import get_or_none as _get_provider_or_none
from . import cross_copy_jobs as ccj

logger = logging.getLogger(__name__)

# 夸克侧中转目录（与订阅离线中转同级的独立目录，删除只删此目录内文件）
CROSS_COPY_STAGING_ROOT = "云下载/跨盘中转"

# 单轮订阅最多注册的补源任务数（防止一次注册太多复制任务抢占带宽）
MAX_REGISTERED_PER_RUN = 3


def _staging_relative_path(task_name: str) -> str:
    """中转目录按订阅任务分子目录，避免多任务文件混在一起。"""
    safe_name = "".join(ch for ch in str(task_name or "") if ch not in '/\\:*?"<>|').strip()
    if not safe_name:
        safe_name = "未命名任务"
    return f"{CROSS_COPY_STAGING_ROOT}/{safe_name[:60]}"


def find_active_cross_copy_job(origin_ref: str, source_share_url: str) -> Optional[Dict[str, Any]]:
    """查重：同一订阅任务 + 同一分享链接是否已有未完成的补源任务。"""
    ensure_db()
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM cross_copy_jobs
            WHERE origin = 'subscription' AND origin_ref = ? AND source_share_url = ?
              AND status IN ('pending', 'running')
            ORDER BY id DESC LIMIT 1
            """,
            (str(origin_ref or ""), str(source_share_url or "")),
        )
        row = cursor.fetchone()
    if not row:
        return None
    from ..db import sqlite_row_to_dict

    return sqlite_row_to_dict(row)


def _extract_share_url(candidate: Dict[str, Any]) -> str:
    item = candidate.get("item", {}) if isinstance(candidate.get("item"), dict) else {}
    return str(item.get("link_url", "") or "").strip()


def _extract_receive_code(candidate: Dict[str, Any]) -> str:
    item = candidate.get("item", {}) if isinstance(candidate.get("item"), dict) else {}
    extra = item.get("extra") if isinstance(item.get("extra"), dict) else {}
    return str(item.get("receive_code", "") or extra.get("receive_code", "") or "").strip()


def _pick_share_candidates(candidates: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """从订阅候选中挑出夸克分享候选（按评分降序，去重分享链接）。"""
    picked: List[Dict[str, Any]] = []
    seen_urls: set = set()
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        item = candidate.get("item", {}) if isinstance(candidate.get("item"), dict) else {}
        link_type = str(item.get("link_type", "") or "").strip().lower()
        share_url = _extract_share_url(candidate)
        if link_type and link_type != "quark":
            continue
        if not share_url or share_url.lower() in seen_urls:
            continue
        seen_urls.add(share_url.lower())
        picked.append(candidate)
        if len(picked) >= limit:
            break
    return picked


def _stage_share_to_quark(
    quark_provider,
    cookie: str,
    share_url: str,
    receive_code: str,
    staging_rel_path: str,
) -> Tuple[str, str]:
    """把夸克分享转存到中转目录，返回 (staging_dir_id, 首个文件 fid)。

    分享可能包含多个文件/目录；复制引擎按目录/文件逐个处理，
    这里返回目录 id，由复制任务按目录展开（当前引擎按单文件设计，
    因此返回分享内第一个文件条目供任务创建；整目录复制留待后续迭代）。
    """
    share_payload = quark_provider.resolve_share_payload(cookie, share_url, "", receive_code)
    entries = quark_provider.list_share_entries(cookie, share_payload, cid="0")
    entries = [entry for entry in (entries or []) if isinstance(entry, dict)]
    if not entries:
        raise RuntimeError("分享内没有可转存的文件")

    folder_id = quark_provider.ensure_folder_id_by_path(cookie, staging_rel_path)
    first = entries[0]
    selected_entries = [
        {
            "id": str(entry.get("id", "") or entry.get("fid", "") or "").strip(),
            "name": str(entry.get("name", "") or entry.get("file_name", "") or "").strip(),
            "size": int(entry.get("size", 0) or 0),
        }
        for entry in entries
    ]
    receive_payload = quark_provider.prepare_share_receive(
        cookie,
        {
            **share_payload,
            "selected_ids": [item["id"] for item in selected_entries if item["id"]],
            "selected_entries": selected_entries,
        },
        cid=str(folder_id or "0"),
    )
    quark_provider.submit_share_receive(cookie, receive_payload, selected_entries)

    # 转存后回目录按「文件名 + 大小」匹配，拿源网盘内真实 fid
    staged_entries = quark_provider.list_entries(cookie, str(folder_id or "0"))
    target_name = str(first.get("name", "") or first.get("file_name", "") or "").strip()
    target_size = int(first.get("size", 0) or 0)
    best_fid = ""
    for entry in staged_entries or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "") or entry.get("file_name", "") or "").strip()
        size = int(entry.get("size", 0) or 0)
        if name == target_name and (target_size <= 0 or size == target_size):
            best_fid = str(entry.get("id", "") or entry.get("fid", "") or "").strip()
            break
    if not best_fid:
        # 名字没匹配上（分享重命名等）时退回第一条，任务内按文件名下载仍可用
        for entry in staged_entries or []:
            if isinstance(entry, dict):
                best_fid = str(entry.get("id", "") or entry.get("fid", "") or "").strip()
                if best_fid:
                    target_name = str(entry.get("name", "") or "").strip()
                    target_size = int(entry.get("size", 0) or 0)
                    break
    if not best_fid:
        raise RuntimeError("转存后未在中转目录找到文件")
    return str(folder_id or "0"), best_fid, target_name, target_size


def register_cross_copy_from_candidates(
    *,
    task: Dict[str, Any],
    cfg: Dict[str, Any],
    candidates: List[Dict[str, Any]],
    origin: str = "subscription",
) -> Dict[str, Any]:
    """把订阅候选中的夸克分享注册为跨盘复制任务。

    :param task: 归一化后的订阅任务（含 cross_copy_mode / cross_copy_sources / provider）
    :param cfg: 应用配置
    :param candidates: ranked_candidates
    :return: {"registered": [...job_id], "skipped": 原因列表, "logs": [(message, level), ...]}

    纯同步函数（供 asyncio.to_thread 调用）；日志消息通过 logs 返回，
    由调用方在事件循环里写入订阅日志。
    """
    logs: List[Tuple[str, str]] = []

    mode = str(task.get("cross_copy_mode", "off") or "off")
    sources = list(task.get("cross_copy_sources", []) or [])
    if mode == "off" or "quark" not in sources:
        return {"registered": [], "skipped": ["补源未开启"], "logs": logs}

    target_provider = str(task.get("provider", "115") or "115").strip()
    if target_provider == "quark":
        return {"registered": [], "skipped": ["任务网盘即夸克，无需补源"], "logs": logs}

    quark_provider = _get_provider_or_none("quark")
    target = _get_provider_or_none(target_provider)
    if quark_provider is None or target is None:
        return {"registered": [], "skipped": ["夸克或目标网盘未注册"], "logs": logs}
    if not getattr(target, "supports_upload", False):
        return {"registered": [], "skipped": [f"{getattr(target, 'label', target_provider)} 暂不支持上传"], "logs": logs}

    cookie_quark = str(cfg.get("cookie_quark", "") or "").strip()
    if not cookie_quark:
        return {"registered": [], "skipped": ["夸克 Cookie 未配置"], "logs": logs}

    picked = _pick_share_candidates(candidates, MAX_REGISTERED_PER_RUN)
    if not picked:
        return {"registered": [], "skipped": ["候选中没有夸克分享"], "logs": logs}

    task_name = str(task.get("name", "") or task.get("title", "") or "").strip()
    staging_rel_path = _staging_relative_path(task_name)
    registered: List[int] = []
    skipped: List[str] = []

    for candidate in picked:
        share_url = _extract_share_url(candidate)
        receive_code = _extract_receive_code(candidate)
        existing = find_active_cross_copy_job(task_name, share_url)
        if existing:
            skipped.append(f"已在补源队列中：{share_url[:60]}")
            continue
        try:
            folder_id, file_fid, file_name, file_size = _stage_share_to_quark(
                quark_provider,
                cookie_quark,
                share_url,
                receive_code,
                staging_rel_path,
            )
        except Exception as exc:
            skipped.append(f"转存失败：{share_url[:40]}（{exc}）")
            logs.append((f"跨盘补源转存失败：{share_url[:60]}（{exc}）", "warn"))
            continue
        job_id = ccj.create_cross_copy_job(
            title=str(task.get("title", "") or task.get("name", "") or file_name or "跨盘复制"),
            source_provider="quark",
            target_provider=target_provider,
            source_file_id=file_fid,
            target_savepath=str(task.get("savepath", "") or ""),
            source_path="/" + "/".join(
                part
                for part in (
                    str(staging_rel_path or "").strip().strip("/"),
                    str(file_name or "").strip().strip("/"),
                )
                if part
            ),
            filename=file_name,
            file_size=file_size,
            source_mode="save_first",
            source_share_url=share_url,
            origin=origin,
            origin_ref=task_name,
        )
        ccj.submit_cross_copy_job(job_id)
        registered.append(job_id)
        logs.append(
            (f"跨盘补源已入队：夸克 → {target_provider}（{file_name or share_url[:40]}，任务 #{job_id}）", "info")
        )

    if registered:
        logs.append((f"跨盘补源：本轮注册 {len(registered)} 个复制任务（目标 {target_provider}）", "info"))
    return {"registered": registered, "skipped": skipped, "logs": logs}
