"""订阅跨盘补源（#10）。

订阅任务（provider ≠ quark）开启 cross_copy_mode 后，把候选中的夸克分享
自动注册为跨盘复制任务，省去手动用 OpenList 跨盘复制的步骤。

流程（save_first，绕开分享直下的限速/次数/验证风险）：
    夸克分享链接 → 转存到夸克专属中转目录 → 列目录匹配拿 fid
    → 创建 cross_copy_jobs（origin=subscription）→ 后台复制到任务网盘

去重：同一订阅任务（origin_ref）+ 同一分享链接 + 同一源条目已有未完成任务时
跳过，避免每轮订阅重复注册；复制成功后按配置删除中转文件。

一个分享里可能有一整个文件夹（比如 12 集），转存后把本次新增的每个条目
都注册成任务：文件夹一个任务（整目录复制），散文件一个任务一个。
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from ..core import get_config
from ..db import db_connection, ensure_db
from ..providers.registry import get_or_none as _get_provider_or_none
from ..providers.registry import label_for as _provider_label
from . import cross_copy_jobs as ccj

logger = logging.getLogger(__name__)

# 夸克侧中转目录（与订阅离线中转同级的独立目录，删除只删此目录内文件）
CROSS_COPY_STAGING_ROOT = "云下载/跨盘中转"

# 单轮订阅最多处理几个分享候选（按分享算，不是按任务算）
MAX_REGISTERED_PER_RUN = 3

# 单个分享内部最多注册多少个条目（防止一个大分享一次注册上百个任务）
# 可用环境变量 CROSS_COPY_SUBSCRIPTION_MAX_FILES 调整
MAX_FILES_PER_SHARE = 50


def _max_files_per_share() -> int:
    """单个分享的注册上限：环境变量优先，非法值回退到 MAX_FILES_PER_SHARE。"""
    raw = str(os.environ.get("CROSS_COPY_SUBSCRIPTION_MAX_FILES", "") or "").strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return int(MAX_FILES_PER_SHARE)
    return value if value > 0 else int(MAX_FILES_PER_SHARE)


def _staging_relative_path(task_name: str) -> str:
    """中转目录按订阅任务分子目录，避免多任务文件混在一起。"""
    safe_name = "".join(ch for ch in str(task_name or "") if ch not in '/\\:*?"<>|').strip()
    if not safe_name:
        safe_name = "未命名任务"
    return f"{CROSS_COPY_STAGING_ROOT}/{safe_name[:60]}"


def find_active_cross_copy_job(
    origin_ref: str,
    source_share_url: str,
    source_file_id: str = "",
) -> Optional[Dict[str, Any]]:
    """查重：同一订阅任务 + 同一分享链接是否已有未完成的补源任务。

    source_file_id 传了就再收窄到「同一源条目」——一个分享会注册多个任务，
    只按分享查重会把同一分享里的第二个条目误判成「已在队列中」。
    不传（默认空）时行为和以前完全一致：只看任务 + 分享链接。
    """
    ensure_db()
    sql = """
        SELECT * FROM cross_copy_jobs
        WHERE origin = 'subscription' AND origin_ref = ? AND source_share_url = ?
          AND status IN ('pending', 'running')
    """
    params: List[Any] = [str(origin_ref or ""), str(source_share_url or "")]
    wanted_file_id = str(source_file_id or "").strip()
    if wanted_file_id:
        sql += " AND source_file_id = ?"
        params.append(wanted_file_id)
    sql += " ORDER BY id DESC LIMIT 1"
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, tuple(params))
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


def _entry_id(entry: Dict[str, Any]) -> str:
    return str(entry.get("id", "") or entry.get("fid", "") or "").strip()


def _entry_name(entry: Dict[str, Any]) -> str:
    return str(entry.get("name", "") or entry.get("file_name", "") or "").strip()


def _entry_size(entry: Dict[str, Any]) -> int:
    try:
        return int(entry.get("size", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _entry_is_dir(entry: Dict[str, Any]) -> bool:
    """这条是文件夹还是文件？只认列表项里的 is_dir。

    夸克的列表项统一经 app/providers/quark.py 的 _parse_quark_entry 解析，
    里面按 file_type / dir / obj_category 算好了 is_dir（bool）。拿不到这个
    字段就一律按文件处理——不凭「大小为 0」「没有后缀」去猜，猜错了要么把
    文件夹当文件注册、要么把文件当文件夹，都是白跑一趟。
    """
    raw = entry.get("is_dir")
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    try:
        return bool(int(raw))
    except (TypeError, ValueError):
        return bool(raw)


def _staging_keys(entries: List[Dict[str, Any]]) -> set:
    """中转目录现有条目的 (名字, 大小) 集合，用来判断哪些是这轮新转存进来的。"""
    keys = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        name = _entry_name(entry)
        if not name:
            continue
        keys.add((name, _entry_size(entry)))
    return keys


def _stage_share_to_quark(
    quark_provider,
    cookie: str,
    share_url: str,
    receive_code: str,
    staging_rel_path: str,
) -> Dict[str, Any]:
    """把夸克分享转存到中转目录，返回本次真正新进来的条目。

    返回结构::

        {
          "folder_id": "中转目录 id",
          "folders": [{"id": ..., "name": ..., "size": ...}],  # 新转存进来的文件夹
          "files":   [{"id": ..., "name": ..., "size": ...}],  # 新转存进来的散文件
          "skipped": ["跳过原因（人话）", ...],
          "already_staged": True/False,  # 分享内容上次已经全部转存过
        }

    中转目录是按订阅任务复用的，里面可能躺着前几轮转存的文件，所以不能
    「转存后把目录里所有东西都注册一遍」——那样会把历史文件重复补一遍。
    这里的做法是：转存前先记下目录里已有的 (名字, 大小)，只把缺的条目发给
    夸克转存，转存后重新列目录，只认不在旧集合里的条目。同名同大小的老条目
    一律当已存在跳过（宁可少补一次，也不要白跑一遍复制）。

    一个分享里可能是文件夹（12 集），也可能是几个散文件，还可能是混合，
    所以返回的是列表而不是单个文件。
    """
    share_payload = quark_provider.resolve_share_payload(cookie, share_url, "", receive_code)
    listed = quark_provider.list_share_entries(cookie, share_payload, cid="0")
    # 真实 provider 返回 {"entries": [...], "summary": ...}（见 base.list_share_entries
    # 的 Dict 契约），不是裸列表；2026-10-20 真机发现这里按列表迭代导致分享永远
    # 被判定为空，订阅补源转存整链不可用。
    raw_entries = listed.get("entries", []) if isinstance(listed, dict) else (listed or [])
    entries = [entry for entry in raw_entries if isinstance(entry, dict)]
    if not entries:
        raise RuntimeError("分享内没有可转存的文件")

    folder_id = str(quark_provider.ensure_folder_id_by_path(cookie, staging_rel_path) or "0")

    skipped: List[str] = []
    share_entries: List[Dict[str, Any]] = []
    for entry in entries:
        entry_id = _entry_id(entry)
        name = _entry_name(entry)
        if not entry_id or not name:
            continue
        share_entries.append(
            {
                "id": entry_id,
                "name": name,
                "size": _entry_size(entry),
                "is_dir": _entry_is_dir(entry),
                "fid_token": str(
                    entry.get("fid_token", "") or entry.get("share_fid_token", "") or ""
                ).strip(),
            }
        )
    if not share_entries:
        raise RuntimeError("分享内没有可转存的文件")

    existing_keys = _staging_keys(quark_provider.list_entries(cookie, folder_id))
    pending: List[Dict[str, Any]] = []
    for item in share_entries:
        if (item["name"], item["size"]) in existing_keys:
            skipped.append(f"中转目录已有同名文件，跳过：{item['name']}")
            continue
        pending.append(item)

    if not pending:
        skipped.append("这个分享的内容上次已经转存过，这次不再重复转存")
        return {
            "folder_id": folder_id,
            "folders": [],
            "files": [],
            "skipped": skipped,
            "already_staged": True,
        }

    # 单个分享最多处理 MAX_FILES_PER_SHARE 个条目，超出的留给下一轮（不静默截断）
    cap = _max_files_per_share()
    if len(pending) > cap:
        skipped.append(
            f"本次分享共 {len(share_entries)} 个条目，这轮只补前 {cap} 个，"
            f"其余 {len(pending) - cap} 个下次再补"
        )
        pending = pending[:cap]

    selected_entries = [
        {
            "id": item["id"],
            "name": item["name"],
            "size": item["size"],
            "is_dir": item["is_dir"],
            "cid": item["id"] if item["is_dir"] else "",
            "fid": "" if item["is_dir"] else item["id"],
            "fid_token": item["fid_token"],
        }
        for item in pending
    ]
    receive_payload = quark_provider.prepare_share_receive(
        cookie,
        {
            **share_payload,
            "selected_ids": [item["id"] for item in selected_entries],
            "selected_entries": selected_entries,
        },
        cid=folder_id,
    )
    quark_provider.submit_share_receive(cookie, receive_payload, selected_entries)

    # 转存后重新列目录：只有不在旧集合里的条目才是这轮新进来的
    staged_entries = [
        entry for entry in (quark_provider.list_entries(cookie, folder_id) or []) if isinstance(entry, dict)
    ]
    has_dir_flag = any("is_dir" in entry for entry in staged_entries)
    new_entries: List[Dict[str, Any]] = []
    seen_keys = set()
    for entry in staged_entries:
        entry_id = _entry_id(entry)
        name = _entry_name(entry)
        if not entry_id or not name:
            continue
        key = (name, _entry_size(entry))
        if key in existing_keys or key in seen_keys:
            continue
        seen_keys.add(key)
        new_entries.append(
            {
                "id": entry_id,
                "name": name,
                "size": _entry_size(entry),
                "is_dir": _entry_is_dir(entry),
            }
        )
    if not new_entries:
        raise RuntimeError("转存后没在中转目录找到新条目")

    if not has_dir_flag:
        # 拿不到文件/文件夹标记时保守处理：全部当文件注册，并在报告里说明
        skipped.append("目录列表没有文件/文件夹标记，这次全部按文件注册")

    return {
        "folder_id": folder_id,
        "folders": [item for item in new_entries if item["is_dir"]],
        "files": [item for item in new_entries if not item["is_dir"]],
        "skipped": skipped,
        "already_staged": False,
    }


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
    # 订阅日志会直接展示给用户，用中文名而不是内部代号（否则会看到 tianyi 这种）
    target_label = _provider_label(target_provider, default=target_provider)
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
        try:
            staged = _stage_share_to_quark(
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

        skipped.extend(staged.get("skipped", []) or [])
        folders: List[Dict[str, Any]] = list(staged.get("folders", []) or [])
        files: List[Dict[str, Any]] = list(staged.get("files", []) or [])
        if not folders and not files:
            # 内容上次已经转存过（原因已写进 skipped），这轮没什么可注册的
            continue

        registered_here = 0
        folders_here = 0
        files_here = 0
        for item, is_folder in [(item, True) for item in folders] + [(item, False) for item in files]:
            item_name = str(item.get("name", "") or "").strip()
            item_id = str(item.get("id", "") or "").strip()
            if find_active_cross_copy_job(task_name, share_url, source_file_id=item_id):
                skipped.append(f"已在补源队列中，跳过：{item_name}")
                continue
            job_id = ccj.create_cross_copy_job(
                title=str(
                    task.get("title", "")
                    or task.get("name", "")
                    or item_name
                    or "跨盘复制"
                ),
                source_provider="quark",
                target_provider=target_provider,
                source_file_id=item_id,
                target_savepath=str(task.get("savepath", "") or ""),
                source_path="/" + "/".join(
                    part
                    for part in (
                        str(staging_rel_path or "").strip().strip("/"),
                        str(item_name or "").strip().strip("/"),
                    )
                    if part
                ),
                filename=item_name,
                file_size=int(item.get("size", 0) or 0) if not is_folder else 0,
                source_mode="save_first",
                source_share_url=share_url,
                origin=origin,
                origin_ref=task_name,
                source_is_dir=is_folder,
            )
            ccj.submit_cross_copy_job(job_id)
            registered.append(job_id)
            registered_here += 1
            if is_folder:
                folders_here += 1
            else:
                files_here += 1
            logs.append(
                (
                    f"跨盘补源已入队：夸克{'文件夹' if is_folder else ''} → {target_label}"
                    f"（{item_name or share_url[:40]}，任务 #{job_id}）",
                    "info",
                )
            )

        if registered_here:
            logs.append(
                (
                    f"本次从该分享注册了 {registered_here} 个任务"
                    f"（{folders_here} 个文件夹、{files_here} 个文件）：{share_url[:60]}",
                    "info",
                )
            )

    if registered:
        logs.append((f"跨盘补源：本轮注册 {len(registered)} 个复制任务（目标 {target_label}）", "info"))
    return {"registered": registered, "skipped": skipped, "logs": logs}
