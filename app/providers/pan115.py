import base64
import hashlib
import hmac
import http.client
import logging
import re
import socket
import threading
from typing import Any, Dict, List, Optional, Tuple

import requests

from .common import parse_int
from .base import CloudProvider, UploadError, UploadResult, upload_part_with_retry
from .pan115_cipher import decode_upload_response, make_upload_payload
from .pan115_downurl import (
    build_115_download_headers,
    collect_115_set_cookie_pairs,
    is_115_size_limit_error,
    resolve_115_download_url_by_downurl,
)
from .registry import register
from ..share_selection import normalize_share_selection_meta
from ..core import *  # noqa: F401,F403
from ..core import (
    _api_115_last_request_monotonic,
    _api_115_list_cache,
    _api_115_list_cache_lock,
    _api_115_rate_limit_lock,
    _share_snap_last_request_monotonic,
    _share_snap_rate_limit_lock,
)

_pan115_http_local = threading.local()
_CLOUD115_WEBAPI_REFERER = "https://servicewechat.com/wx2c744c010a61b0fa/94/page-frame.html"
_CLOUD115_WEBAPI_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 "
    "MicroMessenger/6.8.0(0x16080000) NetType/WIFI MiniProgramEnv/Mac "
    "MacWechat/WMPF MacWechat/3.8.9(0x13080910) XWEB/1227"
)
_115_LIST_PAGE_LIMIT_DEFAULT = 500
_115_LIST_PAGE_LIMIT_MIN = 100
_115_LIST_MAX_PAGES = 80
_115_LIST_RETRY_TOTAL = 2
_115_LIST_RETRY_BASE_SECONDS = 0.6


def _build_115_timing_mark(last_mono: float) -> Tuple[int, float]:
    now_mono = time.monotonic()
    return int((now_mono - last_mono) * 1000), now_mono


def _log_115_share_timing(share_code: str, cid: str, timings: List[Dict[str, Any]], total_ms: int) -> None:
    safe_share_code = str(share_code or "").strip()
    safe_cid = str(cid or "0").strip() or "0"
    stage_text = " / ".join(
        f"{str(item.get('label', '') or item.get('stage', '')).strip()}: {int(item.get('ms', 0) or 0)}ms"
        for item in (timings or [])
        if int(item.get("ms", 0) or 0) >= 0
    )
    logging.info(
        "115 share timing share_code=%s cid=%s total=%sms%s",
        f"{safe_share_code[:6]}***" if len(safe_share_code) > 6 else safe_share_code,
        safe_cid,
        total_ms,
        f" stages=[{stage_text}]" if stage_text else "",
    )


def _get_115_webapi_session() -> requests.Session:
    session = getattr(_pan115_http_local, "webapi_session", None)
    if session is None:
        session = requests.Session()
        _pan115_http_local.webapi_session = session
    return session


def _build_115_webapi_headers(cookie: str, referer: str = "") -> Dict[str, str]:
    return {
        "Cookie": str(cookie or "").strip(),
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
        "xweb_xhr": "1",
        "Referer": str(referer or "").strip() or _CLOUD115_WEBAPI_REFERER,
        "User-Agent": _CLOUD115_WEBAPI_USER_AGENT,
        "Accept-Language": "zh-CN,zh;q=0.9",
    }


def _pick_115_download_headers(info: Any, account_cookie: str) -> Dict[str, str]:
    """取链结果里带回来的下载头优先；缺了就用账号 Cookie 兜底。

    115 的直链按 UA 签名，下载端绝不能自己另编 UA，所以这张表必须由
    取链那一侧产出（``build_115_download_headers``）。
    """
    headers = (info or {}).get("headers") if isinstance(info, dict) else None
    if isinstance(headers, dict) and headers:
        return {
            str(key): str(value)
            for key, value in headers.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    extra_cookie = str((info or {}).get("cookie", "") or "").strip() if isinstance(info, dict) else ""
    return build_115_download_headers(account_cookie, "", extra_cookie)


def _try_115_downurl_fallback(cookie: str, pick_code: str, file_id: str) -> Dict[str, Any]:
    """尽力走一次电脑端 downurl 通道；失败返回空字典，由调用方决定怎么兜。

    只在「webapi 通道拿到了直链但缺下载 Cookie」这种尴尬场景用：此时已经有一条
    可用直链，回退失败也不该把整个取链判死，所以这里吞掉异常。
    """
    try:
        return resolve_115_download_url_by_downurl(cookie, pick_code=pick_code, file_id=file_id) or {}
    except Exception:
        return {}


def _request_115_webapi_json(url: str, headers: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
    response = _get_115_webapi_session().get(
        normalize_http_url(url),
        headers=headers,
        timeout=max(3, int(timeout or 30)),
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("115 返回内容不是有效 JSON") from exc
    return payload if isinstance(payload, dict) else {}


def _response_set_cookie_pairs(response: Any) -> List[str]:
    """取响应里的全部 Set-Cookie 原始行（requests/urllib3 两种结构都兼容）。"""
    raw_headers = getattr(getattr(response, "raw", None), "headers", None)
    for attr in ("getlist", "get_all"):
        getter = getattr(raw_headers, attr, None)
        if callable(getter):
            try:
                return [str(item or "") for item in (getter("Set-Cookie") or [])]
            except Exception:
                continue
    single = ""
    try:
        single = str((getattr(response, "headers", None) or {}).get("Set-Cookie", "") or "")
    except Exception:
        single = ""
    return [single] if single else []


def _request_115_webapi_json_with_cookies(
    url: str,
    headers: Dict[str, str],
    timeout: int = 30,
) -> Tuple[Dict[str, Any], List[str]]:
    """同 ``_request_115_webapi_json``，额外把响应 Set-Cookie 带回来。

    取下载直链时必须留着它：``/files/download`` 会给一个路径级 Cookie，
    下载端不带就 403（UA 不匹配也 403，见 ``build_115_download_headers``）。
    """
    response = _get_115_webapi_session().get(
        normalize_http_url(url),
        headers=headers,
        timeout=max(3, int(timeout or 30)),
    )
    response.raise_for_status()
    set_cookies = _response_set_cookie_pairs(response)
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("115 返回内容不是有效 JSON") from exc
    return (payload if isinstance(payload, dict) else {}), set_cookies


def submit_115_offline_task(cookie: str, resource_url: str, folder_id: str) -> Dict[str, Any]:
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    resource_url = str(resource_url or "").strip()
    if not resource_url:
        raise RuntimeError("资源链接为空")

    headers = {
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://115.com/",
        "Origin": "https://115.com",
        "User-Agent": "Mozilla/5.0 115-media-hub",
    }
    try:
        response = http_request_form_json(
            "https://115.com/web/lixian/?ct=lixian&ac=add_task_url",
            {"url": resource_url, "wp_path_id": folder_id or "0"},
            timeout=45,
            extra_headers=headers,
        )
        accepted = bool(response.get("state")) or int(response.get("errcode", 0) or 0) == 10008
        if not accepted:
            detail = (
                str(response.get("error_msg", "")).strip()
                or str(response.get("message", "")).strip()
                or str(response.get("msg", "")).strip()
                or "115 离线任务提交失败"
            )
            raise RuntimeError(detail)
        mark_cookie_health_success("115", trigger="runtime:submit_115_offline_task")
        return response
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:submit_115_offline_task")
        raise


def _normalize_115_offline_task(item: Any) -> Dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    status = parse_int(source.get("status", 0), default=0)
    percent = 0.0
    try:
        percent = float(source.get("percentDone", 0) or 0)
    except (TypeError, ValueError):
        percent = 0.0
    return {
        "info_hash": str(source.get("info_hash", "") or "").strip(),
        "url": str(source.get("url", "") or "").strip(),
        "name": str(source.get("name", "") or "").strip(),
        "size": parse_int(source.get("size", 0), default=0),
        "status": status,
        "percent": max(0.0, min(100.0, percent)),
        "file_id": str(source.get("file_id", "") or "").strip(),
        "delete_file_id": str(source.get("delete_file_id", "") or "").strip(),
        "wp_path_id": str(source.get("wp_path_id", "") or "").strip(),
        "add_time": parse_int(source.get("add_time", 0), default=0),
    }


def list_115_offline_tasks(cookie: str, page: int = 1) -> Dict[str, Any]:
    """读取 115 官方离线任务列表；返回 {tasks, page, page_count, count, total}。"""
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    normalized_page = max(1, int(page or 1))
    headers = {
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://115.com/",
        "User-Agent": "Mozilla/5.0 115-media-hub",
    }
    url = f"https://lixian.115.com/lixian/?ct=lixian&ac=task_lists&page={normalized_page}"
    try:
        throttle_115_api_requests()
        response = http_request_json(url, extra_headers=headers, timeout=45)
        if not bool((response or {}).get("state", False)):
            detail = (
                str((response or {}).get("error", "")).strip()
                or str((response or {}).get("msg", "")).strip()
                or str((response or {}).get("message", "")).strip()
                or "115 离线任务列表读取失败"
            )
            raise RuntimeError(detail)
        raw_tasks = (response or {}).get("tasks") or []
        tasks = [
            normalized
            for normalized in (
                _normalize_115_offline_task(item) for item in raw_tasks if isinstance(item, dict)
            )
            if normalized.get("info_hash") or normalized.get("url")
        ]
        page_count = max(1, parse_int((response or {}).get("page_count", 1), default=1))
        mark_cookie_health_success("115", trigger="runtime:list_115_offline_tasks")
        return {
            "tasks": tasks,
            "page": parse_int((response or {}).get("page", normalized_page), default=normalized_page),
            "page_count": page_count,
            "count": parse_int((response or {}).get("count", len(tasks)), default=len(tasks)),
            "total": parse_int((response or {}).get("total", len(tasks)), default=len(tasks)),
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:list_115_offline_tasks")
        raise


def submit_115_export_dir(
    cookie: str,
    file_ids: Any,
    target: str = "U_1_0",
    layer_limit: int = 0,
) -> str:
    """提交官方“导出目录树”异步任务，返回任务 id。"""
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    normalized_ids = str(file_ids or "").strip()
    if not normalized_ids:
        raise RuntimeError("导出目录不能为空")
    payload = {"file_ids": normalized_ids, "target": str(target or "U_1_0").strip() or "U_1_0"}
    try:
        layer_limit = max(0, min(25, int(layer_limit or 0)))
    except (TypeError, ValueError):
        layer_limit = 0
    if layer_limit > 0:
        payload["layer_limit"] = layer_limit
    headers = {
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://115.com/",
        "Origin": "https://115.com",
        "User-Agent": "Mozilla/5.0 115-media-hub",
    }
    try:
        response = http_request_form_json(
            "https://webapi.115.com/files/export_dir",
            payload,
            timeout=45,
            extra_headers=headers,
        )
        if not bool((response or {}).get("state", False)):
            detail = (
                str((response or {}).get("error", "")).strip()
                or str((response or {}).get("msg", "")).strip()
                or str((response or {}).get("message", "")).strip()
                or "115 导出目录树提交失败"
            )
            raise RuntimeError(detail)
        data = (response or {}).get("data") or {}
        export_id = str((data or {}).get("export_id", "") or "").strip()
        if not export_id:
            raise RuntimeError("115 导出目录树未返回任务 ID")
        mark_cookie_health_success("115", trigger="runtime:submit_115_export_dir")
        return export_id
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:submit_115_export_dir")
        raise


def query_115_export_dir_status(cookie: str, export_id: str) -> Dict[str, Any]:
    """查询导出目录树任务状态；完成时返回 {export_id,file_id,file_name,pick_code}，否则返回空字典。"""
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    normalized_id = str(export_id or "").strip()
    if not normalized_id:
        raise RuntimeError("导出任务 ID 不能为空")
    url = "https://webapi.115.com/files/export_dir?export_id=" + urllib.parse.quote(normalized_id)
    result = _request_115_webapi_json(url, headers=_build_115_webapi_headers(cookie))
    if not bool((result or {}).get("state", False)):
        detail = (
            str((result or {}).get("error", "")).strip()
            or str((result or {}).get("msg", "")).strip()
            or "115 导出目录树状态查询失败"
        )
        raise RuntimeError(detail)
    data = (result or {}).get("data") or {}
    if not isinstance(data, dict) or not data:
        return {}
    return {
        key: str(data.get(key, "") or "").strip()
        for key in ("export_id", "file_id", "file_name", "pick_code")
        if str(data.get(key, "") or "").strip()
    }


def wait_115_export_dir(
    cookie: str,
    export_id: str,
    timeout_seconds: int = 600,
    check_interval: float = 2.0,
) -> Dict[str, Any]:
    """轮询导出目录树直到完成；超时抛错（任务仍在服务端执行）。"""
    started = time.monotonic()
    interval = max(0.5, float(check_interval or 2.0))
    timeout = max(0, int(timeout_seconds or 600))
    while True:
        throttle_115_api_requests()
        data = query_115_export_dir_status(cookie, export_id)
        if data:
            return data
        if timeout > 0 and time.monotonic() - started >= timeout:
            raise RuntimeError(
                f"115 导出目录树超时（{timeout} 秒，export_id={export_id}），任务可能仍在服务端执行"
            )
        time.sleep(interval)


def get_115_file_sha1_by_id(cookie: str, file_id: str, attempts: int = 3) -> str:
    """按 file_id 取新导出文件的 sha1（优先 get_info 直查）；拿不到时返回空字符串。"""
    cookie = str(cookie or "").strip()
    normalized_id = str(file_id or "").strip()
    if not cookie or not normalized_id:
        return ""
    for _ in range(max(1, int(attempts or 3))):
        try:
            info = get_115_file_info(cookie, normalized_id)
            if info:
                sha1 = str(info.get("sha1", "") or "").strip()
                if sha1:
                    return sha1
        except Exception:
            pass
        try:
            entries = list_115_entries(cookie, "0", force_refresh=True)
            matched = next(
                (item for item in entries if str(item.get("id", "") or "").strip() == normalized_id),
                None,
            )
            if matched:
                sha1 = str(matched.get("sha1", "") or "").strip()
                if sha1:
                    return sha1
        except Exception:
            pass
        time.sleep(1.0)
    return ""


def get_115_file_hash(cookie: str, file_id: str) -> Dict[str, Any]:
    """按 id 取 115 文件的 SHA1 + 大小（跨盘复制秒传探测用，不下载文件）。

    115 的文件信息接口本来就返回 ``sha1``，所以源端是 115 时跨盘复制只需一次
    元信息请求就能拿到秒传所需的 ``sha1 + size``，整文件下载可以被省掉。
    传 pick_code 时先换成真实 fid；目录 / 拿不到哈希时返回 ``{}``。
    """
    cookie = str(cookie or "").strip()
    token = str(file_id or "").strip()
    if not cookie or not token:
        return {}
    fid = ensure_115_file_id(cookie, token, action="读取文件哈希")
    info = get_115_file_info(cookie, fid)
    if not isinstance(info, dict) or bool(info.get("is_dir")):
        return {}
    sha1 = str(info.get("sha1", "") or "").strip().upper()
    size = parse_int(info.get("size", 0), default=0)
    if not sha1 or size <= 0:
        return {}
    return {
        "sha1": sha1,
        "size": size,
        "name": str(info.get("name", "") or "").strip(),
    }


def get_115_file_info(cookie: str, file_id: str) -> Dict[str, Any]:
    """按 file_id 直查 115 文件信息（即时，不受目录列表缓存延迟影响）。"""
    cookie = str(cookie or "").strip()
    normalized_id = str(file_id or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    if not normalized_id:
        raise RuntimeError("文件 ID 不能为空")
    url = "https://webapi.115.com/files/get_info?file_id=" + urllib.parse.quote(normalized_id)
    result = _request_115_webapi_json(url, headers=_build_115_webapi_headers(cookie))
    if not bool((result or {}).get("state", False)):
        detail = (
            str((result or {}).get("error", "")).strip()
            or str((result or {}).get("msg", "")).strip()
            or "115 文件信息查询失败"
        )
        raise RuntimeError(detail)
    raw_items = (result or {}).get("data") or []
    items = raw_items if isinstance(raw_items, list) else ([raw_items] if isinstance(raw_items, dict) else [])
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
        if item_id and item_id != normalized_id:
            continue
        # 115 的 get_info 字段不对称：文件带 ``fid``，父目录在 ``cid``；
        # 目录自身就是 ``cid``，父目录在 ``pid``。回验"搬到哪了"都靠这个。
        item_fid = str(item.get("fid") or "").strip()
        parent_id = (
            str(item.get("cid") or "").strip()
            if item_fid
            else str(item.get("pid") or "").strip()
        )
        return {
            "id": item_id or normalized_id,
            "name": str(item.get("n") or item.get("name") or "").strip(),
            "sha1": str(item.get("sha") or item.get("sha1") or "").strip(),
            "pick_code": str(item.get("pc") or item.get("pick_code") or "").strip(),
            "size": parse_int(item.get("s") or item.get("size") or 0),
            "parent_id": parent_id,
            "is_dir": not bool(item_fid),
        }
    raise RuntimeError(f"115 文件不存在或已删除：{normalized_id}")


# ============================================================
# pick_code → file_id 推导
#
# 115 的分享接收、离线任务、导出目录树等接口有的只回 pick_code（pc），
# 上层把它当 entry id 用时，删除 / 取直链 / 上传回调定位都会失败。
# 这里用 web 版文件信息接口按 pick_code 反查真实 fid：
#   GET https://webapi.115.com/files/get_info?pick_code=<pick_code>
# 响应 {"state": true, "data": [{"fid": "...", "pc": "...", ...}]}。
# 出处：xhofe/115driver pkg/driver/api.go 的 ApiFileInfo 常量，
# 以及 AList drivers/115 中 getFileInfoWithThumb("pick_code", pickCode) 的调用方式；
# 官方开放接口 /open/ufile/downurl 的响应同样以 file_id 作为 map key，
# 可交叉印证 pick_code 对应的就是该 fid。
# ============================================================

_115_PICK_CODE_FID_CACHE_TTL_SECONDS = 10 * 60
_115_PICK_CODE_FID_CACHE_MAX_ROWS = 2000
_115_PICK_CODE_PATTERN = re.compile(r"^[0-9a-z]{6,64}$", re.IGNORECASE)

_115_pick_code_fid_cache: Dict[str, Dict[str, Any]] = {}
_115_pick_code_fid_cache_lock = threading.Lock()


def is_115_pick_code_like(value: Any) -> bool:
    """判断 entry id 是否像 pick_code。

    115 的 fid 是纯数字；pick_code 是 6-64 位字母数字混合串（实际约 17-20 位）。
    纯数字直接透传，避免给正常 fid 平白多发一次请求。
    """
    token = str(value or "").strip()
    if (not token) or token.isdigit():
        return False
    return bool(_115_PICK_CODE_PATTERN.fullmatch(token))


def _prune_115_pick_code_fid_cache_locked(now_ts: float) -> None:
    if not _115_pick_code_fid_cache:
        return
    expired_keys = [
        key
        for key, payload in _115_pick_code_fid_cache.items()
        if now_ts >= float((payload or {}).get("expires_at", 0.0) or 0.0)
    ]
    for key in expired_keys:
        _115_pick_code_fid_cache.pop(key, None)
    max_rows = max(200, int(_115_PICK_CODE_FID_CACHE_MAX_ROWS or 2000))
    if len(_115_pick_code_fid_cache) <= max_rows:
        return
    ordered = sorted(
        _115_pick_code_fid_cache.items(),
        key=lambda item: float((item[1] or {}).get("updated_at", 0.0) or 0.0),
    )
    for key, _ in ordered[: len(_115_pick_code_fid_cache) - max_rows]:
        _115_pick_code_fid_cache.pop(key, None)


def _get_cached_115_file_id(pick_code: str) -> str:
    now_ts = time.time()
    with _115_pick_code_fid_cache_lock:
        _prune_115_pick_code_fid_cache_locked(now_ts)
        payload = _115_pick_code_fid_cache.get(pick_code)
        if not payload:
            return ""
        if now_ts >= float(payload.get("expires_at", 0.0) or 0.0):
            _115_pick_code_fid_cache.pop(pick_code, None)
            return ""
        return str(payload.get("file_id", "") or "").strip()


def _store_cached_115_file_id(pick_code: str, file_id: str) -> None:
    if (not pick_code) or (not file_id):
        return
    now_ts = time.time()
    with _115_pick_code_fid_cache_lock:
        _115_pick_code_fid_cache[pick_code] = {
            "file_id": file_id,
            "expires_at": now_ts + _115_PICK_CODE_FID_CACHE_TTL_SECONDS,
            "updated_at": now_ts,
        }
        _prune_115_pick_code_fid_cache_locked(now_ts)


def invalidate_115_pick_code_fid_cache() -> None:
    """清空 pick_code → fid 缓存（换账号、删除文件后重试时用）。"""
    with _115_pick_code_fid_cache_lock:
        _115_pick_code_fid_cache.clear()


def _extract_115_file_id_from_get_info(payload: Dict[str, Any], pick_code: str) -> str:
    """从 get_info 响应里取 fid；目录没有 fid，视为无法定位（返回空串）。"""
    data = (payload or {}).get("data")
    items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
    for item in items:
        if not isinstance(item, dict):
            continue
        item_pick_code = str(item.get("pc") or item.get("pick_code") or "").strip()
        if item_pick_code and item_pick_code != pick_code:
            continue
        # 只认 fid/file_id：文件的父目录在 cid，绝不能拿 cid 当文件 id。
        file_id = str(item.get("fid") or item.get("file_id") or "").strip()
        if file_id:
            return file_id
    return ""


def resolve_file_id_by_pick_code(cookie: str, pick_code: str) -> str:
    """按 pick_code 反查 115 file_id（fid）。

    成功返回 fid，失败 / 接口异常返回空串（调用方负责降级），不抛异常。
    结果缓存 10 分钟，避免同一文件反复查询。
    """
    cookie = str(cookie or "").strip()
    normalized_pick_code = str(pick_code or "").strip()
    if (not cookie) or (not normalized_pick_code):
        return ""
    if not is_115_pick_code_like(normalized_pick_code):
        return ""

    cached_file_id = _get_cached_115_file_id(normalized_pick_code)
    if cached_file_id:
        return cached_file_id

    try:
        throttle_115_api_requests()
        url = "https://webapi.115.com/files/get_info?pick_code=" + urllib.parse.quote(normalized_pick_code)
        result = _request_115_webapi_json(url, headers=_build_115_webapi_headers(cookie))
    except Exception as exc:
        logging.warning(
            "115 按 pick_code 查询 file_id 失败 pick_code=%s: %s",
            normalized_pick_code[:8] + "***" if len(normalized_pick_code) > 8 else normalized_pick_code,
            exc,
        )
        mark_cookie_health_failure("115", exc, trigger="runtime:resolve_file_id_by_pick_code")
        return ""

    if not bool((result or {}).get("state", False)):
        detail = (
            str((result or {}).get("error", "")).strip()
            or str((result or {}).get("msg", "")).strip()
            or str((result or {}).get("message", "")).strip()
            or "115 文件信息查询失败"
        )
        logging.info("115 按 pick_code 查不到文件 pick_code=%s: %s", normalized_pick_code, detail)
        return ""

    file_id = _extract_115_file_id_from_get_info(result, normalized_pick_code)
    if not file_id:
        logging.info("115 按 pick_code 未取到 fid（可能是目录或文件已删除） pick_code=%s", normalized_pick_code)
        return ""

    _store_cached_115_file_id(normalized_pick_code, file_id)
    mark_cookie_health_success("115", trigger="runtime:resolve_file_id_by_pick_code")
    return file_id


def resolve_file_ids_by_pick_codes(cookie: str, pick_codes: List[str]) -> Dict[str, str]:
    """批量版：逐个解析但复用缓存，返回 {pick_code: fid}（失败的值为空串）。

    get_info 没有可靠的批量参数，这里只做去重 + 缓存复用，避免重复请求。
    """
    resolved: Dict[str, str] = {}
    for raw_code in pick_codes or []:
        pick_code = str(raw_code or "").strip()
        if (not pick_code) or pick_code in resolved:
            continue
        resolved[pick_code] = resolve_file_id_by_pick_code(cookie, pick_code)
    return resolved


def ensure_115_file_id(cookie: str, entry_id: str, *, action: str = "定位文件") -> str:
    """把可能是 pick_code 的 entry id 换成真正的 fid。

    fid 原样返回；pick_code 换不到时报人话错误，不拿 pick_code 硬发请求。
    """
    token = str(entry_id or "").strip()
    if not token:
        raise RuntimeError(f"缺少文件 ID，无法{action}")
    if not is_115_pick_code_like(token):
        return token
    file_id = resolve_file_id_by_pick_code(cookie, token)
    if not file_id:
        raise RuntimeError("无法从 pick_code 定位文件，请刷新目录后重试")
    return file_id


def _collect_115_download_urls(payload: Any) -> List[str]:
    """从 115 下载接口响应里递归收集 http(s) 链接。"""
    urls: List[str] = []
    seen = set()

    def push(value: Any) -> None:
        token = str(value or "").strip()
        if (not token) or (not token.lower().startswith(("http://", "https://"))) or token in seen:
            return
        seen.add(token)
        urls.append(token)

    def walk(node: Any) -> None:
        if isinstance(node, str):
            push(node)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        for key in ("file_url", "url", "download_url", "download_url_web", "download_url_web2"):
            walk(node.get(key))
        for key in ("data", "urls", "result", "info"):
            walk(node.get(key))

    walk(payload)
    return urls


def resolve_115_download_info(cookie: str, file_id: str) -> Dict[str, Any]:
    """取 115 文件下载直链 + 元信息（两段式取链）。

    入口先做 pick_code → fid 归一：传进来的是 pick_code 时先换 fid，
    换不到就报明确错误；传 fid 时按 fid 查 pickcode 再取链。

    第一段走 webapi ``/files/download``（小文件快且稳）；如果 115 明确拒绝
    大文件（errno ``50028``「文件大小超出限制，请用电脑端下载」），第二段改走
    电脑端的加密 downurl 通道（见 ``pan115_downurl``），满足 GB 级影片的取链。
    """
    cookie = str(cookie or "").strip()
    token = str(file_id or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    if not token:
        raise RuntimeError("缺少文件 ID，无法获取下载地址")

    file_name = ""
    size = 0
    sha1 = ""
    if is_115_pick_code_like(token):
        fid = ensure_115_file_id(cookie, token, action="获取下载地址")
        pick_code = token
    else:
        fid = token
        info = get_115_file_info(cookie, fid)
        pick_code = str(info.get("pick_code", "") or "").strip()
        file_name = str(info.get("name", "") or "").strip()
        size = parse_int(info.get("size", 0), default=0)
        sha1 = str(info.get("sha1", "") or "").strip()
        if not pick_code:
            raise RuntimeError("115 文件信息里没有 pickcode，无法获取下载地址，请刷新目录后重试")

    try:
        throttle_115_api_requests()
        url = "https://webapi.115.com/files/download?pickcode=" + urllib.parse.quote(pick_code)
        result, response_set_cookies = _request_115_webapi_json_with_cookies(
            url, headers=_build_115_webapi_headers(cookie)
        )
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:resolve_115_download_info")
        raise RuntimeError(f"115 下载地址获取失败：{exc}") from exc

    if not bool((result or {}).get("state", False)):
        detail = (
            str((result or {}).get("error", "")).strip()
            or str((result or {}).get("msg", "")).strip()
            or str((result or {}).get("message", "")).strip()
            or "115 下载地址解析失败"
        )
        # 大文件被 webapi 拒绝时改走电脑端 downurl：115 对超过阈值（实际就是
        # 绝大多数影片）的文件只给 errno 50028，webapi 这条路永远拿不到直链。
        if is_115_size_limit_error(result):
            try:
                downurl_info = resolve_115_download_url_by_downurl(
                    cookie,
                    pick_code=pick_code,
                    file_id=fid,
                )
            except Exception as exc:
                mark_cookie_health_failure("115", exc, trigger="runtime:resolve_115_download_info_downurl")
                raise RuntimeError(
                    f"115 拒绝取链：{detail}；该文件需要走电脑端下载通道，但 downurl 回退失败（{exc}）"
                ) from exc
            downurl_url = str(downurl_info.get("url", "") or "").strip()
            if not downurl_url:
                raise RuntimeError(
                    f"115 拒绝取链：{detail}；该文件需要走电脑端下载通道，但 downurl 回退未返回下载地址"
                )
            mark_cookie_health_success("115", trigger="runtime:resolve_115_download_info_downurl")
            # 返回结构保持与第一段一致，只把 downurl 能补的文件名/大小补上。
            return {
                "url": downurl_url,
                "file_id": fid,
                "pick_code": pick_code,
                "name": file_name or str(downurl_info.get("file_name", "") or "").strip(),
                "size": size or parse_int(downurl_info.get("size", 0), default=0),
                "sha1": sha1,
                "cookie": str(downurl_info.get("cookie", "") or "").strip(),
                "headers": _pick_115_download_headers(downurl_info, cookie),
            }
        raise RuntimeError(detail)

    download_urls = _collect_115_download_urls(result)
    download_url = str(download_urls[0] if download_urls else "").strip()
    if not download_url:
        raise RuntimeError("115 返回成功但没有解析到下载链接，请稍后重试")

    mark_cookie_health_success("115", trigger="runtime:resolve_115_download_info")
    download_cookie = collect_115_set_cookie_pairs(response_set_cookies)
    if not download_cookie:
        # webapi 通道的直链要求带上响应下发的「路径级」Cookie，只带账号 Cookie
        # 一样 403（实测）。拿不到就改走电脑端 downurl 通道（它只认 UA），
        # 别让任务死在一个没法自证的 403 上。
        downurl_info = _try_115_downurl_fallback(cookie, pick_code, fid)
        downurl_url = str((downurl_info or {}).get("url", "") or "").strip()
        if downurl_url:
            return {
                "url": downurl_url,
                "file_id": fid,
                "pick_code": pick_code,
                "name": file_name or str(downurl_info.get("file_name", "") or "").strip(),
                "size": size or parse_int(downurl_info.get("size", 0), default=0),
                "sha1": sha1,
                "cookie": str(downurl_info.get("cookie", "") or "").strip(),
                "headers": _pick_115_download_headers(downurl_info, cookie),
            }
    return {
        "url": download_url,
        "file_id": fid,
        "pick_code": pick_code,
        "name": file_name,
        "size": size,
        "sha1": sha1,
        "cookie": download_cookie,
        # 下载端必须复用「取链时的 UA + 路径级 Cookie」，否则 403
        "headers": build_115_download_headers(cookie, _CLOUD115_WEBAPI_USER_AGENT, download_cookie),
    }


def resolve_115_folder_path(cookie: str, cid: str) -> Dict[str, Any]:
    """按 cid 直查 115 目录的真实祖先链（一次 medialist 请求）。

    返回 ``{"cid", "path", "ancestors"}``：``ancestors`` 是从根下第一级到目标
    目录本身的 ``{id, name, parent_id}`` 列表，``path`` 为对应的相对路径字符串。
    搜索结果命中网盘任意位置时，用该结果恢复真实路径栏，避免把结果误挂到当前目录下。
    """
    cookie = str(cookie or "").strip()
    normalized_cid = str(cid or "0").strip() or "0"
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    if normalized_cid == "0":
        return {"cid": normalized_cid, "path": "", "ancestors": []}
    try:
        throttle_115_api_requests()
        params = {
            "aid": 1,
            "cid": normalized_cid,
            "limit": 1,
            "type": 6,
            "nf": 1,
            "format": "json",
        }
        url = "https://webapi.115.com/files/medialist?" + urllib.parse.urlencode(params)
        result = _request_115_webapi_json(url, headers=_build_115_webapi_headers(cookie))
        if not bool((result or {}).get("state", False)):
            detail = (
                str((result or {}).get("error", "")).strip()
                or str((result or {}).get("msg", "")).strip()
                or str((result or {}).get("message", "")).strip()
                or "115 目录路径查询失败"
            )
            raise RuntimeError(detail)
        raw_path = (result or {}).get("path") or []
        ancestors = []
        for raw_item in raw_path if isinstance(raw_path, list) else []:
            if not isinstance(raw_item, dict):
                continue
            item_id = str(raw_item.get("cid") or raw_item.get("id") or "").strip()
            name = str(raw_item.get("name") or "").strip()
            parent_id = str(raw_item.get("pid") or raw_item.get("parent_id") or "").strip()
            if item_id and name:
                ancestors.append({"id": item_id, "name": name, "parent_id": parent_id})
        mark_cookie_health_success("115", trigger="runtime:resolve_115_folder_path")
        return {
            "cid": normalized_cid,
            "path": join_relative_path(*[item["name"] for item in ancestors]),
            "ancestors": ancestors,
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:resolve_115_folder_path")
        raise


def throttle_115_api_requests(rate_limit_seconds: float = 0.0) -> None:
    global _api_115_last_request_monotonic
    min_interval = float(rate_limit_seconds or 0.0)
    if min_interval <= 0:
        min_interval = float(get_api_115_runtime_tuning().get("rate_limit_seconds", API_115_RATE_LIMIT_SECONDS) or 0.0)
    if min_interval <= 0:
        return
    with _api_115_rate_limit_lock:
        now_mono = time.monotonic()
        wait_seconds = min_interval - (now_mono - _api_115_last_request_monotonic)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _api_115_last_request_monotonic = time.monotonic()

def _clone_115_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item or {}) for item in (entries or [])]


def _clone_115_entries_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    return {
        "entries": _clone_115_entries(source.get("entries", [])),
        "summary": dict(source.get("summary", {}) if isinstance(source.get("summary"), dict) else {}),
        "entries_complete": bool(source.get("entries_complete", True)),
        "count": parse_int(source.get("count"), default=len(source.get("entries", []) or [])),
        "offset": max(0, parse_int(source.get("offset"), default=0)),
        "next_offset": max(0, parse_int(source.get("next_offset"), default=len(source.get("entries", []) or []))),
        "has_more": bool(source.get("has_more", False)),
        "pages_scanned": max(0, parse_int(source.get("pages_scanned"), default=1)),
    }


def _build_115_list_cache_key(cid: str, folders_only: bool = False) -> str:
    target_cid = str(cid or "0").strip() or "0"
    return f"{target_cid}|{'folders' if folders_only else 'full'}"


def _prune_115_list_cache_locked(now_ts: float) -> None:
    if not _api_115_list_cache:
        return
    expired_keys = [
        key
        for key, payload in _api_115_list_cache.items()
        if now_ts >= float(payload.get("expires_at", 0.0) or 0.0)
    ]
    for key in expired_keys:
        _api_115_list_cache.pop(key, None)
    max_rows = max(200, int(API_115_LIST_CACHE_MAX_ROWS or 2000))
    if len(_api_115_list_cache) <= max_rows:
        return
    ordered = sorted(
        _api_115_list_cache.items(),
        key=lambda item: float((item[1] or {}).get("updated_at", 0.0) or 0.0),
    )
    overflow = len(_api_115_list_cache) - max_rows
    for key, _ in ordered[:overflow]:
        _api_115_list_cache.pop(key, None)


def prune_115_list_cache() -> Dict[str, int]:
    now_ts = time.time()
    with _api_115_list_cache_lock:
        before = len(_api_115_list_cache)
        _prune_115_list_cache_locked(now_ts)
        return {"removed": max(0, before - len(_api_115_list_cache))}


def invalidate_115_entries_cache(cid: str = "") -> None:
    target_cid = str(cid or "").strip()
    with _api_115_list_cache_lock:
        if not target_cid:
            _api_115_list_cache.clear()
            return
        _api_115_list_cache.pop(target_cid, None)
        _api_115_list_cache.pop(_build_115_list_cache_key(target_cid, False), None)
        _api_115_list_cache.pop(_build_115_list_cache_key(target_cid, True), None)


def _normalize_115_file_entry(item: Dict[str, Any]) -> Dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    name = str(source.get("n") or source.get("name") or "").strip()
    folder_id = str(source.get("cid") or "").strip()
    file_id = str(source.get("fid") or source.get("file_id") or source.get("id") or "").strip()
    sha1 = str(source.get("sha1") or source.get("sha") or "").strip()
    is_dir = bool(folder_id) and not file_id and not sha1
    # 只回 pick_code 的接口（分享接收 / 离线结果 / 导出目录树）先用 pc 兜底 entry_id，
    # 保持旧调用方行为；payload 里单独保留 pick_code，真正需要 fid 时
    # （取下载直链等）再由 ensure_115_file_id() 反查，列表 normalize 不发网络请求。
    pick_code = str(source.get("pick_code") or source.get("pc") or "").strip()
    entry_id = folder_id if is_dir else (file_id or pick_code or sha1)
    if not name or not entry_id:
        return {}
    payload = {
        "id": entry_id,
        "cid": folder_id if is_dir else "",
        "name": name,
        "is_dir": is_dir,
        "size": parse_int(source.get("s") or source.get("size") or 0),
        "pick_code": pick_code,
        "sha1": sha1,
        "modified_at": str(source.get("te") or source.get("t") or source.get("tp") or source.get("tu") or "").strip(),
    }
    parent_id = str(source.get("pid") or source.get("parent_id") or "").strip()
    if parent_id:
        payload["parent_id"] = parent_id
    path = str(source.get("path") or "").strip()
    if path:
        payload["path"] = path
    return payload


def _is_retryable_115_list_error(exc: Exception) -> bool:
    """115 目录读取的断连/超时类错误可以重试；HTTP 状态错误（如 405 风控）不重试。"""
    if exc is None:
        return False
    if isinstance(exc, urllib.error.HTTPError):
        return False
    if isinstance(
        exc,
        (
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
            socket.timeout,
            TimeoutError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            urllib.error.URLError,
        ),
    ):
        return True
    return type(exc).__name__ in {
        "ProtocolError",
        "BrokenPipeError",
        "ConnectionResetError",
        "ConnectionAbortedError",
        "JSONDecodeError",
    }


def _request_115_entries_page(
    cookie: str,
    target_cid: str,
    offset: int,
    limit: int,
    *,
    fast: bool,
    timeout: int = 30,
) -> Dict[str, Any]:
    """按服务端 offset/limit 拉取一页 115 目录条目，返回 {items, raw_count, count}。

    ``items`` 保持服务端顺序，不做本地排序；排序由完整模式合并后或前端显示层负责。
    """
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, min(int(limit or _115_LIST_PAGE_LIMIT_DEFAULT), 1000))
    if fast:
        params = {
            "aid": 1,
            "cid": target_cid,
            "o": "user_ptime",
            "asc": 1,
            "offset": safe_offset,
            "show_dir": 1,
            "limit": safe_limit,
            "type": 0,
            "format": "json",
            "star": 0,
            "suffix": "",
            "natsort": 0,
            "snap": 0,
            "record_open_time": 1,
            "fc_mix": 0,
        }
        url = f"https://webapi.115.com/files?{urllib.parse.urlencode(params)}"
        result = _request_115_webapi_json(
            url,
            headers=_build_115_webapi_headers(cookie),
            timeout=timeout,
        )
    else:
        headers = {
            "Cookie": cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }
        url = (
            "https://aps.115.com/natsort/files.php"
            f"?aid=1&cid={urllib.parse.quote(target_cid)}&offset={safe_offset}"
            f"&limit={safe_limit}&show_dir=1&natsort=1&format=json"
        )
        result = http_request_json(url, extra_headers=headers, timeout=timeout)
    if not bool(result.get("state", False)):
        detail = str(result.get("error", "") or result.get("msg", "") or "读取 115 文件夹失败").strip()
        raise RuntimeError(detail)
    raw_items = result.get("data") or []
    raw_count = len(raw_items) if isinstance(raw_items, list) else 0
    items = [
        entry
        for entry in (_normalize_115_file_entry(item) for item in raw_items)
        if entry.get("id") and entry.get("name")
    ]
    return {
        "items": items,
        "raw_count": raw_count,
        "count": max(0, parse_int(result.get("count") or result.get("total") or 0)),
    }


def _request_115_entries_page_with_retry(
    cookie: str,
    target_cid: str,
    offset: int,
    limit: int,
    *,
    fast: bool,
    timeout: int = 30,
) -> Dict[str, Any]:
    """拉取一页并对 IncompleteRead/断连/超时做指数退避重试。"""
    last_error: Optional[Exception] = None
    for attempt in range(0, _115_LIST_RETRY_TOTAL + 1):
        try:
            return _request_115_entries_page(
                cookie,
                target_cid,
                offset,
                limit,
                fast=fast,
                timeout=timeout,
            )
        except Exception as exc:
            last_error = exc
            if (not _is_retryable_115_list_error(exc)) or attempt >= _115_LIST_RETRY_TOTAL:
                raise
            time.sleep(_115_LIST_RETRY_BASE_SECONDS * (attempt + 1))
    raise RuntimeError(str(last_error or "读取 115 文件夹失败").strip() or "读取 115 文件夹失败") from last_error


def list_115_entries_payload(
    cookie: str,
    cid: str = "0",
    force_refresh: bool = False,
    folders_only: bool = False,
    offset: int = 0,
    limit: int = 0,
    max_pages: int = 0,
) -> Dict[str, Any]:
    """读取 115 目录条目，支持两种模式：

    - 完整模式（默认，``limit<=0``）：按 offset 逐页拉取直到读完（最多 80 页），
      合并后按“文件夹在前、名称排序”返回，适合内部按名/按 id 查找；
    - 分页模式（``limit>0``）：只返回从 ``offset`` 开始的单页窗口，并给出
      ``count/next_offset/has_more`` 元数据，适合前端“加载更多”。

    分页模式下 ``folders_only`` 会连续扫描服务端页面直到集齐 ``limit`` 个文件夹，
    避免文件夹被文件穿插时漏页。每页请求都会对断连/超时做重试。
    """
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    target_cid = str(cid or "0").strip() or "0"
    folder_only_mode = bool(folders_only)
    start_offset = max(0, int(offset or 0))
    paged = int(limit or 0) > 0
    page_limit = (
        max(20, min(int(limit or _115_LIST_PAGE_LIMIT_DEFAULT), 1000))
        if paged
        else _115_LIST_PAGE_LIMIT_DEFAULT
    )
    max_pages_limit = max(0, int(max_pages or 0))
    runtime_tuning = get_api_115_runtime_tuning()
    cache_ttl_seconds = max(0, int(runtime_tuning.get("list_cache_ttl_seconds", API_115_LIST_CACHE_TTL_SECONDS) or 0))
    cache_key = _build_115_list_cache_key(target_cid, folder_only_mode)

    if (not force_refresh) and (not paged) and cache_ttl_seconds > 0:
        now_ts = time.time()
        with _api_115_list_cache_lock:
            cached = _api_115_list_cache.get(cache_key)
            if (not folder_only_mode) and not cached:
                cached = _api_115_list_cache.get(target_cid)
            if cached and now_ts < float(cached.get("expires_at", 0.0) or 0.0):
                return _clone_115_entries_payload(cached)

    try:
        throttle_115_api_requests()
        entries: List[Dict[str, Any]] = []
        cursor = start_offset
        server_count = 0
        pages_scanned = 0
        reached_end = False
        hit_page_cap = False
        # 每个调用固定使用一条路径，避免中途切换端点导致服务端分页顺序不一致。
        use_fast = folder_only_mode
        current_page_limit = page_limit

        while True:
            try:
                page = _request_115_entries_page_with_retry(
                    cookie,
                    target_cid,
                    cursor,
                    current_page_limit,
                    fast=use_fast,
                    timeout=30 if use_fast else 45,
                )
            except Exception as exc:
                if folder_only_mode and use_fast and pages_scanned == 0:
                    use_fast = False
                    try:
                        page = _request_115_entries_page_with_retry(
                            cookie,
                            target_cid,
                            cursor,
                            current_page_limit,
                            fast=False,
                            timeout=45,
                        )
                    except Exception as legacy_exc:
                        if _is_retryable_115_list_error(legacy_exc) and current_page_limit > _115_LIST_PAGE_LIMIT_MIN:
                            current_page_limit = max(_115_LIST_PAGE_LIMIT_MIN, current_page_limit // 2)
                            continue
                        raise
                elif _is_retryable_115_list_error(exc) and current_page_limit > _115_LIST_PAGE_LIMIT_MIN:
                    # 大响应被服务端掐断时，自动缩小单页大小重试（更小的响应更不容易被截断）。
                    current_page_limit = max(_115_LIST_PAGE_LIMIT_MIN, current_page_limit // 2)
                    continue
                else:
                    raise
            raw_count = max(0, int(page.get("raw_count", 0) or 0))
            server_count = max(0, int(page.get("count", 0) or 0))
            raw_items = page.get("items") or []
            entries.extend(
                item
                for item in raw_items
                if (not folder_only_mode) or item.get("is_dir")
            )
            pages_scanned += 1
            cursor += raw_count

            if paged and not folder_only_mode:
                # 普通目录：请求窗口本身就是一页，直接返回。
                reached_end = (
                    (not raw_items)
                    or raw_count < current_page_limit
                    or (server_count and cursor >= server_count)
                )
                break
            reached_end = (
                (not raw_items)
                or raw_count < current_page_limit
                or (server_count and cursor >= server_count)
            )
            if folder_only_mode and paged and len(entries) >= current_page_limit:
                # 仅文件夹：集齐一页文件夹即返回，避免被文件条目撑大响应。
                reached_end = False
                break
            if reached_end:
                break
            effective_cap = max_pages_limit if max_pages_limit > 0 else _115_LIST_MAX_PAGES
            if pages_scanned >= effective_cap:
                hit_page_cap = True
                break

        if paged:
            entries_complete = False
            has_more = not reached_end
            next_offset = cursor
        else:
            entries_complete = (not hit_page_cap) and reached_end
            has_more = not entries_complete
            next_offset = cursor
            if not folder_only_mode:
                entries.sort(key=lambda item: (0 if item["is_dir"] else 1, str(item["name"]).lower()))

        folder_count = sum(1 for item in entries if item.get("is_dir"))
        if folder_only_mode:
            file_count = max(0, server_count - folder_count) if server_count else 0
        else:
            file_count = len(entries) - folder_count
        payload = {
            "entries": entries,
            "summary": {
                "folder_count": folder_count,
                "file_count": file_count,
            },
            "entries_complete": entries_complete,
            "count": server_count or len(entries),
            "offset": start_offset,
            "next_offset": next_offset,
            "has_more": has_more,
            "pages_scanned": pages_scanned,
        }
        if (not paged) and cache_ttl_seconds > 0:
            now_ts = time.time()
            with _api_115_list_cache_lock:
                _api_115_list_cache[cache_key] = {
                    "entries": _clone_115_entries(entries),
                    "summary": dict(payload["summary"]),
                    "entries_complete": entries_complete,
                    "count": payload["count"],
                    "offset": payload["offset"],
                    "next_offset": payload["next_offset"],
                    "has_more": has_more,
                    "pages_scanned": pages_scanned,
                    "updated_at": now_ts,
                    "expires_at": now_ts + cache_ttl_seconds,
                }
                _prune_115_list_cache_locked(now_ts)
        mark_cookie_health_success("115", trigger="runtime:list_115_entries")
        return payload
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:list_115_entries")
        raise


def list_115_entries(
    cookie: str,
    cid: str = "0",
    force_refresh: bool = False,
    folders_only: bool = False,
) -> List[Dict[str, Any]]:
    payload = list_115_entries_payload(cookie, cid, force_refresh=force_refresh, folders_only=folders_only)
    return _clone_115_entries(payload.get("entries", []))


def search_115_entries(
    cookie: str,
    cid: str = "0",
    keyword: str = "",
    offset: int = 0,
    limit: int = 300,
    timeout: int = 45,
) -> Dict[str, Any]:
    """使用 115 官方搜索接口（webapi.115.com/files/search）在服务端搜索目录内容。

    返回结构与 ``list_115_entries_payload`` 的分页模式一致，避免“拉全量再本地过滤”。
    """
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    normalized_keyword = str(keyword or "").strip()
    if not normalized_keyword:
        raise RuntimeError("115 搜索关键词不能为空")
    target_cid = str(cid or "0").strip() or "0"
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(20, min(int(limit or 300), 1000))
    try:
        result: Dict[str, Any] = {}
        last_error: Optional[Exception] = None
        for attempt in range(0, _115_LIST_RETRY_TOTAL + 1):
            try:
                throttle_115_api_requests()
                params = {
                    "aid": 1,
                    "cid": target_cid,
                    "search_value": normalized_keyword,
                    "offset": safe_offset,
                    "limit": safe_limit,
                    "format": "json",
                }
                url = f"https://webapi.115.com/files/search?{urllib.parse.urlencode(params)}"
                result = _request_115_webapi_json(
                    url,
                    headers=_build_115_webapi_headers(cookie),
                    timeout=timeout,
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if (not _is_retryable_115_list_error(exc)) or attempt >= _115_LIST_RETRY_TOTAL:
                    raise
                time.sleep(_115_LIST_RETRY_BASE_SECONDS * (attempt + 1))
        if not bool(result.get("state", False)):
            detail = (
                str(result.get("error", "")).strip()
                or str(result.get("msg", "")).strip()
                or str(result.get("message", "")).strip()
                or "115 搜索失败"
            )
            raise RuntimeError(detail)
        raw_items = result.get("data") or []
        raw_count = len(raw_items) if isinstance(raw_items, list) else 0
        entries = [
            entry
            for entry in (_normalize_115_file_entry(item) for item in raw_items)
            if entry.get("id") and entry.get("name")
        ]
        total_count = max(0, parse_int(result.get("count") or result.get("total") or 0))
        next_offset = safe_offset + raw_count
        has_more = raw_count >= safe_limit or (total_count and next_offset < total_count)
        mark_cookie_health_success("115", trigger="runtime:search_115_entries")
        return {
            "entries": entries,
            "summary": {
                "folder_count": sum(1 for item in entries if item.get("is_dir")),
                "file_count": sum(1 for item in entries if not item.get("is_dir")),
            },
            "entries_complete": not has_more,
            "count": total_count or len(entries),
            "offset": safe_offset,
            "next_offset": next_offset,
            "has_more": has_more,
            "pages_scanned": 1,
            "search": True,
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:search_115_entries")
        raise

def create_115_folder(cookie: str, cid: str = "0", folder_name: str = "") -> Dict[str, Any]:
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")

    parent_cid = str(cid or "0").strip() or "0"
    normalized_name = str(folder_name or "").strip()
    if not normalized_name:
        raise RuntimeError("文件夹名称不能为空")
    if any(ch in normalized_name for ch in ("/", "\\")):
        raise RuntimeError("文件夹名称不能包含 / 或 \\")
    if normalized_name in (".", ".."):
        raise RuntimeError("文件夹名称不合法")
    if len(normalized_name) > 120:
        raise RuntimeError("文件夹名称过长")

    try:
        headers = {
            "Cookie": cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "Origin": "https://115.com",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }
        response = http_request_form_json(
            "https://webapi.115.com/files/add",
            {"pid": parent_cid, "cname": normalized_name},
            timeout=45,
            extra_headers=headers,
        )

        def resolve_folder_id_from_response(payload: Dict[str, Any]) -> str:
            candidates: List[str] = []
            for key in ("cid", "id", "folder_id", "file_id"):
                candidates.append(str(payload.get(key, "")).strip())
            data = payload.get("data")
            if isinstance(data, dict):
                for key in ("cid", "id", "folder_id", "file_id"):
                    candidates.append(str(data.get(key, "")).strip())
            return next((item for item in candidates if item and item != "0"), "")

        def find_existing_folder_id() -> str:
            entries = list_115_entries(cookie, parent_cid, True, folders_only=True)
            matched = next(
                (
                    entry
                    for entry in entries
                    if entry.get("is_dir") and str(entry.get("name", "")).strip() == normalized_name
                ),
                None,
            )
            return str((matched or {}).get("id", "")).strip()

        folder_id = resolve_folder_id_from_response(response if isinstance(response, dict) else {})
        success = bool((response or {}).get("state"))
        if not success and not folder_id:
            folder_id = find_existing_folder_id()

        if not success and not folder_id:
            detail = (
                str((response or {}).get("error", "")).strip()
                or str((response or {}).get("msg", "")).strip()
                or str((response or {}).get("message", "")).strip()
                or "新建 115 文件夹失败"
            )
            raise RuntimeError(detail)

        if not folder_id:
            folder_id = find_existing_folder_id()
        if not folder_id:
            raise RuntimeError("文件夹已创建，但未获取到目录 ID")
        invalidate_115_entries_cache(parent_cid)
        mark_cookie_health_success("115", trigger="runtime:create_115_folder")

        return {
            "id": folder_id,
            "name": normalized_name,
            "cid": parent_cid,
            "created": success,
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:create_115_folder")
        raise


def _validate_115_entry_name(value: str) -> str:
    normalized_name = str(value or "").strip()
    if not normalized_name:
        raise RuntimeError("文件名称不能为空")
    if any(ch in normalized_name for ch in ("/", "\\")):
        raise RuntimeError("文件名称不能包含 / 或 \\")
    if normalized_name in (".", ".."):
        raise RuntimeError("文件名称不合法")
    if len(normalized_name) > 240:
        raise RuntimeError("文件名称过长")
    return normalized_name


def _build_115_indexed_fid_payload(ids: List[str]) -> Dict[str, str]:
    return {
        f"fid[{index}]": str(entry_id).strip()
        for index, entry_id in enumerate(ids)
        if str(entry_id or "").strip()
    }


# ---------------------------------------------------------------------------
# 115 写操作（移动 / 复制 / 改名 / 删除）的排队重试与落地确认
# ---------------------------------------------------------------------------
# 115 的文件写操作是"受理 + 服务端排队执行"：接口返回 {"state": true} 只代表请求进了队列。
# 同一账号上一次任务没跑完就再发，会拿到 990019「移动[...]操作尚未执行完成，请稍后再试！」
# 这类"忙"响应（对照 p115client 的 errno 表：990005 / 990009 / 990019 / 590075 都是 EBUSY
# 语义）。一次整理里连着搬多个条目很容易撞上，必须退避重试，不能当成"搬运失败"。
_115_BUSY_ERRNOS = (990005, 990009, 990019, 590075, 51012)
_115_BUSY_ERROR_HINTS = ("尚未执行完成", "类似任务正在处理", "操作太频繁", "请稍后再试", "稍候再试")
_115_WRITE_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0)
_115_MOVE_PROGRESS_INTERVAL_SECONDS = 2.0
# 移动任务进度最多等多久：115 没有完成回调，progress 到 100 只是"服务端说跑完了"，
# 真正落地还要靠回验；超时就退回既有的 needs_reconcile 兜底，不判死。
_115_MOVE_PROGRESS_TIMEOUT_SECONDS = max(
    5.0,
    min(300.0, float(os.environ.get("API_115_MOVE_WAIT_SECONDS", 30.0) or 30.0)),
)
_115_RENAME_WAIT_SECONDS = max(
    3.0,
    min(120.0, float(os.environ.get("API_115_RENAME_WAIT_SECONDS", 12.0) or 12.0)),
)
_115_LANDING_VERIFY_ROUNDS = 3
# 逐条 get_info 回验最多查多少条：正常一轮接收夹整理只有个位数条目，
# 超大 batch（整理任务的 100 条分片）不必逐条问，避免把请求数放大。
_115_LANDING_VERIFY_MAX_ENTRIES = 50


def _extract_115_errno(payload: Dict[str, Any]) -> Optional[int]:
    for key in ("errno", "errNo", "err_no", "errcode", "errCode"):
        value = (payload or {}).get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            return int(float(text))
        except Exception:
            continue
    return None


def _is_115_busy_write_response(payload: Dict[str, Any]) -> bool:
    """115 写操作返回的"忙"响应（排队 / 限频）：可以安全退避重试。"""
    if _is_115_mutation_success(payload):
        return False
    if _extract_115_errno(payload) in _115_BUSY_ERRNOS:
        return True
    message = str(
        (payload or {}).get("error", "")
        or (payload or {}).get("msg", "")
        or (payload or {}).get("message", "")
    ).strip()
    return any(hint in message for hint in _115_BUSY_ERROR_HINTS)


def _115_write_error_detail(payload: Dict[str, Any], fallback: str) -> str:
    return (
        str((payload or {}).get("error", "")).strip()
        or str((payload or {}).get("msg", "")).strip()
        or str((payload or {}).get("message", "")).strip()
        or str((payload or {}).get("error_msg", "")).strip()
        or str((payload or {}).get("err_msg", "")).strip()
        or fallback
    )


def _generate_115_move_proid() -> str:
    """移动任务 id 由客户端生成（115 按它记 /files/move_progress）。"""
    return str(int(time.time() * 1000))


def _call_115_write_with_retry(call, *, label: str = "115 写操作") -> Dict[str, Any]:
    """执行一次 115 写操作；"忙"响应按退避重试，其余响应交给调用方判断。"""
    backoffs = (0.0,) + tuple(_115_WRITE_RETRY_BACKOFF_SECONDS)
    attempts = 0
    waited = 0.0
    response: Dict[str, Any] = {}
    for index, delay in enumerate(backoffs):
        if delay > 0:
            time.sleep(delay)
            waited += delay
        attempts += 1
        throttle_115_api_requests()
        response = call() or {}
        if _is_115_mutation_success(response) or not _is_115_busy_write_response(response):
            break
        next_delay = backoffs[index + 1] if index + 1 < len(backoffs) else 0.0
        logging.info(
            "%s 被 115 排队拒绝（errno=%s），%.1fs 后重试（第 %s 次）",
            label,
            _extract_115_errno(response),
            next_delay,
            attempts,
        )
    return {"response": response, "attempts": attempts, "retry_wait_seconds": waited}


def wait_115_move_progress(
    cookie: str,
    move_proid: str,
    *,
    timeout_seconds: Optional[float] = None,
    interval_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """轮询 ``/files/move_progress``，``progress >= 100`` 视为服务端任务跑完。

    返回 ``status``：``completed`` / ``unknown`` / ``timeout`` / ``skipped``。
    ``unknown`` 表示 115 返回 990003（任务记录查不到，通常是已结束并被清理）——
    这时不能判失败，交给落地回验决定。
    """
    normalized_cookie = str(cookie or "").strip()
    task_id = str(move_proid or "").strip()
    if not normalized_cookie or not task_id:
        return {"status": "skipped", "progress": None, "polls": 0, "waited_seconds": 0.0}
    timeout = float(
        timeout_seconds if timeout_seconds is not None else _115_MOVE_PROGRESS_TIMEOUT_SECONDS
    )
    interval = max(
        0.5,
        float(
            interval_seconds
            if interval_seconds is not None
            else _115_MOVE_PROGRESS_INTERVAL_SECONDS
        ),
    )
    started = time.monotonic()
    polls = 0
    progress: Optional[int] = None
    while True:
        throttle_115_api_requests()
        polls += 1
        try:
            response = _request_115_webapi_json(
                "https://webapi.115.com/files/move_progress?move_proid="
                + urllib.parse.quote(task_id),
                headers=_build_115_webapi_headers(normalized_cookie),
                timeout=20,
            )
        except Exception as exc:
            logging.info("115 移动进度查询失败（继续等待）：%s", str(exc)[:120])
            response = {}
        if _extract_115_errno(response) == 990003:
            return {
                "status": "unknown",
                "progress": progress,
                "polls": polls,
                "waited_seconds": time.monotonic() - started,
            }
        if _is_115_mutation_success(response):
            try:
                progress = int(float(str((response or {}).get("progress", "") or 0).strip() or 0))
            except Exception:
                progress = None
            if progress is not None and progress >= 100:
                return {
                    "status": "completed",
                    "progress": progress,
                    "polls": polls,
                    "waited_seconds": time.monotonic() - started,
                }
        if time.monotonic() - started >= timeout:
            return {
                "status": "timeout",
                "progress": progress,
                "polls": polls,
                "waited_seconds": time.monotonic() - started,
            }
        time.sleep(interval)


def _probe_115_entry_placement(cookie: str, entry_id: str) -> Dict[str, Any]:
    """直查单个条目当前所在目录（get_info 即时，不受目录列表缓存影响）。"""
    try:
        info = get_115_file_info(cookie, entry_id)
    except Exception as exc:
        return {"ok": False, "parent_id": "", "name": "", "error": str(exc)[:120]}
    return {
        "ok": True,
        "parent_id": str(info.get("parent_id", "") or "").strip(),
        "name": str(info.get("name", "") or "").strip(),
        "error": "",
    }


def _verify_115_entries_placement(
    cookie: str,
    expects: List[Dict[str, Any]],
) -> Tuple[List[str], List[str], List[str]]:
    """逐条回验：返回 (已落地, 明确还没落地, 查不出来) 的条目 ID 列表。"""
    landed: List[str] = []
    pending: List[str] = []
    unknown: List[str] = []
    for expect in expects or []:
        entry_id = str(expect.get("id", "") or "").strip()
        if not entry_id:
            continue
        target_parent = str(expect.get("parent_id", "") or "").strip()
        expected_name = str(expect.get("name", "") or "").strip()
        probe = _probe_115_entry_placement(cookie, entry_id)
        if probe.get("ok"):
            if probe.get("parent_id") == target_parent and (
                not expected_name or probe.get("name") == expected_name
            ):
                landed.append(entry_id)
            else:
                pending.append(entry_id)
            continue
        # get_info 查不到（条目已删除 / 查不了）：退回源目录确认它确实搬走了。
        source_parent = str(expect.get("source_parent_id", "") or "").strip()
        if source_parent and _verify_115_entries_removed(cookie, [entry_id], source_parent):
            landed.append(entry_id)
            continue
        unknown.append(entry_id)
    return landed, pending, unknown


def wait_115_writes_landed(
    cookie: str,
    expects: List[Dict[str, Any]],
    *,
    move_proid: str = "",
    timeout_seconds: Optional[float] = None,
    interval_seconds: Optional[float] = None,
    label: str = "115 搬运",
) -> Dict[str, Any]:
    """等 115 写操作真正落地：先看移动任务进度，再逐条回验父目录 / 名字。

    ``expects`` 每项形如 ``{"id", "parent_id", "name", "source_parent_id"}``。
    返回 ``status``：``landed``（全部确认）/ ``pending``（有条目明确还没落地）/
    ``unknown``（查不出来，不阻塞，但要记日志）。
    """
    normalized = [
        dict(item)
        for item in (expects or [])
        if isinstance(item, dict) and str(item.get("id", "") or "").strip()
    ]
    if not normalized:
        return {
            "status": "landed",
            "pending_ids": [],
            "unknown_ids": [],
            "progress_status": "skipped",
            "waited_seconds": 0.0,
            "attempts": 0,
        }
    timeout = float(
        timeout_seconds if timeout_seconds is not None else _115_MOVE_PROGRESS_TIMEOUT_SECONDS
    )
    interval = max(
        0.5,
        float(
            interval_seconds
            if interval_seconds is not None
            else _115_MOVE_PROGRESS_INTERVAL_SECONDS
        ),
    )
    started = time.monotonic()
    progress = (
        wait_115_move_progress(
            cookie,
            move_proid,
            timeout_seconds=timeout,
            interval_seconds=interval,
        )
        if str(move_proid or "").strip()
        else {"status": "skipped", "progress": None, "polls": 0, "waited_seconds": 0.0}
    )
    checkable = normalized[:_115_LANDING_VERIFY_MAX_ENTRIES]
    # 超过逐条回验上限的条目（整理任务一批 100 条）只靠服务端进度兜底：
    # progress 没跑到 100 就算「待确认」，不能默认当成功。
    overflow_ids = [
        str(item.get("id", "") or "").strip()
        for item in normalized[_115_LANDING_VERIFY_MAX_ENTRIES:]
    ]
    pending_ids: List[str] = []
    unknown_ids: List[str] = []
    rounds = 0
    while True:
        rounds += 1
        _, pending_ids, unknown_ids = _verify_115_entries_placement(cookie, checkable)
        if not pending_ids or rounds >= _115_LANDING_VERIFY_ROUNDS:
            break
        if time.monotonic() - started >= timeout:
            break
        time.sleep(interval)
    if overflow_ids and str(progress.get("status", "") or "") != "completed":
        pending_ids = list(dict.fromkeys(list(pending_ids) + overflow_ids))
    waited = time.monotonic() - started
    if pending_ids:
        status = "pending"
    elif unknown_ids:
        status = "unknown"
    else:
        status = "landed"
    if status != "landed":
        logging.info(
            "%s 落地确认未全部通过（status=%s，等待 %.1fs，进度=%s，pending=%s，unknown=%s）",
            label,
            status,
            waited,
            progress.get("status"),
            pending_ids[:5],
            unknown_ids[:5],
        )
    return {
        "status": status,
        "pending_ids": pending_ids,
        "unknown_ids": unknown_ids,
        "progress_status": progress.get("status"),
        "progress": progress.get("progress"),
        "waited_seconds": waited,
        "attempts": rounds,
    }


def rename_115_entries(cookie: str, renames: Dict[str, str], parent_cid: str = "") -> Dict[str, Any]:
    """一次请求批量重命名多个条目（官方 batch_rename 支持多个 files_new_name[id]=名字）。"""
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("115 Cookie 未配置")
    normalized_renames: Dict[str, str] = {}
    for raw_id, raw_name in (renames or {}).items():
        normalized_id = str(raw_id or "").strip()
        if not normalized_id:
            continue
        normalized_renames[normalized_id] = _validate_115_entry_name(raw_name)
    if not normalized_renames:
        raise RuntimeError("重命名条目不能为空")
    try:
        headers = {
            "Cookie": normalized_cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "Origin": "https://115.com",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }
        outcome = _call_115_write_with_retry(
            lambda: http_request_form_json(
                "https://webapi.115.com/files/batch_rename",
                {
                    f"files_new_name[{entry_id}]": name
                    for entry_id, name in normalized_renames.items()
                },
                timeout=60,
                extra_headers=headers,
            ),
            label="115 重命名",
        )
        response = outcome["response"]
        if not _is_115_mutation_success(response):
            raise RuntimeError(_115_write_error_detail(response, "115 重命名失败"))
        invalidate_115_entries_cache(parent_cid)
        mark_cookie_health_success("115", trigger="runtime:rename_115_entries")
        return {
            "renames": normalized_renames,
            "response": response,
            "attempts": outcome["attempts"],
            "retry_wait_seconds": outcome["retry_wait_seconds"],
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:rename_115_entries")
        raise


def rename_115_entry(cookie: str, entry_id: str, new_name: str, parent_cid: str = "") -> Dict[str, Any]:
    """单个条目重命名（复用官方批量重命名接口）。"""
    normalized_id = str(entry_id or "").strip()
    if not normalized_id:
        raise RuntimeError("文件 ID 不能为空")
    result = rename_115_entries(cookie, {normalized_id: new_name}, parent_cid=parent_cid)
    return {
        "id": normalized_id,
        "name": str(new_name or "").strip(),
        "response": result.get("response", {}),
    }


def move_115_entries(
    cookie: str,
    entry_ids: List[str],
    target_cid: str,
    source_cid: str = "",
    *,
    move_proid: str = "",
) -> Dict[str, Any]:
    """移动文件/目录。

    带上客户端生成的 ``move_proid``：115 会按这个 id 记 ``/files/move_progress``，
    调用方据此判断"这次移动跑完了没有"。同一个 id 跨重试复用，服务端不会重复执行。
    """
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("115 Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要移动的文件")
    target_id = str(target_cid or "0").strip() or "0"
    task_id = str(move_proid or "").strip() or _generate_115_move_proid()
    try:
        headers = {
            "Cookie": normalized_cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "Origin": "https://115.com",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }

        def _call() -> Dict[str, Any]:
            payload = {"pid": target_id, "move_proid": task_id}
            payload.update(_build_115_indexed_fid_payload(ids))
            return http_request_form_json(
                "https://webapi.115.com/files/move",
                payload,
                timeout=60,
                extra_headers=headers,
            )

        outcome = _call_115_write_with_retry(_call, label="115 移动")
        response = outcome["response"]
        if not _is_115_mutation_success(response):
            raise RuntimeError(_115_write_error_detail(response, "115 移动失败"))
        invalidate_115_entries_cache(source_cid)
        invalidate_115_entries_cache(target_id)
        mark_cookie_health_success("115", trigger="runtime:move_115_entries")
        return {
            "ids": ids,
            "target_cid": target_id,
            "response": response,
            "move_proid": task_id,
            "attempts": outcome["attempts"],
            "retry_wait_seconds": outcome["retry_wait_seconds"],
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:move_115_entries")
        raise


def copy_115_entries(cookie: str, entry_ids: List[str], target_cid: str, source_cid: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("115 Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要复制的文件")
    target_id = str(target_cid or "0").strip() or "0"
    try:
        headers = {
            "Cookie": normalized_cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "Origin": "https://115.com",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }

        def _call() -> Dict[str, Any]:
            payload = {"pid": target_id}
            payload.update(_build_115_indexed_fid_payload(ids))
            return http_request_form_json(
                "https://webapi.115.com/files/copy",
                payload,
                timeout=60,
                extra_headers=headers,
            )

        outcome = _call_115_write_with_retry(_call, label="115 复制")
        response = outcome["response"]
        if not _is_115_mutation_success(response):
            raise RuntimeError(_115_write_error_detail(response, "115 复制失败"))
        invalidate_115_entries_cache(target_id)
        mark_cookie_health_success("115", trigger="runtime:copy_115_entries")
        return {
            "ids": ids,
            "target_cid": target_id,
            "response": response,
            "attempts": outcome["attempts"],
            "retry_wait_seconds": outcome["retry_wait_seconds"],
        }
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:copy_115_entries")
        raise


def _is_115_mutation_success(response: Dict[str, Any]) -> bool:
    payload = response if isinstance(response, dict) else {}
    if bool(payload.get("state")) or bool(payload.get("success")):
        return True
    errno_value = None
    for key in ("errno", "errNo", "err_no", "errcode"):
        raw_value = payload.get(key)
        if raw_value is None:
            continue
        text = str(raw_value).strip()
        if not text:
            continue
        try:
            errno_value = int(float(text))
            break
        except Exception:
            continue
    return errno_value == 0


def _verify_115_entries_removed(cookie: str, ids: List[str], parent_cid: str) -> bool:
    """删除接口返回不明确时，用父目录列表复核条目是否真的已经不在。

    115 的删除接口在目录已被移走 / 已经删除时会返回无法识别的响应；只要父目录里
    查不到这些 ID，就按删除成功处理，避免把“其实已经清掉”误报成删除失败。
    """
    normalized_parent = str(parent_cid or "").strip()
    if not normalized_parent or not ids:
        return False
    try:
        invalidate_115_entries_cache(normalized_parent)
        entries = list_115_entries(cookie, normalized_parent, force_refresh=True)
    except Exception:
        return False
    remaining = {
        str(item.get("id") or item.get("cid") or item.get("fid") or "").strip()
        for item in entries
        if isinstance(item, dict)
    }
    return not (set(ids) & remaining)


def _request_115_delete_payload(
    cookie: str,
    ids: List[str],
    *,
    parent_cid: str = "",
    use_app_endpoint: bool = False,
) -> Dict[str, Any]:
    headers = {
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://115.com/",
        "Origin": "https://115.com",
        "User-Agent": "Mozilla/5.0 115-media-hub",
    }
    if use_app_endpoint:
        payload = {"file_ids": ",".join(ids), "user_id": ""}
        if parent_cid:
            payload["pid"] = parent_cid
        return http_request_form_json(
            "https://proapi.115.com/android/rb/delete",
            payload,
            timeout=60,
            extra_headers=headers,
        )
    payload = _build_115_indexed_fid_payload(ids)
    payload["ignore_warn"] = "1"
    if parent_cid:
        payload["pid"] = parent_cid
    return http_request_form_json(
        "https://webapi.115.com/rb/delete",
        payload,
        timeout=60,
        extra_headers=headers,
    )


def delete_115_entries(cookie: str, entry_ids: List[str], parent_cid: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("115 Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要删除的文件")
    try:
        responses = []
        last_error = ""
        last_exc = None
        for use_app_endpoint in (False, True):
            try:
                outcome = _call_115_write_with_retry(
                    lambda use_app_endpoint=use_app_endpoint: _request_115_delete_payload(
                        normalized_cookie,
                        ids,
                        parent_cid=parent_cid,
                        use_app_endpoint=use_app_endpoint,
                    ),
                    label="115 删除",
                )
                response = outcome["response"]
            except Exception as exc:
                last_exc = exc
                last_error = str(exc).strip() or "115 删除失败"
                continue
            responses.append(response)
            if _is_115_mutation_success(response):
                invalidate_115_entries_cache(parent_cid)
                mark_cookie_health_success("115", trigger="runtime:delete_115_entries")
                return {"ids": ids, "response": response}
            last_error = _115_write_error_detail(response, "115 删除失败")
        detail = last_error or "115 删除失败"
        if len(responses) > 1:
            detail = f"{detail}（webapi/proapi 均未成功）"
        # 响应无法识别时用父目录列表复核：条目已经不在就按成功处理。
        if _verify_115_entries_removed(normalized_cookie, ids, parent_cid):
            mark_cookie_health_success("115", trigger="runtime:delete_115_entries")
            return {"ids": ids, "response": {"verified_removed": True}}
        raise RuntimeError(detail) from last_exc
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:delete_115_entries")
        raise


def sanitize_115_folder_name(value: str, fallback: str = "未命名") -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]+", " ", str(value or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned or cleaned in (".", ".."):
        cleaned = fallback
    return cleaned[:120]

def ensure_115_folder_id_by_path(cookie: str, relative_path: str) -> str:
    normalized_path = normalize_relative_path(relative_path)
    if not normalized_path:
        return "0"
    current_cid = "0"
    for raw_part in [segment for segment in normalized_path.split("/") if segment]:
        part = str(raw_part or "").strip()
        if not part:
            continue
        entries = list_115_entries(cookie, current_cid, folders_only=True)
        matched = next(
            (
                entry
                for entry in entries
                if entry.get("is_dir") and str(entry.get("name", "")).strip() == part
            ),
            None,
        )
        if matched:
            current_cid = str(matched.get("id", "") or matched.get("cid", "") or "").strip() or "0"
            continue
        created = create_115_folder(cookie, current_cid, part)
        current_cid = str(created.get("id", "")).strip() or current_cid
    return current_cid

def resolve_115_folder_id_by_path(cookie: str, relative_path: str) -> str:
    normalized_path = normalize_relative_path(relative_path)
    if not normalized_path:
        return "0"

    current_cid = "0"
    walked_parts: List[str] = []
    for part in [segment for segment in normalized_path.split("/") if segment]:
        walked_parts.append(part)
        entries = list_115_entries(cookie, current_cid, folders_only=True)
        matched = next(
            (
                entry
                for entry in entries
                if entry.get("is_dir") and str(entry.get("name", "")).strip() == part
            ),
            None,
        )
        if not matched:
            raise RuntimeError(f"115 网盘目录不存在：{join_relative_path(*walked_parts)}")
        current_cid = str(matched.get("id", "") or matched.get("cid", "") or "").strip() or "0"
    return current_cid

def resolve_115_entry_by_name(cookie: str, parent_cid: str, entry_name: str) -> Dict[str, Any]:
    """按名称在 115 目录中查找文件或文件夹（自动分页，最多 80 页）。

    返回与 ``list_115_entries`` 一致的规范化条目；未找到时返回空字典。
    供按路径操作（刮削 rename/move/copy/delete 等）复用，避免大目录漏匹配。
    """
    target_name = str(entry_name or "").strip()
    if not target_name:
        return {}
    normalized_cid = str(parent_cid or "0").strip() or "0"
    max_pages = 80
    offset = 0

    def matches(entry: Dict[str, Any]) -> bool:
        return bool(entry) and str(entry.get("name", "") or "").strip() == target_name

    pages_scanned = 0
    while pages_scanned < max_pages:
        pages_scanned += 1
        throttle_115_api_requests()
        page = _request_115_entries_page_with_retry(
            cookie,
            normalized_cid,
            offset,
            _115_LIST_PAGE_LIMIT_DEFAULT,
            fast=False,
            timeout=45,
        )
        raw_items = page.get("items") or []
        if not raw_items:
            break
        for entry in raw_items:
            if matches(entry):
                return entry
        raw_count = max(0, int(page.get("raw_count", 0) or 0))
        if raw_count < _115_LIST_PAGE_LIMIT_DEFAULT:
            break
        offset += raw_count
    return {}

def list_115_folders(cookie: str, cid: str = "0") -> List[Dict[str, str]]:
    return [
        {"id": str(entry.get("id", "")).strip(), "name": str(entry.get("name", "")).strip()}
        for entry in list_115_entries(cookie, cid, folders_only=True)
        if entry.get("is_dir")
    ]

def resolve_115_share_payload(cookie: str, share_url: str, raw_text: str = "", receive_code: str = "") -> Dict[str, str]:
    parsed = parse_115_share_payload(share_url, raw_text, receive_code)
    if parsed.get("share_code"):
        return parsed
    headers = {
        "Cookie": str(cookie or "").strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://115.com/",
        "User-Agent": "Mozilla/5.0 115-media-hub",
    }
    resolved = http_resolve_url(share_url, timeout=30, extra_headers=headers)
    parsed = parse_115_share_payload(resolved, raw_text, receive_code)
    if not parsed.get("share_code"):
        raise RuntimeError("未能识别 115 分享链接")
    return parsed

def _build_115_share_cache_key(
    cache_kind: str,
    share_code: str,
    receive_code: str,
    cid: str,
    extra: str = "",
) -> str:
    source = (
        f"{str(cache_kind or 'snap').strip()}|"
        f"{str(share_code or '').strip()}|"
        f"{str(receive_code or '').strip()}|"
        f"{str(cid or '0').strip() or '0'}|"
        f"{str(extra or '').strip()}"
    )
    return hashlib.sha1(source.encode("utf-8")).hexdigest()

def _build_115_share_snap_cache_key(share_code: str, receive_code: str, cid: str) -> str:
    return _build_115_share_cache_key("snap", share_code, receive_code, cid)

def _build_115_share_page_cache_key(
    share_code: str,
    receive_code: str,
    cid: str,
    offset: int,
    limit: int,
    folders_only: bool,
) -> str:
    return _build_115_share_cache_key(
        "page",
        share_code,
        receive_code,
        cid,
        extra=f"{max(0, int(offset or 0))}|{max(20, min(400, int(limit or 200)))}|{1 if folders_only else 0}",
    )

def _throttle_115_share_snap_requests(rate_limit_seconds: float = 0.0) -> None:
    global _share_snap_last_request_monotonic
    requested_interval = float(rate_limit_seconds or 0.0)
    if requested_interval > 0:
        min_interval = max(0.0, requested_interval)
    else:
        min_interval = max(0.0, float(SHARE_SNAP_RATE_LIMIT_SECONDS or 0.0))
    if min_interval <= 0:
        return
    with _share_snap_rate_limit_lock:
        now_mono = time.monotonic()
        wait_seconds = min_interval - (now_mono - _share_snap_last_request_monotonic)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _share_snap_last_request_monotonic = time.monotonic()

def _is_retryable_115_share_snap_error(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        status_code = int(getattr(response, "status_code", 0) or 0)
        return status_code in (405, 408, 409, 425, 429, 500, 502, 503, 504)
    if isinstance(exc, requests.RequestException):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return int(getattr(exc, "code", 0) or 0) in (405, 408, 409, 425, 429, 500, 502, 503, 504)
    if isinstance(exc, urllib.error.URLError):
        return True
    message = str(exc or "").strip().lower()
    if not message:
        return False
    if any(token in message for token in ("http error 405", "http error 429", "http error 5")):
        return True
    return any(
        token in message
        for token in (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "remote end closed",
            "bad gateway",
            "service unavailable",
            "too many requests",
        )
    )

def _load_share_entries_cache_payload(cache_key: str, allow_expired: bool = False) -> Dict[str, Any]:
    normalized_cache_key = str(cache_key or "").strip()
    if not normalized_cache_key:
        return {}
    ensure_db()
    conn = open_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT payload_json, expires_at FROM share_entries_cache WHERE cache_key = ?", (normalized_cache_key,))
        row = cursor.fetchone()
        if not row:
            return {}
        expires_at = str(row["expires_at"] or "").strip()
        now_iso = now_text()
        if (not allow_expired) and expires_at and expires_at <= now_iso:
            return {}
        payload = safe_json_loads(row["payload_json"], {})
        return payload if isinstance(payload, dict) else {}
    finally:
        conn.close()

def load_115_share_snap_cache(
    share_code: str,
    receive_code: str,
    cid: str,
    allow_expired: bool = False,
) -> Dict[str, Any]:
    normalized_share_code = str(share_code or "").strip()
    if not normalized_share_code:
        return {}
    cache_key = _build_115_share_snap_cache_key(normalized_share_code, receive_code, cid)
    return _load_share_entries_cache_payload(cache_key, allow_expired=allow_expired)

def load_115_share_page_cache(
    share_code: str,
    receive_code: str,
    cid: str,
    offset: int,
    limit: int,
    folders_only: bool = False,
    allow_expired: bool = False,
) -> Dict[str, Any]:
    normalized_share_code = str(share_code or "").strip()
    if not normalized_share_code:
        return {}
    cache_key = _build_115_share_page_cache_key(normalized_share_code, receive_code, cid, offset, limit, folders_only)
    return _load_share_entries_cache_payload(cache_key, allow_expired=allow_expired)

def _save_share_entries_cache_payload(
    cache_key: str,
    share_code: str,
    receive_code: str,
    cid: str,
    payload: Dict[str, Any],
    ttl_seconds: int = SHARE_SNAP_CACHE_TTL_SECONDS,
) -> None:
    normalized_cache_key = str(cache_key or "").strip()
    normalized_share_code = str(share_code or "").strip()
    if not normalized_cache_key or not normalized_share_code or not isinstance(payload, dict):
        return
    now_iso = now_text()
    expires_at = (datetime.now() + timedelta(seconds=max(1, int(ttl_seconds or SHARE_SNAP_CACHE_TTL_SECONDS)))).isoformat(
        timespec="seconds"
    )
    ensure_db()
    conn = open_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO share_entries_cache(
                cache_key, share_code, receive_code, cid, payload_json, created_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_cache_key,
                normalized_share_code,
                str(receive_code or "").strip(),
                str(cid or "0").strip() or "0",
                safe_json_dumps(payload),
                now_iso,
                expires_at,
            ),
        )
        cursor.execute("DELETE FROM share_entries_cache WHERE expires_at <> '' AND expires_at <= ?", (now_iso,))
        max_rows = max(200, int(SHARE_SNAP_CACHE_MAX_ROWS or 3000))
        cursor.execute("SELECT COUNT(1) FROM share_entries_cache")
        total_rows = int(cursor.fetchone()[0] or 0)
        if total_rows > max_rows:
            overflow = total_rows - max_rows
            cursor.execute(
                "SELECT cache_key FROM share_entries_cache ORDER BY created_at ASC LIMIT ?",
                (overflow,),
            )
            stale_keys = [str(row["cache_key"] or "").strip() for row in cursor.fetchall() if str(row["cache_key"] or "").strip()]
            if stale_keys:
                placeholders = ",".join(["?"] * len(stale_keys))
                cursor.execute(f"DELETE FROM share_entries_cache WHERE cache_key IN ({placeholders})", tuple(stale_keys))
        conn.commit()
    finally:
        conn.close()

def save_115_share_snap_cache(
    share_code: str,
    receive_code: str,
    cid: str,
    payload: Dict[str, Any],
    ttl_seconds: int = SHARE_SNAP_CACHE_TTL_SECONDS,
) -> None:
    normalized_share_code = str(share_code or "").strip()
    if not normalized_share_code or not isinstance(payload, dict):
        return
    cache_key = _build_115_share_snap_cache_key(normalized_share_code, receive_code, cid)
    _save_share_entries_cache_payload(cache_key, normalized_share_code, receive_code, cid, payload, ttl_seconds=ttl_seconds)

def save_115_share_page_cache(
    share_code: str,
    receive_code: str,
    cid: str,
    offset: int,
    limit: int,
    folders_only: bool,
    payload: Dict[str, Any],
    ttl_seconds: int = SHARE_SNAP_CACHE_TTL_SECONDS,
) -> None:
    normalized_share_code = str(share_code or "").strip()
    if not normalized_share_code or not isinstance(payload, dict):
        return
    cache_key = _build_115_share_page_cache_key(normalized_share_code, receive_code, cid, offset, limit, folders_only)
    _save_share_entries_cache_payload(cache_key, normalized_share_code, receive_code, cid, payload, ttl_seconds=ttl_seconds)

def _slice_115_share_cache_payload(
    payload: Dict[str, Any],
    offset: int = 0,
    limit: int = 200,
    folders_only: bool = False,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    source_entries = payload.get("entries", [])
    if not isinstance(source_entries, list):
        return {}
    start_offset = max(0, int(offset or 0))
    page_limit = max(20, min(400, int(limit or 200)))
    filtered_entries = [entry for entry in source_entries if (not folders_only) or bool((entry or {}).get("is_dir"))]
    page_entries = filtered_entries[start_offset:start_offset + page_limit]
    filtered_total = len(filtered_entries)
    next_offset = start_offset + len(page_entries)
    return {
        "entries": page_entries,
        "summary": {
            "folder_count": sum(1 for item in page_entries if (item or {}).get("is_dir")),
            "file_count": sum(1 for item in page_entries if not (item or {}).get("is_dir")),
        },
        "share_code": str(payload.get("share_code", "") or "").strip(),
        "receive_code": str(payload.get("receive_code", "") or "").strip(),
        "share_title": str(payload.get("share_title", "") or "").strip(),
        "current_cid": str(payload.get("current_cid", "") or "0").strip() or "0",
        "count": filtered_total if folders_only else max(filtered_total, int(payload.get("count", filtered_total) or filtered_total)),
        "offset": start_offset,
        "next_offset": next_offset,
        "has_more": next_offset < filtered_total,
        "pages_scanned": int(payload.get("pages_scanned", 0) or 0),
        "cache_derived": True,
        "elapsed_ms": int(payload.get("elapsed_ms", 0) or 0),
        "timings": payload.get("timings", []) if isinstance(payload.get("timings"), list) else [],
    }

def list_115_share_entries(
    cookie: str,
    share_url: str,
    raw_text: str = "",
    cid: str = "0",
    receive_code: str = "",
    force_refresh: bool = False,
    request_timeout: int = 45,
    rate_limit_seconds: float = 0.0,
    max_request_retries: int = 2,
    offset: int = 0,
    limit: int = 200,
    max_pages: int = 0,
    folders_only: bool = False,
) -> Dict[str, Any]:
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    try:
        started_mono = time.monotonic()
        last_mono = started_mono
        timings: List[Dict[str, Any]] = []
        parsed = resolve_115_share_payload(cookie, share_url, raw_text, receive_code)
        elapsed_ms, last_mono = _build_115_timing_mark(last_mono)
        timings.append({"stage": "parse", "label": "链接", "ms": elapsed_ms})
        share_code = str(parsed.get("share_code", "") or "").strip()
        receive_code = str(parsed.get("receive_code", "") or "").strip()
        current_cid = str(cid or "0").strip() or "0"
        start_offset = max(0, int(offset or 0))
        page_limit = max(20, min(400, int(limit or 200)))
        max_pages_limit = max(0, int(max_pages or 0))
        folder_only_mode = bool(folders_only)
        use_full_cache = start_offset == 0 and max_pages_limit <= 0 and not folder_only_mode

        def load_stale_cache_payload() -> Dict[str, Any]:
            if use_full_cache:
                stale_full_cache = load_115_share_snap_cache(share_code, receive_code, current_cid, allow_expired=True)
                if stale_full_cache:
                    return dict(stale_full_cache)
            stale_page_cache = load_115_share_page_cache(
                share_code,
                receive_code,
                current_cid,
                start_offset,
                page_limit,
                folder_only_mode,
                allow_expired=True,
            )
            if stale_page_cache:
                return dict(stale_page_cache)
            stale_full_cache = load_115_share_snap_cache(share_code, receive_code, current_cid, allow_expired=True)
            if stale_full_cache:
                return _slice_115_share_cache_payload(
                    stale_full_cache,
                    offset=start_offset,
                    limit=page_limit,
                    folders_only=folder_only_mode,
                )
            return {}

        if not force_refresh:
            if use_full_cache:
                fresh_full_cache = load_115_share_snap_cache(share_code, receive_code, current_cid, allow_expired=False)
                if fresh_full_cache:
                    return fresh_full_cache
            else:
                fresh_page_cache = load_115_share_page_cache(
                    share_code,
                    receive_code,
                    current_cid,
                    start_offset,
                    page_limit,
                    folder_only_mode,
                    allow_expired=False,
                )
                if fresh_page_cache:
                    return fresh_page_cache
                fresh_full_cache = load_115_share_snap_cache(share_code, receive_code, current_cid, allow_expired=False)
                if fresh_full_cache:
                    sliced_payload = _slice_115_share_cache_payload(
                        fresh_full_cache,
                        offset=start_offset,
                        limit=page_limit,
                        folders_only=folder_only_mode,
                    )
                    if sliced_payload:
                        return sliced_payload

        request_timeout_value = max(5, int(request_timeout or 45))
        retry_total = max(0, int(max_request_retries or 0))

        headers = _build_115_webapi_headers(cookie)
        entries: List[Dict[str, Any]] = []
        offset_cursor = start_offset
        total_count = 0
        pages_scanned = 0

        while True:
            query = urllib.parse.urlencode(
                {
                    "share_code": share_code,
                    "receive_code": receive_code,
                    "cid": current_cid,
                    "offset": offset_cursor,
                    "limit": page_limit,
                    "asc": 1,
                    "o": "file_name",
                    "format": "json",
                }
            )
            result: Dict[str, Any] = {}
            last_request_error: Optional[Exception] = None
            for attempt in range(0, retry_total + 1):
                try:
                    request_started_mono = time.monotonic()
                    _throttle_115_share_snap_requests(rate_limit_seconds=rate_limit_seconds)
                    result = _request_115_webapi_json(
                        f"https://webapi.115.com/share/snap?{query}",
                        headers=headers,
                        timeout=request_timeout_value,
                    )
                    elapsed_ms = int((time.monotonic() - request_started_mono) * 1000)
                    last_mono = time.monotonic()
                    timings.append({"stage": f"snap_page_{pages_scanned + 1}", "label": f"目录P{pages_scanned + 1}", "ms": elapsed_ms})
                    last_request_error = None
                    break
                except Exception as exc:
                    last_request_error = exc
                    if (not _is_retryable_115_share_snap_error(exc)) or attempt >= retry_total:
                        break
                    time.sleep(0.6 * (attempt + 1))
            if last_request_error is not None:
                stale_payload = load_stale_cache_payload()
                if stale_payload:
                    stale_payload["cache_stale"] = True
                    stale_payload["cache_error"] = str(last_request_error or "").strip()[:180]
                    stale_payload["cache_cid"] = current_cid
                    return stale_payload
                raise RuntimeError(str(last_request_error or "读取 115 分享内容失败").strip() or "读取 115 分享内容失败")
            payload = result.get("data") if isinstance(result, dict) else {}
            if payload is None:
                payload = {}
            batch = payload.get("list") or []
            if not batch and not payload.get("shareinfo") and not bool(result.get("state", False)):
                detail = (
                    str(result.get("error", "")).strip()
                    or str(result.get("msg", "")).strip()
                    or str(result.get("message", "")).strip()
                    or "读取 115 分享内容失败"
                )
                stale_payload = load_stale_cache_payload()
                if stale_payload:
                    stale_payload["cache_stale"] = True
                    stale_payload["cache_error"] = detail[:180]
                    stale_payload["cache_cid"] = current_cid
                    return stale_payload
                raise RuntimeError(detail)

            total_count = parse_int(payload.get("count") or total_count)
            for item in batch:
                fid = str(item.get("fid") or "").strip()
                dir_cid = str(item.get("cid") or "").strip()
                is_dir = not fid
                entry_id = dir_cid if is_dir else fid
                name = str(item.get("n") or item.get("name") or "").strip()
                if not entry_id or not name:
                    continue
                if folder_only_mode and not is_dir:
                    continue
                entries.append(
                    {
                        "id": entry_id,
                        "name": name,
                        "is_dir": is_dir,
                        "parent_id": current_cid,
                        "cid": dir_cid if is_dir else "",
                        "fid": fid if not is_dir else "",
                        "size": parse_int(item.get("s") or item.get("size") or 0),
                        "pick_code": str(item.get("pick_code") or item.get("pc") or "").strip(),
                        "sha1": str(item.get("sha1") or item.get("sha") or "").strip(),
                        "icon": str(item.get("ico") or "").strip(),
                        "modified_at": str(item.get("t") or item.get("te") or item.get("tp") or "").strip(),
                    }
                )

            pages_scanned += 1
            next_offset = offset_cursor + len(batch)
            reached_end = not batch or len(batch) < page_limit or (total_count and next_offset >= total_count)
            reached_page_cap = max_pages_limit > 0 and pages_scanned >= max_pages_limit

            if reached_end or reached_page_cap:
                shareinfo = payload.get("shareinfo") or {}
                entries.sort(key=lambda item: (0 if item.get("is_dir") else 1, str(item.get("name", "")).lower()))
                result_payload = {
                    "entries": entries,
                    "summary": {
                        "folder_count": sum(1 for item in entries if item.get("is_dir")),
                        "file_count": sum(1 for item in entries if not item.get("is_dir")),
                    },
                    "share_code": share_code,
                    "receive_code": receive_code,
                    "share_title": str(shareinfo.get("share_title") or "").strip(),
                    "current_cid": current_cid,
                    "count": total_count or len(entries),
                    "offset": start_offset,
                    "next_offset": next_offset,
                    "has_more": not reached_end,
                    "pages_scanned": pages_scanned,
                    "elapsed_ms": int((time.monotonic() - started_mono) * 1000),
                    "timings": timings,
                }
                _log_115_share_timing(share_code, current_cid, timings, int(result_payload.get("elapsed_ms", 0) or 0))
                if use_full_cache or (start_offset == 0 and reached_end and not folder_only_mode):
                    save_115_share_snap_cache(
                        share_code,
                        receive_code,
                        current_cid,
                        result_payload,
                        ttl_seconds=SHARE_SNAP_CACHE_TTL_SECONDS,
                    )
                else:
                    save_115_share_page_cache(
                        share_code,
                        receive_code,
                        current_cid,
                        start_offset,
                        page_limit,
                        folder_only_mode,
                        result_payload,
                        ttl_seconds=SHARE_SNAP_CACHE_TTL_SECONDS,
                    )
                return result_payload
            offset_cursor = next_offset
    except Exception:
        raise

def prepare_115_share_receive(
    cookie: str,
    share_url: str,
    raw_text: str = "",
    selected_ids: Optional[List[str]] = None,
    receive_code: str = "",
) -> Dict[str, Any]:
    parsed = resolve_115_share_payload(cookie, share_url, raw_text, receive_code)
    normalized_ids: List[str] = []
    seen_ids: Set[str] = set()
    for raw_id in selected_ids or []:
        entry_id = str(raw_id or "").strip()
        if not entry_id or entry_id in seen_ids:
            continue
        seen_ids.add(entry_id)
        normalized_ids.append(entry_id)

    selection: Dict[str, Any] = {}
    if not normalized_ids:
        snapshot = list_115_share_entries(
            cookie,
            parsed.get("url", share_url),
            raw_text,
            "0",
            str(parsed.get("receive_code", "") or ""),
        )
        normalized_ids = [str(entry.get("id", "")).strip() for entry in snapshot.get("entries", []) if str(entry.get("id", "")).strip()]
        selection = normalize_share_selection_meta(
            {
                "selected_ids": normalized_ids,
                "selected_entries": snapshot.get("entries", []),
                "share_root_title": snapshot.get("share_title", ""),
            }
        )
    if not normalized_ids:
        raise RuntimeError("分享内容为空，无法转存")
    return {
        "share_code": str(parsed.get("share_code", "")).strip(),
        "receive_code": str(parsed.get("receive_code", "")).strip(),
        "file_id": ",".join(normalized_ids),
        "selection": selection,
    }

def is_115_share_receive_duplicate_response(response: Any) -> bool:
    payload = response if isinstance(response, dict) else {}
    errno = parse_int(payload.get("errno") or payload.get("errNo") or payload.get("err_no"), default=0)
    if errno == 4100024:
        return True

    message = " ".join(
        [
            str(payload.get("error", "") or "").strip(),
            str(payload.get("msg", "") or "").strip(),
            str(payload.get("message", "") or "").strip(),
            str(payload.get("error_msg", "") or "").strip(),
        ]
    ).strip()
    if not message:
        return False
    normalized_message = message.lower()
    duplicate_hints = (
        "文件已接收",
        "无需重复接收",
        "已接收，无需重复接收",
        "already received",
        "already saved",
        "duplicate receive",
    )
    return any(hint.lower() in normalized_message for hint in duplicate_hints)

def submit_115_share_receive(
    cookie: str,
    share_url: str,
    folder_id: str,
    raw_text: str = "",
    selected_ids: Optional[List[str]] = None,
    receive_code: str = "",
) -> Dict[str, Any]:
    cookie = str(cookie or "").strip()
    if not cookie:
        raise RuntimeError("115 Cookie 未配置")
    try:
        prepared = prepare_115_share_receive(cookie, share_url, raw_text, selected_ids, receive_code)

        headers = {
            "Cookie": cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
            "Origin": "https://115.com",
            "User-Agent": "Mozilla/5.0 115-media-hub",
        }
        payload = {
            "share_code": prepared.get("share_code", ""),
            "receive_code": prepared.get("receive_code", ""),
            "file_id": prepared.get("file_id", ""),
            "cid": folder_id or "0",
            "is_check": 0,
        }
        response = http_request_form_json(
            "https://115cdn.com/webapi/share/receive",
            payload,
            timeout=45,
            extra_headers=headers,
        )
        success = bool(response.get("state")) or is_115_share_receive_duplicate_response(response)
        if not success:
            detail = (
                str(response.get("error", "")).strip()
                or str(response.get("msg", "")).strip()
                or str(response.get("message", "")).strip()
                or "115 网盘转存失败"
            )
            raise RuntimeError(detail)
        return {
            "response": response,
            "selection": prepared.get("selection", {}),
            "duplicate_receive": is_115_share_receive_duplicate_response(response),
        }
    except Exception:
        raise


class Pan115Provider(CloudProvider):
    name = "115"
    label = "115网盘"
    link_type = "115share"
    auth_type = "cookie"
    config_keys = ["cookie_115"]
    supports_subscription = True
    supports_offline = True
    supports_fixed_share_link = True
    supports_strm = True
    supports_monitor = True
    supports_rename = True
    supports_move = True
    supports_copy = True
    supports_delete = True
    supports_upload = True
    supports_rapid_upload = True
    supports_upload_resume = True
    rate_limit_seconds = 0.35

    def list_entries_payload(self, cookie, cid="0", folders_only=False, force_refresh=False):
        return list_115_entries_payload(cookie, cid, force_refresh, folders_only)

    def list_entries(self, cookie, cid="0"):
        return list_115_entries(cookie, cid)

    def create_folder(self, cookie, cid="0", folder_name=""):
        return create_115_folder(cookie, cid, folder_name)

    def resolve_folder_id_by_path(self, cookie, relative_path):
        return resolve_115_folder_id_by_path(cookie, relative_path)

    def ensure_folder_id_by_path(self, cookie, relative_path):
        return ensure_115_folder_id_by_path(cookie, relative_path)

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        payload = resolve_115_share_payload(cookie, share_url, raw_text, receive_code)
        payload["raw_text"] = str(raw_text or "")
        return payload

    def list_share_entries(self, cookie, share_payload, cid="0", offset=0, limit=200):
        payload = share_payload if isinstance(share_payload, dict) else {}
        return list_115_share_entries(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("raw_text", "") or ""),
            cid,
            str(payload.get("receive_code", "") or "").strip(),
            False,
            45,
            0,
            1,
            max(0, int(offset or 0)),
            max(20, min(int(limit or 200), 400)),
            1,
            False,
        )

    def prepare_share_receive(self, cookie, share_payload, cid="0"):
        payload = share_payload if isinstance(share_payload, dict) else {}
        selected_ids = payload.get("selected_ids", []) if isinstance(payload.get("selected_ids"), list) else []
        prepared = prepare_115_share_receive(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("raw_text", "") or ""),
            selected_ids,
            str(payload.get("receive_code", "") or "").strip(),
        )
        return {**payload, **prepared, "target_cid": str(cid or "0").strip() or "0"}

    def submit_share_receive(self, cookie, receive_payload, files):
        payload = receive_payload if isinstance(receive_payload, dict) else {}
        selected_ids = payload.get("selected_ids", []) if isinstance(payload.get("selected_ids"), list) else []
        if not selected_ids:
            selected_ids = [
                str(item.get("id", "")).strip()
                for item in (files or [])
                if isinstance(item, dict) and str(item.get("id", "")).strip()
            ]
        return submit_115_share_receive(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("target_cid", "") or payload.get("folder_id", "") or "0").strip() or "0",
            str(payload.get("raw_text", "") or ""),
            selected_ids,
            str(payload.get("receive_code", "") or "").strip(),
        )

    def submit_offline_task(self, cookie, resource_url, folder_id="0"):
        return submit_115_offline_task(cookie, resource_url, folder_id)

    def query_offline_tasks(self, cookie, page=1):
        return list_115_offline_tasks(cookie, page)

    def probe_connectivity(self, cookie):
        try:
            list_115_entries_payload(cookie, "0", folders_only=True, max_pages=1)
            return True
        except Exception:
            return False

    def resolve_download_url(self, cookie, file_id):
        """取 115 下载直链；传入 pick_code 时先换成真实 fid。"""
        return str(resolve_115_download_info(cookie, file_id).get("url", "") or "")

    def resolve_download_info(self, cookie, file_id):
        """下载直链 + 元信息（复制引擎用：URL 过期时重取、校验大小）。"""
        return resolve_115_download_info(cookie, file_id)

    def get_file_hash(self, cookie, file_id):
        """取文件的 SHA1 + 大小（跨盘复制零流量秒传探测用；不下载文件）。"""
        return get_115_file_hash(cookie, file_id)

    def rename_entry(self, cookie, entry_id, new_name, parent_id=""):
        return rename_115_entry(cookie, entry_id, new_name, parent_id)

    def move_entries(self, cookie, entry_ids, target_id, source_id=""):
        return move_115_entries(cookie, entry_ids, target_id, source_id)

    def copy_entries(self, cookie, entry_ids, target_id, source_id=""):
        return copy_115_entries(cookie, entry_ids, target_id, source_id)

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        return delete_115_entries(cookie, entry_ids, parent_id)

    def upload_file(
        self,
        cookie,
        sink,
        filename,
        target_dir_id="0",
        *,
        sha1="",
        md5="",
        size=-1,
        progress_cb=None,
        cancel_check=None,
        **kwargs,
    ):
        return upload_115_file(
            cookie,
            sink,
            filename,
            target_dir_id,
            sha1=sha1,
            md5=md5,
            size=size,
            progress_cb=progress_cb,
            cancel_check=cancel_check,
            **kwargs,
        )

    def abort_upload(self, cookie, upload_handle):
        return abort_115_upload(cookie, upload_handle)


# ============================================================
# 上传 / 秒传（P0-A 跨盘复制地基）
#
# 链路：uplb 4.0/initupload（秒传探测，ECDH+AES 加密载荷）
#   ├─ status=2 秒传命中 → 零上传流量，直接拿到 pickcode
#   ├─ status=7 二次验证 → 读文件指定字节段算 SHA1 后重试
#   └─ status=1 未命中   → gettoken 拿 OSS STS → 阿里云 OSS 上传
#         （小文件单次 PUT，大文件分片 + 断点续传）
# ============================================================

_115_UPLOAD_APP_VERSION = "36.2.28"
_115_UPLOAD_DEFAULT_BASE_URL = "https://uplb.115.com"
_115_OSS_DEFAULT_BUCKET = "fhnfile"
_115_OSS_DEFAULT_ENDPOINT = "https://oss-cn-shenzhen.aliyuncs.com"
_115_OSS_MIN_PART_SIZE = 100 * 1024
_115_UPLOAD_USER_INFO_TTL_SECONDS = 6 * 3600
_115_UPLOAD_RETRY_BASE_SECONDS = 2.0
_115_UPLOAD_RETRY_MAX_ROUNDS = 4

_115_upload_user_info_cache: Dict[str, Any] = {}
_115_upload_base_url_cache: Dict[str, Tuple[float, str]] = {}
_115_upload_user_info_lock = threading.Lock()

_UPLOAD_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
)


class _UploadRateLimited(Exception):
    """上传接口返回 405/429，需要退避重试。"""


def get_115_user_upload_info(cookie: str) -> Tuple[int, str]:
    """获取 115 上传必需的 userid 与 userkey（进程内缓存 6 小时）。

    接口：https://proapi.115.com/app/uploadinfo （只需 Cookie）
    实测响应把 user_id/userkey/url_upload 等放在**顶层**（部分旧账号形态在 data 下，
    两种都兼容）。
    """
    cookie = str(cookie or "").strip()
    if not cookie:
        raise UploadError("115 Cookie 未配置", provider="115")
    now = time.monotonic()
    with _115_upload_user_info_lock:
        cached = _115_upload_user_info_cache.get(cookie)
        if cached and cached[0] > now:
            return int(cached[1]), str(cached[2])
    try:
        payload = http_request_json(
            "https://proapi.115.com/app/uploadinfo",
            extra_headers={
                "Cookie": cookie,
                "User-Agent": _UPLOAD_BROWSER_UA,
                "Referer": "https://115.com/",
            },
            timeout=20,
        )
    except Exception as exc:
        mark_cookie_health_failure("115", exc, trigger="runtime:get_115_user_upload_info")
        raise UploadError("115 上传信息获取失败（网络错误）", retriable=True, provider="115") from exc
    data = payload if isinstance(payload, dict) else {}
    nested = data.get("data") if isinstance(data.get("data"), dict) else {}
    userid = int(data.get("user_id") or data.get("userid") or nested.get("user_id") or nested.get("userid") or 0)
    userkey = str(
        data.get("userkey") or data.get("user_key") or nested.get("userkey") or nested.get("user_key") or ""
    ).strip()
    if not userid or not userkey:
        mark_cookie_health_failure("115", payload, trigger="runtime:get_115_user_upload_info")
        raise UploadError("115 登录已失效，请更新 Cookie 后重试", provider="115")
    base_url = str(data.get("url_upload") or nested.get("url_upload") or "").strip().rstrip("/")
    if not base_url.startswith("https://"):
        base_url = _115_UPLOAD_DEFAULT_BASE_URL
    with _115_upload_user_info_lock:
        _115_upload_user_info_cache[cookie] = (
            now + _115_UPLOAD_USER_INFO_TTL_SECONDS,
            userid,
            userkey,
        )
        _115_upload_base_url_cache[cookie] = (now + _115_UPLOAD_USER_INFO_TTL_SECONDS, base_url)
    return userid, userkey


def _get_115_upload_base_url(cookie: str) -> str:
    """上传基站 URL：取 uploadinfo 返回 url_upload 的 origin（协议+主机），
    兜底内置默认值。

    注意：url_upload 是 legacy 2.0 上传端点的**完整地址**（实测为
    https://uplb.115.com/2.0/upload.php），不能直接当路径前缀拼接
    /4.0/initupload.php——那样会产出 .../2.0/upload.php/4.0/initupload.php
    的畸形 URL，服务端回 414「客户端版本太低」。参考实现
    （p115client/p115oss）的 initupload/gettoken 固定在
    https://uplb.115.com 根路径下；这里保留 origin 提取仅为兼容
    极端情况下上传主机变更，路径段一律丢弃。
    """
    cookie = str(cookie or "").strip()
    now = time.monotonic()
    with _115_upload_user_info_lock:
        cached = _115_upload_base_url_cache.get(cookie)
        if cached and cached[0] > now and cached[1]:
            raw = str(cached[1]).strip()
            try:
                from urllib.parse import urlsplit

                parsed = urlsplit(raw)
                if parsed.scheme == "https" and parsed.hostname:
                    return f"{parsed.scheme}://{parsed.hostname}"
            except Exception:
                pass
            return _115_UPLOAD_DEFAULT_BASE_URL
    return _115_UPLOAD_DEFAULT_BASE_URL


def _115_upload_resp_fields(resp: Dict[str, Any]) -> Dict[str, Any]:
    """把 uplb initupload 响应拍平，便于统一读取字段。

    cookie 模式（POST uplb.115.com/4.0/initupload.php）的响应是**扁平结构**：
    status/pickcode/bucket/object/callback/sign_key/sign_check 全部在顶层
    （参考实现 p115oss.upload_file_init 对非 open 客户端直接读顶层，
    ``data = resp`` 不做解包）。这里顶层优先，仅在缺失时回退到 data 子对象
    （兼容 open 接口形态与历史 mock）。
    """
    flat = dict(resp)
    nested = resp.get("data")
    if isinstance(nested, dict):
        for key in (
            "status", "pickcode", "pick_code", "bucket", "object",
            "callback", "sign_key", "sign_check", "statuscode", "statusmsg",
        ):
            flat.setdefault(key, nested.get(key))
    return flat


def _upload_115_initupload(
    cookie: str,
    userid: int,
    userkey: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """调用 uplb 4.0/initupload.php（加密请求，解密响应）。"""
    request_payload = {
        "userid": userid,
        "userkey": userkey,
        "appversion": _115_UPLOAD_APP_VERSION,
        "appid": 0,
        "topupload": "true",
        **payload,
    }
    packed = make_upload_payload(request_payload)
    session = _get_115_webapi_session()
    response = session.post(
        f"{_get_115_upload_base_url(cookie)}/4.0/initupload.php",
        params=packed["params"],
        data=packed["data"],
        headers={
            "Cookie": cookie,
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": (
                f"Mozilla/5.0 115disk/{_115_UPLOAD_APP_VERSION} "
                f"115Browser/{_115_UPLOAD_APP_VERSION} "
                f"115wangpan_android/{_115_UPLOAD_APP_VERSION}"
            ),
            "Referer": "https://115.com/",
        },
        timeout=60,
    )
    if response.status_code in (405, 429):
        raise _UploadRateLimited(f"115 上传接口限流（HTTP {response.status_code}）")
    response.raise_for_status()
    try:
        result = decode_upload_response(response.content)
    except ValueError as exc:
        raise UploadError("115 上传响应解密失败，请稍后重试", retriable=True, provider="115") from exc
    return result


def _upload_115_get_token(cookie: str) -> Dict[str, Any]:
    """获取 OSS STS 临时凭证（gettoken.php）。"""
    response = _get_115_webapi_session().get(
        f"{_get_115_upload_base_url(cookie)}/3.0/gettoken.php",
        headers={
            "Cookie": cookie,
            "User-Agent": (
                f"Mozilla/5.0 115disk/{_115_UPLOAD_APP_VERSION} "
                f"115Browser/{_115_UPLOAD_APP_VERSION} "
                f"115wangpan_android/{_115_UPLOAD_APP_VERSION}"
            ),
            "Referer": "https://115.com/",
        },
        timeout=30,
    )
    if response.status_code in (405, 429):
        raise _UploadRateLimited(f"115 token 接口限流（HTTP {response.status_code}）")
    response.raise_for_status()
    token = response.json()
    if not isinstance(token, dict) or not token.get("AccessKeyId"):
        raise UploadError("115 上传凭证获取失败", retriable=True, provider="115")
    return token


def _oss_sign(token: Dict[str, Any], method: str, url: str, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """OSS V1（HMAC-SHA1）签名，返回完整请求头。"""
    from email.utils import formatdate
    from urllib.parse import parse_qsl, urlencode as _urlencode, urlsplit

    urlp = urlsplit(url)
    bucket = (urlp.hostname or "").partition(".")[0]
    headers = {str(k).lower(): str(v) for k, v in (extra_headers or {}).items()}
    headers["x-oss-security-token"] = str(token["SecurityToken"])
    date = headers["date"] = formatdate(usegmt=True)
    path = urlp.path or "/"
    sub_keys = (
        "uploads", "sequential", "partNumber", "uploadId", "x-oss-process",
    )
    query = urlp.query or ""
    if query:
        query = _urlencode(sorted((k, v) for k, v in parse_qsl(query) if k in sub_keys))
    if query:
        query = "?" + query
    canonical = (
        f"{method.upper()}\n"
        f"{headers.get('content-md5', '')}\n"
        f"{headers.get('content-type', '')}\n"
        f"{date}\n"
        + "\n".join(f"{k}:{v}" for k, v in sorted(headers.items()) if k.startswith("x-oss-"))
        + f"\n/{bucket}{path}{query}"
    )
    signature = base64.b64encode(
        hmac.new(str(token["AccessKeySecret"]).encode("utf-8"), canonical.encode("utf-8"), "sha1").digest()
    ).decode("ascii")
    headers["authorization"] = f"OSS {token['AccessKeyId']}:{signature}"
    return headers


def _oss_request(
    token: Dict[str, Any],
    method: str,
    url: str,
    *,
    data=None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 120,
) -> requests.Response:
    """签名后直连阿里云 OSS。"""
    signed = _oss_sign(token, method, url, headers or {})
    return _get_115_webapi_session().request(
        method,
        url,
        data=data,
        headers=signed,
        timeout=timeout,
    )


def _oss_object_url(bucket: str, endpoint: str, key: str) -> str:
    host = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    scheme = "https" if endpoint.startswith("https") else "http"
    return f"{scheme}://{bucket}.{host}/{key}"


def _oss_upload_whole(
    token: Dict[str, Any],
    bucket: str,
    endpoint: str,
    key: str,
    data: bytes,
    callback: Dict[str, Any],
) -> None:
    """小文件单次 PUT（≤ 分片大小）。"""
    url = _oss_object_url(bucket, endpoint, key)
    headers = {
        "x-oss-callback": base64.b64encode(str(callback.get("callback", "")).encode("utf-8")).decode("ascii"),
        "x-oss-callback-var": base64.b64encode(str(callback.get("callback_var", "")).encode("utf-8")).decode("ascii"),
    }
    response = _oss_request(token, "PUT", url, data=data, headers=headers)
    if response.status_code >= 300:
        raise UploadError(
            f"115 上传失败（OSS HTTP {response.status_code}）", retriable=True, provider="115"
        )


def _oss_multipart_init(token: Dict[str, Any], bucket: str, endpoint: str, key: str) -> str:
    """初始化分片上传，返回 uploadId。"""
    url = _oss_object_url(bucket, endpoint, key)
    url = f"{url}?sequential=1&uploads=1"
    response = _oss_request(token, "POST", url)
    if response.status_code >= 300:
        raise UploadError(
            f"115 分片上传初始化失败（OSS HTTP {response.status_code}）", retriable=True, provider="115"
        )
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(response.content)
        upload_id = (root.findtext("UploadId") or "").strip()
    except ET.ParseError as exc:
        raise UploadError("115 分片上传初始化响应解析失败", retriable=True, provider="115") from exc
    if not upload_id:
        raise UploadError("115 分片上传初始化未返回 UploadId", retriable=True, provider="115")
    return upload_id


def _oss_upload_part(
    token: Dict[str, Any],
    bucket: str,
    endpoint: str,
    key: str,
    upload_id: str,
    part_number: int,
    data: bytes,
) -> str:
    """上传单个分片，返回 ETag。"""
    url = f"{_oss_object_url(bucket, endpoint, key)}?partNumber={part_number}&uploadId={upload_id}"
    response = _oss_request(token, "PUT", url, data=data)
    if response.status_code >= 300:
        raise _UploadRateLimited(
            f"115 分片 {part_number} 上传失败（OSS HTTP {response.status_code}）"
        ) if response.status_code in (405, 429) else UploadError(
            f"115 分片 {part_number} 上传失败（OSS HTTP {response.status_code}）",
            retriable=True,
            provider="115",
        )
    etag = (response.headers.get("ETag") or "").strip().strip('"')
    if not etag:
        raise UploadError(f"115 分片 {part_number} 上传未返回 ETag", retriable=True, provider="115")
    return etag


def _oss_multipart_complete(
    token: Dict[str, Any],
    bucket: str,
    endpoint: str,
    key: str,
    upload_id: str,
    parts: List[Dict[str, Any]],
    callback: Dict[str, Any],
) -> None:
    """完成分片上传（带 115 回调，回调负责把 OSS 对象转存到网盘）。"""
    url = f"{_oss_object_url(bucket, endpoint, key)}?uploadId={upload_id}"
    body = b"<CompleteMultipartUpload>" + b"".join(
        f"<Part><PartNumber>{p['PartNumber']}</PartNumber><ETag>{p['ETag']}</ETag></Part>".encode("ascii")
        for p in parts
    ) + b"</CompleteMultipartUpload>"
    headers = {
        "x-oss-callback": base64.b64encode(str(callback.get("callback", "")).encode("utf-8")).decode("ascii"),
        "x-oss-callback-var": base64.b64encode(str(callback.get("callback_var", "")).encode("utf-8")).decode("ascii"),
        "content-type": "text/xml",
    }
    response = _oss_request(token, "POST", url, data=body, headers=headers)
    if response.status_code >= 300:
        raise UploadError(
            f"115 分片上传合并失败（OSS HTTP {response.status_code}）", retriable=True, provider="115"
        )


def _oss_abort_multipart(token: Dict[str, Any], bucket: str, endpoint: str, key: str, upload_id: str) -> bool:
    """取消分片上传（尽力而为，失败仅记日志）。"""
    try:
        url = f"{_oss_object_url(bucket, endpoint, key)}?uploadId={upload_id}"
        response = _oss_request(token, "DELETE", url, timeout=30)
        return response.status_code < 300
    except Exception as exc:  # pragma: no cover - 取消路径尽力而为
        logging.warning("115 上传取消失败（OSS abort）: %s", exc)
        return False


def _oss_list_parts(token: Dict[str, Any], bucket: str, endpoint: str, key: str, upload_id: str) -> List[Dict[str, Any]]:
    """查询已上传分片（断点续传）。"""
    import xml.etree.ElementTree as ET

    url = f"{_oss_object_url(bucket, endpoint, key)}?uploadId={upload_id}"
    response = _oss_request(token, "GET", url, timeout=30)
    if response.status_code >= 300:
        return []
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return []
    parts = []
    for el in root.iterfind("Part"):
        try:
            parts.append({
                "PartNumber": int(el.findtext("PartNumber")),
                "ETag": str(el.findtext("ETag") or "").strip().strip('"'),
                "Size": int(el.findtext("Size") or 0),
            })
        except (TypeError, ValueError):
            continue
    return sorted(parts, key=lambda p: p["PartNumber"])


def _read_sign_check_range(sink: Any, sign_check: str) -> bytes:
    """二次验证：读取 sign_check 指定的文件字节段（格式 "start-end"）。"""
    try:
        start, end = (int(x) for x in str(sign_check).split("-", 1))
    except (TypeError, ValueError) as exc:
        raise UploadError("115 二次验证参数无效", provider="115") from exc
    sink.seek(start)
    data = sink.read(end - start + 1)
    if len(data) != end - start + 1:
        raise UploadError("115 二次验证读取文件片段失败", provider="115")
    return data


def upload_115_file(
    cookie: str,
    sink: Any,
    filename: str,
    target_dir_id: str = "0",
    *,
    sha1: str = "",
    md5: str = "",
    size: int = -1,
    progress_cb=None,
    cancel_check=None,
    part_size_mb: int = 10,
) -> UploadResult:
    """把 sink 上传到 115 网盘（秒传优先，未命中走 OSS 分片）。

    详见模块头部链路注释。sha1/size 由复制引擎在下载时同步算出；
    缺省时本函数自行读取 sink 计算（会多读一遍）。
    """
    cookie = str(cookie or "").strip()
    filename = str(filename or "").strip()
    if not cookie:
        raise UploadError("115 Cookie 未配置", provider="115")
    if not filename:
        raise UploadError("上传文件名为空", provider="115")
    if sink is None:
        raise UploadError("上传数据源为空", provider="115")

    total = int(size if size is not None and size >= 0 else getattr(sink, "size", -1) or -1)
    if total < 0:
        raise UploadError("无法确定上传文件大小", provider="115")
    file_sha1 = str(sha1 or getattr(sink, "sha1", "") or "").strip().upper()
    if not file_sha1:
        sink.seek(0)
        hasher = hashlib.sha1()
        while True:
            chunk = sink.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
        file_sha1 = hasher.hexdigest().upper()
        sink.seek(0)

    userid, userkey = get_115_user_upload_info(cookie)
    target = f"U_1_{str(target_dir_id or '0')}"
    base_payload = {
        "fileid": file_sha1,
        "filename": filename,
        "filesize": total,
        "target": target,
        "path": "",
    }

    def _check_cancel():
        if cancel_check is not None and cancel_check():
            raise UploadError("上传已取消", provider="115")

    # ---- 秒传探测（含 405 限流退避与二次验证） ----
    resp: Dict[str, Any] = {}
    rounds = 0
    payload = dict(base_payload)
    while True:
        try:
            resp = _upload_115_initupload(cookie, userid, userkey, payload)
            break
        except _UploadRateLimited:
            rounds += 1
            if rounds > _115_UPLOAD_RETRY_MAX_ROUNDS:
                raise UploadError(
                    "115 上传接口持续限流，请稍后再试", retriable=True, provider="115"
                )
            time.sleep(_115_UPLOAD_RETRY_BASE_SECONDS * (2 ** (rounds - 1)))
            continue

    if not resp.get("state", True) and "status" not in resp:
        mark_cookie_health_failure("115", resp, trigger="runtime:upload_115_file")
        raise UploadError(_map_115_upload_error(resp), provider="115")

    # cookie 模式响应为扁平结构（字段在顶层），见 _115_upload_resp_fields
    fields = _115_upload_resp_fields(resp)
    status = int(fields.get("status") or 0)

    if status == 7:
        # 二次验证：读指定字节段算 SHA1 后重试一次
        sign_check = str(fields.get("sign_check") or "")
        sign_key = str(fields.get("sign_key") or "")
        if not sign_check:
            raise UploadError(
                "115 账号开启了二次验证且未提供验证参数，无法自动上传", provider="115"
            )
        payload = dict(base_payload)
        payload["sign_key"] = sign_key
        payload["sign_val"] = hashlib.sha1(_read_sign_check_range(sink, sign_check)).hexdigest().upper()
        resp = _upload_115_initupload(cookie, userid, userkey, payload)
        if not resp.get("state", True) and "status" not in resp:
            raise UploadError(_map_115_upload_error(resp), provider="115")
        fields = _115_upload_resp_fields(resp)
        status = int(fields.get("status") or 0)
        if status == 7:
            raise UploadError(
                "115 账号开启了二次验证，当前无法自动完成上传（需要人工验证）", provider="115"
            )

    if status == 2:
        # 秒传命中：零上传流量
        mark_cookie_health_success("115", trigger="runtime:upload_115_file")
        pick_code = str(fields.get("pickcode") or fields.get("pick_code") or "")
        return UploadResult(
            status="rapid",
            pick_code=pick_code,
            name=filename,
            size=total,
            sha1=file_sha1,
            message="秒传命中",
        )

    if status != 1:
        raise UploadError(_map_115_upload_error(resp), provider="115")

    # ---- 秒传未命中：OSS 上传 ----
    bucket = str(fields.get("bucket") or _115_OSS_DEFAULT_BUCKET)
    key = str(fields.get("object") or "")
    callback = fields.get("callback") or {}
    if not key:
        raise UploadError("115 上传配置缺失（object 为空）", provider="115")

    try:
        token = _upload_115_get_token(cookie)
    except _UploadRateLimited:
        time.sleep(_115_UPLOAD_RETRY_BASE_SECONDS)
        token = _upload_115_get_token(cookie)

    endpoint = _115_OSS_DEFAULT_ENDPOINT
    part_size = max(int(part_size_mb or 10) * 1024 * 1024, _115_OSS_MIN_PART_SIZE)

    _check_cancel()
    if total <= part_size:
        sink.seek(0)
        payload_bytes = sink.read(total)
        if len(payload_bytes) != total:
            raise UploadError("读取上传数据不完整", provider="115")
        _oss_upload_whole(token, bucket, endpoint, key, payload_bytes, callback)
        if progress_cb:
            try:
                progress_cb(total, total)
            except Exception:
                pass
    else:
        upload_id = _oss_multipart_init(token, bucket, endpoint, key)
        parts: List[Dict[str, Any]] = []
        sent = 0
        part_number = 1
        sink.seek(0)
        while True:
            _check_cancel()
            chunk = sink.read(part_size)
            if not chunk:
                break
            def _send_part(_part_number=part_number, _chunk=chunk):
                try:
                    return _oss_upload_part(
                        token, bucket, endpoint, key, upload_id, _part_number, _chunk
                    )
                except _UploadRateLimited as exc:
                    # 405/429 是限流语义，属于可重试的瞬时故障
                    raise UploadError(str(exc), retriable=True, provider="115") from exc

            etag = upload_part_with_retry(
                _send_part,
                part_number=part_number,
                label="115",
                cancel_check=(lambda: bool(cancel_check is not None and cancel_check())),
            )
            parts.append({"PartNumber": part_number, "ETag": etag})
            sent += len(chunk)
            part_number += 1
            if progress_cb:
                try:
                    progress_cb(sent, total)
                except Exception:
                    pass
        _oss_multipart_complete(token, bucket, endpoint, key, upload_id, parts, callback)

    mark_cookie_health_success("115", trigger="runtime:upload_115_file")
    return UploadResult(
        status="uploaded",
        pick_code=str(fields.get("pickcode") or fields.get("pick_code") or ""),
        name=filename,
        size=total,
        sha1=file_sha1,
        message="上传完成",
    )


def abort_115_upload(cookie: str, upload_handle: Dict[str, Any]) -> bool:
    """取消一次进行中的 115 上传（尽力清理 OSS 分片会话）。"""
    handle = upload_handle or {}
    token = handle.get("token")
    if not token:
        return False
    return _oss_abort_multipart(
        token,
        str(handle.get("bucket") or _115_OSS_DEFAULT_BUCKET),
        _115_OSS_DEFAULT_ENDPOINT,
        str(handle.get("key") or ""),
        str(handle.get("upload_id") or ""),
    )


def _map_115_upload_error(resp: Dict[str, Any]) -> str:
    """把 115 上传接口错误映射为面向用户的文案。"""
    errcode = int(resp.get("errcode", 0) or 0)
    status = int((resp.get("data") or {}).get("status", 0) or 0)
    message = (
        str(resp.get("error_msg", "") or "").strip()
        or str(resp.get("msg", "") or "").strip()
        or str(resp.get("error", "") or "").strip()
    )
    if status == 7 or "二次验证" in message:
        return "115 账号开启了二次验证，无法自动完成上传（需要人工验证）"
    if errcode in (20000, 20001) or "登录" in message or "权限" in message:
        return "115 登录已失效，请更新 Cookie 后重试"
    if errcode == 10008 or "频繁" in message or "limit" in message.lower():
        return "115 操作过于频繁，请稍后再试"
    return message or "115 上传失败，请稍后重试"


register(Pan115Provider())
