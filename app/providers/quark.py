import base64
import hashlib
import json
import logging
import mimetypes
import threading
import time

import requests

from .base import CloudProvider, UploadError, UploadResult, upload_part_with_retry
from .common import parse_int
from .registry import register
from ..share_selection import normalize_share_selection_entry, normalize_share_selection_meta
from ..core import *  # noqa: F401,F403

_quark_http_local = threading.local()

# 夸克每次 API 响应都会用 Set-Cookie 轮换 __puus（有时含 __pus），下载直链的 CDN 回调
# 按它校验登录态。只带配置里的静态 cookie 去下载会被拒：
# 403 RequestDeniedByCallback / require login [auth expired]。
# 这里缓存轮换结果，并节流写回配置，保证后续请求和下载都用最新值。
_QUARK_COOKIE_LOCK = threading.RLock()
_QUARK_COOKIE_ROTATED: Dict[str, str] = {}
_QUARK_COOKIE_PERSIST_AT = 0.0
_QUARK_COOKIE_PERSIST_INTERVAL = 60.0
_QUARK_COOKIE_CACHE_MAX_ROWS = 64
_QUARK_ROTATION_NAMES = ("__puus", "__pus")
_QUARK_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
)
# 夸克网关按身份限制 file/download：浏览器身份取直链时，单文件超过免费额度直接返回
# HTTP 400 / code 23018 "download file size limit"（实测 2.6GB 视频必拦）；
# 换成客户端身份（quark-cloud-drive + Channel/pckk_other_ch）可正常拿到直链。
# 注意：只对取直链这一个接口用客户端 UA，其余接口仍用浏览器 UA。
_QUARK_CLIENT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) quark-cloud-drive/2.5.56 Chrome/100.0.4896.160 "
    "Electron/18.3.5.12 Safari/537.36 Channel/pckk_other_ch"
)
_QUARK_SIZE_LIMIT_CODES = (23018,)
_QUARK_SIZE_LIMIT_KEYWORD = "download file size limit"
_QUARK_SIZE_LIMIT_HINT = (
    "夸克以网页身份拒绝下载该大文件（非会员单文件限制，code 23018）。"
    "已尝试以客户端身份重试仍失败，请开通夸克会员或改用其他来源。"
)
_QUARK_SHARE_CACHE_TTL_SECONDS = max(60, min(30 * 60, int(SHARE_SNAP_CACHE_TTL_SECONDS or 300)))
_QUARK_SHARE_TOKEN_CACHE_MAX_ROWS = 256
_QUARK_SHARE_PAGE_CACHE_MAX_ROWS = 1200
_QUARK_SHARE_RESULT_CACHE_MAX_ROWS = 1200
_QUARK_SHARE_FAST_DEADLINE_SECONDS = max(
    1.0,
    min(8.0, float(os.environ.get("QUARK_SHARE_FAST_DEADLINE_SECONDS", 3.0) or 3.0)),
)
_QUARK_SHARE_FAST_CONNECT_TIMEOUT_SECONDS = max(
    0.2,
    min(2.0, float(os.environ.get("QUARK_SHARE_FAST_CONNECT_TIMEOUT_SECONDS", 0.8) or 0.8)),
)
_QUARK_SHARE_FAST_READ_TIMEOUT_SECONDS = max(
    0.5,
    min(5.0, float(os.environ.get("QUARK_SHARE_FAST_READ_TIMEOUT_SECONDS", 2.4) or 2.4)),
)
_quark_share_token_cache: Dict[str, Dict[str, Any]] = {}
_quark_share_page_cache: Dict[str, Dict[str, Any]] = {}
_quark_share_result_cache: Dict[str, Dict[str, Any]] = {}
_quark_share_token_cache_lock = threading.Lock()
_quark_share_page_cache_lock = threading.Lock()
_quark_share_result_cache_lock = threading.Lock()
_quark_share_singleflight: Dict[str, Dict[str, Any]] = {}
_quark_share_singleflight_lock = threading.Lock()


def _build_quark_timing_mark(started_mono: float, last_mono: float) -> Tuple[int, float]:
    now_mono = time.monotonic()
    return int((now_mono - last_mono) * 1000), now_mono


def _log_quark_share_timing(mode: str, pwd_id: str, cid: str, timings: List[Dict[str, Any]], total_ms: int) -> None:
    safe_pwd_id = str(pwd_id or "").strip()
    safe_cid = str(cid or "0").strip() or "0"
    stage_text = " / ".join(
        f"{str(item.get('label', '') or item.get('stage', '')).strip()}: {int(item.get('ms', 0) or 0)}ms"
        for item in (timings or [])
        if int(item.get("ms", 0) or 0) >= 0
    )
    logging.info(
        "Quark share %s timing pwd_id=%s cid=%s total=%sms%s",
        str(mode or "read").strip() or "read",
        f"{safe_pwd_id[:6]}***" if len(safe_pwd_id) > 6 else safe_pwd_id,
        safe_cid,
        total_ms,
        f" stages=[{stage_text}]" if stage_text else "",
    )


def _get_quark_http_session() -> requests.Session:
    session = getattr(_quark_http_local, "session", None)
    if session is None:
        session = requests.Session()
        _quark_http_local.session = session
    return session


def _clone_quark_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item or {}) for item in (entries or [])]


def _clone_quark_share_page_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    return {
        "entries": _clone_quark_entries(source.get("entries", [])),
        "share": dict(source.get("share", {}) if isinstance(source.get("share"), dict) else {}),
        "has_more": bool(source.get("has_more", False)),
        "total": int(source.get("total", 0) or 0),
        "next_page": int(source.get("next_page", 0) or 0),
        "page": int(source.get("page", 1) or 1),
        "size": int(source.get("size", 200) or 200),
        "cache_derived": bool(source.get("cache_derived", False)),
    }


def _clone_quark_share_result_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    cloned = {
        "entries": _clone_quark_entries(source.get("entries", [])),
        "summary": dict(source.get("summary", {}) if isinstance(source.get("summary"), dict) else {}),
        "share_code": str(source.get("share_code", "") or "").strip(),
        "receive_code": str(source.get("receive_code", "") or "").strip(),
        "share_title": str(source.get("share_title", "") or "").strip(),
        "current_cid": str(source.get("current_cid", "") or "0").strip() or "0",
        "count": int(source.get("count", 0) or 0),
        "offset": int(source.get("offset", 0) or 0),
        "next_offset": int(source.get("next_offset", 0) or 0),
        "has_more": bool(source.get("has_more", False)),
        "pages_scanned": int(source.get("pages_scanned", 0) or 0),
        "stoken": str(source.get("stoken", "") or "").strip(),
        "cache_derived": bool(source.get("cache_derived", False)),
    }
    for key in ("fast_path", "cache_stale"):
        if key in source:
            cloned[key] = bool(source.get(key, False))
    for key in ("elapsed_ms",):
        if key in source:
            cloned[key] = int(source.get(key, 0) or 0)
    if isinstance(source.get("timings"), list):
        cloned["timings"] = [
            {
                "stage": str(item.get("stage", "") or "").strip(),
                "label": str(item.get("label", "") or "").strip(),
                "ms": int(item.get("ms", 0) or 0),
            }
            for item in source.get("timings", [])
            if isinstance(item, dict)
        ]
    for key in ("cache_error", "cache_cid"):
        if key in source:
            cloned[key] = str(source.get(key, "") or "").strip()
    return cloned


def _build_quark_share_cache_key(
    cache_kind: str,
    cookie: str,
    pwd_id: str,
    receive_or_token: str,
    cid: str,
    extra: str = "",
) -> str:
    cookie_hash = hashlib.sha1(str(cookie or "").strip().encode("utf-8")).hexdigest()[:16]
    source = (
        f"{str(cache_kind or '').strip()}|{cookie_hash}|"
        f"{str(pwd_id or '').strip()}|{str(receive_or_token or '').strip()}|"
        f"{str(cid or '0').strip() or '0'}|{str(extra or '').strip()}"
    )
    return hashlib.sha1(source.encode("utf-8")).hexdigest()


def _prune_quark_memory_cache_locked(cache: Dict[str, Dict[str, Any]], now_ts: float, max_rows: int) -> None:
    expired_keys = [
        key
        for key, payload in cache.items()
        if now_ts >= float((payload or {}).get("expires_at", 0.0) or 0.0)
    ]
    for key in expired_keys:
        cache.pop(key, None)
    if len(cache) <= max_rows:
        return
    overflow = len(cache) - max_rows
    ordered = sorted(cache.items(), key=lambda item: float((item[1] or {}).get("updated_at", 0.0) or 0.0))
    for key, _payload in ordered[:overflow]:
        cache.pop(key, None)


def prune_quark_share_memory_caches() -> Dict[str, int]:
    now_ts = time.time()
    detail: Dict[str, int] = {}
    for name, cache, lock, max_rows in (
        ("token", _quark_share_token_cache, _quark_share_token_cache_lock, _QUARK_SHARE_TOKEN_CACHE_MAX_ROWS),
        ("page", _quark_share_page_cache, _quark_share_page_cache_lock, _QUARK_SHARE_PAGE_CACHE_MAX_ROWS),
        ("result", _quark_share_result_cache, _quark_share_result_cache_lock, _QUARK_SHARE_RESULT_CACHE_MAX_ROWS),
    ):
        with lock:
            before = len(cache)
            _prune_quark_memory_cache_locked(cache, now_ts, max_rows)
            detail[name] = max(0, before - len(cache))
    return detail


def _get_quark_memory_cache_payload(
    cache: Dict[str, Dict[str, Any]],
    lock: threading.Lock,
    key: str,
    clone_func,
    allow_expired: bool = False,
) -> Dict[str, Any]:
    cache_key = str(key or "").strip()
    if not cache_key:
        return {}
    now_ts = time.time()
    with lock:
        cached = cache.get(cache_key)
        if not cached:
            return {}
        expired = now_ts >= float(cached.get("expires_at", 0.0) or 0.0)
        if expired and not allow_expired:
            cache.pop(cache_key, None)
            return {}
        payload = clone_func(cached.get("payload", {}))
        if payload:
            payload["cache_derived"] = True
            if expired:
                payload["cache_stale"] = True
        return payload


def _set_quark_memory_cache_payload(
    cache: Dict[str, Dict[str, Any]],
    lock: threading.Lock,
    key: str,
    payload: Dict[str, Any],
    clone_func,
    max_rows: int,
) -> None:
    cache_key = str(key or "").strip()
    if not cache_key or not isinstance(payload, dict):
        return
    now_ts = time.time()
    with lock:
        cache[cache_key] = {
            "payload": clone_func(payload),
            "updated_at": now_ts,
            "expires_at": now_ts + _QUARK_SHARE_CACHE_TTL_SECONDS,
        }
        _prune_quark_memory_cache_locked(cache, now_ts, max_rows)


def _run_quark_share_singleflight(key: str, deadline_ts: float, work) -> Any:
    singleflight_key = str(key or "").strip()
    if not singleflight_key:
        return work()
    owner = False
    with _quark_share_singleflight_lock:
        state = _quark_share_singleflight.get(singleflight_key)
        if not state:
            state = {"event": threading.Event(), "result": None, "error": None}
            _quark_share_singleflight[singleflight_key] = state
            owner = True
    if owner:
        try:
            state["result"] = work()
            return state["result"]
        except Exception as exc:
            state["error"] = exc
            raise
        finally:
            state["event"].set()
            with _quark_share_singleflight_lock:
                if _quark_share_singleflight.get(singleflight_key) is state:
                    _quark_share_singleflight.pop(singleflight_key, None)

    wait_seconds = max(0.05, float(deadline_ts or 0.0) - time.monotonic())
    if not state["event"].wait(wait_seconds):
        raise TimeoutError("夸克分享读取等待超时")
    if state.get("error") is not None:
        raise state["error"]
    return state.get("result")


def _quark_timeout_from_deadline(deadline_ts: float) -> Tuple[float, float]:
    remaining = float(deadline_ts or 0.0) - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("夸克分享读取超时")
    connect_timeout = min(remaining, max(0.05, _QUARK_SHARE_FAST_CONNECT_TIMEOUT_SECONDS))
    read_timeout = min(remaining, max(0.05, _QUARK_SHARE_FAST_READ_TIMEOUT_SECONDS))
    return (connect_timeout, read_timeout)


def _request_quark_json_fast(
    method: str,
    url: str,
    headers: Dict[str, str],
    deadline_ts: float,
    payload: Optional[Dict[str, Any]] = None,
    fallback: str = "夸克网盘请求失败",
) -> Dict[str, Any]:
    try:
        response = _get_quark_http_session().request(
            str(method or "GET").strip().upper() or "GET",
            normalize_http_url(url),
            json=payload if isinstance(payload, dict) else None,
            headers=headers,
            timeout=_quark_timeout_from_deadline(deadline_ts),
        )
        response.raise_for_status()
        try:
            payload_json = response.json()
        except ValueError as exc:
            raise RuntimeError("夸克网盘返回内容不是有效 JSON") from exc
        return payload_json if isinstance(payload_json, dict) else {}
    except Exception as exc:
        _raise_quark_http_error(exc, fallback=fallback)
        return {}

def http_request_json_payload(
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    method: str = "POST",
    proxy_url: str = "",
) -> Dict[str, Any]:
    url = normalize_http_url(url)
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    body_payload = payload if isinstance(payload, dict) else {}
    body_bytes = json.dumps(body_payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body_bytes,
        headers=headers,
        method=str(method or "POST").strip().upper() or "POST",
    )
    opener = urllib.request.build_opener()
    if proxy_url:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
    with opener.open(request, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="ignore")
    payload_json = safe_json_loads(raw, {})
    return payload_json if isinstance(payload_json, dict) else {}

def _split_quark_cookie(cookie: str) -> List[Any]:
    pairs: List[Any] = []
    for raw in str(cookie or "").split(";"):
        item = raw.strip()
        if "=" not in item:
            continue
        name, value = item.split("=", 1)
        name = name.strip()
        if name:
            pairs.append((name, value))
    return pairs


def _current_quark_cookie(cookie: str) -> str:
    """返回当前最新 cookie（已包含轮换后的 __puus）。"""
    raw = str(cookie or "").strip()
    if not raw:
        return ""
    with _QUARK_COOKIE_LOCK:
        return _QUARK_COOKIE_ROTATED.get(raw, raw)


def _persist_rotated_quark_cookie(cookie: str) -> None:
    """把轮换后的 cookie 节流写回配置（失败不影响主流程）。"""
    global _QUARK_COOKIE_PERSIST_AT
    now = time.monotonic()
    with _QUARK_COOKIE_LOCK:
        if now - _QUARK_COOKIE_PERSIST_AT < _QUARK_COOKIE_PERSIST_INTERVAL:
            return
        _QUARK_COOKIE_PERSIST_AT = now
    try:
        cfg = get_config()
        if str(cfg.get("cookie_quark", "") or "").strip() == cookie:
            return
        cfg["cookie_quark"] = cookie
        save_config(cfg)
    except Exception as exc:  # pragma: no cover - 落盘失败不影响请求
        logging.debug("夸克 cookie 轮换写回配置失败: %s", exc)


def _rotate_quark_cookie(cookie: str, response) -> str:
    """把响应 Set-Cookie 里的 __puus/__pus 合并进 cookie，返回最新 cookie。"""
    raw = str(cookie or "").strip()
    if not raw or response is None:
        return raw
    try:
        cookies = getattr(response, "cookies", None)
        if cookies is None:
            return raw
        updates: Dict[str, str] = {}
        for name in _QUARK_ROTATION_NAMES:
            try:
                value = cookies.get(name)
            except Exception:
                value = None
            if value:
                updates[name] = str(value)
        if not updates:
            return raw

        parts = [f"{name}={updates.get(name, value)}" for name, value in _split_quark_cookie(raw)]
        known = {name for name, _ in _split_quark_cookie(raw)}
        for name, value in updates.items():
            if name not in known:
                parts.append(f"{name}={value}")
        merged = "; ".join(parts)
        if merged == raw:
            return raw

        with _QUARK_COOKIE_LOCK:
            if len(_QUARK_COOKIE_ROTATED) >= _QUARK_COOKIE_CACHE_MAX_ROWS:
                _QUARK_COOKIE_ROTATED.clear()
            _QUARK_COOKIE_ROTATED[raw] = merged
            for key, value in list(_QUARK_COOKIE_ROTATED.items()):
                if value == raw:
                    _QUARK_COOKIE_ROTATED[key] = merged
        _persist_rotated_quark_cookie(merged)
        return merged
    except Exception as exc:  # pragma: no cover - 轮换失败降级为原 cookie
        logging.debug("解析夸克 Set-Cookie 失败: %s", exc)
        return raw


def _build_quark_headers(cookie: str, referer: str = "https://pan.quark.cn/") -> Dict[str, str]:
    return {
        "Cookie": _current_quark_cookie(cookie),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Referer": str(referer or "https://pan.quark.cn/").strip() or "https://pan.quark.cn/",
        "Origin": "https://pan.quark.cn",
        "Priority": "u=1, i",
        "Sec-Ch-Ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": _QUARK_BROWSER_USER_AGENT,
    }

def _build_quark_client_headers(cookie: str) -> Dict[str, str]:
    """取下载直链专用头：模拟夸克桌面客户端，绕开网页身份的单文件体积限制。"""
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    for key in ("Sec-Ch-Ua", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Platform", "Priority"):
        headers.pop(key, None)
    headers["User-Agent"] = _QUARK_CLIENT_USER_AGENT
    return headers

def _extract_quark_error(payload: Any, fallback: str = "夸克网盘请求失败") -> str:
    data = payload if isinstance(payload, dict) else {}
    parts = [
        str(data.get("message", "")).strip(),
        str(data.get("msg", "")).strip(),
        str(data.get("error", "")).strip(),
        str(data.get("error_msg", "")).strip(),
    ]
    nested = data.get("data")
    if isinstance(nested, dict):
        parts.extend(
            [
                str(nested.get("message", "")).strip(),
                str(nested.get("msg", "")).strip(),
                str(nested.get("error", "")).strip(),
            ]
        )
    detail = next((part for part in parts if part), "")
    return detail or str(fallback or "夸克网盘请求失败")

def _is_quark_size_limit_error(payload: Any, detail: str = "") -> bool:
    """夸克"网页身份下载大文件被拒"（code 23018 / download file size limit）。"""
    data = payload if isinstance(payload, dict) else {}
    code = parse_int(data.get("code"), default=0)
    text = f"{detail or ''} {_extract_quark_error(data, '')}".lower()
    return code in _QUARK_SIZE_LIMIT_CODES or _QUARK_SIZE_LIMIT_KEYWORD in text

def _raise_quark_http_error(exc: Exception, fallback: str = "夸克网盘请求失败") -> None:
    status_code = 0
    payload: Dict[str, Any] = {}
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        status_code = int(getattr(response, "status_code", 0) or 0)
        raw_body = str(getattr(response, "text", "") or "")
        payload_obj = safe_json_loads(raw_body, {})
        if isinstance(payload_obj, dict):
            payload = payload_obj
    if isinstance(exc, urllib.error.HTTPError):
        status_code = int(exc.code or 0)
        try:
            raw_body = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            raw_body = ""
        payload_obj = safe_json_loads(raw_body, {})
        if isinstance(payload_obj, dict):
            payload = payload_obj
    detail = _extract_quark_error(payload, "")
    if status_code > 0 and _is_quark_size_limit_error(payload, detail):
        raise RuntimeError(_QUARK_SIZE_LIMIT_HINT) from exc
    if status_code > 0:
        message = f"HTTP {status_code}"
        if detail:
            message = f"{message}: {detail}"
        raise RuntimeError(message) from exc
    if detail:
        raise RuntimeError(detail) from exc
    if isinstance(exc, requests.RequestException):
        message = str(exc or "").strip()
        raise RuntimeError(message or str(fallback or "夸克网盘请求失败")) from exc
    raise RuntimeError(str(fallback or "夸克网盘请求失败")) from exc

def _request_quark_json(url: str, headers: Dict[str, str], timeout: int = 45, fallback: str = "夸克网盘请求失败") -> Dict[str, Any]:
    try:
        response = _get_quark_http_session().get(
            normalize_http_url(url),
            headers=headers,
            timeout=max(3, int(timeout or 45)),
        )
        _rotate_quark_cookie(headers.get("Cookie", ""), response)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("夸克网盘返回内容不是有效 JSON") from exc
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        _raise_quark_http_error(exc, fallback=fallback)
        return {}

def _request_quark_json_payload(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 45,
    method: str = "POST",
    fallback: str = "夸克网盘请求失败",
) -> Dict[str, Any]:
    try:
        response = _get_quark_http_session().request(
            str(method or "POST").strip().upper() or "POST",
            normalize_http_url(url),
            json=payload if isinstance(payload, dict) else {},
            headers=headers,
            timeout=max(3, int(timeout or 45)),
        )
        _rotate_quark_cookie(headers.get("Cookie", ""), response)
        response.raise_for_status()
        try:
            payload_json = response.json()
        except ValueError as exc:
            raise RuntimeError("夸克网盘返回内容不是有效 JSON") from exc
        return payload_json if isinstance(payload_json, dict) else {}
    except Exception as exc:
        _raise_quark_http_error(exc, fallback=fallback)
        return {}

def _is_quark_success(payload: Any) -> bool:
    data = payload if isinstance(payload, dict) else {}
    code = parse_int(data.get("code"), default=0)
    status = parse_int(data.get("status"), default=0)
    success_hints = (
        bool(data.get("success", False)),
        bool(data.get("state", False)),
        code == 0,
        status in (0, 200),
    )
    return any(success_hints)

def resolve_quark_share_payload(cookie: str, share_url: str, raw_text: str = "", receive_code: str = "") -> Dict[str, str]:
    parsed = parse_quark_share_payload(share_url, raw_text, receive_code)
    if parsed.get("pwd_id"):
        return parsed
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    resolved_url = http_resolve_url(
        share_url,
        timeout=30,
        extra_headers=headers,
    )
    parsed = parse_quark_share_payload(resolved_url, raw_text, receive_code)
    if not parsed.get("pwd_id"):
        raise RuntimeError("未能识别夸克分享链接")
    return parsed


def probe_quark_connectivity(cookie: str = "", timeout: float = 5.0) -> Dict[str, Any]:
    request_timeout = max(1.0, min(15.0, float(timeout or 5.0)))
    headers = _build_quark_headers(str(cookie or "").strip(), referer="https://pan.quark.cn/")
    probes: List[Dict[str, Any]] = []
    started = time.monotonic()
    for label, url in (
        ("分享页", "https://pan.quark.cn"),
        ("接口", "https://drive-h.quark.cn"),
        ("网盘", "https://drive-pc.quark.cn"),
    ):
        item_started = time.monotonic()
        row: Dict[str, Any] = {"label": label, "url": url, "ok": False, "status": 0, "elapsed_ms": 0, "error": ""}
        try:
            response = _get_quark_http_session().get(
                url,
                headers=headers,
                timeout=(min(2.0, request_timeout), request_timeout),
            )
            row["status"] = int(response.status_code or 0)
            row["ok"] = int(response.status_code or 0) < 500
        except Exception as exc:
            row["error"] = str(exc or "请求失败").strip()[:240]
        finally:
            row["elapsed_ms"] = int((time.monotonic() - item_started) * 1000)
            probes.append(row)
    total_ms = int((time.monotonic() - started) * 1000)
    logging.info(
        "Quark connectivity probe total=%sms probes=%s",
        total_ms,
        " / ".join(f"{item['label']}:{item['elapsed_ms']}ms:{item.get('status') or item.get('error')}" for item in probes),
    )
    return {
        "ok": any(bool(item.get("ok")) for item in probes),
        "elapsed_ms": total_ms,
        "probes": probes,
    }

def _build_quark_api_url(path: str, query: Optional[Dict[str, Any]] = None, host: str = "https://drive-pc.quark.cn") -> str:
    normalized_path = str(path or "").strip()
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"
    params = {"pr": "ucpro", "fr": "pc"}
    if isinstance(query, dict):
        for key, value in query.items():
            if value is None:
                continue
            params[str(key)] = str(value)
    query_string = urllib.parse.urlencode(params)
    return f"{host.rstrip('/')}{normalized_path}?{query_string}"

def _parse_quark_entry(item: Dict[str, Any], parent_id: str = "0") -> Dict[str, Any]:
    payload = item if isinstance(item, dict) else {}
    name = str(payload.get("file_name", "") or payload.get("name", "") or payload.get("title", "")).strip()
    entry_id = str(payload.get("fid", "") or payload.get("id", "") or payload.get("file_id", "")).strip()
    if not name or not entry_id:
        return {}
    is_dir = bool(payload.get("dir")) or str(payload.get("obj_category", "")).strip().lower() in ("dir", "folder")
    file_type = parse_int(payload.get("file_type"), default=-1)
    if file_type == 0:
        is_dir = True
    parent_fid = str(payload.get("pdir_fid", "") or parent_id or "0").strip() or "0"
    fid_token = str(
        payload.get("share_fid_token", "")
        or payload.get("fid_token", "")
        or payload.get("share_token", "")
        or payload.get("file_token", "")
        or ""
    ).strip()
    return {
        "id": entry_id,
        "cid": entry_id if is_dir else "",
        "name": name,
        "is_dir": bool(is_dir),
        "size": parse_int(payload.get("size"), default=0),
        "parent_id": parent_fid,
        "fid": entry_id if not is_dir else "",
        "fid_token": fid_token,
        "modified_at": str(payload.get("updated_at", "") or payload.get("last_update_at", "") or payload.get("create_time", "")).strip(),
    }

def _list_quark_folder_page(cookie: str, pdir_fid: str = "0", page: int = 1, page_size: int = 200) -> Dict[str, Any]:
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    url = _build_quark_api_url(
        "/1/clouddrive/file/sort",
        {
            "uc_param_str": "",
            "pdir_fid": str(pdir_fid or "0").strip() or "0",
            "_page": max(1, int(page or 1)),
            "_size": max(20, min(200, int(page_size or 200))),
            "_fetch_total": 1,
            "_fetch_sub_dirs": 1,
            "_sort": "file_type:asc,file_name:asc",
            "__dt": 2093126,
            "__t": int(time.time() * 1000),
        },
    )
    result = _request_quark_json(url, headers, timeout=45, fallback="读取夸克目录失败")
    if not _is_quark_success(result):
        raise RuntimeError(_extract_quark_error(result, "读取夸克目录失败"))
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    raw_list = data.get("list") if isinstance(data.get("list"), list) else []
    entries = []
    parent_id = str(pdir_fid or "0").strip() or "0"
    for raw_item in raw_list:
        parsed_entry = _parse_quark_entry(raw_item if isinstance(raw_item, dict) else {}, parent_id=parent_id)
        if parsed_entry:
            entries.append(parsed_entry)
    has_more = bool(data.get("has_more", False))
    if not has_more:
        next_page = parse_int(data.get("next_page"), default=0)
        if next_page > max(1, int(page or 1)):
            has_more = True
    total = parse_int(data.get("total"), default=len(entries))
    return {
        "entries": entries,
        "has_more": has_more,
        "total": total,
        "page": max(1, int(page or 1)),
        "size": max(20, min(200, int(page_size or 200))),
    }

def list_quark_entries_payload(cookie: str, cid: str = "0", folders_only: bool = False) -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    try:
        parent_id = str(cid or "0").strip() or "0"
        page = 1
        page_size = 200
        folder_only_mode = bool(folders_only)
        entries: List[Dict[str, Any]] = []
        total_count = 0
        loaded_file_count = 0
        pages_scanned = 0
        while True:
            page_payload = _list_quark_folder_page(normalized_cookie, pdir_fid=parent_id, page=page, page_size=page_size)
            current_entries = page_payload.get("entries", []) if isinstance(page_payload.get("entries"), list) else []
            total_count = max(total_count, parse_int(page_payload.get("total"), default=0))
            pages_scanned += 1
            if folder_only_mode:
                current_file_count = sum(1 for entry in current_entries if not entry.get("is_dir"))
                loaded_file_count += current_file_count
                entries.extend([entry for entry in current_entries if entry.get("is_dir")])
                if current_file_count > 0:
                    break
            else:
                entries.extend(current_entries)
            if not bool(page_payload.get("has_more", False)):
                break
            if len(entries) >= 5000:
                break
            page += 1
        entries.sort(key=lambda item: (0 if item.get("is_dir") else 1, str(item.get("name", "")).lower()))
        folder_count = sum(1 for item in entries if item.get("is_dir"))
        file_count = (
            max(loaded_file_count, (total_count - folder_count) if total_count > 0 else 0)
            if folder_only_mode
            else sum(1 for item in entries if not item.get("is_dir"))
        )
        mark_cookie_health_success("quark", trigger="runtime:list_quark_entries")
        return {
            "entries": entries,
            "summary": {
                "folder_count": folder_count,
                "file_count": file_count,
            },
            "pages_scanned": pages_scanned,
            "total": total_count or len(entries),
            "folders_only": folder_only_mode,
        }
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:list_quark_entries")
        raise


def list_quark_entries(cookie: str, cid: str = "0") -> List[Dict[str, Any]]:
    payload = list_quark_entries_payload(cookie, cid, folders_only=False)
    return payload.get("entries", []) if isinstance(payload.get("entries"), list) else []


def create_quark_folder(cookie: str, cid: str = "0", folder_name: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    parent_id = str(cid or "0").strip() or "0"
    normalized_name = sanitize_115_folder_name(folder_name, fallback="")
    if not normalized_name:
        raise RuntimeError("文件夹名称不能为空")

    def _find_existing_folder() -> str:
        folder_payload = list_quark_entries_payload(normalized_cookie, parent_id, folders_only=True)
        entries = folder_payload.get("entries", []) if isinstance(folder_payload.get("entries"), list) else []
        matched = next(
            (
                entry
                for entry in entries
                if entry.get("is_dir") and str(entry.get("name", "")).strip() == normalized_name
            ),
            None,
        )
        return str((matched or {}).get("id", "") or "").strip()

    try:
        headers = _build_quark_headers(normalized_cookie, referer="https://pan.quark.cn/")
        url = _build_quark_api_url("/1/clouddrive/file")
        payload = {
            "pdir_fid": parent_id,
            "file_name": normalized_name,
            "dir_path": "",
            "dir_init_lock": False,
        }
        # 夸克在父目录刚创建后立刻建子目录，会瞬时假报「file is doloading[同名冲突]」。
        # 这类冲突稍等即消失，重试并顺手复用已存在的同名目录，避免跨盘复制因此中断。
        last_conflict: Optional[Exception] = None
        response: Dict[str, Any] = {}
        for attempt in range(4):
            if attempt:
                time.sleep(1.2 * attempt)
            try:
                response = _request_quark_json_payload(
                    url,
                    payload,
                    headers,
                    timeout=45,
                    method="POST",
                    fallback="新建夸克目录失败",
                )
                break
            except RuntimeError as exc:
                message = str(exc)
                if "同名冲突" not in message and "doloading" not in message:
                    raise
                last_conflict = exc
                existing_id = _find_existing_folder()
                if existing_id:
                    mark_cookie_health_success("quark", trigger="runtime:create_quark_folder")
                    return {
                        "id": existing_id,
                        "name": normalized_name,
                        "cid": parent_id,
                        "created": False,
                    }
        else:
            raise last_conflict or RuntimeError("新建夸克目录失败：同名冲突")

        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        folder_id = str(data.get("fid", "") or data.get("id", "") or "").strip()
        success = _is_quark_success(response)
        if (not success) or (not folder_id):
            folder_id = _find_existing_folder()
        if not folder_id:
            raise RuntimeError(_extract_quark_error(response, "新建夸克目录失败"))
        mark_cookie_health_success("quark", trigger="runtime:create_quark_folder")
        return {
            "id": folder_id,
            "name": normalized_name,
            "cid": parent_id,
            "created": success,
        }
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:create_quark_folder")
        raise


def _validate_quark_entry_name(value: str) -> str:
    normalized_name = sanitize_115_folder_name(value, fallback="")
    if not normalized_name:
        raise RuntimeError("文件名称不能为空")
    if len(normalized_name) > 240:
        raise RuntimeError("文件名称过长")
    return normalized_name


def rename_quark_entry(cookie: str, entry_id: str, new_name: str, parent_id: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    normalized_id = str(entry_id or "").strip()
    if not normalized_id:
        raise RuntimeError("文件 ID 不能为空")
    normalized_name = _validate_quark_entry_name(new_name)
    try:
        headers = _build_quark_headers(normalized_cookie, referer="https://pan.quark.cn/")
        url = _build_quark_api_url("/1/clouddrive/file/rename")
        response = _request_quark_json_payload(
            url,
            {"fid": normalized_id, "file_name": normalized_name},
            headers,
            timeout=45,
            method="POST",
            fallback="夸克重命名失败",
        )
        if not _is_quark_success(response):
            raise RuntimeError(_extract_quark_error(response, "夸克重命名失败"))
        mark_cookie_health_success("quark", trigger="runtime:rename_quark_entry")
        return {"id": normalized_id, "name": normalized_name, "response": response}
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:rename_quark_entry")
        raise


def move_quark_entries(cookie: str, entry_ids: List[str], target_cid: str, source_cid: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要移动的文件")
    target_id = str(target_cid or "0").strip() or "0"
    try:
        headers = _build_quark_headers(normalized_cookie, referer="https://pan.quark.cn/")
        url = _build_quark_api_url("/1/clouddrive/file/move")
        # Quark derives the source directory from file ids here; current_dir_fid
        # conflicts with filelist and causes "不能同时存在值" errors.
        response = _request_quark_json_payload(
            url,
            {
                "action_type": 1,
                "to_pdir_fid": target_id,
                "filelist": ids,
                "fid_list": ids,
                "exclude_fids": [],
            },
            headers,
            timeout=60,
            method="POST",
            fallback="夸克移动失败",
        )
        if not _is_quark_success(response):
            raise RuntimeError(_extract_quark_error(response, "夸克移动失败"))
        mark_cookie_health_success("quark", trigger="runtime:move_quark_entries")
        return {"ids": ids, "target_cid": target_id, "response": response}
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:move_quark_entries")
        raise


def copy_quark_entries(cookie: str, entry_ids: List[str], target_cid: str, source_cid: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要复制的文件")
    target_id = str(target_cid or "0").strip() or "0"
    try:
        headers = _build_quark_headers(normalized_cookie, referer="https://pan.quark.cn/")
        url = _build_quark_api_url("/1/clouddrive/file/copy")
        response = _request_quark_json_payload(
            url,
            {
                "action_type": 1,
                "to_pdir_fid": target_id,
                "filelist": ids,
                "fid_list": ids,
                "exclude_fids": [],
            },
            headers,
            timeout=60,
            method="POST",
            fallback="夸克复制失败",
        )
        if not _is_quark_success(response):
            raise RuntimeError(_extract_quark_error(response, "夸克复制失败"))
        mark_cookie_health_success("quark", trigger="runtime:copy_quark_entries")
        return {"ids": ids, "target_cid": target_id, "response": response}
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:copy_quark_entries")
        raise


def delete_quark_entries(cookie: str, entry_ids: List[str], parent_cid: str = "") -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    ids = [str(item or "").strip() for item in (entry_ids or []) if str(item or "").strip()]
    if not ids:
        raise RuntimeError("请选择要删除的文件")
    try:
        headers = _build_quark_headers(normalized_cookie, referer="https://pan.quark.cn/")
        url = _build_quark_api_url("/1/clouddrive/file/delete")
        response = _request_quark_json_payload(
            url,
            {
                "action_type": 2,
                "filelist": ids,
                "exclude_fids": [],
            },
            headers,
            timeout=60,
            method="POST",
            fallback="夸克删除失败",
        )
        if not _is_quark_success(response):
            raise RuntimeError(_extract_quark_error(response, "夸克删除失败"))
        mark_cookie_health_success("quark", trigger="runtime:delete_quark_entries")
        return {"ids": ids, "response": response}
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:delete_quark_entries")
        raise


def ensure_quark_folder_id_by_path(cookie: str, relative_path: str) -> str:
    normalized_path = normalize_relative_path(relative_path)
    if not normalized_path:
        return "0"
    current_id = "0"
    for raw_part in [segment for segment in normalized_path.split("/") if segment]:
        part = str(raw_part or "").strip()
        if not part:
            continue
        folder_payload = list_quark_entries_payload(cookie, current_id, folders_only=True)
        entries = folder_payload.get("entries", []) if isinstance(folder_payload.get("entries"), list) else []
        matched = next(
            (
                entry
                for entry in entries
                if entry.get("is_dir") and str(entry.get("name", "")).strip() == part
            ),
            None,
        )
        if matched:
            current_id = str(matched.get("id", "") or "").strip() or current_id
            continue
        created = create_quark_folder(cookie, current_id, part)
        current_id = str(created.get("id", "")).strip() or current_id
    return current_id

def resolve_quark_folder_id_by_path(cookie: str, relative_path: str) -> str:
    normalized_path = normalize_relative_path(relative_path)
    if not normalized_path:
        return "0"
    current_id = "0"
    walked_parts: List[str] = []
    for part in [segment for segment in normalized_path.split("/") if segment]:
        walked_parts.append(part)
        folder_payload = list_quark_entries_payload(cookie, current_id, folders_only=True)
        entries = folder_payload.get("entries", []) if isinstance(folder_payload.get("entries"), list) else []
        matched = next(
            (
                entry
                for entry in entries
                if entry.get("is_dir") and str(entry.get("name", "")).strip() == part
            ),
            None,
        )
        if not matched:
            raise RuntimeError(f"夸克网盘目录不存在：{join_relative_path(*walked_parts)}")
        current_id = str(matched.get("id", "")).strip() or "0"
    return current_id

def _request_quark_share_token(
    cookie: str,
    pwd_id: str,
    passcode: str = "",
    force_refresh: bool = False,
    timeout: int = 45,
) -> str:
    normalized_pwd_id = str(pwd_id or "").strip()
    normalized_passcode = normalize_receive_code(passcode)
    cache_key = _build_quark_share_cache_key("token", cookie, normalized_pwd_id, normalized_passcode, "0")
    if not force_refresh:
        cached = _get_quark_memory_cache_payload(
            _quark_share_token_cache,
            _quark_share_token_cache_lock,
            cache_key,
            lambda value: dict(value if isinstance(value, dict) else {}),
        )
        cached_stoken = str(cached.get("stoken", "") or "").strip()
        if cached_stoken:
            return cached_stoken
    headers = _build_quark_headers(cookie, referer=f"https://pan.quark.cn/s/{pwd_id}")
    url = _build_quark_api_url(
        "/1/clouddrive/share/sharepage/token",
        {
            "uc_param_str": "",
            "__dt": 994,
            "__t": int(time.time() * 1000),
        },
        host="https://drive-h.quark.cn",
    )
    payload = {
        "pwd_id": normalized_pwd_id,
        "passcode": normalized_passcode,
    }
    response = _request_quark_json_payload(
        url,
        payload,
        headers,
        timeout=timeout,
        method="POST",
        fallback="夸克分享访问失败",
    )
    if not _is_quark_success(response):
        raise RuntimeError(_extract_quark_error(response, "夸克分享访问失败"))
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    stoken = str(data.get("stoken", "") or data.get("token", "")).strip()
    if not stoken:
        raise RuntimeError("夸克分享令牌获取失败")
    _set_quark_memory_cache_payload(
        _quark_share_token_cache,
        _quark_share_token_cache_lock,
        cache_key,
        {"stoken": stoken},
        lambda value: dict(value if isinstance(value, dict) else {}),
        _QUARK_SHARE_TOKEN_CACHE_MAX_ROWS,
    )
    return stoken

def _list_quark_share_page(
    cookie: str,
    pwd_id: str,
    stoken: str,
    pdir_fid: str = "0",
    page: int = 1,
    page_size: int = 200,
    force_refresh: bool = False,
    timeout: int = 45,
) -> Dict[str, Any]:
    normalized_pwd_id = str(pwd_id or "").strip()
    parent_id = str(pdir_fid or "0").strip() or "0"
    page_no = max(1, int(page or 1))
    normalized_size = max(20, min(200, int(page_size or 200)))
    cache_key = _build_quark_share_cache_key(
        "page",
        cookie,
        normalized_pwd_id,
        str(stoken or "").strip(),
        parent_id,
        extra=f"{page_no}|{normalized_size}",
    )
    if not force_refresh:
        cached = _get_quark_memory_cache_payload(
            _quark_share_page_cache,
            _quark_share_page_cache_lock,
            cache_key,
            _clone_quark_share_page_payload,
        )
        if cached:
            return cached
    headers = _build_quark_headers(cookie, referer=f"https://pan.quark.cn/s/{normalized_pwd_id}")
    # stoken 原样交给 urlencode 做单次编码。2026-10-20 真机实测：夸克服务端已
    # 改为单次 decode，旧的双重编码（手动 quote 再 urlencode）会让含 +/=/ 的
    # stoken 签名失配，返回 400「非法token」；纯字母数字的 stoken 碰巧两边
    # 相等所以时好时坏。
    stoken_query_value = str(stoken or "").strip()
    url = _build_quark_api_url(
        "/1/clouddrive/share/sharepage/detail",
        {
            "uc_param_str": "",
            "pwd_id": normalized_pwd_id,
            "stoken": stoken_query_value,
            "pdir_fid": parent_id,
            "force": 0,
            "_page": page_no,
            "_size": normalized_size,
            "_fetch_banner": 1,
            "_fetch_share": 1,
            "_fetch_total": 1,
            "_sort": "file_type:asc,file_name:asc",
            "__dt": 1589,
            "__t": int(time.time() * 1000),
        },
        host="https://drive-h.quark.cn",
    )
    result = _request_quark_json(url, headers, timeout=timeout, fallback="读取夸克分享目录失败")
    if not _is_quark_success(result):
        raise RuntimeError(_extract_quark_error(result, "读取夸克分享目录失败"))
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    raw_list = data.get("list") if isinstance(data.get("list"), list) else []
    entries = []
    for raw_item in raw_list:
        parsed_entry = _parse_quark_entry(raw_item if isinstance(raw_item, dict) else {}, parent_id=parent_id)
        if parsed_entry:
            entries.append(parsed_entry)
    share_info = data.get("share") if isinstance(data.get("share"), dict) else {}
    if not share_info:
        share_info = data.get("share_info") if isinstance(data.get("share_info"), dict) else {}
    has_more = bool(data.get("has_more", False))
    next_page = parse_int(data.get("next_page"), default=0)
    if not has_more and next_page > max(1, int(page or 1)):
        has_more = True
    total = parse_int(data.get("total"), default=len(entries))
    page_payload = {
        "entries": entries,
        "share": share_info,
        "has_more": has_more,
        "total": total,
        "next_page": next_page,
        "page": page_no,
        "size": normalized_size,
    }
    _set_quark_memory_cache_payload(
        _quark_share_page_cache,
        _quark_share_page_cache_lock,
        cache_key,
        page_payload,
        _clone_quark_share_page_payload,
        _QUARK_SHARE_PAGE_CACHE_MAX_ROWS,
    )
    return _clone_quark_share_page_payload(page_payload)


def _request_quark_share_token_fast(
    cookie: str,
    pwd_id: str,
    passcode: str,
    force_refresh: bool,
    deadline_ts: float,
) -> str:
    normalized_pwd_id = str(pwd_id or "").strip()
    normalized_passcode = normalize_receive_code(passcode)
    cache_key = _build_quark_share_cache_key("token", cookie, normalized_pwd_id, normalized_passcode, "0")
    if not force_refresh:
        cached = _get_quark_memory_cache_payload(
            _quark_share_token_cache,
            _quark_share_token_cache_lock,
            cache_key,
            lambda value: dict(value if isinstance(value, dict) else {}),
        )
        cached_stoken = str(cached.get("stoken", "") or "").strip()
        if cached_stoken:
            return cached_stoken

    def request_token() -> str:
        if not force_refresh:
            cached_inner = _get_quark_memory_cache_payload(
                _quark_share_token_cache,
                _quark_share_token_cache_lock,
                cache_key,
                lambda value: dict(value if isinstance(value, dict) else {}),
            )
            cached_inner_stoken = str(cached_inner.get("stoken", "") or "").strip()
            if cached_inner_stoken:
                return cached_inner_stoken

        headers = _build_quark_headers(cookie, referer=f"https://pan.quark.cn/s/{normalized_pwd_id}")
        url = _build_quark_api_url(
            "/1/clouddrive/share/sharepage/token",
            {
                "uc_param_str": "",
                "__dt": 994,
                "__t": int(time.time() * 1000),
            },
            host="https://drive-h.quark.cn",
        )
        response = _request_quark_json_fast(
            "POST",
            url,
            headers,
            deadline_ts,
            payload={
                "pwd_id": normalized_pwd_id,
                "passcode": normalized_passcode,
            },
            fallback="夸克分享访问失败",
        )
        if not _is_quark_success(response):
            raise RuntimeError(_extract_quark_error(response, "夸克分享访问失败"))
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        stoken = str(data.get("stoken", "") or data.get("token", "")).strip()
        if not stoken:
            raise RuntimeError("夸克分享令牌获取失败")
        _set_quark_memory_cache_payload(
            _quark_share_token_cache,
            _quark_share_token_cache_lock,
            cache_key,
            {"stoken": stoken},
            lambda value: dict(value if isinstance(value, dict) else {}),
            _QUARK_SHARE_TOKEN_CACHE_MAX_ROWS,
        )
        return stoken

    try:
        return _run_quark_share_singleflight(f"quark-token:{cache_key}", deadline_ts, request_token)
    except Exception:
        stale = _get_quark_memory_cache_payload(
            _quark_share_token_cache,
            _quark_share_token_cache_lock,
            cache_key,
            lambda value: dict(value if isinstance(value, dict) else {}),
            allow_expired=True,
        )
        stale_stoken = str(stale.get("stoken", "") or "").strip()
        if stale_stoken:
            return stale_stoken
        raise


def _list_quark_share_page_fast(
    cookie: str,
    pwd_id: str,
    stoken: str,
    pdir_fid: str,
    page: int,
    page_size: int,
    force_refresh: bool,
    deadline_ts: float,
) -> Dict[str, Any]:
    normalized_pwd_id = str(pwd_id or "").strip()
    parent_id = str(pdir_fid or "0").strip() or "0"
    page_no = max(1, int(page or 1))
    normalized_size = max(20, min(100, int(page_size or 50)))
    cache_key = _build_quark_share_cache_key(
        "fast_page",
        cookie,
        normalized_pwd_id,
        str(stoken or "").strip(),
        parent_id,
        extra=f"{page_no}|{normalized_size}",
    )
    if not force_refresh:
        cached = _get_quark_memory_cache_payload(
            _quark_share_page_cache,
            _quark_share_page_cache_lock,
            cache_key,
            _clone_quark_share_page_payload,
        )
        if cached:
            return cached

    def request_page() -> Dict[str, Any]:
        if not force_refresh:
            cached_inner = _get_quark_memory_cache_payload(
                _quark_share_page_cache,
                _quark_share_page_cache_lock,
                cache_key,
                _clone_quark_share_page_payload,
            )
            if cached_inner:
                return cached_inner

        headers = _build_quark_headers(cookie, referer=f"https://pan.quark.cn/s/{normalized_pwd_id}")
        # 单次编码，原因同 _list_quark_share_page：服务端单次 decode，双编码会 400「非法token」。
        stoken_query_value = str(stoken or "").strip()
        url = _build_quark_api_url(
            "/1/clouddrive/share/sharepage/detail",
            {
                "uc_param_str": "",
                "pwd_id": normalized_pwd_id,
                "stoken": stoken_query_value,
                "pdir_fid": parent_id,
                "force": 0,
                "_page": page_no,
                "_size": normalized_size,
                "_fetch_banner": 1,
                "_fetch_share": 1,
                "_fetch_total": 1,
                "_sort": "file_type:asc,updated_at:desc",
                "__dt": 1589,
                "__t": int(time.time() * 1000),
            },
            host="https://drive-h.quark.cn",
        )
        result = _request_quark_json_fast("GET", url, headers, deadline_ts, fallback="读取夸克分享目录失败")
        if not _is_quark_success(result):
            raise RuntimeError(_extract_quark_error(result, "读取夸克分享目录失败"))
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        raw_list = data.get("list") if isinstance(data.get("list"), list) else []
        entries = []
        for raw_item in raw_list:
            parsed_entry = _parse_quark_entry(raw_item if isinstance(raw_item, dict) else {}, parent_id=parent_id)
            if parsed_entry:
                entries.append(parsed_entry)
        share_info = data.get("share") if isinstance(data.get("share"), dict) else {}
        if not share_info:
            share_info = data.get("share_info") if isinstance(data.get("share_info"), dict) else {}
        has_more = bool(data.get("has_more", False))
        next_page = parse_int(data.get("next_page"), default=0)
        if not has_more and next_page > page_no:
            has_more = True
        page_payload = {
            "entries": entries,
            "share": share_info,
            "has_more": has_more,
            "total": parse_int(data.get("total"), default=len(entries)),
            "next_page": next_page,
            "page": page_no,
            "size": normalized_size,
        }
        _set_quark_memory_cache_payload(
            _quark_share_page_cache,
            _quark_share_page_cache_lock,
            cache_key,
            page_payload,
            _clone_quark_share_page_payload,
            _QUARK_SHARE_PAGE_CACHE_MAX_ROWS,
        )
        return _clone_quark_share_page_payload(page_payload)

    try:
        return _run_quark_share_singleflight(f"quark-page:{cache_key}", deadline_ts, request_page)
    except Exception:
        stale = _get_quark_memory_cache_payload(
            _quark_share_page_cache,
            _quark_share_page_cache_lock,
            cache_key,
            _clone_quark_share_page_payload,
            allow_expired=True,
        )
        if stale:
            stale["cache_error"] = "夸克上游响应超时，已返回旧缓存"
            return stale
        raise


def list_quark_share_entries_fast(
    cookie: str,
    share_url: str,
    raw_text: str = "",
    cid: str = "0",
    receive_code: str = "",
    force_refresh: bool = False,
    request_timeout: int = 3,
    offset: int = 0,
    limit: int = 50,
    max_pages: int = 1,
    folders_only: bool = False,
) -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")

    parsed = parse_quark_share_payload(share_url, raw_text, receive_code)
    pwd_id = str(parsed.get("pwd_id", "") or parsed.get("share_code", "")).strip()
    if not pwd_id:
        raise RuntimeError("未能识别夸克分享链接")
    receive_code_value = normalize_receive_code(parsed.get("receive_code", ""))
    current_cid = str(cid or "0").strip() or "0"
    page_limit = max(20, min(100, int(limit or 50)))
    start_offset = max(0, int(offset or 0))
    page_no = max(1, (start_offset // page_limit) + 1)
    skip_in_page = start_offset % page_limit
    folder_only_mode = bool(folders_only)
    deadline_seconds = max(1.0, min(8.0, float(request_timeout or _QUARK_SHARE_FAST_DEADLINE_SECONDS)))
    deadline_ts = time.monotonic() + deadline_seconds
    started_mono = time.monotonic()
    last_mono = started_mono
    timings: List[Dict[str, Any]] = []
    result_cache_key = _build_quark_share_cache_key(
        "fast_result",
        normalized_cookie,
        pwd_id,
        receive_code_value,
        current_cid,
        extra=f"{start_offset}|{page_limit}|{page_no}|{1 if folder_only_mode else 0}",
    )
    if not force_refresh:
        cached_result = _get_quark_memory_cache_payload(
            _quark_share_result_cache,
            _quark_share_result_cache_lock,
            result_cache_key,
            _clone_quark_share_result_payload,
        )
        if cached_result:
            return cached_result

    def request_result() -> Dict[str, Any]:
        nonlocal last_mono
        if not force_refresh:
            cached_inner = _get_quark_memory_cache_payload(
                _quark_share_result_cache,
                _quark_share_result_cache_lock,
                result_cache_key,
                _clone_quark_share_result_payload,
            )
            if cached_inner:
                return cached_inner

        stoken = _request_quark_share_token_fast(
            normalized_cookie,
            pwd_id,
            receive_code_value,
            force_refresh=force_refresh,
            deadline_ts=deadline_ts,
        )
        elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
        timings.append({"stage": "token", "label": "令牌", "ms": elapsed_ms})
        last_mono = next_last_mono
        page_payload = _list_quark_share_page_fast(
            normalized_cookie,
            pwd_id,
            stoken,
            current_cid,
            page=page_no,
            page_size=page_limit,
            force_refresh=force_refresh,
            deadline_ts=deadline_ts,
        )
        elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
        timings.append({"stage": "detail", "label": "目录", "ms": elapsed_ms})
        last_mono = next_last_mono
        page_entries = page_payload.get("entries", []) if isinstance(page_payload.get("entries"), list) else []
        if skip_in_page > 0:
            page_entries = page_entries[skip_in_page:]
        if folder_only_mode:
            page_entries = [entry for entry in page_entries if entry.get("is_dir")]
        total_count = max(0, int(page_payload.get("total", 0) or 0))
        next_offset = start_offset + len(page_entries)
        share_info = page_payload.get("share", {}) if isinstance(page_payload.get("share"), dict) else {}
        result_payload = {
            "entries": page_entries,
            "summary": {
                "folder_count": sum(1 for item in page_entries if item.get("is_dir")),
                "file_count": sum(1 for item in page_entries if not item.get("is_dir")),
            },
            "share_code": pwd_id,
            "receive_code": receive_code_value,
            "share_title": str(share_info.get("title", "") or share_info.get("share_name", "") or "").strip(),
            "current_cid": current_cid,
            "count": total_count or len(page_entries),
            "offset": start_offset,
            "next_offset": next_offset,
            "has_more": bool(page_payload.get("has_more", False)),
            "pages_scanned": 1,
            "stoken": stoken,
            "fast_path": True,
            "elapsed_ms": int((time.monotonic() - started_mono) * 1000),
            "timings": timings,
        }
        _log_quark_share_timing("fast", pwd_id, current_cid, timings, int(result_payload.get("elapsed_ms", 0) or 0))
        _set_quark_memory_cache_payload(
            _quark_share_result_cache,
            _quark_share_result_cache_lock,
            result_cache_key,
            result_payload,
            _clone_quark_share_result_payload,
            _QUARK_SHARE_RESULT_CACHE_MAX_ROWS,
        )
        return _clone_quark_share_result_payload(result_payload)

    try:
        return _run_quark_share_singleflight(f"quark-result:{result_cache_key}", deadline_ts, request_result)
    except Exception as exc:
        stale_result = _get_quark_memory_cache_payload(
            _quark_share_result_cache,
            _quark_share_result_cache_lock,
            result_cache_key,
            _clone_quark_share_result_payload,
            allow_expired=True,
        )
        if stale_result:
            stale_result["cache_error"] = str(exc or "夸克上游响应超时").strip()[:180]
            stale_result["cache_cid"] = current_cid
            return stale_result
        raise

def _collect_quark_share_entries_with_stoken(
    cookie: str,
    pwd_id: str,
    stoken: str,
    cid: str = "0",
    page_size: int = 200,
    max_pages: int = 0,
    folders_only: bool = False,
    force_refresh: bool = False,
    request_timeout: int = 45,
) -> Dict[str, Any]:
    current_cid = str(cid or "0").strip() or "0"
    page_limit = max(20, min(200, int(page_size or 200)))
    max_pages_limit = max(0, int(max_pages or 0))
    folder_only_mode = bool(folders_only)
    entries: List[Dict[str, Any]] = []
    pages_scanned = 0
    total_count = 0
    share_title = ""
    last_has_more = False
    reached_page_cap = False

    page_no = 1
    while True:
        page_payload = _list_quark_share_page(
            cookie,
            pwd_id,
            stoken,
            current_cid,
            page=page_no,
            page_size=page_limit,
            force_refresh=force_refresh,
            timeout=request_timeout,
        )
        page_entries = page_payload.get("entries", []) if isinstance(page_payload.get("entries"), list) else []
        if folder_only_mode:
            page_entries = [entry for entry in page_entries if entry.get("is_dir")]
        entries.extend(page_entries)
        total_count = max(total_count, int(page_payload.get("total", 0) or 0))
        pages_scanned += 1
        share_info = page_payload.get("share", {}) if isinstance(page_payload.get("share"), dict) else {}
        if share_info and not share_title:
            share_title = str(share_info.get("title", "") or share_info.get("share_name", "") or "").strip()
        last_has_more = bool(page_payload.get("has_more", False))
        reached_page_cap = max_pages_limit > 0 and pages_scanned >= max_pages_limit
        if not last_has_more:
            break
        if reached_page_cap:
            break
        page_no += 1

    entries.sort(key=lambda item: (0 if item.get("is_dir") else 1, str(item.get("name", "")).lower()))
    return {
        "entries": entries,
        "share_title": share_title,
        "count": total_count or len(entries),
        "pages_scanned": pages_scanned,
        "has_more": bool(last_has_more and reached_page_cap),
    }

def list_quark_share_entries(
    cookie: str,
    share_url: str,
    raw_text: str = "",
    cid: str = "0",
    receive_code: str = "",
    force_refresh: bool = False,
    request_timeout: int = 45,
    offset: int = 0,
    limit: int = 200,
    max_pages: int = 0,
    folders_only: bool = False,
) -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    try:
        started_mono = time.monotonic()
        last_mono = started_mono
        timings: List[Dict[str, Any]] = []
        parsed = resolve_quark_share_payload(normalized_cookie, share_url, raw_text, receive_code)
        elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
        timings.append({"stage": "parse", "label": "链接", "ms": elapsed_ms})
        last_mono = next_last_mono
        pwd_id = str(parsed.get("pwd_id", "") or parsed.get("share_code", "")).strip()
        if not pwd_id:
            raise RuntimeError("未能识别夸克分享链接")
        receive_code_value = normalize_receive_code(parsed.get("receive_code", ""))
        current_cid = str(cid or "0").strip() or "0"
        page_limit = max(20, min(200, int(limit or 200)))
        start_offset = max(0, int(offset or 0))
        max_pages_limit = max(0, int(max_pages or 0))
        folder_only_mode = bool(folders_only)
        request_timeout_value = max(5, int(request_timeout or 45))

        result_cache_key = _build_quark_share_cache_key(
            "result",
            normalized_cookie,
            pwd_id,
            receive_code_value,
            current_cid,
            extra=f"{start_offset}|{page_limit}|{max_pages_limit}|{1 if folder_only_mode else 0}",
        )
        if not force_refresh:
            cached_result = _get_quark_memory_cache_payload(
                _quark_share_result_cache,
                _quark_share_result_cache_lock,
                result_cache_key,
                _clone_quark_share_result_payload,
            )
            if cached_result:
                return cached_result

        stoken = _request_quark_share_token(
            normalized_cookie,
            pwd_id,
            receive_code_value,
            force_refresh=force_refresh,
            timeout=request_timeout_value,
        )
        elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
        timings.append({"stage": "token", "label": "令牌", "ms": elapsed_ms})
        last_mono = next_last_mono
        entries: List[Dict[str, Any]] = []
        total_count = 0
        pages_scanned = 0
        share_title = ""

        if start_offset <= 0:
            page_no = 1
            last_has_more = False
            reached_page_cap = False
            while True:
                page_payload = _list_quark_share_page(
                    normalized_cookie,
                    pwd_id,
                    stoken,
                    current_cid,
                    page=page_no,
                    page_size=page_limit,
                    force_refresh=force_refresh,
                    timeout=request_timeout_value,
                )
                elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
                timings.append({"stage": f"detail_page_{page_no}", "label": f"目录P{page_no}", "ms": elapsed_ms})
                last_mono = next_last_mono
                page_entries = page_payload.get("entries", []) if isinstance(page_payload.get("entries"), list) else []
                if folder_only_mode:
                    page_entries = [entry for entry in page_entries if entry.get("is_dir")]
                entries.extend(page_entries)
                total_count = max(total_count, int(page_payload.get("total", 0) or 0))
                pages_scanned += 1
                share_info = page_payload.get("share", {}) if isinstance(page_payload.get("share"), dict) else {}
                if share_info and not share_title:
                    share_title = str(share_info.get("title", "") or share_info.get("share_name", "") or "").strip()
                last_has_more = bool(page_payload.get("has_more", False))
                reached_page_cap = max_pages_limit > 0 and pages_scanned >= max_pages_limit
                if not last_has_more:
                    break
                if reached_page_cap:
                    break
                page_no += 1
            has_more = bool(last_has_more and reached_page_cap)
            next_offset = len(entries)
        else:
            page_no = max(1, (start_offset // page_limit) + 1)
            skip_in_page = start_offset % page_limit
            page_payload = _list_quark_share_page(
                normalized_cookie,
                pwd_id,
                stoken,
                current_cid,
                page=page_no,
                page_size=page_limit,
                force_refresh=force_refresh,
                timeout=request_timeout_value,
            )
            elapsed_ms, next_last_mono = _build_quark_timing_mark(started_mono, last_mono)
            timings.append({"stage": f"detail_page_{page_no}", "label": f"目录P{page_no}", "ms": elapsed_ms})
            last_mono = next_last_mono
            page_entries = page_payload.get("entries", []) if isinstance(page_payload.get("entries"), list) else []
            if skip_in_page > 0:
                page_entries = page_entries[skip_in_page:]
            if folder_only_mode:
                page_entries = [entry for entry in page_entries if entry.get("is_dir")]
            entries = page_entries
            total_count = max(0, int(page_payload.get("total", 0) or 0))
            pages_scanned = 1
            has_more = bool(page_payload.get("has_more", False))
            next_offset = start_offset + len(entries)
            share_info = page_payload.get("share", {}) if isinstance(page_payload.get("share"), dict) else {}
            share_title = str(share_info.get("title", "") or share_info.get("share_name", "") or "").strip()
            if max_pages_limit > 0 and max_pages_limit <= pages_scanned:
                has_more = False

        entries.sort(key=lambda item: (0 if item.get("is_dir") else 1, str(item.get("name", "")).lower()))
        result_payload = {
            "entries": entries,
            "summary": {
                "folder_count": sum(1 for item in entries if item.get("is_dir")),
                "file_count": sum(1 for item in entries if not item.get("is_dir")),
            },
            "share_code": pwd_id,
            "receive_code": receive_code_value,
            "share_title": share_title,
            "current_cid": current_cid,
            "count": total_count or len(entries),
            "offset": start_offset,
            "next_offset": next_offset,
            "has_more": bool(has_more),
            "pages_scanned": pages_scanned,
            "truncated": bool(has_more),
            "truncated_reason": "page_cap" if bool(has_more) else "",
            "stoken": stoken,
            "elapsed_ms": int((time.monotonic() - started_mono) * 1000),
            "timings": timings,
        }
        _log_quark_share_timing("full", pwd_id, current_cid, timings, int(result_payload.get("elapsed_ms", 0) or 0))
        _set_quark_memory_cache_payload(
            _quark_share_result_cache,
            _quark_share_result_cache_lock,
            result_cache_key,
            result_payload,
            _clone_quark_share_result_payload,
            _QUARK_SHARE_RESULT_CACHE_MAX_ROWS,
        )
        return _clone_quark_share_result_payload(result_payload)
    except Exception:
        raise

def prepare_quark_share_save(
    cookie: str,
    share_url: str,
    raw_text: str = "",
    selected_ids: Optional[List[str]] = None,
    receive_code: str = "",
    selected_entries: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    parsed = resolve_quark_share_payload(cookie, share_url, raw_text, receive_code)
    pwd_id = str(parsed.get("pwd_id", "") or parsed.get("share_code", "")).strip()
    receive_code_value = normalize_receive_code(parsed.get("receive_code", ""))
    stoken = _request_quark_share_token(cookie, pwd_id, receive_code_value)

    normalized_ids: List[str] = []
    seen_ids: Set[str] = set()
    selected_entry_token_map: Dict[str, str] = {}
    for entry in selected_entries or []:
        normalized_entry = normalize_share_selection_entry(entry)
        entry_id = str(normalized_entry.get("id", "")).strip()
        if not entry_id:
            continue
        token = str(normalized_entry.get("fid_token", "") or "").strip()
        if token:
            selected_entry_token_map[entry_id] = token

    for raw_id in selected_ids or []:
        entry_id = str(raw_id or "").strip()
        if not entry_id or entry_id in seen_ids:
            continue
        seen_ids.add(entry_id)
        normalized_ids.append(entry_id)

    selection: Dict[str, Any] = {}
    fid_token_list: List[str] = []
    if not normalized_ids:
        snapshot = _collect_quark_share_entries_with_stoken(
            cookie,
            pwd_id,
            stoken,
            cid="0",
            page_size=200,
            max_pages=0,
        )
        snapshot_entries = snapshot.get("entries", []) if isinstance(snapshot.get("entries"), list) else []
        normalized_ids = [str(entry.get("id", "")).strip() for entry in snapshot_entries if str(entry.get("id", "")).strip()]
        fid_token_list = [str(entry.get("fid_token", "") or "").strip() for entry in snapshot_entries if str(entry.get("id", "")).strip()]
        selection = normalize_share_selection_meta(
            {
                "selected_ids": normalized_ids,
                "selected_entries": snapshot_entries,
                "share_root_title": snapshot.get("share_title", ""),
            }
        )
    else:
        snapshot_token_map: Dict[str, str] = dict(selected_entry_token_map)
        scan_cids: List[str] = ["0"]
        seen_scan_cids: Set[str] = {"0"}
        for entry in selected_entries or []:
            normalized_entry = normalize_share_selection_entry(entry)
            parent_id = str(normalized_entry.get("parent_id", "") or "").strip()
            if parent_id and parent_id not in seen_scan_cids:
                seen_scan_cids.add(parent_id)
                scan_cids.append(parent_id)
            if bool(normalized_entry.get("is_dir")):
                child_cid = str(normalized_entry.get("cid", "") or normalized_entry.get("id", "") or "").strip()
                if child_cid and child_cid not in seen_scan_cids:
                    seen_scan_cids.add(child_cid)
                    scan_cids.append(child_cid)

        for scan_cid in scan_cids:
            try:
                snapshot = _collect_quark_share_entries_with_stoken(
                    cookie,
                    pwd_id,
                    stoken,
                    cid=scan_cid,
                    page_size=200,
                    max_pages=0,
                )
            except Exception:
                continue
            snapshot_entries = snapshot.get("entries", []) if isinstance(snapshot.get("entries"), list) else []
            for entry in snapshot_entries:
                entry_id = str(entry.get("id", "")).strip()
                if not entry_id or entry_id in snapshot_token_map:
                    continue
                fid_token = str(entry.get("fid_token", "") or "").strip()
                if fid_token:
                    snapshot_token_map[entry_id] = fid_token

        fid_token_list = [str(snapshot_token_map.get(entry_id, "") or "").strip() for entry_id in normalized_ids]
        missing_token_ids = [entry_id for index, entry_id in enumerate(normalized_ids) if not fid_token_list[index]]
        if missing_token_ids:
            raise RuntimeError(
                f"夸克分享条目 token 刷新失败，缺失 {len(missing_token_ids)} 项（示例: {missing_token_ids[0]}）"
            )

    if len(fid_token_list) != len(normalized_ids):
        fid_token_list = []
    elif normalized_ids and not any(token for token in fid_token_list):
        fid_token_list = []

    if not normalized_ids:
        raise RuntimeError("分享内容为空，无法转存")
    return {
        "pwd_id": pwd_id,
        "stoken": stoken,
        "receive_code": receive_code_value,
        "fid_list": normalized_ids,
        "fid_token_list": fid_token_list,
        "selection": selection,
        "pdir_fid": "0",
    }

def submit_quark_share_save(
    cookie: str,
    share_url: str,
    folder_id: str,
    raw_text: str = "",
    selected_ids: Optional[List[str]] = None,
    receive_code: str = "",
    selected_entries: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    try:
        prepared = prepare_quark_share_save(
            normalized_cookie,
            share_url,
            raw_text,
            selected_ids,
            receive_code,
            selected_entries=selected_entries,
        )

        headers = _build_quark_headers(
            normalized_cookie,
            referer=f"https://pan.quark.cn/s/{str(prepared.get('pwd_id', '')).strip()}",
        )
        payload = {
            "pwd_id": str(prepared.get("pwd_id", "")).strip(),
            "stoken": str(prepared.get("stoken", "")).strip(),
            "pdir_fid": str(prepared.get("pdir_fid", "0")).strip() or "0",
            "to_pdir_fid": str(folder_id or "0").strip() or "0",
            "fid_list": prepared.get("fid_list", []),
            "fid_token_list": prepared.get("fid_token_list", []),
            "scene": "link",
        }

        hosts = ("https://drive-h.quark.cn", "https://drive-pc.quark.cn")
        last_error = ""
        response: Dict[str, Any] = {}
        for host in hosts:
            url = _build_quark_api_url("/1/clouddrive/share/sharepage/save", host=host)
            try:
                response = _request_quark_json_payload(
                    url,
                    payload,
                    headers,
                    timeout=45,
                    method="POST",
                    fallback="夸克网盘转存失败",
                )
            except RuntimeError as exc:
                last_error = str(exc or "夸克网盘转存失败").strip() or "夸克网盘转存失败"
                continue

            if _is_quark_success(response):
                last_error = ""
                break
            last_error = _extract_quark_error(response, "夸克网盘转存失败")

        if last_error:
            raise RuntimeError(last_error)

        return {
            "response": response,
            "selection": prepared.get("selection", {}),
        }
    except Exception:
        raise


# === 上传能力（秒传优先：md5+sha1 双哈希 → 未命中走 OSS 分片） ===
# 链路（对齐社区成熟实现 athunderx/OpenList quark driver）：
#   /file/upload/pre   → task_id / bucket / obj_key / upload_id / upload_url / auth_info / callback / part_size
#   /file/update/hash  → data.finish=true 即秒传命中（零上传流量）
#   每片 /file/upload/auth 拿 auth_key → OSS PUT（partNumber + uploadId）
#   /file/upload/auth（commit 版）→ OSS POST CompleteMultipartUpload XML（Content-MD5 + x-oss-callback）
#   /file/upload/finish 收尾
_QUARK_UPLOAD_DEFAULT_PART_SIZE = 4 * 1024 * 1024
_QUARK_UPLOAD_PART_SIZE_MIN = 100 * 1024
_QUARK_UPLOAD_PART_SIZE_MAX = 64 * 1024 * 1024
_QUARK_OSS_SDK_USER_AGENT = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"


def _guess_quark_mime_type(filename: str) -> str:
    guessed, _ = mimetypes.guess_type(str(filename or ""))
    return str(guessed or "application/octet-stream")


def _map_quark_api_error(resp: Any) -> str:
    """把夸克接口错误映射为面向用户的文案（上传/下载等写操作共用）。"""
    payload = resp if isinstance(resp, dict) else {}
    code = parse_int(payload.get("code"), default=0)
    message = _extract_quark_error(payload, "")
    lowered = message.lower()
    if code in (32011, 32012, 32013) or "cookie" in lowered or "登录" in message or "登录" in lowered:
        return "夸克登录已失效，请更新 Cookie 后重试"
    if "验证" in message or "captcha" in lowered or "风控" in message:
        return "夸克触发安全验证，请稍后再试或更新 Cookie"
    if _is_quark_size_limit_error(payload, message):
        return _QUARK_SIZE_LIMIT_HINT
    if "频繁" in message or "limit" in lowered or "频率" in message:
        return "夸克操作过于频繁，请稍后再试"
    return message or "夸克上传失败，请稍后重试"


def _quark_upload_pre(
    cookie: str, filename: str, mime_type: str, pdir_fid: str, size: int
) -> Dict[str, Any]:
    """上传预检：换取 task_id 与 OSS 上传参数。"""
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    url = _build_quark_api_url("/1/clouddrive/file/upload/pre")
    now_ms = int(time.time() * 1000)
    payload = {
        "ccp_hash_update": True,
        "dir_name": "",
        "file_name": str(filename or ""),
        "format_type": str(mime_type or "application/octet-stream"),
        "l_created_at": now_ms,
        "l_updated_at": now_ms,
        "pdir_fid": str(pdir_fid or "0").strip() or "0",
        "size": max(0, int(size or 0)),
    }
    response = _request_quark_json_payload(
        url,
        payload,
        headers,
        timeout=45,
        method="POST",
        fallback="夸克上传初始化失败",
    )
    if not _is_quark_success(response):
        raise UploadError(_map_quark_api_error(response), provider="quark")
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    metadata = response.get("metadata") if isinstance(response.get("metadata"), dict) else {}
    if not str(data.get("task_id", "") or "").strip():
        raise UploadError("夸克上传初始化失败（未返回任务 ID）", retriable=True, provider="quark")
    pre = dict(data)
    pre["part_size"] = parse_int(
        metadata.get("part_size"), default=_QUARK_UPLOAD_DEFAULT_PART_SIZE
    )
    return pre


def _quark_upload_hash(cookie: str, task_id: str, md5: str, sha1: str) -> bool:
    """秒传检测：finish=true 表示云端已有同哈希文件。

    **md5 与 sha1 都必须传小写**（真机 2026-10-20 实测）：夸克这个接口在命中
    云端已有对象时会拿小写 sha1 去比对，传大写直接返回 400
    ``invalid file [sha1 非法]``；同一请求改成小写立刻 200 且 ``finish=true``。
    未命中时它不校验大小写，所以大写能"看起来正常"地返回 finish=false，
    直到哪天云端真的有这个文件才炸——这正是 115 → 夸克 复制失败的根因。
    """
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    url = _build_quark_api_url("/1/clouddrive/file/update/hash")
    response = _request_quark_json_payload(
        url,
        {"md5": str(md5 or "").strip().lower(), "sha1": str(sha1 or "").strip().lower(), "task_id": str(task_id or "")},
        headers,
        timeout=45,
        method="POST",
        fallback="夸克秒传检测失败",
    )
    if not _is_quark_success(response):
        raise UploadError(_map_quark_api_error(response), provider="quark")
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    return bool(data.get("finish", False))


def _quark_oss_upload_url(pre: Dict[str, Any]) -> str:
    """由 upload_pre 返回构造 OSS 端点（upload_url 形如 https://upload-host）。"""
    bucket = str(pre.get("bucket", "") or "").strip()
    obj_key = str(pre.get("obj_key", "") or "").strip()
    upload_url = str(pre.get("upload_url", "") or "").strip()
    if upload_url.startswith("https://"):
        upload_url = upload_url[len("https://"):]
    elif upload_url.startswith("http://"):
        upload_url = upload_url[len("http://"):]
    if not bucket or not obj_key or not upload_url:
        raise UploadError("夸克上传配置缺失（OSS 参数不完整）", retriable=True, provider="quark")
    return f"https://{bucket}.{upload_url.rstrip('/')}/{obj_key}"


def _quark_upload_auth(cookie: str, pre: Dict[str, Any], auth_meta: str) -> str:
    """向夸克换取 OSS 临时授权 auth_key。"""
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    url = _build_quark_api_url("/1/clouddrive/file/upload/auth")
    response = _request_quark_json_payload(
        url,
        {
            "auth_info": str(pre.get("auth_info", "") or ""),
            "auth_meta": auth_meta,
            "task_id": str(pre.get("task_id", "") or ""),
        },
        headers,
        timeout=45,
        method="POST",
        fallback="夸克上传授权失败",
    )
    if not _is_quark_success(response):
        raise UploadError(_map_quark_api_error(response), provider="quark")
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    auth_key = str(data.get("auth_key", "") or "").strip()
    if not auth_key:
        raise UploadError("夸克上传授权失败（未返回 auth_key）", retriable=True, provider="quark")
    return auth_key


def _quark_upload_part(
    cookie: str, pre: Dict[str, Any], mime_type: str, part_number: int, chunk: bytes
) -> str:
    """单片上传：授权 → OSS PUT。返回 ETag。"""
    bucket = str(pre.get("bucket", "") or "").strip()
    obj_key = str(pre.get("obj_key", "") or "").strip()
    upload_id = str(pre.get("upload_id", "") or "").strip()
    time_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    is_multipart = bool(upload_id)
    resource = f"/{bucket}/{obj_key}"
    if is_multipart:
        resource = f"{resource}?partNumber={int(part_number)}&uploadId={upload_id}"
    auth_meta = (
        f"PUT\n\n{mime_type}\n{time_str}\n"
        f"x-oss-date:{time_str}\n"
        f"x-oss-user-agent:{_QUARK_OSS_SDK_USER_AGENT}\n"
        f"{resource}"
    )
    auth_key = _quark_upload_auth(cookie, pre, auth_meta)
    oss_url = _quark_oss_upload_url(pre)
    try:
        response = _get_quark_http_session().put(
            normalize_http_url(oss_url),
            params={"partNumber": str(int(part_number)), "uploadId": upload_id} if is_multipart else None,
            data=chunk,
            headers={
                "Authorization": auth_key,
                "Content-Type": str(mime_type or "application/octet-stream"),
                "Referer": "https://pan.quark.cn/",
                "x-oss-date": time_str,
                "x-oss-user-agent": _QUARK_OSS_SDK_USER_AGENT,
            },
            timeout=max(30, min(600, 60 + len(chunk) // (1024 * 1024))),
        )
    except Exception as exc:
        raise UploadError(
            f"夸克分片 {part_number} 上传失败（网络错误）", retriable=True, provider="quark"
        ) from exc
    if int(response.status_code or 0) != 200:
        raise UploadError(
            f"夸克分片 {part_number} 上传失败（HTTP {int(response.status_code or 0)}）",
            retriable=True,
            provider="quark",
        )
    etag = str(response.headers.get("ETag", "") or "").strip().strip('"')
    if not etag:
        raise UploadError(f"夸克分片 {part_number} 未返回 ETag", retriable=True, provider="quark")
    return etag


def _quark_upload_commit(cookie: str, pre: Dict[str, Any], etags: List[str]) -> None:
    """OSS CompleteMultipartUpload（callback 由夸克服务端把对象转存进网盘）。"""
    bucket = str(pre.get("bucket", "") or "").strip()
    obj_key = str(pre.get("obj_key", "") or "").strip()
    upload_id = str(pre.get("upload_id", "") or "").strip()
    time_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    body_parts = ['<?xml version="1.0" encoding="UTF-8"?>\n<CompleteMultipartUpload>\n']
    for index, etag in enumerate(etags or []):
        body_parts.append(
            f"<Part>\n<PartNumber>{index + 1}</PartNumber>\n<ETag>{etag}</ETag>\n</Part>\n"
        )
    body_parts.append("</CompleteMultipartUpload>")
    body = "".join(body_parts)
    content_md5 = base64.b64encode(hashlib.md5(body.encode("utf-8")).digest()).decode("ascii")
    callback_bytes = json.dumps(
        pre.get("callback") if isinstance(pre.get("callback"), dict) else {},
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    callback_b64 = base64.b64encode(callback_bytes).decode("ascii")
    auth_meta = (
        f"POST\n{content_md5}\napplication/xml\n{time_str}\n"
        f"x-oss-callback:{callback_b64}\n"
        f"x-oss-date:{time_str}\n"
        f"x-oss-user-agent:{_QUARK_OSS_SDK_USER_AGENT}\n"
        f"/{bucket}/{obj_key}?uploadId={upload_id}"
    )
    auth_key = _quark_upload_auth(cookie, pre, auth_meta)
    oss_url = _quark_oss_upload_url(pre)
    try:
        response = _get_quark_http_session().post(
            normalize_http_url(oss_url),
            params={"uploadId": upload_id} if upload_id else None,
            data=body.encode("utf-8"),
            headers={
                "Authorization": auth_key,
                "Content-MD5": content_md5,
                "Content-Type": "application/xml",
                "Referer": "https://pan.quark.cn/",
                "x-oss-callback": callback_b64,
                "x-oss-date": time_str,
                "x-oss-user-agent": _QUARK_OSS_SDK_USER_AGENT,
            },
            timeout=60,
        )
    except Exception as exc:
        raise UploadError("夸克上传提交失败（网络错误）", retriable=True, provider="quark") from exc
    if int(response.status_code or 0) != 200:
        raise UploadError(
            f"夸克上传提交失败（HTTP {int(response.status_code or 0)}）", retriable=True, provider="quark"
        )


def _quark_upload_finish(cookie: str, pre: Dict[str, Any]) -> None:
    """上传收尾（失败不阻断主流程，OSS 侧已完成转存）。"""
    headers = _build_quark_headers(cookie, referer="https://pan.quark.cn/")
    url = _build_quark_api_url("/1/clouddrive/file/upload/finish")
    try:
        response = _request_quark_json_payload(
            url,
            {"obj_key": str(pre.get("obj_key", "") or ""), "task_id": str(pre.get("task_id", "") or "")},
            headers,
            timeout=30,
            method="POST",
            fallback="夸克上传收尾失败",
        )
        if not _is_quark_success(response):
            logging.info(
                "Quark upload finish non-fatal: %s", _extract_quark_error(response, "")
            )
    except Exception as exc:
        logging.info("Quark upload finish non-fatal: %s", exc)


def upload_quark_file(
    cookie: str,
    sink: Any,
    filename: str,
    target_dir_id: str = "0",
    *,
    md5: str = "",
    sha1: str = "",
    size: int = -1,
    progress_cb=None,
    cancel_check=None,
) -> UploadResult:
    """把 sink 上传到夸克网盘（秒传优先，未命中走 OSS 分片）。

    md5/sha1 由复制引擎在下载时同步算出；缺省时本函数读取 sink 计算（一遍同时算两个）。
    """
    cookie = str(cookie or "").strip()
    filename = str(filename or "").strip()
    if not cookie:
        raise UploadError("夸克 Cookie 未配置", provider="quark")
    if not filename:
        raise UploadError("上传文件名为空", provider="quark")
    if sink is None:
        raise UploadError("上传数据源为空", provider="quark")

    total = int(size if size is not None and size >= 0 else getattr(sink, "size", -1) or -1)
    if total < 0:
        raise UploadError("无法确定上传文件大小", provider="quark")
    if total == 0:
        raise UploadError("不能上传空文件", provider="quark")

    file_md5 = str(md5 or "").strip().lower()
    file_sha1 = str(sha1 or getattr(sink, "sha1", "") or "").strip().upper()
    if not file_md5 or not file_sha1:
        sink.seek(0)
        md5_hasher = hashlib.md5()
        sha1_hasher = hashlib.sha1()
        while True:
            chunk = sink.read(1024 * 1024)
            if not chunk:
                break
            md5_hasher.update(chunk)
            sha1_hasher.update(chunk)
        if not file_md5:
            file_md5 = md5_hasher.hexdigest()
        if not file_sha1:
            file_sha1 = sha1_hasher.hexdigest().upper()
        sink.seek(0)

    mime_type = _guess_quark_mime_type(filename)
    target_id = str(target_dir_id or "0").strip() or "0"

    def _check_cancel():
        if cancel_check is not None and cancel_check():
            raise UploadError("上传已取消", provider="quark")

    try:
        pre = _quark_upload_pre(cookie, filename, mime_type, target_id, total)
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:upload_quark_file")
        raise

    # ---- 秒传探测 ----
    try:
        finished = _quark_upload_hash(cookie, str(pre.get("task_id", "") or ""), file_md5, file_sha1)
    except Exception as exc:
        mark_cookie_health_failure("quark", exc, trigger="runtime:upload_quark_file")
        raise
    if finished:
        mark_cookie_health_success("quark", trigger="runtime:upload_quark_file")
        return UploadResult(
            status="rapid",
            name=filename,
            size=total,
            sha1=file_sha1,
            message="秒传命中",
        )

    # ---- 秒传未命中：OSS 分片上传 ----
    part_size = parse_int(pre.get("part_size"), default=_QUARK_UPLOAD_DEFAULT_PART_SIZE)
    part_size = max(_QUARK_UPLOAD_PART_SIZE_MIN, min(int(part_size or 0) or _QUARK_UPLOAD_DEFAULT_PART_SIZE, _QUARK_UPLOAD_PART_SIZE_MAX))

    try:
        etags: List[str] = []
        sent = 0
        part_number = 1
        sink.seek(0)
        while True:
            _check_cancel()
            chunk = sink.read(part_size)
            if not chunk:
                break
            etag = upload_part_with_retry(
                lambda: _quark_upload_part(cookie, pre, mime_type, part_number, chunk),
                part_number=part_number,
                label="quark",
                # 分片重试期间也要能响应取消，否则用户点了取消还要等退避跑完
                cancel_check=(lambda: bool(cancel_check is not None and cancel_check())),
            )
            etags.append(etag)
            sent += len(chunk)
            part_number += 1
            if progress_cb:
                try:
                    progress_cb(sent, total)
                except Exception:
                    pass
        _quark_upload_commit(cookie, pre, etags)
        _quark_upload_finish(cookie, pre)
    except UploadError:
        raise
    except Exception as exc:
        raise UploadError(f"夸克上传失败：{exc}", provider="quark") from exc

    mark_cookie_health_success("quark", trigger="runtime:upload_quark_file")
    return UploadResult(
        status="uploaded",
        name=filename,
        size=total,
        sha1=file_sha1,
        message="上传完成",
    )


def resolve_quark_download_url(cookie: str, file_id: str) -> Dict[str, Any]:
    """获取夸克网盘文件（自己盘内）的下载直链，支持 Range。

    复制引擎的夸克→X 链路用它取源数据。直链有时效（分钟级~小时），
    过期后需重新调用本函数换链（复制引擎的重试逻辑会处理）。
    """
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise RuntimeError("Quark Cookie 未配置")
    fid = str(file_id or "").strip()
    if not fid:
        raise RuntimeError("文件 ID 不能为空")
    headers = _build_quark_client_headers(normalized_cookie)
    url = _build_quark_api_url("/1/clouddrive/file/download")
    response = _request_quark_json_payload(
        url,
        {"fids": [fid]},
        headers,
        timeout=45,
        method="POST",
        fallback="获取夸克下载地址失败",
    )
    if not _is_quark_success(response):
        raise RuntimeError(_map_quark_api_error(response))
    data = response.get("data") if isinstance(response.get("data"), list) else []
    first = data[0] if data and isinstance(data[0], dict) else {}
    download_url = str(first.get("download_url", "") or "").strip()
    if not download_url:
        raise RuntimeError("夸克下载地址获取失败（返回为空，文件可能已被清理）")
    mark_cookie_health_success("quark", trigger="runtime:resolve_quark_download_url")
    return {
        "url": download_url,
        "fid": fid,
        "size": parse_int(first.get("size"), default=0),
        "file_name": str(first.get("file_name", "") or "").strip(),
        # 下载必须用刚轮换过的 cookie（CDN 回调校验 __puus），否则 403 auth expired
        "cookie": _current_quark_cookie(normalized_cookie),
    }


class QuarkProvider(CloudProvider):
    name = "quark"
    label = "夸克网盘"
    link_type = "quark"
    auth_type = "cookie"
    config_keys = ["cookie_quark"]
    supports_subscription = True
    supports_offline = False
    supports_fixed_share_link = True
    supports_strm = False
    supports_monitor = False
    supports_rename = True
    supports_move = True
    supports_copy = True
    supports_delete = True
    supports_upload = True
    supports_rapid_upload = True
    supports_upload_resume = False

    def list_entries_payload(self, cookie, cid="0", folders_only=False):
        return list_quark_entries_payload(cookie, cid, folders_only)

    def list_entries(self, cookie, cid="0"):
        return list_quark_entries(cookie, cid)

    def create_folder(self, cookie, cid="0", folder_name=""):
        return create_quark_folder(cookie, cid, folder_name)

    def resolve_folder_id_by_path(self, cookie, relative_path):
        return resolve_quark_folder_id_by_path(cookie, relative_path)

    def ensure_folder_id_by_path(self, cookie, relative_path):
        return ensure_quark_folder_id_by_path(cookie, relative_path)

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        payload = resolve_quark_share_payload(cookie, share_url, raw_text, receive_code)
        payload["url"] = str(share_url or "").strip()
        payload["raw_text"] = str(raw_text or "")
        return payload

    def list_share_entries(self, cookie, share_payload, cid="0", offset=0, limit=200):
        payload = share_payload if isinstance(share_payload, dict) else {}
        return list_quark_share_entries(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("raw_text", "") or ""),
            cid,
            str(payload.get("receive_code", "") or "").strip(),
            False,
            45,
            max(0, int(offset or 0)),
            max(20, min(int(limit or 200), 400)),
            1,
            False,
        )

    def prepare_share_receive(self, cookie, share_payload, cid="0"):
        payload = share_payload if isinstance(share_payload, dict) else {}
        selected_ids = payload.get("selected_ids", []) if isinstance(payload.get("selected_ids"), list) else []
        selected_entries = payload.get("selected_entries", []) if isinstance(payload.get("selected_entries"), list) else []
        prepared = prepare_quark_share_save(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("raw_text", "") or ""),
            selected_ids,
            str(payload.get("receive_code", "") or "").strip(),
            selected_entries=selected_entries,
        )
        return {**payload, **prepared, "target_cid": str(cid or "0").strip() or "0"}

    def submit_share_receive(self, cookie, receive_payload, files):
        payload = receive_payload if isinstance(receive_payload, dict) else {}
        selected_entries = payload.get("selected_entries", []) if isinstance(payload.get("selected_entries"), list) else []
        if not selected_entries:
            selected_entries = files or []
        selected_ids = payload.get("selected_ids", []) if isinstance(payload.get("selected_ids"), list) else []
        if not selected_ids:
            selected_ids = [
                str(item.get("id", "")).strip()
                for item in selected_entries
                if isinstance(item, dict) and str(item.get("id", "")).strip()
            ]
        return submit_quark_share_save(
            cookie,
            str(payload.get("url", "") or payload.get("share_url", "") or "").strip(),
            str(payload.get("target_cid", "") or payload.get("folder_id", "") or "0").strip() or "0",
            str(payload.get("raw_text", "") or ""),
            selected_ids,
            str(payload.get("receive_code", "") or "").strip(),
            selected_entries,
        )

    def probe_connectivity(self, cookie):
        try:
            result = probe_quark_connectivity(cookie)
            return bool(result.get("ok"))
        except Exception:
            return False

    def submit_offline_task(self, cookie, resource_url, folder_id="0"):
        raise NotImplementedError("Quark does not support offline download")

    def resolve_download_url(self, cookie, file_id):
        result = resolve_quark_download_url(cookie, file_id)
        return str(result.get("url", "") or "")

    def resolve_download_info(self, cookie, file_id):
        """下载直链 + 元信息（复制引擎用：URL 过期时重取、校验大小）。"""
        return resolve_quark_download_url(cookie, file_id)

    def rename_entry(self, cookie, entry_id, new_name, parent_id=""):
        return rename_quark_entry(cookie, entry_id, new_name, parent_id)

    def move_entries(self, cookie, entry_ids, target_id, source_id=""):
        return move_quark_entries(cookie, entry_ids, target_id, source_id)

    def copy_entries(self, cookie, entry_ids, target_id, source_id=""):
        return copy_quark_entries(cookie, entry_ids, target_id, source_id)

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        return delete_quark_entries(cookie, entry_ids, parent_id)

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
        upload_resume=None,
        **kwargs,
    ):
        # upload_resume：断点续传句柄。夸克本期不实现会话级上传续传——
        # pre 会话的跨进程复用未经真机验证，不冒险；本地数据已由任务层保留，
        # 下载不再重来，上传从头再来（设计文档 §3.4）。
        return upload_quark_file(
            cookie,
            sink,
            filename,
            target_dir_id,
            md5=md5,
            sha1=sha1,
            size=size,
            progress_cb=progress_cb,
            cancel_check=cancel_check,
        )


register(QuarkProvider())
