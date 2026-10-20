import gzip
import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

# 所有可打印 ASCII（0x21~0x7E）。作为 quote 的 safe 集合，等于「一个 ASCII 字节都别动」。
_ASCII_PRINTABLE_SAFE = "".join(chr(code) for code in range(0x21, 0x7F))


def normalize_http_url(url: str) -> str:
    """把 URL 里的非 ASCII 字符编码成百分号转义（urllib 只接受 ASCII 请求 URL）。

    **绝不能碰 URL 里已有的转义序列。** 早先的实现是 unquote 再 quote，会把
    ``%3D`` 还原成 ``=``、``%2B`` 还原成 ``+``；而阿里云盘 / OSS 这类按原始
    query 串校验签名的直链，改动任意一个字节都会签名失配，服务端直接 403
    （2026-10-20 真机复现：阿里作源的 3MB 文件复制必失败，归一化前后长度
    2307 → 2281）。

    所以这里只编码「不在 ASCII 可打印区间」的字符，ASCII 一律原样保留，
    对已经编码好的直链是恒等变换。
    """
    text = str(url or "").strip()
    if not text:
        return ""
    parts = urllib.parse.urlsplit(text)
    path = urllib.parse.quote(parts.path, safe=_ASCII_PRINTABLE_SAFE)
    query = urllib.parse.quote(parts.query, safe=_ASCII_PRINTABLE_SAFE)
    fragment = urllib.parse.quote(parts.fragment, safe=_ASCII_PRINTABLE_SAFE)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, query, fragment))


def build_http_opener(proxy_url: str = ""):
    if proxy_url:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
    return urllib.request.build_opener()


def decode_response_body(resp) -> str:
    """Read HTTP response body, handling gzip compression transparently."""
    raw = resp.read()
    content_encoding = (resp.headers.get("Content-Encoding") or "").strip().lower()
    if content_encoding == "gzip" or (len(raw) >= 2 and raw[:2] == b'\x1f\x8b'):
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    charset = resp.headers.get_content_charset() or "utf-8"
    return raw.decode(charset, errors="ignore")


def http_request_json(
    url: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    token: str = "",
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> Dict[str, Any]:
    normalized_url = normalize_http_url(url)
    headers = {}
    if extra_headers:
        headers.update(extra_headers)
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if token:
        headers["Authorization"] = token
    req = urllib.request.Request(normalized_url, data=data, headers=headers, method=method)
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        body = decode_response_body(resp)
    stripped = (body or "").strip()
    if not stripped:
        return {}
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        preview = stripped[:200].replace('\n', ' ').replace('\r', '')
        raise json.JSONDecodeError(
            f"响应不是 JSON（前 200 字符）: {preview}",
            doc=stripped, pos=0,
        )


def http_request_form_json(
    url: str,
    form_data: Dict[str, Any],
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    normalized_url = normalize_http_url(url)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if extra_headers:
        headers.update(extra_headers)
    encoded = urllib.parse.urlencode({k: "" if v is None else str(v) for k, v in form_data.items()}).encode("utf-8")
    req = urllib.request.Request(normalized_url, data=encoded, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = decode_response_body(resp)
    stripped = (body or "").strip()
    if not stripped:
        return {}
    return json.loads(stripped)


def http_request_text(
    url: str,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> str:
    normalized_url = normalize_http_url(url)
    headers = dict(extra_headers or {})
    req = urllib.request.Request(normalized_url, headers=headers, method="GET")
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        return decode_response_body(resp)


def http_request_bytes(
    url: str,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> bytes:
    """读取响应原始字节，用于透传图片等二进制内容。"""
    normalized_url = normalize_http_url(url)
    headers = dict(extra_headers or {})
    req = urllib.request.Request(normalized_url, headers=headers, method="GET")
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        return resp.read()


def http_request_text_with_final_url(
    url: str,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> Tuple[str, str]:
    normalized_url = normalize_http_url(url)
    headers = dict(extra_headers or {})
    req = urllib.request.Request(normalized_url, headers=headers, method="GET")
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        body = decode_response_body(resp)
        return body, str(resp.geturl() or normalized_url)


def http_request_binary(
    url: str,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> Tuple[bytes, str]:
    normalized_url = normalize_http_url(url)
    headers = dict(extra_headers or {})
    req = urllib.request.Request(normalized_url, headers=headers, method="GET")
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        content_type = resp.headers.get_content_type() or "application/octet-stream"
        return resp.read(), content_type


def http_resolve_url(
    url: str,
    timeout: int = 30,
    extra_headers: Optional[Dict[str, str]] = None,
    proxy_url: str = "",
) -> str:
    normalized_url = normalize_http_url(url)
    headers = dict(extra_headers or {})
    req = urllib.request.Request(normalized_url, headers=headers, method="GET")
    with build_http_opener(proxy_url).open(req, timeout=timeout) as resp:
        return str(resp.geturl() or normalized_url)
