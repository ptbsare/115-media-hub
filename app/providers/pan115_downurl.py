"""115「电脑端」downurl 取链通道（RSA + 自定义 XXTEA 加密协议）。

背景：115 webapi 的 ``/files/download`` 对超过阈值的大文件直接拒绝，返回
errno ``50028``「文件大小超出限制，请用电脑端下载」。网盘里基本都是 GB 级
影片，所以走 webapi 拿直链的那条路对大文件是坏的。

电脑端走的是另一套接口::

    POST https://proapi.115.com/app/chrome/downurl?t=<timestamp>
    body: data=<base64 密文>    (x-www-form-urlencoded)

协议细节（**与迁移前 ``app/routes/strm.py`` 的实现逐行一致**，两边改动必须一起验证）::

    payload = {"pickcode": <pick_code>}                    # 载荷里只有 pickcode
    key     = md5(f"!@###@#{timestamp}DFDR@#@#")            # 16 字节，时间戳必须秒级一致
    明文     = key[:16] + XXTEA_encode(payload, key)         # 前 16 字节是带回来的解密钥
    密文     = RSA_encrypt(明文)                             # 128 字节分块，PKCS#1 v1.5 填充
    响应     = {"state": true, "data": "<base64>"} → RSA 解密 → 用 key 解 XXTEA → JSON

这段逻辑原先只活在 STRM 播放链路里（``_resolve_115_download_payload_by_chrome_api``）。
跨盘复制要拿 115 大文件直链，于是原样下沉到 provider 层，供两条链路共用。

约定：
- 纯函数 + 纯 IO，不依赖 FastAPI 的 Request，可在线程池里直接调用；
- 失败一律抛 ``Pan115DownurlError``（``RuntimeError`` 子类），**不返回空串**；
- 错误文案与 strm.py 里的原文案逐字一致（STRM 会把它拼进对外提示）。
"""

import base64
import hashlib
import json
import time
import urllib.parse
import urllib.request
from http.cookies import SimpleCookie
from typing import Any, Dict, List, Optional, Tuple

from ..db import safe_json_loads
from .common import parse_int

# ============================================================
# 加密常量（从 app/routes/strm.py 原样搬来，一个数字都不能改）
# ============================================================

_RSA_115_MODULUS = int(
    (
        "8686980c0f5a24c4b9d43020cd2c22703ff3f450756529058b1cf88f09b8602136477198a6e2683149659bd122c33592"
        "fdb5ad47944ad1ea4d36c6b172aad6338c3bb6ac6227502d010993ac967d1aef00f0c8e038de2e4d3bc2ec368af2e9f10a6f"
        "1eda4f7262f136420c07c331b871bf139f74f3010e3c4fe57df3afb71683"
    ),
    16,
)
_RSA_115_EXPONENT = int("10001", 16)
_M115_G_KTS = [
    240,
    229,
    105,
    174,
    191,
    220,
    191,
    138,
    26,
    69,
    232,
    190,
    125,
    166,
    115,
    184,
    222,
    143,
    231,
    196,
    69,
    218,
    134,
    196,
    155,
    100,
    139,
    20,
    106,
    180,
    241,
    170,
    56,
    1,
    53,
    158,
    38,
    105,
    44,
    134,
    0,
    107,
    79,
    165,
    54,
    52,
    98,
    166,
    42,
    150,
    104,
    24,
    242,
    74,
    253,
    189,
    107,
    151,
    143,
    77,
    143,
    137,
    19,
    183,
    108,
    142,
    147,
    237,
    14,
    13,
    72,
    62,
    215,
    47,
    136,
    216,
    254,
    254,
    126,
    134,
    80,
    149,
    79,
    209,
    235,
    131,
    38,
    52,
    219,
    102,
    123,
    156,
    126,
    157,
    122,
    129,
    50,
    234,
    182,
    51,
    222,
    58,
    169,
    89,
    52,
    102,
    59,
    170,
    186,
    129,
    96,
    72,
    185,
    213,
    129,
    156,
    248,
    108,
    132,
    119,
    255,
    84,
    120,
    38,
    95,
    190,
    232,
    30,
    54,
    159,
    52,
    128,
    92,
    69,
    44,
    155,
    118,
    213,
    27,
    143,
    204,
    195,
    184,
    245,
]
_M115_G_KEY_S = [0x29, 0x23, 0x21, 0x5E]
_M115_G_KEY_L = [120, 6, 173, 76, 51, 134, 93, 24, 76, 1, 63, 70]

_DEFAULT_115_USER_AGENT = "Mozilla/5.0 115-media-hub"
_DOWNURL_ENDPOINT = "https://proapi.115.com/app/chrome/downurl"


class Pan115DownurlError(RuntimeError):
    """115 downurl 取链失败（message 是人话，可直接展示给用户）。"""


# ============================================================
# 错误识别
# ============================================================


def extract_115_download_error_detail(payload: Any, fallback: str = "115 下载地址解析失败") -> str:
    """从 115 响应里抽出人话错误信息（error/msg/message 依次兜底）。"""
    result = payload if isinstance(payload, dict) else {}
    return (
        str(result.get("error", "")).strip()
        or str(result.get("msg", "")).strip()
        or str(result.get("message", "")).strip()
        or fallback
    )


def is_115_size_limit_error(payload: Any) -> bool:
    """判断 115 响应是不是「文件太大，webapi 不给链接」。

    命中条件（与原 strm 实现一致）：errno/msg_code 为 ``50028``，或者文案同时
    出现「文件大小超出限制」与「电脑端下载」（新版接口只给文案不给码）。
    """
    result = payload if isinstance(payload, dict) else {}
    code = str(result.get("msg_code", "") or result.get("errno", "") or "").strip()
    message = extract_115_download_error_detail(result, fallback="").strip()
    return code == "50028" or ("文件大小超出限制" in message and "电脑端下载" in message)


# ============================================================
# 加密原语（RSA 分块 + 115 自定义 XXTEA 变体）
# ============================================================


def _rsa_115_encrypt_block(block: bytes) -> str:
    block_size = 128
    if len(block) + 11 > block_size:
        raise Pan115DownurlError("115 downurl 加密块过长")
    padded = b"\x00\x02" + (b"\xff" * (block_size - len(block) - 3)) + b"\x00" + block
    number = int.from_bytes(padded, byteorder="big", signed=False)
    encrypted = pow(number, _RSA_115_EXPONENT, _RSA_115_MODULUS)
    return f"{encrypted:0256x}"


def _rsa_115_public_decrypt_block(raw_block: bytes) -> bytes:
    number = int.from_bytes(bytes(raw_block), byteorder="big", signed=False)
    decoded = pow(number, _RSA_115_EXPONENT, _RSA_115_MODULUS)
    hex_text = f"{decoded:x}"
    if len(hex_text) % 2:
        hex_text = "0" + hex_text
    payload = bytes.fromhex(hex_text)
    idx = 1
    while idx < len(payload) and payload[idx] != 0:
        idx += 1
    if idx + 1 >= len(payload):
        return b""
    return payload[idx + 1 :]


def _m115_getkey(length: int, key: Optional[List[int]] = None) -> List[int]:
    if key is not None:
        return [
            ((int(key[i]) + _M115_G_KTS[length * i]) & 0xFF) ^ _M115_G_KTS[length * (length - 1 - i)]
            for i in range(length)
        ]
    return _M115_G_KEY_L[:] if length == 12 else _M115_G_KEY_S[:]


def _m115_xor(src: List[int], key: List[int]) -> List[int]:
    src_len = len(src)
    key_len = len(key)
    mod4 = src_len % 4
    result: List[int] = []
    if mod4:
        for i in range(mod4):
            result.append(int(src[i]) ^ int(key[i % key_len]))
    for i in range(mod4, src_len):
        result.append(int(src[i]) ^ int(key[(i - mod4) % key_len]))
    return result


def _m115_sym_encode(src: List[int], key1: List[int], key2: Optional[List[int]]) -> List[int]:
    k1 = _m115_getkey(4, key1)
    k2 = _m115_getkey(12, key2)
    result = _m115_xor(src, k1)
    result.reverse()
    return _m115_xor(result, k2)


def _m115_sym_decode(src: List[int], key1: List[int], key2: List[int]) -> List[int]:
    k1 = _m115_getkey(4, key1)
    k2 = _m115_getkey(12, key2)
    result = _m115_xor(src, k2)
    result.reverse()
    return _m115_xor(result, k1)


def _m115_asym_encode(src: List[int]) -> str:
    chunk_size = 128 - 11
    encrypted_hex_chunks: List[str] = []
    for offset in range(0, len(src), chunk_size):
        encrypted_hex_chunks.append(_rsa_115_encrypt_block(bytes(src[offset : offset + chunk_size])))
    all_hex = "".join(encrypted_hex_chunks)
    return base64.b64encode(bytes.fromhex(all_hex)).decode("ascii")


def _m115_asym_decode(src: List[int]) -> List[int]:
    block_size = 128
    raw = bytes(src)
    result: List[int] = []
    for offset in range(0, len(raw), block_size):
        result.extend(_rsa_115_public_decrypt_block(raw[offset : offset + block_size]))
    return result


def _m115_encode_downurl_payload(payload_text: str, timestamp: int) -> Tuple[str, List[int]]:
    """加密 downurl 请求载荷，返回 (base64 密文, 解响应用的 key)。"""
    key = list(hashlib.md5(f"!@###@#{timestamp}DFDR@#@#".encode("utf-8")).digest())
    src = list(payload_text.encode("latin1"))
    encrypted = _m115_sym_encode(src, key1=key, key2=None)
    mixed = key[:16] + encrypted
    return _m115_asym_encode(mixed), key


def _m115_decode_downurl_payload(encoded_text: str, key: List[int]) -> str:
    """解密 downurl 响应载荷（用请求时同一把 key）。"""
    raw = list(base64.b64decode(str(encoded_text or "").strip()))
    decoded = _m115_asym_decode(raw)
    if len(decoded) < 16:
        raise Pan115DownurlError("115 downurl 返回数据异常")
    payload_bytes = bytes(_m115_sym_decode(decoded[16:], key1=key, key2=decoded[:16]))
    try:
        return payload_bytes.decode("utf-8")
    except Exception:
        return payload_bytes.decode("latin1", errors="ignore")


# ============================================================
# 响应解析
# ============================================================


def collect_115_download_urls(payload: Any) -> List[str]:
    """从 115 下载响应里递归收集 http(s) 链接。

    与 strm.py 的实现保持一致：除了常见的 url/file_url 字段，还要兜底遍历
    所有 value——downurl 返回的是 ``{ "<file_id>": { url: { url: "..." } } }``
    这种以 fid 为 key 的结构。
    """
    urls: List[str] = []
    seen = set()

    def push(url_value: Any) -> None:
        token = str(url_value or "").strip()
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
        for key in ("url", "download_url", "file_url", "download_url_web", "download_url_web2"):
            walk(node.get(key))
        for key in ("data", "urls", "result", "info"):
            walk(node.get(key))
        # 兼容 downurl 返回的 { "<file_id>": { url: { url: "..." } } } 结构。
        for value in node.values():
            walk(value)

    walk(payload)
    return urls


def collect_115_set_cookie_pairs(response_set_cookies: List[str]) -> str:
    """把响应的多条 Set-Cookie 压成一行 Cookie 头（下载 CDN 有时会下发）。"""
    extra_cookie_pairs: List[str] = []
    for raw_cookie in response_set_cookies:
        jar = SimpleCookie()
        try:
            jar.load(str(raw_cookie or ""))
        except Exception:
            continue
        for key, morsel in jar.items():
            token = f"{str(key or '').strip()}={str(morsel.value or '').strip()}"
            if token and token not in extra_cookie_pairs:
                extra_cookie_pairs.append(token)
    return "; ".join(extra_cookie_pairs)


def _extract_115_downurl_file_meta(payload: Any) -> Dict[str, Any]:
    """从解密后的响应里捞 file_name / size（结构随版本漂移，做宽松匹配）。"""
    meta: Dict[str, Any] = {"file_name": "", "size": 0}
    stack: List[Any] = [payload]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        if not meta["file_name"]:
            name = node.get("file_name") or node.get("name")
            if isinstance(name, str) and name.strip():
                meta["file_name"] = name.strip()
        if not meta["size"]:
            for key in ("file_size", "size"):
                value = node.get(key)
                if value in (None, ""):
                    continue
                try:
                    size = int(str(value).strip())
                except (TypeError, ValueError):
                    continue
                if size > 0:
                    meta["size"] = size
                    break
        for value in node.values():
            if isinstance(value, (dict, list)):
                stack.append(value)
    return meta


# ============================================================
# 对外接口
# ============================================================


def normalize_115_downurl_user_agent(value: Any) -> str:
    """downurl 请求的 UA 归一（空值兜底成项目内置 UA，和 strm 一致）。"""
    ua = str(value or "").strip()
    return ua or _DEFAULT_115_USER_AGENT


def build_115_download_headers(cookie: str = "", user_agent: str = "", extra_cookie: str = "") -> Dict[str, str]:
    """产出 115 直链的下载请求头（取链与下载必须共用同一份）。

    两个坑（2026-10 实测）：

    1. **直链按 UA 签名**。同一链接用 Chrome UA 下载 → ``403 Forbidden``，
       用取链时的 UA（默认 ``Mozilla/5.0 115-media-hub``）→ ``206``。
       所以下载端的 UA 不能自己另编一个。
    2. **webapi ``/files/download`` 会下发路径级 Cookie**（形如
       ``acw_tc=...; <hash>=...; path=/<目录 hash>/``），下载时必须带上，
       只带账号 Cookie 同样 403。

    ``extra_cookie`` 用来拼取链响应里拿到的 Set-Cookie。
    """
    merged_cookie = str(cookie or "").strip()
    extra = str(extra_cookie or "").strip()
    if extra:
        merged_cookie = f"{merged_cookie}; {extra}" if merged_cookie else extra
    headers = {
        "Accept": "*/*",
        "Referer": "https://115.com/",
        "Origin": "https://115.com",
        "User-Agent": normalize_115_downurl_user_agent(user_agent),
    }
    if merged_cookie:
        headers["Cookie"] = merged_cookie
    return headers


def _throttle_115_api_requests() -> None:
    """复用 115 provider 的全局限速器（懒导入，避免与 pan115.py 循环依赖）。

    限速本身失败不该阻断取链，因此异常一律吞掉。
    """
    try:
        from .pan115 import throttle_115_api_requests

        throttle_115_api_requests()
    except Exception:
        return


def _pick_code_from_file_id(cookie: str, file_id: str) -> str:
    """把 file_id（fid）换成 pick_code；本身就长得像 pick_code 的直接用。

    115 的 fid 一定是纯数字，pick_code 是字母数字混合串，据此区分。
    """
    token = str(file_id or "").strip()
    if not token:
        return ""
    if not token.isdigit():
        return token
    try:
        from .pan115 import get_115_file_info

        info = get_115_file_info(cookie, token)
    except Exception as exc:
        raise Pan115DownurlError(f"按 file_id 反查 pickcode 失败：{exc}") from exc
    pick_code = str((info or {}).get("pick_code", "") or "").strip()
    if not pick_code:
        raise Pan115DownurlError("115 文件信息里没有 pickcode，无法调用电脑端下载通道")
    return pick_code


def get_115_downurl_user_id(cookie: str) -> str:
    """取 115 的数字 user_id（复用上传链路 uploadinfo 的 6 小时缓存）。

    注意：当前 downurl 协议的加密载荷**只带 pickcode**，user_id 不参与请求
    （鉴权靠 Cookie）。保留该函数给排查 / 未来协议变化用，主链路不要强依赖它。
    """
    try:
        from .pan115 import get_115_user_upload_info

        userid, _userkey = get_115_user_upload_info(cookie)
    except Exception as exc:
        raise Pan115DownurlError(f"115 user_id 获取失败：{exc}") from exc
    return str(userid or "")


def resolve_115_downurl_payload(
    cookie: str,
    pick_code: str,
    user_agent: str = "",
) -> Tuple[str, str, Dict[str, Any]]:
    """底层取链：返回 ``(下载直链, 下载用 Cookie 头, {file_name, size})``。

    STRM 链路用的就是这个函数（它只需要前两个返回值）。失败抛
    ``Pan115DownurlError``，错误文案与 strm.py 原实现逐字一致。
    """
    normalized_cookie = str(cookie or "").strip()
    normalized_pick_code = str(pick_code or "").strip()
    if not normalized_cookie:
        raise Pan115DownurlError("115 Cookie 未配置")
    if not normalized_pick_code:
        raise Pan115DownurlError("缺少 pickcode，无法调用 115 电脑端下载通道")

    _throttle_115_api_requests()
    timestamp = int(time.time())
    payload_text = json.dumps({"pickcode": normalized_pick_code}, ensure_ascii=False, separators=(",", ":"))
    encoded_data, decode_key = _m115_encode_downurl_payload(payload_text, timestamp)
    headers = build_115_download_headers(normalized_cookie, user_agent)
    headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded",
        }
    )
    body = urllib.parse.urlencode({"data": encoded_data}).encode("utf-8")
    request = urllib.request.Request(
        _DOWNURL_ENDPOINT + "?t=" + str(timestamp),
        headers=headers,
        method="POST",
        data=body,
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            body_text = resp.read().decode(charset, errors="ignore")
            result = safe_json_loads(body_text, {})
            response_set_cookies = resp.headers.get_all("Set-Cookie") or []
    except Exception as exc:
        # 保留原始错误文本（strm 会把它拼进「downurl 回退失败: ...」，文案不能变），
        # 只把异常类型统一成 Pan115DownurlError。
        raise Pan115DownurlError(str(exc) or "115 downurl 请求失败") from exc

    if not isinstance(result, dict):
        raise Pan115DownurlError("115 downurl 返回异常")
    if not bool(result.get("state", False)):
        raise Pan115DownurlError(extract_115_download_error_detail(result, fallback="115 downurl 解析失败"))
    encrypted_payload = str(result.get("data", "")).strip()
    if not encrypted_payload:
        raise Pan115DownurlError("115 downurl 返回为空")
    decoded_payload_text = _m115_decode_downurl_payload(encrypted_payload, decode_key)
    decoded_payload = safe_json_loads(decoded_payload_text, {})
    download_urls = collect_115_download_urls(decoded_payload)
    download_url = str(download_urls[0] if download_urls else "").strip()
    if not download_url:
        raise Pan115DownurlError("115 downurl 未解析到下载链接")
    download_cookie = collect_115_set_cookie_pairs(response_set_cookies)
    return download_url, download_cookie, _extract_115_downurl_file_meta(decoded_payload)


def resolve_115_download_url_by_downurl(
    cookie: str,
    user_id: str = "",
    pick_code: str = "",
    file_id: str = "",
    user_agent: str = "",
) -> Dict[str, Any]:
    """高层取链（provider 层用）：返回 ``{"url", "file_name", "size"}``。

    参数优先级：``pick_code`` > ``file_id``（纯数字 fid 会先反查 pickcode；
    非纯数字则按 pick_code 处理）。``user_id`` 目前**不参与加密载荷**，仅为
    契约保留（详见 ``get_115_downurl_user_id`` 的说明）。

    失败抛 ``Pan115DownurlError``（人话 message），绝不返回空 url。
    """
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise Pan115DownurlError("115 Cookie 未配置")
    normalized_pick_code = str(pick_code or "").strip()
    if not normalized_pick_code:
        normalized_pick_code = _pick_code_from_file_id(normalized_cookie, file_id)
    if not normalized_pick_code:
        raise Pan115DownurlError("缺少 pickcode，无法调用 115 电脑端下载通道")

    download_url, download_cookie, meta = resolve_115_downurl_payload(
        normalized_cookie,
        normalized_pick_code,
        user_agent,
    )
    return {
        "url": str(download_url or "").strip(),
        "file_name": str(meta.get("file_name", "") or "").strip(),
        "size": parse_int(meta.get("size", 0), default=0),
        # 下载端必须复用取链时的 UA + Cookie，否则 CDN 直接 403（见 build_115_download_headers）
        "cookie": str(download_cookie or "").strip(),
        "headers": build_115_download_headers(normalized_cookie, user_agent, download_cookie),
    }
