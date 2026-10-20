import base64
import hashlib
import logging
import math
import os
import re
import threading
import time
from typing import Any, Dict, List

import requests

from .base import CloudProvider, UploadError, UploadResult, upload_part_with_retry
from .aliyun_oauth import ALIYUN_OPEN_CLIENT_ID, build_aliyun_refresh_payload
from .registry import register


ALIYUN_API_BASE = "https://api.alipan.com"
# OpenAPI 专用域名：扫码授权（公开客户端 access_token 模式）的令牌只能调
# openapi.alipan.com 的 /adrive/v1.0/* 端点；打到 api.alipan.com 网页端点会
# 401 AccessTokenInvalid（两种令牌体系不通用）。网页会话模式仍走 ALIYUN_API_BASE。
ALIYUN_OPEN_API_BASE = "https://openapi.alipan.com"
ALIYUN_CANARY_HEADER = "client=web,app=share,version=v2.3.1"
ALIYUN_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

# OpenAPI access_token 模式端点（协议事实来自官方文档与 OpenList aliyundrive_open 驱动）：
#   用户/网盘信息:  POST /adrive/v1.0/user/getDriveInfo
#   文件列表:       POST /adrive/v1.0/openFile/list      （marker 分页）
#   建夹/上传创建:  POST /adrive/v1.0/openFile/create
#   上传收尾:       POST /adrive/v1.0/openFile/complete
#   下载直链:       POST /adrive/v1.0/openFile/getDownloadUrl
#   移动/复制/改名: POST /adrive/v1.0/openFile/move|copy|update
#   删除:           POST /adrive/v1.0/openFile/delete
# 注意：开放平台**没有**「转存他人分享」端点（只有创建自己的分享），分享转存
# 仅在网页会话（refresh_token）模式下可用。

# === 上传能力 ===
# 链路（对齐社区成熟实现 OpenList aliyundrive driver）：
#   1) createWithFolders 带 pre_hash（前 1024KB 的 SHA1）探测，命中 PreHashMatched 进入二段
#   2) 二段带 content_hash（完整 SHA1）+ content_hash_name=sha1 + proof_code + proof_version=v1
#      - proof_code v1 = base64(文件[md5(access_token)[:16] % size, +8) 的 8 字节)
#      - rapid_upload=true 即秒传命中（零上传流量）
#   3) 未命中：part_info_list 逐片 PUT 预签名 URL（10MB/片，裸传无额外头）
#   4) /v2/file/complete（drive_id + file_id + upload_id）收尾
_ALIYUN_UPLOAD_PART_SIZE = 10 * 1024 * 1024
_ALIYUN_UPLOAD_PREHASH_SIZE = 1024

# === 下载直链（跨盘复制 / STRM 的源端能力）===
# 与上传一致按令牌模式分流，两边的端点和字段位置不同：
#   扫码授权（OpenAPI）: POST https://openapi.alipan.com/adrive/v1.0/openFile/getDownloadUrl
#   网页会话（refresh_token）: POST https://api.alipan.com/v2/file/get_download_url
# 两边 body 相同：{drive_id, file_id, expire_sec}；返回体都带签名直链 url，
# 网页版还会顺带返回 name/size（OpenAPI 不一定有，缺了才补一次元信息请求）。
# expire_sec 由服务端限制在 14400 秒（4 小时）以内，超出会被拒绝。
ALIYUN_DOWNLOAD_EXPIRE_SEC = 14400


def _aliyun_download_expire_sec() -> int:
    """直链有效期（秒）。默认 14400，可用环境变量 ALIYUN_DOWNLOAD_EXPIRE_SEC 调小/调大，
    最终仍夹在 [60, 14400] 区间内（超过服务端上限会直接报错）。"""
    try:
        value = int(
            str(os.environ.get("ALIYUN_DOWNLOAD_EXPIRE_SEC", "") or "").strip()
            or ALIYUN_DOWNLOAD_EXPIRE_SEC
        )
    except (TypeError, ValueError):
        return ALIYUN_DOWNLOAD_EXPIRE_SEC
    return max(60, min(value, ALIYUN_DOWNLOAD_EXPIRE_SEC))


def _extract_aliyun_download_url(data: Any, depth: int = 0) -> str:
    """从取链响应里挖出直链。

    两种模式的字段位置不完全一致（顶层 ``url`` / ``download_url`` /
    ``streams_url`` 字典），部分网关还会再包一层 ``data``/``body``/``result``，
    因此按「顶层优先、逐层下钻」的顺序兼容取值。
    """
    if not isinstance(data, dict) or depth > 2:
        return ""
    for key in ("url", "download_url"):
        value = str(data.get(key, "") or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
    streams = data.get("streams_url")
    if isinstance(streams, dict):
        for value in streams.values():
            candidate = str(value or "").strip()
            if candidate.startswith("http://") or candidate.startswith("https://"):
                return candidate
    elif isinstance(streams, str):
        candidate = streams.strip()
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
    for wrapper in ("data", "body", "result"):
        nested = data.get(wrapper)
        if isinstance(nested, dict):
            found = _extract_aliyun_download_url(nested, depth + 1)
            if found:
                return found
    return ""


def _extract_aliyun_download_meta(data: Any) -> tuple:
    """从取链响应里取 ``(file_name, size)``；取不到就返回空串 / 0（调用方补元信息）。"""
    file_name = ""
    size = 0
    stack: List[Any] = [data]
    seen = 0
    while stack and seen < 8:
        current = stack.pop()
        seen += 1
        if not isinstance(current, dict):
            continue
        if not file_name:
            file_name = str(
                current.get("file_name", "") or current.get("name", "") or ""
            ).strip()
        if size <= 0:
            try:
                size = int(current.get("size", 0) or 0)
            except (TypeError, ValueError):
                size = 0
        for wrapper in ("data", "body", "result"):
            nested = current.get(wrapper)
            if isinstance(nested, dict):
                stack.append(nested)
    return file_name, max(0, size)


def _map_aliyun_download_error(exc: Exception) -> str:
    """把阿里云盘取直链的失败映射为面向用户的文案。

    跨盘复制会把它原样拼进「源文件下载失败：…」，所以这里既不能吞异常
    返回空串，也不能只把服务端错误码抛给用户。
    """
    text = str(exc or "").strip()
    lower = text.lower()
    if (
        "accesstokeninvalid" in lower
        or "accesstokenexpired" in lower
        or "i400jd" in lower
        or "invalid_grant" in lower
        or "api 401" in lower
        or "授权已过期" in text
    ):
        return "阿里云盘授权已过期，请到设置页重新扫码授权"
    if (
        "forbiddenfilenotfound" in lower
        or "notfound" in lower
        or "api 404" in lower
        or "文件不存在" in text
    ):
        return "阿里云盘文件不存在或已被删除，请刷新目录后重试"
    if (
        "api 403" in lower
        or "permissiondenied" in lower
        or "forbidden" in lower
        or "accessdenied" in lower
    ):
        return "阿里云盘无权限读取该文件（授权范围不足），请到设置页重新扫码授权"
    if "toomanyrequests" in lower or "频繁" in text:
        return "阿里云盘请求过于频繁，请稍后再试"
    return f"阿里云盘下载直链获取失败：{text}" if text else "阿里云盘下载直链获取失败，请稍后重试"


def _aliyun_proof_code_v1(access_token: str, size: int, sink: Any) -> str:
    """proof_code v1：偏移量由 md5(access_token) 前 16 位十六进制对 size 取模得到，取该处 8 字节。"""
    token = str(access_token or "")
    digest = hashlib.md5(token.encode("utf-8")).hexdigest()
    offset = int(digest[:16], 16) % size if size > 0 else 0
    sink.seek(offset)
    chunk = sink.read(8)
    if len(chunk) < 8:  # 文件不足 8 字节时从头补足
        sink.seek(0)
        chunk = sink.read(8)
    return base64.b64encode(chunk[:8]).decode("ascii")


def _map_aliyun_upload_error(exc: Exception) -> str:
    """把阿里云盘上传错误映射为面向用户的文案。"""
    text = str(exc or "")
    if "PreHashMatched" in text:
        return "阿里云盘秒传校验失败，请稍后重试"
    if "AccessTokenInvalid" in text or "AccessTokenExpired" in text or "I400JD" in text:
        return "阿里云盘登录已失效，请重新授权后重试"
    if "ForbiddenNoSpace" in text:
        return "阿里云盘空间不足，请清理后重试"
    if "InvalidResource.ParentNotExist" in text:
        return "阿里云盘目标目录不存在"
    if "TooManyRequests" in text or "频繁" in text:
        return "阿里云盘操作过于频繁，请稍后再试"
    return text or "阿里云盘上传失败，请稍后重试"


class AliyunProvider(CloudProvider):
    name = "aliyun"
    label = "阿里云盘"
    link_type = "aliyun"
    auth_type = "refresh_token"
    config_keys = ["aliyun_refresh_token"]
    supports_subscription = True
    supports_offline = False
    supports_fixed_share_link = True
    supports_rename = True
    supports_move = True
    supports_copy = True
    supports_delete = True
    supports_upload = True
    supports_rapid_upload = True
    supports_upload_resume = False

    def __init__(self):
        super().__init__()
        self._access_token = None
        self._token_expiry = 0.0
        self._drive_id = None
        self._token_lock = threading.Lock()

    def _ensure_access_token(self, refresh_token: str) -> str:
        now = time.time()
        with self._token_lock:
            # 公开客户端（无 AppSecret）授权拿到的是 30 天有效期的 access_token 且不支持刷新，
            # 因此标记后直接作为 Bearer 使用；手动/旧三方 refresh_token 仍走刷新流程（兼容）。
            is_access_mode = False
            stored_access = ""
            expires_at = 0.0
            try:
                from ..core import get_config as _get_aliyun_config

                cfg = _get_aliyun_config()
                is_access_mode = (
                    str(cfg.get("aliyun_token_is_access", "") or "").strip().lower()
                    in ("1", "true", "yes")
                )
                stored_access = str(cfg.get("aliyun_refresh_token", "") or "").strip()
                expires_at = float(cfg.get("aliyun_access_expires_at", 0) or 0)
            except Exception:
                is_access_mode = False
            if is_access_mode:
                if not stored_access:
                    raise RuntimeError("阿里云盘 access_token 为空，请重新扫码授权")
                if expires_at and now >= expires_at - 60:
                    raise RuntimeError("阿里云盘 access_token 已过期，请重新扫码授权")
                if self._access_token != stored_access:
                    # 配置中的令牌已变化（重新授权 / 手动更新）：
                    # 清空网盘 ID 缓存，避免新账号沿用旧 drive_id
                    self._drive_id = None
                self._access_token = stored_access
                self._token_expiry = expires_at or now + 30 * 86400
                return stored_access
            # refresh 模式：内存缓存有效则直接复用
            if self._access_token and now < self._token_expiry - 60:
                return self._access_token
            last_error = None
            # 官方 Open API 优先（公开客户端 + client_id）；其余阿里云盘自有域名端点保留作回退，
            # 兼容早期手动粘贴的 refresh_token。不再依赖第三方中继（api.alistgo.com）刷新。
            token_endpoints = [
                ("https://openapi.alipan.com/oauth/access_token", {"client_id": ALIYUN_OPEN_CLIENT_ID}),
                ("https://auth.alipan.com/v2/account/token", {}),
                ("https://auth.aliyundrive.com/v2/account/token", {}),
            ]
            for url, extra in token_endpoints:
                try:
                    if extra:
                        # 官方 Open API 上报 application/x-www-form-urlencoded
                        resp = requests.post(
                            url,
                            data=build_aliyun_refresh_payload(refresh_token),
                            timeout=15,
                        )
                    else:
                        resp = requests.post(
                            url,
                            json={"grant_type": "refresh_token", "refresh_token": refresh_token},
                            headers={"Content-Type": "application/json"},
                            timeout=15,
                        )
                    resp.raise_for_status()
                    data = resp.json()
                    token = data.get("access_token")
                    if token:
                        self._access_token = token
                        self._token_expiry = now + int(data.get("expires_in", 7200))
                        drive_id = str(data.get("default_drive_id", "") or "").strip()
                        if drive_id:
                            self._drive_id = drive_id
                        return token
                    err_msg = str(
                        data.get("message", "")
                        or data.get("error_description", "")
                        or data.get("error", "")
                        or data.get("msg", "")
                    ).strip()
                    # 官方端点报 invalid_client / invalid_grant（token 不匹配该 client）时，
                    # 记下错误并继续尝试回退端点，不在此处中断。
                    if err_msg:
                        last_error = err_msg
                        continue
                except requests.RequestException as exc:
                    last_error = str(exc)
                    continue
            if last_error:
                raise RuntimeError(f"阿里云盘 access_token 获取失败: {last_error}")
            raise RuntimeError("阿里云盘 access_token 获取失败，请检查 refresh_token 是否有效")

    def _is_open_access_mode(self) -> bool:
        """是否处于 OpenAPI access_token 模式（扫码授权的公开客户端令牌）。

        该模式令牌只能调 openapi.alipan.com 的 /adrive/v1.0/* 端点；
        网页会话（refresh_token）模式走 api.alipan.com 网页端点。
        """
        try:
            from ..core import get_config as _get_aliyun_config

            cfg = _get_aliyun_config()
            return str(cfg.get("aliyun_token_is_access", "") or "").strip().lower() in (
                "1",
                "true",
                "yes",
            )
        except Exception:
            return False

    def _api_base(self) -> str:
        """按令牌模式返回 API 基础域名。"""
        return ALIYUN_OPEN_API_BASE if self._is_open_access_mode() else ALIYUN_API_BASE

    def _base_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": ALIYUN_USER_AGENT,
            "Origin": "https://www.alipan.com",
            "Referer": "https://www.alipan.com/",
            "X-Canary": ALIYUN_CANARY_HEADER,
        }

    def _api_headers(self, refresh_token: str, share_token: str = "") -> Dict[str, str]:
        headers = self._base_headers()
        token = self._ensure_access_token(refresh_token)
        headers["Authorization"] = f"Bearer {token}"
        if share_token:
            headers["x-share-token"] = share_token
        return headers

    @staticmethod
    def _raise_for_api_error(data: Dict[str, Any], prefix: str = "阿里云盘 API 错误") -> None:
        code = str(data.get("code", "") or data.get("error", "") or "").strip()
        if not code or code.lower() in ("success", "ok", "200"):
            return
        message = str(
            data.get("message", "")
            or data.get("error_description", "")
            or data.get("msg", "")
            or code
        ).strip()
        if message and message != code:
            raise RuntimeError(f"{prefix}: {code} - {message}")
        raise RuntimeError(f"{prefix}: {code}")

    def _post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
        self.throttle()
        resp = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=timeout,
        )
        if resp.status_code >= 400:
            # 不要直接 raise_for_status：阿里云盘的错误码/消息在响应体里（如 PreHashMatched），
            # 丢掉 body 会让上游无法做两段式秒传和准确报错。
            detail = ""
            try:
                payload = resp.json()
                if isinstance(payload, dict):
                    detail = " - ".join(
                        str(v)
                        for v in (
                            payload.get("code"),
                            payload.get("message") or payload.get("error_description"),
                        )
                        if v
                    )
                else:
                    detail = str(payload)[:200]
            except Exception:
                detail = resp.text.strip()[:200]
            raise RuntimeError(
                f"阿里云盘 API {resp.status_code}: {detail or resp.reason}"
            )
        if resp.status_code == 204 or not resp.text.strip():
            return {}
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError("阿里云盘 API 返回格式异常")
        return data

    def _api_post(
        self,
        refresh_token: str,
        url: str,
        body: Dict[str, Any],
        timeout: int = 30,
        share_token: str = "",
    ) -> Dict[str, Any]:
        data = self._post_json(url, body, self._api_headers(refresh_token, share_token), timeout)
        self._raise_for_api_error(data)
        return data

    def _resolve_drive_id(self, refresh_token: str) -> str:
        if self._drive_id:
            return self._drive_id
        if self._is_open_access_mode():
            # OpenAPI：getDriveInfo（返回 default/resource/backup 三个 drive_id）
            data = self._api_post(
                refresh_token, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/user/getDriveInfo", {}
            )
        else:
            data = self._api_post(refresh_token, f"{ALIYUN_API_BASE}/v2/user/get", {})
        drive_id = str(data.get("default_drive_id", "") or "").strip()
        if not drive_id:
            raise RuntimeError("阿里云盘默认网盘 ID 获取失败")
        self._drive_id = drive_id
        return drive_id

    @staticmethod
    def _normalize_cid(cid: str) -> str:
        cid = str(cid or "").strip()
        return "root" if not cid or cid == "0" else cid

    @staticmethod
    def _normalize_entry(item: Dict[str, Any], cid: str) -> Dict[str, Any]:
        """把网页/OpenAPI 两种列表接口的条目拍平成统一结构（字段名一致）。"""
        is_dir = item.get("type") == "folder"
        item_id = str(item.get("file_id", ""))
        return {
            "id": item_id,
            "name": str(item.get("name", "")),
            "type": "folder" if is_dir else "file",
            "is_dir": is_dir,
            "cid": item_id if is_dir else "",
            "fid": "" if is_dir else item_id,
            "size": int(item.get("size", 0) or 0),
            "parent_id": str(item.get("parent_file_id", cid) or cid),
        }

    def list_entries_payload(self, cookie, cid="root", folders_only=False):
        cid = self._normalize_cid(cid)
        drive_id = self._resolve_drive_id(refresh_token=cookie)
        entries: List[Dict[str, Any]] = []
        if self._is_open_access_mode():
            # OpenAPI：openFile/list 用 marker 分页，翻页取全
            marker = ""
            while True:
                body = {
                    "drive_id": drive_id,
                    "parent_file_id": cid,
                    "limit": 200,
                    "marker": marker,
                    "order_by": "updated_at",
                    "order_direction": "DESC",
                }
                if folders_only:
                    body["type"] = "folder"
                data = self._api_post(
                    cookie, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/list", body
                )
                items = data.get("items", []) if isinstance(data.get("items", []), list) else []
                for item in items:
                    if isinstance(item, dict):
                        entries.append(self._normalize_entry(item, cid))
                marker = str(data.get("next_marker", "") or "").strip()
                if not marker or not items:
                    break
        else:
            body = {
                "drive_id": drive_id,
                "parent_file_id": cid,
                "limit": 200,
                "order_by": "updated_at",
                "order_direction": "DESC",
            }
            if folders_only:
                body["type"] = "folder"
            data = self._api_post(cookie, f"{ALIYUN_API_BASE}/v2/file/list", body)
            for item in data.get("items", []):
                entries.append(self._normalize_entry(item, cid))
        folder_count = sum(1 for item in entries if item.get("is_dir"))
        file_count = sum(1 for item in entries if not item.get("is_dir"))
        return {
            "entries": entries,
            "total": len(entries),
            "summary": {"folder_count": folder_count, "file_count": file_count},
        }

    def list_entries(self, cookie, cid="root"):
        return self.list_entries_payload(cookie, cid)["entries"]

    def create_folder(self, cookie, cid="root", folder_name=""):
        cid = self._normalize_cid(cid)
        if self._is_open_access_mode():
            create_url = f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/create"
        else:
            create_url = f"{ALIYUN_API_BASE}/adrive/v2/file/createWithFolders"
        data = self._api_post(cookie, create_url, {
            "drive_id": self._resolve_drive_id(refresh_token=cookie),
            "parent_file_id": cid,
            "name": folder_name,
            "type": "folder",
            "check_name_mode": "refuse",
        })
        return {"cid": str(data.get("file_id", "")), "name": folder_name}

    def resolve_folder_id_by_path(self, cookie, relative_path):
        parts = [p.strip() for p in str(relative_path).split("/") if p.strip()]
        cid = "root"
        for name in parts:
            entries = self.list_entries(cookie, cid)
            found = next((e for e in entries if e.get("name") == name), None)
            if not found:
                return ""
            cid = found["id"]
        return cid

    def ensure_folder_id_by_path(self, cookie, relative_path):
        parts = [p.strip() for p in str(relative_path).split("/") if p.strip()]
        cid = "root"
        for name in parts:
            entries = self.list_entries(cookie, cid)
            found = next((e for e in entries if e.get("name") == name), None)
            if found:
                cid = found["id"]
            else:
                result = self.create_folder(cookie, cid, name)
                cid = result["cid"]
        return cid

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        share_text = str(share_url or "")
        share_id_match = re.search(r"/s/([A-Za-z0-9]+)", share_text)
        if not share_id_match:
            raise RuntimeError("无法识别阿里云盘分享链接")
        payload = {
            "share_id": share_id_match.group(1),
            "receive_code": str(receive_code or "").strip(),
            "url": share_text.strip(),
        }
        folder_match = re.search(r"/folder/([A-Za-z0-9]+)", share_text)
        if folder_match:
            payload["folder_id"] = folder_match.group(1)
        return payload

    def _get_share_token(self, share_id: str, receive_code: str = "") -> str:
        body = {"share_id": share_id}
        if receive_code:
            body["share_pwd"] = receive_code
        data = self._post_json(
            f"{ALIYUN_API_BASE}/v2/share_link/get_share_token",
            body,
            self._base_headers(),
            timeout=15,
        )
        self._raise_for_api_error(data, prefix="阿里云盘分享令牌获取失败")
        share_token = str(data.get("share_token", "") or "").strip()
        if not share_token:
            raise RuntimeError("阿里云盘分享令牌获取失败：返回为空，请检查分享链接或提取码")
        return share_token

    def _list_share_page(
        self,
        cookie: str,
        share_id: str,
        share_token: str,
        cid: str,
        marker: str,
        limit: int,
    ) -> Dict[str, Any]:
        data = self._api_post(
            cookie,
            f"{ALIYUN_API_BASE}/adrive/v3/file/list",
            {
                "share_id": share_id,
                "parent_file_id": cid,
                "limit": limit,
                "marker": marker,
                "order_by": "updated_at",
                "order_direction": "DESC",
                "image_thumbnail_process": "image/resize,w_160/format,jpeg",
                "image_url_process": "image/resize,w_1920/format,jpeg",
                "video_thumbnail_process": "video/snapshot,t_1000,f_jpg,ar_auto,w_300",
            },
            share_token=share_token,
        )
        return data

    @staticmethod
    def _normalize_share_entry(item: Dict[str, Any], share_id: str, share_token: str, cid: str) -> Dict[str, Any]:
        is_dir = item.get("type") == "folder"
        item_id = str(item.get("file_id", ""))
        return {
            "id": item_id,
            "name": str(item.get("name", "")),
            "type": "folder" if is_dir else "file",
            "is_dir": is_dir,
            "cid": item_id if is_dir else "",
            "fid": "" if is_dir else item_id,
            "size": int(item.get("size", 0) or 0),
            "parent_id": str(item.get("parent_file_id", cid) or cid),
            "share_id": share_id,
            "share_token": share_token,
            "drive_id": str(item.get("drive_id", "") or ""),
        }

    def _guard_share_support(self) -> None:
        """分享转存守卫：开放平台没有「转存他人分享」端点，扫码授权模式下不可用。

        网页会话（refresh_token）模式的令牌才能调网页分享接口，这里提前给出
        人话提示，替代服务端 401 的晦涩报错。
        """
        if self._is_open_access_mode():
            raise RuntimeError(
                "阿里云盘当前使用扫码授权模式，暂不支持分享转存；"
                "如需使用阿里云盘分享转存，请改用网页版授权（refresh_token）方式登录"
            )

    def list_share_entries(self, cookie, share_payload, cid="root", offset=0, limit=200):
        self._guard_share_support()
        cid = self._normalize_cid(cid)
        if cid == "root":
            cid = str(share_payload.get("folder_id", "") or "").strip() or cid
        share_id = share_payload["share_id"]
        receive_code = share_payload.get("receive_code", "")
        share_token = str(share_payload.get("share_token", "") or "").strip()
        if not share_token:
            share_token = self._get_share_token(share_id, receive_code)

        normalized_offset = max(0, int(offset or 0))
        normalized_limit = max(1, min(int(limit or 200), 400))
        marker = ""
        skipped = 0
        entries = []
        next_marker = ""
        while len(entries) < normalized_limit:
            entries_data = self._list_share_page(
                cookie,
                share_id,
                share_token,
                cid,
                marker,
                min(200, normalized_limit + normalized_offset - skipped),
            )
            page_items = entries_data.get("items", []) if isinstance(entries_data.get("items", []), list) else []
            next_marker = str(entries_data.get("next_marker", "") or "").strip()
            if skipped + len(page_items) <= normalized_offset:
                skipped += len(page_items)
            else:
                start = max(0, normalized_offset - skipped)
                for item in page_items[start:]:
                    if not isinstance(item, dict):
                        continue
                    entries.append(self._normalize_share_entry(item, share_id, share_token, cid))
                    if len(entries) >= normalized_limit:
                        break
                skipped += len(page_items)
            marker = next_marker
            if not marker or not page_items:
                break
        next_offset = normalized_offset + len(entries)
        share_meta = {**share_payload, "share_token": share_token}
        if entries:
            drive_id = str(entries[0].get("drive_id", "") or "").strip()
            if drive_id:
                share_meta["drive_id"] = drive_id
        folder_count = sum(1 for item in entries if item.get("is_dir"))
        file_count = sum(1 for item in entries if not item.get("is_dir"))
        return {
            "entries": entries,
            "total": next_offset + (1 if next_marker else 0),
            "offset": normalized_offset,
            "next_offset": next_offset,
            "has_more": bool(next_marker),
            "share": share_meta,
            "share_title": str(share_payload.get("title", "") or share_payload.get("share_name", "") or "").strip(),
            "summary": {"folder_count": folder_count, "file_count": file_count},
        }

    def prepare_share_receive(self, cookie, share_payload, cid="root"):
        self._guard_share_support()
        cid = self._normalize_cid(cid)
        share_id = share_payload["share_id"]
        receive_code = str(share_payload.get("receive_code", "") or "").strip()
        share_token = str(share_payload.get("share_token", "") or "").strip()
        if not share_token:
            share_token = self._get_share_token(share_id, receive_code)
        return {**share_payload, "share_token": share_token, "target_cid": cid}

    def _submit_copy_batch(
        self,
        cookie: str,
        share_id: str,
        share_token: str,
        drive_id: str,
        target_cid: str,
        file_ids: List[str],
    ) -> Dict[str, Any]:
        requests_payload = [
            {
                "body": {
                    "file_id": fid,
                    "share_id": share_id,
                    "auto_rename": True,
                    "to_parent_file_id": target_cid,
                    "to_drive_id": drive_id,
                },
                "headers": {"Content-Type": "application/json"},
                "id": str(index),
                "method": "POST",
                "url": "/file/copy",
            }
            for index, fid in enumerate(file_ids)
        ]
        data = self._api_post(
            cookie,
            f"{ALIYUN_API_BASE}/adrive/v4/batch",
            {"requests": requests_payload, "resource": "file"},
            timeout=60,
            share_token=share_token,
        )
        responses = data.get("responses", []) if isinstance(data.get("responses", []), list) else []
        errors = []
        for item in responses:
            if not isinstance(item, dict):
                continue
            status = int(item.get("status", 0) or 0)
            body = item.get("body", {}) if isinstance(item.get("body"), dict) else {}
            code = str(body.get("code", "") or body.get("error", "") or "").strip()
            if status >= 400 or code:
                message = str(body.get("message", "") or body.get("msg", "") or code or status).strip()
                errors.append(message)
        if errors:
            raise RuntimeError(f"阿里云盘转存失败: {'; '.join(errors[:3])}")
        return data

    def submit_share_receive(self, cookie, receive_payload, files):
        self._guard_share_support()
        share_id = receive_payload["share_id"]
        share_token = receive_payload.get("share_token", "")
        if not share_token:
            share_token = self._get_share_token(share_id, receive_payload.get("receive_code", ""))
        target_cid = self._normalize_cid(receive_payload.get("target_cid", "root"))
        drive_id = self._resolve_drive_id(refresh_token=cookie)

        file_ids = [
            str(f.get("id", "")).strip()
            for f in (files or [])
            if str(f.get("id", "")).strip()
        ]
        if not file_ids:
            raise RuntimeError("未选择要转存的文件")

        for start in range(0, len(file_ids), 20):
            self._submit_copy_batch(
                cookie,
                share_id,
                share_token,
                drive_id,
                target_cid,
                file_ids[start:start + 20],
            )

        return {"success": True, "count": len(file_ids)}

    def probe_connectivity(self, cookie):
        """真实连接检测：token 有效性 + 文件读取权限。

        仅校验 token 存在会把「登录着但没文件权限」（scope 不足 403）误报为可用，
        所以这里实际读一次根目录（覆盖 file:all:read 权限）。
        """
        try:
            self._ensure_access_token(cookie)
            self.list_entries_payload(cookie, "root", folders_only=True)
            return True
        except Exception as e:
            logging.warning(f"阿里云盘连接检测失败: {e}")
            return False

    def rename_entry(self, cookie, entry_id, new_name, parent_id=""):
        drive_id = self._resolve_drive_id(cookie)
        body = {
            "drive_id": drive_id,
            "file_id": str(entry_id).strip(),
            "name": str(new_name).strip(),
        }
        if self._is_open_access_mode():
            return self._api_post(
                cookie, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/update", body
            )
        return self._api_post(cookie, f"{ALIYUN_API_BASE}/v3/file/update", body)

    def move_entries(self, cookie, entry_ids, target_id, source_id=""):
        drive_id = self._resolve_drive_id(cookie)
        target_pid = self._normalize_cid(target_id)
        open_mode = self._is_open_access_mode()
        for eid in entry_ids:
            body = {
                "drive_id": drive_id,
                "file_id": str(eid).strip(),
                "to_parent_file_id": target_pid,
            }
            if open_mode:
                body["check_name_mode"] = "ignore"
                self._api_post(
                    cookie, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/move", body
                )
            else:
                self._api_post(cookie, f"{ALIYUN_API_BASE}/v3/file/move", body)
        return {"ok": True, "ids": entry_ids, "target_cid": target_id}

    def copy_entries(self, cookie, entry_ids, target_id, source_id=""):
        drive_id = self._resolve_drive_id(cookie)
        target_pid = self._normalize_cid(target_id)
        open_mode = self._is_open_access_mode()
        for eid in entry_ids:
            body = {
                "drive_id": drive_id,
                "file_id": str(eid).strip(),
                "to_parent_file_id": target_pid,
            }
            if open_mode:
                body["auto_rename"] = False
                self._api_post(
                    cookie, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/copy", body
                )
            else:
                self._api_post(cookie, f"{ALIYUN_API_BASE}/v3/file/copy", body)
        return {"ok": True, "ids": entry_ids, "target_cid": target_id}

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        drive_id = self._resolve_drive_id(cookie)
        headers = self._api_headers(cookie)
        delete_url = (
            f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/delete"
            if self._is_open_access_mode()
            else f"{ALIYUN_API_BASE}/v3/file/delete"
        )
        for eid in entry_ids:
            body = {
                "drive_id": drive_id,
                "file_id": str(eid).strip(),
            }
            self.throttle()
            resp = requests.post(
                delete_url,
                headers=headers,
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            # 阿里云盘删除接口返回 204 No Content 或空 body
            if resp.status_code != 204 and resp.text.strip():
                self._raise_for_api_error(resp.json())
        return {"ok": True, "ids": entry_ids}

    def get_file_hash(self, cookie, file_id):
        """取文件的 SHA1（``content_hash``）+ 大小（跨盘复制秒传探测用，不下载）。

        阿里云盘文件元信息的 ``content_hash`` 在 ``content_hash_name == "sha1"`` 时
        就是文件 SHA1，可直接用于目标端秒传探测。目录 / 拿不到哈希时返回 ``{}``。
        """
        refresh_token = str(cookie or "").strip()
        token = str(file_id or "").strip()
        if not refresh_token or not token:
            return {}
        drive_id = self._resolve_drive_id(refresh_token)
        body = {"drive_id": drive_id, "file_id": token}
        if self._is_open_access_mode():
            data = self._api_post(
                refresh_token, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/get", body
            )
        else:
            data = self._api_post(refresh_token, f"{ALIYUN_API_BASE}/v2/file/get", body)
        if not isinstance(data, dict):
            return {}
        if str(data.get("type", "file") or "file").strip().lower() != "file":
            return {}
        # 少数文件可能用 crc64 等其他摘要，只有 sha1 才能用于秒传探测
        hash_name = str(data.get("content_hash_name", "") or "").strip().lower()
        if hash_name and hash_name != "sha1":
            return {}
        sha1 = str(data.get("content_hash", "") or "").strip().upper()
        try:
            size = int(data.get("size", 0) or 0)
        except (TypeError, ValueError):
            return {}
        if not sha1 or size <= 0:
            return {}
        return {
            "sha1": sha1,
            "size": size,
            "name": str(data.get("name", "") or "").strip(),
        }

    def _download_url_endpoint(self) -> str:
        """按令牌模式返回取直链端点（两种令牌体系不通用，见模块头部注释）。"""
        if self._is_open_access_mode():
            return f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/getDownloadUrl"
        return f"{ALIYUN_API_BASE}/v2/file/get_download_url"

    def _get_file_meta(self, refresh_token: str, drive_id: str, file_id: str) -> Dict[str, Any]:
        """读一次文件元信息（取链响应缺文件名/大小时才用）。"""
        body = {"drive_id": drive_id, "file_id": file_id}
        if self._is_open_access_mode():
            return self._api_post(
                refresh_token, f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/get", body
            )
        return self._api_post(refresh_token, f"{ALIYUN_API_BASE}/v2/file/get", body)

    def resolve_download_info(self, cookie, file_id):
        """取下载直链 + 元信息（阿里云盘作跨盘复制源时用）。

        返回 ``{"url", "file_name", "size", "cookie"}``：
          - ``url``：带签名的直链，有效期见 ``_aliyun_download_expire_sec()``；
          - ``file_name`` / ``size``：优先取自取链响应，缺了才补一次元信息请求；
          - ``cookie``：回传给复制引擎拼下载请求头（阿里直链本身靠签名鉴权，
            发送凭据无害；结构与 115 保持一致）。

        失败直接抛 ``RuntimeError``（文案已映射成人话），不返回空串——
        复制引擎需要拿到具体原因才能给用户可用提示。
        """
        refresh_token = str(cookie or "").strip()
        token = str(file_id or "").strip()
        if not refresh_token:
            raise RuntimeError("阿里云盘 refresh_token 未配置，无法获取下载地址")
        if not token:
            raise RuntimeError("缺少文件 ID，无法获取下载地址")

        try:
            drive_id = self._resolve_drive_id(refresh_token)
            data = self._api_post(
                refresh_token,
                self._download_url_endpoint(),
                {
                    "drive_id": drive_id,
                    "file_id": token,
                    "expire_sec": _aliyun_download_expire_sec(),
                },
            )
        except Exception as exc:
            raise RuntimeError(_map_aliyun_download_error(exc)) from exc

        url = _extract_aliyun_download_url(data)
        if not url:
            raise RuntimeError("阿里云盘未返回下载直链，请稍后重试")

        file_name, size = _extract_aliyun_download_meta(data)
        if not file_name or size <= 0:
            # 取链已成功，元信息只用来补文件名/大小：失败不算致命错误
            try:
                meta = self._get_file_meta(refresh_token, drive_id, token)
            except Exception as exc:
                logging.info("阿里云盘下载元信息获取失败（已忽略）: %s", exc)
            else:
                if isinstance(meta, dict):
                    if not file_name:
                        file_name = str(
                            meta.get("name", "") or meta.get("file_name", "") or ""
                        ).strip()
                    if size <= 0:
                        try:
                            size = max(0, int(meta.get("size", 0) or 0))
                        except (TypeError, ValueError):
                            size = 0

        return {
            "url": url,
            "file_name": file_name,
            "size": size,
            "cookie": refresh_token,
        }

    def resolve_download_url(self, cookie, file_id):
        """取阿里云盘下载直链（STRM / 跨盘复制共用入口）。"""
        return str(self.resolve_download_info(cookie, file_id).get("url", "") or "")

    def upload_file(
        self,
        cookie,
        sink,
        filename,
        target_dir_id="root",
        *,
        sha1="",
        md5="",
        size=-1,
        progress_cb=None,
        cancel_check=None,
        **kwargs,
    ):
        return self._upload_file_flow(
            cookie,
            sink,
            filename,
            target_dir_id,
            sha1=sha1,
            size=size,
            progress_cb=progress_cb,
            cancel_check=cancel_check,
        )

    def _upload_file_flow(
        self,
        refresh_token: str,
        sink: Any,
        filename: str,
        target_dir_id: str = "root",
        *,
        sha1: str = "",
        size: int = -1,
        progress_cb=None,
        cancel_check=None,
    ) -> UploadResult:
        """把 sink 上传到阿里云盘（两段式秒传优先，未命中走 10MB 分片）。

        详见模块头部链路注释。sha1 由复制引擎在下载时同步算出；缺省时本函数读取计算。
        """
        refresh_token = str(refresh_token or "").strip()
        filename = str(filename or "").strip()
        if not refresh_token:
            raise UploadError("阿里云盘 refresh_token 未配置", provider="aliyun")
        if not filename:
            raise UploadError("上传文件名为空", provider="aliyun")
        if sink is None:
            raise UploadError("上传数据源为空", provider="aliyun")

        total = int(size if size is not None and size >= 0 else getattr(sink, "size", -1) or -1)
        if total < 0:
            raise UploadError("无法确定上传文件大小", provider="aliyun")
        if total == 0:
            raise UploadError("不能上传空文件", provider="aliyun")

        file_sha1 = str(sha1 or getattr(sink, "sha1", "") or "").strip().lower()

        # ---- 第一段：pre_hash（前 1024 字节 SHA1）探测 ----
        sink.seek(0)
        pre_hasher = hashlib.sha1()
        remaining = _ALIYUN_UPLOAD_PREHASH_SIZE
        while remaining > 0:
            chunk = sink.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            pre_hasher.update(chunk)
            remaining -= len(chunk)
        pre_hash = pre_hasher.hexdigest()
        sink.seek(0)

        drive_id = self._resolve_drive_id(refresh_token)
        target_pid = self._normalize_cid(target_dir_id)
        part_count = max(1, math.ceil(total / _ALIYUN_UPLOAD_PART_SIZE))
        open_mode = self._is_open_access_mode()
        base_body: Dict[str, Any] = {
            # OpenAPI 重名策略只支持 ignore/auto_rename/refuse；网页版可用 overwrite
            "check_name_mode": "ignore" if open_mode else "overwrite",
            "drive_id": drive_id,
            "name": filename,
            "parent_file_id": target_pid,
            "part_info_list": [{"part_number": i} for i in range(1, part_count + 1)],
            "size": total,
            "type": "file",
        }

        def _check_cancel():
            if cancel_check is not None and cancel_check():
                raise UploadError("上传已取消", provider="aliyun")

        if open_mode:
            create_url = f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/create"
            complete_url = f"{ALIYUN_OPEN_API_BASE}/adrive/v1.0/openFile/complete"
        else:
            create_url = f"{ALIYUN_API_BASE}/adrive/v2/file/createWithFolders"
            complete_url = f"{ALIYUN_API_BASE}/v2/file/complete"
        try:
            resp = self._api_post(
                refresh_token, create_url, {**base_body, "pre_hash": pre_hash}
            )
        except RuntimeError as exc:
            if "PreHashMatched" not in str(exc):
                raise UploadError(_map_aliyun_upload_error(exc), provider="aliyun") from exc
            # ---- 第二段：完整 SHA1 + proof_code v1 ----
            if not file_sha1:
                sink.seek(0)
                hasher = hashlib.sha1()
                while True:
                    chunk = sink.read(1024 * 1024)
                    if not chunk:
                        break
                    hasher.update(chunk)
                file_sha1 = hasher.hexdigest()
                sink.seek(0)
            access_token = self._ensure_access_token(refresh_token)
            proof_code = _aliyun_proof_code_v1(access_token, total, sink)
            try:
                resp = self._api_post(
                    refresh_token,
                    create_url,
                    {
                        **base_body,
                        "content_hash": file_sha1,
                        "content_hash_name": "sha1",
                        "proof_code": proof_code,
                        "proof_version": "v1",
                    },
                )
            except RuntimeError as exc2:
                raise UploadError(_map_aliyun_upload_error(exc2), provider="aliyun") from exc2

        if bool(resp.get("rapid_upload", False)):
            # OpenAPI 的 create 即使秒传命中也会返回 upload_id，参考实现
            # （OpenList aliyundrive_open）在秒传后仍调 complete 收尾；
            # 网页版 createWithFolders 秒传即完成，无需该调用。
            rapid_file_id = str(resp.get("file_id", "") or "")
            rapid_upload_id = str(resp.get("upload_id", "") or "")
            if open_mode and rapid_upload_id:
                try:
                    self._api_post(
                        refresh_token,
                        complete_url,
                        {
                            "drive_id": drive_id,
                            "file_id": rapid_file_id,
                            "upload_id": rapid_upload_id,
                        },
                    )
                except RuntimeError as exc:
                    raise UploadError(
                        _map_aliyun_upload_error(exc), provider="aliyun"
                    ) from exc
            return UploadResult(
                status="rapid",
                file_id=rapid_file_id,
                name=filename,
                size=total,
                sha1=file_sha1.upper(),
                message="秒传命中",
            )

        file_id = str(resp.get("file_id", "") or "")
        upload_id = str(resp.get("upload_id", "") or "")
        parts = resp.get("part_info_list") if isinstance(resp.get("part_info_list"), list) else []
        if not file_id or not upload_id or not parts:
            raise UploadError(
                "阿里云盘上传初始化失败（未返回分片信息）", retriable=True, provider="aliyun"
            )

        # ---- 分片 PUT（裸传预签名 URL） ----
        sent = 0
        try:
            for index, part in enumerate(parts):
                _check_cancel()
                if not isinstance(part, dict):
                    raise UploadError(
                        f"阿里云盘分片 {index + 1} 配置异常", retriable=True, provider="aliyun"
                    )
                upload_url = str(
                    part.get("upload_url", "") or part.get("internal_upload_url", "") or ""
                ).strip()
                if not upload_url:
                    raise UploadError(
                        f"阿里云盘分片 {index + 1} 上传地址缺失", retriable=True, provider="aliyun"
                    )
                chunk = sink.read(_ALIYUN_UPLOAD_PART_SIZE)
                if not chunk:
                    raise UploadError("读取上传数据不完整", provider="aliyun")

                def _send_part(_url=upload_url, _chunk=chunk, _index=index):
                    """发一片（可被重试复用：chunk 已读在内存里，重发不再回源）。"""
                    self.throttle()
                    try:
                        put_resp = requests.put(
                            _url,
                            data=_chunk,
                            timeout=max(30, min(600, 60 + len(_chunk) // (1024 * 1024))),
                        )
                    except Exception as exc:
                        raise UploadError(
                            f"阿里云盘分片 {_index + 1} 上传失败（网络错误）",
                            retriable=True,
                            provider="aliyun",
                        ) from exc
                    if int(put_resp.status_code or 0) // 100 != 2:
                        raise UploadError(
                            f"阿里云盘分片 {_index + 1} 上传失败（HTTP {int(put_resp.status_code or 0)}）",
                            retriable=True,
                            provider="aliyun",
                        )
                    return put_resp

                upload_part_with_retry(
                    _send_part,
                    part_number=index + 1,
                    label="aliyun",
                    cancel_check=(lambda: bool(cancel_check is not None and cancel_check())),
                )
                sent += len(chunk)
                if progress_cb:
                    try:
                        progress_cb(sent, total)
                    except Exception:
                        pass
            # ---- complete 收尾 ----
            complete_resp = self._api_post(
                refresh_token,
                complete_url,
                {"drive_id": drive_id, "file_id": file_id, "upload_id": upload_id},
            )
        except UploadError:
            raise
        except RuntimeError as exc:
            raise UploadError(_map_aliyun_upload_error(exc), provider="aliyun") from exc

        completed_id = str(complete_resp.get("file_id", "") or "")
        if completed_id and completed_id != file_id:
            raise UploadError("阿里云盘上传提交结果异常", retriable=True, provider="aliyun")
        return UploadResult(
            status="uploaded",
            file_id=completed_id or file_id,
            name=filename,
            size=total,
            sha1=file_sha1.upper(),
            message="上传完成",
        )


register(AliyunProvider())
