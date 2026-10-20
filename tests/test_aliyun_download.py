"""阿里云盘「按 file_id 取下载直链」单元测试（mock 全部 HTTP，无真实网络）。

覆盖：
  - 双模式路由：扫码授权（OpenAPI）↔ 网页会话（refresh_token）各打各的端点
  - 请求体：drive_id / file_id / expire_sec（环境变量可调、夹在上限内）
  - 直链提取：顶层 url、download_url、streams_url、以及再包一层 data/body/result
  - 元信息：取链响应自带 name/size 时不再发第二次请求；缺了才补一次
  - 返回结构：{url, file_name, size, cookie}
  - 错误映射：401/403/404/限流 → 人话，且不返回空串

注：本机因安全策略禁用删除操作，Python 的 tempfile 不可用 → 本文件**未在本机执行**，
仅做过语法（ast.parse）与静态审查。测试本身不依赖 tempfile。
"""
import unittest
from unittest.mock import patch

from app.http_utils import normalize_http_url
from app.providers.aliyun import (
    ALIYUN_DOWNLOAD_EXPIRE_SEC,
    AliyunProvider,
    _aliyun_download_expire_sec,
    _extract_aliyun_download_meta,
    _extract_aliyun_download_url,
    _map_aliyun_download_error,
)

OPEN_API = "https://openapi.alipan.com"
WEB_API = "https://api.alipan.com"
OPEN_DOWNLOAD_URL = f"{OPEN_API}/adrive/v1.0/openFile/getDownloadUrl"
OPEN_GET_URL = f"{OPEN_API}/adrive/v1.0/openFile/get"
WEB_DOWNLOAD_URL = f"{WEB_API}/v2/file/get_download_url"
WEB_GET_URL = f"{WEB_API}/v2/file/get"


class AliyunDownloadTestBase(unittest.TestCase):
    def setUp(self):
        self.provider = AliyunProvider()
        self.calls = []
        self.open_mode = False
        # url -> dict 或 Exception
        self.responses = {}
        self.default_meta = {"type": "file", "name": "meta.mkv", "size": 4096}

        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.calls.append({"url": url, "body": body, "token": refresh_token})
            item = self.responses.get(url)
            if isinstance(item, Exception):
                raise item
            if item is not None:
                return item
            if url in (OPEN_GET_URL, WEB_GET_URL):
                return self.default_meta
            return {}

        for patcher in [
            patch.object(
                self.provider, "_is_open_access_mode", side_effect=lambda: self.open_mode
            ),
            patch.object(self.provider, "_resolve_drive_id", return_value="drive-1"),
            patch.object(self.provider, "_api_post", side_effect=fake_api_post),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _urls(self):
        return [call["url"] for call in self.calls]


class TestAliyunDownloadRouting(AliyunDownloadTestBase):
    def test_web_session_uses_web_endpoint(self):
        self.responses[WEB_DOWNLOAD_URL] = {
            "url": "https://cdn.example.com/a.mkv?sig=1",
            "name": "a.mkv",
            "size": 2048,
        }
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(self._urls(), [WEB_DOWNLOAD_URL])
        self.assertEqual(self.calls[0]["body"], {
            "drive_id": "drive-1",
            "file_id": "file-1",
            "expire_sec": ALIYUN_DOWNLOAD_EXPIRE_SEC,
        })
        self.assertEqual(info["url"], "https://cdn.example.com/a.mkv?sig=1")
        self.assertEqual(info["file_name"], "a.mkv")
        self.assertEqual(info["size"], 2048)
        self.assertEqual(info["cookie"], "rt-1")

    def test_open_mode_uses_openapi_endpoint(self):
        self.open_mode = True
        self.responses[OPEN_DOWNLOAD_URL] = {"url": "https://cdn.example.com/b.mkv?sig=2"}
        info = self.provider.resolve_download_info("at-1", "file-2")
        self.assertEqual(self._urls()[:1], [OPEN_DOWNLOAD_URL])
        self.assertEqual(self.calls[0]["body"]["drive_id"], "drive-1")
        self.assertEqual(self.calls[0]["body"]["file_id"], "file-2")
        self.assertEqual(info["url"], "https://cdn.example.com/b.mkv?sig=2")

    def test_resolve_download_url_returns_str(self):
        self.responses[WEB_DOWNLOAD_URL] = {"url": "https://cdn.example.com/c.mkv", "size": 9}
        url = self.provider.resolve_download_url("rt-1", "file-1")
        self.assertIsInstance(url, str)
        self.assertEqual(url, "https://cdn.example.com/c.mkv")


class TestAliyunDownloadMeta(AliyunDownloadTestBase):
    def test_meta_request_skipped_when_response_has_name_and_size(self):
        self.responses[WEB_DOWNLOAD_URL] = {
            "url": "https://cdn.example.com/a.mkv",
            "file_name": "a.mkv",
            "size": 100,
        }
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(self._urls(), [WEB_DOWNLOAD_URL])
        self.assertEqual(info["file_name"], "a.mkv")
        self.assertEqual(info["size"], 100)

    def test_meta_fetched_once_when_missing(self):
        self.responses[WEB_DOWNLOAD_URL] = {"url": "https://cdn.example.com/a.mkv"}
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(self._urls(), [WEB_DOWNLOAD_URL, WEB_GET_URL])
        self.assertEqual(self.calls[1]["body"], {"drive_id": "drive-1", "file_id": "file-1"})
        self.assertEqual(info["file_name"], "meta.mkv")
        self.assertEqual(info["size"], 4096)

    def test_meta_uses_openapi_endpoint_in_open_mode(self):
        self.open_mode = True
        self.responses[OPEN_DOWNLOAD_URL] = {"url": "https://cdn.example.com/a.mkv"}
        self.provider.resolve_download_info("at-1", "file-1")
        self.assertEqual(self._urls(), [OPEN_DOWNLOAD_URL, OPEN_GET_URL])

    def test_meta_only_fills_missing_size(self):
        self.responses[WEB_DOWNLOAD_URL] = {"url": "https://cdn.example.com/a.mkv", "name": "a.mkv"}
        self.default_meta = {"name": "other.mkv", "size": 777}
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(info["file_name"], "a.mkv")  # 已有文件名不被覆盖
        self.assertEqual(info["size"], 777)

    def test_meta_failure_does_not_break_download(self):
        self.responses[WEB_DOWNLOAD_URL] = {"url": "https://cdn.example.com/a.mkv"}
        self.responses[WEB_GET_URL] = RuntimeError("阿里云盘 API 500: InternalError")
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(info["url"], "https://cdn.example.com/a.mkv")
        self.assertEqual(info["file_name"], "")
        self.assertEqual(info["size"], 0)


class TestAliyunDownloadUrlExtraction(AliyunDownloadTestBase):
    def test_url_under_streams_url(self):
        self.responses[WEB_DOWNLOAD_URL] = {
            "streams_url": {"FHD": "https://cdn.example.com/fhd.m3u8"},
            "name": "a.mkv",
            "size": 10,
        }
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(info["url"], "https://cdn.example.com/fhd.m3u8")

    def test_url_under_download_url_key(self):
        self.responses[WEB_DOWNLOAD_URL] = {
            "download_url": "https://cdn.example.com/d.mkv",
            "name": "a.mkv",
            "size": 10,
        }
        self.assertEqual(
            self.provider.resolve_download_info("rt-1", "file-1")["url"],
            "https://cdn.example.com/d.mkv",
        )

    def test_url_nested_under_data(self):
        self.responses[WEB_DOWNLOAD_URL] = {
            "data": {"url": "https://cdn.example.com/n.mkv", "name": "n.mkv", "size": 5}
        }
        info = self.provider.resolve_download_info("rt-1", "file-1")
        self.assertEqual(info["url"], "https://cdn.example.com/n.mkv")
        self.assertEqual(info["file_name"], "n.mkv")
        self.assertEqual(info["size"], 5)

    def test_empty_url_raises_human_message(self):
        self.responses[WEB_DOWNLOAD_URL] = {"url": "", "name": "a.mkv", "size": 10}
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("未返回下载直链", str(ctx.exception))


class TestAliyunDownloadErrors(AliyunDownloadTestBase):
    def test_token_expired_maps_to_reauth(self):
        self.responses[WEB_DOWNLOAD_URL] = RuntimeError(
            "阿里云盘 API 401: AccessTokenInvalid - AccessTokenInvalid"
        )
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        message = str(ctx.exception)
        self.assertIn("授权已过期", message)
        self.assertIn("重新扫码授权", message)
        self.assertNotIn("AccessTokenInvalid", message)

    def test_forbidden_maps_to_scope_hint(self):
        self.responses[WEB_DOWNLOAD_URL] = RuntimeError(
            "阿里云盘 API 403: Forbidden - no permission"
        )
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("无权限读取", str(ctx.exception))

    def test_not_found_maps_to_file_missing(self):
        self.responses[WEB_DOWNLOAD_URL] = RuntimeError(
            "阿里云盘 API 404: NotFound.File - file not found"
        )
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("文件不存在", str(ctx.exception))

    def test_too_many_requests_maps_to_friendly_text(self):
        self.responses[WEB_DOWNLOAD_URL] = RuntimeError(
            "阿里云盘 API 429: TooManyRequests - slow down"
        )
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("过于频繁", str(ctx.exception))

    def test_unknown_error_keeps_detail(self):
        self.responses[WEB_DOWNLOAD_URL] = RuntimeError("阿里云盘 API 500: InternalError")
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("InternalError", str(ctx.exception))

    def test_missing_cookie_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("", "file-1")
        self.assertIn("refresh_token", str(ctx.exception))
        self.assertFalse(self.calls)

    def test_missing_file_id_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.resolve_download_info("rt-1", "")
        self.assertIn("文件 ID", str(ctx.exception))
        self.assertFalse(self.calls)

    def test_drive_id_failure_is_mapped(self):
        with patch.object(
            self.provider,
            "_resolve_drive_id",
            side_effect=RuntimeError("阿里云盘 API 401: AccessTokenExpired - expired"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.provider.resolve_download_info("rt-1", "file-1")
        self.assertIn("授权已过期", str(ctx.exception))


class TestAliyunDownloadExpireSec(unittest.TestCase):
    def test_default_is_max(self):
        with patch.dict("os.environ", {}, clear=False):
            import os

            os.environ.pop("ALIYUN_DOWNLOAD_EXPIRE_SEC", None)
            self.assertEqual(_aliyun_download_expire_sec(), ALIYUN_DOWNLOAD_EXPIRE_SEC)

    def test_env_override(self):
        with patch.dict("os.environ", {"ALIYUN_DOWNLOAD_EXPIRE_SEC": "600"}):
            self.assertEqual(_aliyun_download_expire_sec(), 600)

    def test_env_clamped_to_server_limit(self):
        with patch.dict("os.environ", {"ALIYUN_DOWNLOAD_EXPIRE_SEC": "999999"}):
            self.assertEqual(_aliyun_download_expire_sec(), ALIYUN_DOWNLOAD_EXPIRE_SEC)
        with patch.dict("os.environ", {"ALIYUN_DOWNLOAD_EXPIRE_SEC": "1"}):
            self.assertEqual(_aliyun_download_expire_sec(), 60)

    def test_invalid_env_falls_back_to_default(self):
        with patch.dict("os.environ", {"ALIYUN_DOWNLOAD_EXPIRE_SEC": "abc"}):
            self.assertEqual(_aliyun_download_expire_sec(), ALIYUN_DOWNLOAD_EXPIRE_SEC)


class TestAliyunDownloadHelpers(unittest.TestCase):
    def test_extract_url_ignores_non_http(self):
        self.assertEqual(_extract_aliyun_download_url({"url": "not-a-url"}), "")
        self.assertEqual(_extract_aliyun_download_url(None), "")

    def test_extract_meta_tolerates_bad_size(self):
        self.assertEqual(_extract_aliyun_download_meta({"name": "a", "size": "x"}), ("a", 0))
        self.assertEqual(_extract_aliyun_download_meta({"size": -5}), ("", 0))

    def test_map_error_does_not_return_empty(self):
        self.assertTrue(_map_aliyun_download_error(RuntimeError("")))
        self.assertTrue(_map_aliyun_download_error(RuntimeError("weird")))


class TestNormalizeHttpUrlKeepsSignature(unittest.TestCase):
    """归一化不得改坏已签名的直链（2026-10-20 真机 403 事故的回归钉）。

    事故：老实现 unquote 再 quote，把 ``%3D%3D`` 还原成 ``==``；阿里云盘 / OSS
    按原始 query 串算签名，一个字节变了就 403。阿里作源的跨盘复制必失败。
    """

    # 事故现场的 URL 形状（参数值截取）：callback base64 + OSS 签名参数
    SIGNED = (
        "https://dl1-v6.aliyundrive.cloud/file/abc123"
        "?callback=eyJjYWxsYmFja1VybCI6Imh0dHA6Ly9iai5hcGkuYWxpeXVucGRzLmNvbSJ9%3D%3D"
        "&callback-var=eyJ4OmRvbWFpbl9pZCI6ImJqMjkifQ%3D%3D"
        "&x-oss-credential=STS.NZH66eC2wPA7M3iHbQj9hbLB%2F20261020%2Fcn-beijing%2Foss%2Faliyun_v4_request"
        "&x-oss-security-token=CAISvgJ1q6Ft5B2yfSjIr5n9fYzRru1W54%2BOK%2Fcalq5rR4Q"
        "&pds-params=%7B%22ap%22%3A%225509%22%7D"
        "&response-content-disposition=attachment%3B%20filename%2A=UTF-8%27%27a.bin"
    )

    def test_existing_escapes_are_untouched(self):
        self.assertEqual(normalize_http_url(self.SIGNED), self.SIGNED)

    def test_plain_equals_and_plus_survive(self):
        url = "https://h.example.com/a?token=a+b%2Bc&sig=x%3D%3D&v=%2Fpath"
        self.assertEqual(normalize_http_url(url), url)

    def test_non_ascii_is_encoded(self):
        out = normalize_http_url("https://h.example.com/中文目录/文件.mp4?a=1")
        self.assertEqual(out, "https://h.example.com/%E4%B8%AD%E6%96%87%E7%9B%AE%E5%BD%95/%E6%96%87%E4%BB%B6.mp4?a=1")

    def test_idempotent(self):
        once = normalize_http_url(self.SIGNED + "#frag%20x")
        self.assertEqual(normalize_http_url(once), once)

    def test_empty_and_blank(self):
        self.assertEqual(normalize_http_url(""), "")
        self.assertEqual(normalize_http_url("   "), "")

    def test_space_is_encoded(self):
        self.assertEqual(
            normalize_http_url("https://h.example.com/a b?q=c d"),
            "https://h.example.com/a%20b?q=c%20d",
        )


if __name__ == "__main__":
    unittest.main()
