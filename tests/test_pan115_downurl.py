"""115 电脑端 downurl 取链 + 两段式取链回退的单测（全 mock，无真实网络）。

覆盖：
1. ``pan115_downurl.resolve_115_downurl_payload`` 的加密请求 / 响应解密 / 解析；
2. downurl 各类失败都抛 ``Pan115DownurlError`` 且文案是人话；
3. ``pan115.resolve_115_download_info`` 三态：
   - 普通接口成功 → **不**触发 downurl 回退；
   - 普通接口 errno 50028 → 回退 downurl 成功；
   - 两者都失败 → 错误文案同时带上两段原因。

注意：本机 Python 的 tempfile 不可用（安全策略禁止删除临时文件），``pytest``
起不来；本文件用标准库 unittest 直接执行：

    .venv/bin/python -m unittest tests.test_pan115_downurl

加密链路只能验到「请求构造 + 响应解析」，完整加解密往返需要 115 服务端私钥，
无法在本地自造，属于真机待验部分。
"""

import base64
import json
import unittest
import urllib.parse
from unittest.mock import patch

from app.providers import pan115_downurl as downurl
from app.providers import pan115


class _FakeHeaders:
    def __init__(self, set_cookies=None):
        self._set_cookies = list(set_cookies or [])

    def get_content_charset(self):
        return "utf-8"

    def get_all(self, name):
        if str(name).lower() == "set-cookie":
            return self._set_cookies
        return []


class _FakeResponse:
    """最小的 urlopen 响应替身（上下文管理器 + read + headers）。"""

    def __init__(self, payload, set_cookies=None):
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = _FakeHeaders(set_cookies)

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class Pan115DownurlModuleTests(unittest.TestCase):
    """downurl 模块本体。"""

    def setUp(self):
        # 限速器会 sleep，并与真实 115 provider 共享全局限速状态，测试里直接短路。
        patcher = patch.object(downurl, "_throttle_115_api_requests", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_is_115_size_limit_error_by_errno(self):
        self.assertTrue(downurl.is_115_size_limit_error({"state": False, "errno": 50028}))
        self.assertTrue(downurl.is_115_size_limit_error({"msg_code": "50028"}))

    def test_is_115_size_limit_error_by_message(self):
        payload = {"state": False, "msg": "文件大小超出限制，请用电脑端下载"}
        self.assertTrue(downurl.is_115_size_limit_error(payload))
        # 只有半句文案不算命中，避免误伤其它错误。
        self.assertFalse(downurl.is_115_size_limit_error({"msg": "文件大小超出限制"}))

    def test_resolve_payload_encrypts_request(self):
        """校验请求姿势（URL/方法/头/body 与 RSA 分块长度）。

        注意：115 的 downurl 是「客户端用公钥指数加密、服务端用私钥指数签回」的
        不对称玩法，手上没有私钥，所以**无法**在本地自造一条能解开的响应；响应
        解析因此单独用 mock 覆盖（见下一个用例）。
        """
        record = {}

        def fake_urlopen(request, timeout=0):
            record["request"] = request
            record["timeout"] = timeout
            return _FakeResponse({"state": True, "data": "QUJD"})

        with patch.object(downurl.urllib.request, "urlopen", side_effect=fake_urlopen), patch.object(
            downurl, "_m115_decode_downurl_payload", return_value='{"10086":{"url":"http://cdn.example.com/a.mkv"}}'
        ):
            url, cookie, _meta = downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567", "UA-1")

        request = record["request"]
        self.assertTrue(request.full_url.startswith("https://proapi.115.com/app/chrome/downurl?t="))
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Cookie"), "UID=abc")
        self.assertEqual(request.get_header("Referer"), "https://115.com/")
        self.assertEqual(request.get_header("Origin"), "https://115.com")
        self.assertEqual(request.get_header("User-agent"), "UA-1")
        self.assertEqual(request.get_header("Content-type"), "application/x-www-form-urlencoded")
        self.assertEqual(record["timeout"], 45)

        body = request.data.decode("utf-8")
        encoded = urllib.parse.parse_qs(body)["data"][0]
        raw = base64.b64decode(encoded)
        # RSA 按 128 字节分块，密文长度必须是块长的整数倍。
        self.assertEqual(len(raw) % 128, 0)
        self.assertGreater(len(raw), 0)
        self.assertEqual(url, "http://cdn.example.com/a.mkv")
        self.assertEqual(cookie, "")

    def test_resolve_payload_parses_response_url_and_meta(self):
        def fake_urlopen(request, timeout=0):
            return _FakeResponse({"state": True, "data": "QUJD"}, set_cookies=["UID=abc; Path=/"])

        decoded = {
            "10086": {
                "file_name": "电影.mkv",
                "file_size": "8589934592",
                "url": {"url": "http://cdn.example.com/movie.mkv"},
            }
        }
        with patch.object(downurl.urllib.request, "urlopen", side_effect=fake_urlopen), patch.object(
            downurl, "_m115_decode_downurl_payload", return_value=json.dumps(decoded, ensure_ascii=False)
        ):
            url, cookie, meta = downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567", "UA-1")

        self.assertEqual(url, "http://cdn.example.com/movie.mkv")
        self.assertEqual(cookie, "UID=abc")
        self.assertEqual(meta["file_name"], "电影.mkv")
        self.assertEqual(meta["size"], 8589934592)

    def test_resolve_payload_rejects_truncated_response_data(self):
        with patch.object(
            downurl.urllib.request,
            "urlopen",
            return_value=_FakeResponse({"state": True, "data": base64.b64encode(b"short").decode("ascii")}),
        ):
            with self.assertRaises(downurl.Pan115DownurlError) as ctx:
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(str(ctx.exception), "115 downurl 返回数据异常")

    def test_resolve_payload_without_user_agent_uses_default(self):
        record = {}

        def fake_urlopen(request, timeout=0):
            record["request"] = request
            return _FakeResponse({"state": False, "msg": "x"})

        with patch.object(downurl.urllib.request, "urlopen", side_effect=fake_urlopen):
            with self.assertRaises(downurl.Pan115DownurlError):
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(record["request"].get_header("User-agent"), "Mozilla/5.0 115-media-hub")

    def test_resolve_payload_state_false_raises_human_error(self):
        with patch.object(
            downurl.urllib.request,
            "urlopen",
            return_value=_FakeResponse({"state": False, "msg": "登录已失效"}),
        ):
            with self.assertRaises(downurl.Pan115DownurlError) as ctx:
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(str(ctx.exception), "登录已失效")

    def test_resolve_payload_empty_data_raises(self):
        with patch.object(
            downurl.urllib.request,
            "urlopen",
            return_value=_FakeResponse({"state": True, "data": ""}),
        ):
            with self.assertRaises(downurl.Pan115DownurlError) as ctx:
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(str(ctx.exception), "115 downurl 返回为空")

    def test_resolve_payload_without_url_raises(self):
        with patch.object(
            downurl.urllib.request,
            "urlopen",
            return_value=_FakeResponse({"state": True, "data": "QUJD"}),
        ), patch.object(
            downurl,
            "_m115_decode_downurl_payload",
            return_value='{"10086":{"file_name":"x.mkv"}}',
        ):
            with self.assertRaises(downurl.Pan115DownurlError) as ctx:
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(str(ctx.exception), "115 downurl 未解析到下载链接")

    def test_resolve_payload_network_error_keeps_original_text(self):
        # 泛型异常文案要原样透传（strm 会拼进「downurl 回退失败: ...」，文案不能变）。
        with patch.object(downurl.urllib.request, "urlopen", side_effect=OSError("连接被重置")):
            with self.assertRaises(downurl.Pan115DownurlError) as ctx:
                downurl.resolve_115_downurl_payload("UID=abc", "abcdefg1234567")
        self.assertEqual(str(ctx.exception), "连接被重置")

    def test_resolve_payload_requires_pick_code_and_cookie(self):
        with self.assertRaises(downurl.Pan115DownurlError):
            downurl.resolve_115_downurl_payload("", "abcdefg1234567")
        with self.assertRaises(downurl.Pan115DownurlError):
            downurl.resolve_115_downurl_payload("UID=abc", "")

    def test_high_level_uses_given_pick_code(self):
        with patch.object(
            downurl,
            "resolve_115_downurl_payload",
            return_value=("http://cdn.example.com/a.mkv", "", {"file_name": "a.mkv", "size": 12}),
        ) as mocked:
            info = downurl.resolve_115_download_url_by_downurl(
                "UID=abc", user_id="12345", pick_code="abcdefg1234567"
            )
        self.assertEqual(info["url"], "http://cdn.example.com/a.mkv")
        self.assertEqual(info["file_name"], "a.mkv")
        self.assertEqual(info["size"], 12)
        self.assertEqual(mocked.call_args[0][1], "abcdefg1234567")

    def test_high_level_returns_download_headers_bound_to_link_user_agent(self):
        """直链按 UA 签名：取链用什么 UA，下载头就必须是什么 UA（否则 403）。"""
        with patch.object(
            downurl,
            "resolve_115_downurl_payload",
            return_value=("http://cdn.example.com/a.mkv", "acw_tc=1; path-hash=2", {"file_name": "a.mkv", "size": 12}),
        ):
            info = downurl.resolve_115_download_url_by_downurl(
                "UID=abc; CID=def", pick_code="abcdefg1234567"
            )
        headers = info["headers"]
        self.assertEqual(headers["User-Agent"], downurl._DEFAULT_115_USER_AGENT)
        # 账号 Cookie 与取链响应下发的路径级 Cookie 都要带上
        self.assertIn("UID=abc", headers["Cookie"])
        self.assertIn("acw_tc=1", headers["Cookie"])
        self.assertEqual(info["cookie"], "acw_tc=1; path-hash=2")

    def test_high_level_resolves_pick_code_from_file_id(self):
        with patch.object(pan115, "get_115_file_info", return_value={"pick_code": "abcdefg1234567"}):
            with patch.object(
                downurl,
                "resolve_115_downurl_payload",
                return_value=("http://cdn.example.com/a.mkv", "", {"file_name": "", "size": 0}),
            ) as mocked:
                downurl.resolve_115_download_url_by_downurl("UID=abc", file_id="308123456789")
        self.assertEqual(mocked.call_args[0][1], "abcdefg1234567")

    def test_high_level_without_any_code_raises(self):
        with self.assertRaises(downurl.Pan115DownurlError):
            downurl.resolve_115_download_url_by_downurl("UID=abc")

    def test_collect_urls_handles_fid_keyed_payload(self):
        payload = {"10086": {"url": {"url": "http://cdn.example.com/a.mkv"}}}
        self.assertEqual(downurl.collect_115_download_urls(payload), ["http://cdn.example.com/a.mkv"])


class Pan115TwoStageResolveTests(unittest.TestCase):
    """provider 层两段式取链。"""

    COOKIE = "UID=abc"
    FILE_ID = "308123456789"
    FILE_INFO = {
        "id": "308123456789",
        "name": "电影.mkv",
        "pick_code": "abcdefg1234567",
        "size": 1024,
        "sha1": "SHA1X",
        "is_dir": False,
    }
    SIZE_LIMIT_PAYLOAD = {
        "state": False,
        "errno": 50028,
        "msg": "文件大小超出限制，请用电脑端下载",
    }

    def _call(self, webapi_result, downurl_side_effect, set_cookies=None):
        with patch.object(pan115, "throttle_115_api_requests", return_value=None), patch.object(
            pan115, "get_115_file_info", return_value=dict(self.FILE_INFO)
        ), patch.object(
            pan115,
            "_request_115_webapi_json_with_cookies",
            return_value=(webapi_result, list(set_cookies or [])),
        ), patch.object(
            pan115, "resolve_115_download_url_by_downurl", side_effect=downurl_side_effect
        ) as mocked_downurl, patch.object(pan115, "mark_cookie_health_success", return_value=None), patch.object(
            pan115, "mark_cookie_health_failure", return_value=None
        ):
            try:
                return pan115.resolve_115_download_info(self.COOKIE, self.FILE_ID), mocked_downurl
            except Exception as exc:  # noqa: BLE001 - 测试要检查异常文案
                return exc, mocked_downurl

    def test_normal_path_does_not_fall_back(self):
        def boom(*args, **kwargs):
            raise AssertionError("普通接口成功时不应触发 downurl 回退")

        info, mocked_downurl = self._call(
            {"state": True, "data": {"url": "http://cdn.example.com/small.mkv"}},
            boom,
            # webapi 正常会给一条路径级 Cookie，带上就不该再回退
            set_cookies=["abc123=xyz; path=/6ac7/"],
        )
        self.assertFalse(mocked_downurl.called)
        self.assertEqual(info["url"], "http://cdn.example.com/small.mkv")
        self.assertEqual(info["pick_code"], "abcdefg1234567")
        self.assertEqual(info["name"], "电影.mkv")
        self.assertEqual(info["size"], 1024)
        self.assertEqual(info["sha1"], "SHA1X")
        self.assertEqual(info["file_id"], self.FILE_ID)

    def test_size_limit_falls_back_to_downurl(self):
        info, mocked_downurl = self._call(
            self.SIZE_LIMIT_PAYLOAD,
            lambda *args, **kwargs: {"url": "http://cdn.example.com/big.mkv", "file_name": "大电影.mkv", "size": 8589934592},
        )
        self.assertTrue(mocked_downurl.called)
        self.assertEqual(info["url"], "http://cdn.example.com/big.mkv")
        # 第二段能补的文件名/大小只在第一段缺省时使用。
        self.assertEqual(info["name"], "电影.mkv")
        self.assertEqual(info["size"], 1024)

    def test_size_limit_fills_missing_meta_from_downurl(self):
        file_info = dict(self.FILE_INFO, name="", size=0, sha1="")
        with patch.object(pan115, "throttle_115_api_requests", return_value=None), patch.object(
            pan115, "get_115_file_info", return_value=file_info
        ), patch.object(
            pan115,
            "_request_115_webapi_json_with_cookies",
            return_value=(self.SIZE_LIMIT_PAYLOAD, []),
        ), patch.object(
            pan115,
            "resolve_115_download_url_by_downurl",
            return_value={
                "url": "http://cdn.example.com/big.mkv",
                "file_name": "大电影.mkv",
                "size": 8589934592,
                "headers": {"User-Agent": "Mozilla/5.0 115-media-hub", "Cookie": "UID=abc"},
            },
        ), patch.object(
            pan115, "mark_cookie_health_success", return_value=None
        ), patch.object(
            pan115, "mark_cookie_health_failure", return_value=None
        ):
            info = pan115.resolve_115_download_info(self.COOKIE, self.FILE_ID)
        self.assertEqual(info["name"], "大电影.mkv")
        self.assertEqual(info["size"], 8589934592)

    def test_webapi_path_returns_headers_matching_kuaichuan_user_agent_and_path_cookie(self):
        """webapi 直链：下载头必须用取链那套 UA + 响应下发的路径级 Cookie。"""
        info, _ = self._call(
            {"state": True, "data": {"url": "http://cdn.example.com/small.mkv"}},
            lambda *a, **k: self.fail("不该回退"),
            set_cookies=["acw_tc=1; path=/; HttpOnly", "abc123=xyz; path=/6ac7/; domain=115.com"],
        )
        headers = info["headers"]
        self.assertEqual(headers["User-Agent"], pan115._CLOUD115_WEBAPI_USER_AGENT)
        self.assertIn("UID=abc", headers["Cookie"])
        self.assertIn("acw_tc=1", headers["Cookie"])
        self.assertIn("abc123=xyz", headers["Cookie"])
        self.assertIn("acw_tc=1", info["cookie"])

    def test_webapi_missing_path_cookie_falls_back_to_downurl(self):
        """webapi 直链缺路径级 Cookie 时必然 403，此时应改走电脑端通道。"""
        info, mocked_downurl = self._call(
            {"state": True, "data": {"url": "http://cdn.example.com/will-403.mkv"}},
            lambda *a, **k: {
                "url": "http://cdn.example.com/ok.mkv",
                "cookie": "acw_tc=7",
                "headers": {
                    "User-Agent": "Mozilla/5.0 115-media-hub",
                    "Referer": "https://115.com/",
                    "Cookie": "UID=abc; acw_tc=7",
                },
            },
            set_cookies=[],
        )
        self.assertTrue(mocked_downurl.called)
        self.assertEqual(info["url"], "http://cdn.example.com/ok.mkv")
        self.assertEqual(info["headers"]["User-Agent"], "Mozilla/5.0 115-media-hub")

    def test_webapi_missing_path_cookie_keeps_link_when_fallback_fails(self):
        """回退失败也不该把已经拿到的直链判死，仍返回 webapi 结果。"""

        def boom(*args, **kwargs):
            raise downurl.Pan115DownurlError("downurl 临时不可用")

        info, mocked_downurl = self._call(
            {"state": True, "data": {"url": "http://cdn.example.com/keep.mkv"}},
            boom,
            set_cookies=[],
        )
        self.assertTrue(mocked_downurl.called)
        self.assertEqual(info["url"], "http://cdn.example.com/keep.mkv")
        self.assertEqual(info["headers"]["User-Agent"], pan115._CLOUD115_WEBAPI_USER_AGENT)

    def test_downurl_path_returns_headers_with_link_user_agent(self):
        info, _ = self._call(
            self.SIZE_LIMIT_PAYLOAD,
            lambda *a, **k: {
                "url": "http://cdn.example.com/big.mkv",
                "file_name": "大电影.mkv",
                "size": 8589934592,
                "cookie": "acw_tc=9",
                "headers": {
                    "User-Agent": "Mozilla/5.0 115-media-hub",
                    "Referer": "https://115.com/",
                    "Cookie": "UID=abc; acw_tc=9",
                },
            },
        )
        self.assertEqual(info["headers"]["User-Agent"], "Mozilla/5.0 115-media-hub")
        self.assertIn("acw_tc=9", info["headers"]["Cookie"])

    def test_both_fail_message_contains_both_reasons(self):
        def downurl_boom(*args, **kwargs):
            raise downurl.Pan115DownurlError("115 downurl 解析失败: 服务端掐断")

        exc, mocked_downurl = self._call(self.SIZE_LIMIT_PAYLOAD, downurl_boom)
        self.assertTrue(mocked_downurl.called)
        self.assertIsInstance(exc, RuntimeError)
        message = str(exc)
        self.assertIn("该文件需要走电脑端下载通道", message)
        self.assertIn("文件大小超出限制", message)
        self.assertIn("服务端掐断", message)

    def test_other_errors_do_not_fall_back(self):
        def boom(*args, **kwargs):
            raise AssertionError("非 50028 错误不应触发 downurl 回退")

        exc, mocked_downurl = self._call({"state": False, "msg": "登录已失效"}, boom)
        self.assertFalse(mocked_downurl.called)
        self.assertIsInstance(exc, RuntimeError)
        self.assertEqual(str(exc), "登录已失效")


if __name__ == "__main__":
    unittest.main()
