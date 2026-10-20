# -*- coding: utf-8 -*-
"""夸克分享目录 stoken 编码回归测试（2026-10-20 真机 bug）。

背景：sharepage/detail 的 stoken 传参曾经是「手动 quote + urlencode」双重编码，
当时夸克服务端双 decode，正好对上。2026-10-20 真机实测服务端已改为单次 decode，
含 +/=/ 的 stoken（base64 绝大多数都有）双编码后签名失配，返回 400「非法token」；
纯字母数字的 stoken 两种编码结果碰巧一致，所以问题时好时坏。

这两条用例钉死：出站 URL 必须是 urlencode 单次编码（%2B，而不是 %252B）。
"""
import unittest
from unittest import mock

from app.providers import quark


_SPECIAL_STOKEN = "ab+cd/ef=gh=="  # base64 典型特殊字符全覆盖


class TestShareDetailStokenEncoding(unittest.TestCase):
    def test_slow_path_stoken_single_encoded(self):
        captured = {}

        def _fake_request(url, headers, timeout=45, fallback=""):
            captured["url"] = url
            return {"status": 200, "code": 0, "data": {"list": [], "share": {"pwd_id": "pwd1"}}}

        with mock.patch.object(quark, "_request_quark_json", side_effect=_fake_request):
            quark._list_quark_share_page(
                "cookie", "pwd1", _SPECIAL_STOKEN, force_refresh=True
            )
        url = captured.get("url", "")
        self.assertIn("sharepage/detail", url)
        self.assertIn("stoken=ab%2Bcd%2Fef%3Dgh%3D%3D", url)
        self.assertNotIn("%252B", url)

    def test_fast_path_stoken_single_encoded(self):
        captured = {}

        def _fake_request(method, url, headers, deadline_ts, fallback=""):
            captured["url"] = url
            return {"status": 200, "code": 0, "data": {"list": [], "share": {"pwd_id": "pwd1"}}}

        with mock.patch.object(quark, "_request_quark_json_fast", side_effect=_fake_request):
            quark._list_quark_share_page_fast(
                "cookie", "pwd1", _SPECIAL_STOKEN, "0", 1, 50, True, 0.0
            )
        url = captured.get("url", "")
        self.assertIn("sharepage/detail", url)
        self.assertIn("stoken=ab%2Bcd%2Fef%3Dgh%3D%3D", url)
        self.assertNotIn("%252B", url)


if __name__ == "__main__":
    unittest.main()
