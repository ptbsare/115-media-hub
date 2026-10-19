"""115 pick_code → file_id 推导的单元测试（全部 mock，不发真实请求）。"""

import unittest
from unittest import mock

from app.providers import pan115


_COOKIE = "UID=10086; CID=abc; SEID=test-seid"
_PICK_CODE = "abc123def456ghi789"
_FID = "2960000000000000001"


class Pan115PickCodeResolveTest(unittest.TestCase):
    def setUp(self):
        pan115.invalidate_115_pick_code_fid_cache()
        self.addCleanup(pan115.invalidate_115_pick_code_fid_cache)
        self.patches = [
            mock.patch.object(pan115, "throttle_115_api_requests"),
            mock.patch.object(pan115, "mark_cookie_health_success"),
            mock.patch.object(pan115, "mark_cookie_health_failure"),
        ]
        for patcher in self.patches:
            patcher.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])

    def test_pick_code_shape_detection(self):
        self.assertFalse(pan115.is_115_pick_code_like(_FID))
        self.assertFalse(pan115.is_115_pick_code_like("0"))
        self.assertFalse(pan115.is_115_pick_code_like(""))
        self.assertTrue(pan115.is_115_pick_code_like(_PICK_CODE))

    def test_fid_passthrough_does_not_resolve(self):
        # fid 形态原样透传，不触发 pick_code 查询
        with mock.patch.object(pan115, "resolve_file_id_by_pick_code") as resolve_mock:
            self.assertEqual(pan115.ensure_115_file_id(_COOKIE, _FID), _FID)
        resolve_mock.assert_not_called()

    def test_pick_code_download_resolves_to_fid(self):
        provider = pan115.Pan115Provider()
        with mock.patch.object(
            pan115, "resolve_file_id_by_pick_code", return_value=_FID
        ) as resolve_mock, mock.patch.object(
            pan115,
            "_request_115_webapi_json",
            return_value={"state": True, "file_url": "https://dl.115.com/file/1"},
        ) as request_mock:
            url = provider.resolve_download_url(_COOKIE, _PICK_CODE)

        self.assertEqual(url, "https://dl.115.com/file/1")
        resolve_mock.assert_called_once_with(_COOKIE, _PICK_CODE)
        # 取链请求使用 pickcode，且确实发出了请求
        self.assertIn("files/download?pickcode=", request_mock.call_args[0][0])

    def test_fid_download_looks_up_pick_code_without_resolve(self):
        provider = pan115.Pan115Provider()
        with mock.patch.object(pan115, "resolve_file_id_by_pick_code") as resolve_mock, mock.patch.object(
            pan115,
            "get_115_file_info",
            return_value={"id": _FID, "name": "a.mkv", "pick_code": _PICK_CODE, "size": 10, "sha1": "SHA"},
        ), mock.patch.object(
            pan115,
            "_request_115_webapi_json",
            return_value={"state": True, "file_url": "https://dl.115.com/file/2"},
        ):
            info = provider.resolve_download_info(_COOKIE, _FID)

        resolve_mock.assert_not_called()
        self.assertEqual(info["file_id"], _FID)
        self.assertEqual(info["pick_code"], _PICK_CODE)
        self.assertEqual(info["url"], "https://dl.115.com/file/2")

    def test_resolve_hits_cache_and_skips_second_request(self):
        payload = {"state": True, "data": [{"fid": _FID, "pc": _PICK_CODE}]}
        with mock.patch.object(
            pan115, "_request_115_webapi_json", return_value=payload
        ) as request_mock:
            first = pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE)
            second = pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE)

        self.assertEqual(first, _FID)
        self.assertEqual(second, _FID)
        request_mock.assert_called_once()

    def test_expired_cache_re_resolves(self):
        payload = {"state": True, "data": [{"fid": _FID, "pc": _PICK_CODE}]}
        with mock.patch.object(
            pan115, "_request_115_webapi_json", return_value=payload
        ) as request_mock:
            pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE)
            with pan115._115_pick_code_fid_cache_lock:
                pan115._115_pick_code_fid_cache[_PICK_CODE]["expires_at"] = 0
            pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE)

        self.assertEqual(request_mock.call_count, 2)

    def test_resolve_returns_empty_when_api_fails(self):
        with mock.patch.object(
            pan115, "_request_115_webapi_json", return_value={"state": False, "msg": "文件不存在"}
        ):
            self.assertEqual(pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE), "")

    def test_resolve_returns_empty_on_exception(self):
        with mock.patch.object(
            pan115, "_request_115_webapi_json", side_effect=RuntimeError("网络断了")
        ):
            self.assertEqual(pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE), "")

    def test_resolve_failure_raises_clear_error_without_download_request(self):
        provider = pan115.Pan115Provider()
        with mock.patch.object(
            pan115, "resolve_file_id_by_pick_code", return_value=""
        ), mock.patch.object(pan115, "_request_115_webapi_json") as request_mock:
            with self.assertRaises(RuntimeError) as ctx:
                provider.resolve_download_info(_COOKIE, _PICK_CODE)

        self.assertIn("无法从 pick_code 定位文件，请刷新目录后重试", str(ctx.exception))
        # 不能拿 pick_code 硬发下载请求
        request_mock.assert_not_called()

    def test_batch_resolve_reuses_cache(self):
        payload = {"state": True, "data": [{"fid": _FID, "pc": _PICK_CODE}]}
        other_code = "xyz987654321abc"
        other_payload = {"state": True, "data": [{"fid": "2960000000000000002", "pc": other_code}]}

        def fake_request(url, headers=None, timeout=30):
            return other_payload if other_code in url else payload

        with mock.patch.object(pan115, "_request_115_webapi_json", side_effect=fake_request) as request_mock:
            result = pan115.resolve_file_ids_by_pick_codes(
                _COOKIE, [_PICK_CODE, _PICK_CODE, other_code]
            )
            # 单条再查一次，命中缓存不应新增请求
            again = pan115.resolve_file_id_by_pick_code(_COOKIE, _PICK_CODE)

        self.assertEqual(
            result,
            {_PICK_CODE: _FID, other_code: "2960000000000000002"},
        )
        self.assertEqual(again, _FID)
        self.assertEqual(request_mock.call_count, 2)

    def test_normalize_keeps_pick_code_without_network(self):
        with mock.patch.object(pan115, "_request_115_webapi_json") as request_mock, mock.patch.object(
            pan115, "resolve_file_id_by_pick_code"
        ) as resolve_mock:
            entry = pan115._normalize_115_file_entry(
                {"n": "a.mkv", "pc": _PICK_CODE, "s": 1024, "sha1": "deadbeef"}
            )

        self.assertEqual(entry["id"], _PICK_CODE)
        self.assertEqual(entry["pick_code"], _PICK_CODE)
        self.assertFalse(entry["is_dir"])
        request_mock.assert_not_called()
        resolve_mock.assert_not_called()

    def test_normalize_prefers_real_fid(self):
        entry = pan115._normalize_115_file_entry(
            {"n": "a.mkv", "fid": _FID, "pc": _PICK_CODE, "s": 1024}
        )
        self.assertEqual(entry["id"], _FID)
        self.assertEqual(entry["pick_code"], _PICK_CODE)

    def test_get_file_hash_resolves_pick_code_and_uppercases(self):
        provider = pan115.Pan115Provider()
        with mock.patch.object(
            pan115, "resolve_file_id_by_pick_code", return_value=_FID
        ) as resolve_mock, mock.patch.object(
            pan115,
            "get_115_file_info",
            return_value={
                "id": _FID,
                "name": "a.mkv",
                "sha1": "abcdef0123456789abcdef0123456789abcdef01",
                "size": 1024,
                "is_dir": False,
            },
        ):
            info = provider.get_file_hash(_COOKIE, _PICK_CODE)

        resolve_mock.assert_called_once_with(_COOKIE, _PICK_CODE)
        self.assertEqual(info["sha1"], "ABCDEF0123456789ABCDEF0123456789ABCDEF01")
        self.assertEqual(info["size"], 1024)
        self.assertEqual(info["name"], "a.mkv")

    def test_get_file_hash_returns_empty_without_sha1(self):
        with mock.patch.object(
            pan115,
            "get_115_file_info",
            return_value={"id": _FID, "sha1": "", "size": 1024, "is_dir": False},
        ):
            self.assertEqual(pan115.get_115_file_hash(_COOKIE, _FID), {})

    def test_get_file_hash_returns_empty_for_directory(self):
        with mock.patch.object(
            pan115,
            "get_115_file_info",
            return_value={"id": _FID, "sha1": "A" * 40, "size": 0, "is_dir": True},
        ):
            self.assertEqual(pan115.get_115_file_hash(_COOKIE, _FID), {})


if __name__ == "__main__":
    unittest.main()
