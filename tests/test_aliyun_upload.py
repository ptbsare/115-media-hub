"""阿里云盘上传能力单元测试（mock 全部 HTTP，无真实网络请求）。

覆盖：一段/两段式秒传命中、分片上传、pre_hash、proof_code、取消、错误映射、参数校验。
"""
import base64
import hashlib
import io
import math
import unittest
from unittest.mock import patch

from app.providers import aliyun
from app.providers.aliyun import AliyunProvider
from app.providers.base import UploadError, UploadResult


PART_SIZE = aliyun._ALIYUN_UPLOAD_PART_SIZE
CREATE_URL = "https://api.alipan.com/adrive/v2/file/createWithFolders"
COMPLETE_URL = "https://api.alipan.com/v2/file/complete"


class _MemSink(io.BytesIO):
    def __init__(self, data: bytes, sha1: str = ""):
        super().__init__(data)
        self.size = len(data)
        if sha1:
            self.sha1 = sha1


class AliyunUploadTestBase(unittest.TestCase):
    def setUp(self):
        self.provider = AliyunProvider()
        self.put_calls = []
        self.api_calls = []
        self.create_response = None  # 可配置：第一段响应或抛 RuntimeError
        self.put_status = 200
        self.complete_response = None  # None = 回显 create 的 file_id（真实行为）

        def fake_resolve_drive_id(refresh_token):
            return "drive-1"

        def fake_ensure_token(refresh_token):
            return "fake-access-token"

        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            if url == CREATE_URL:
                if isinstance(self.create_response, Exception):
                    raise self.create_response
                return self.create_response
            if url == COMPLETE_URL:
                # 真实服务端返回完成后的 file_id（与 create 一致）；测试可覆盖为异常值
                if self.complete_response is not None:
                    return self.complete_response
                return {"file_id": body.get("file_id", "")}
            raise AssertionError(f"未预期的接口调用: {url}")

        def fake_put(url, data=None, timeout=None, **kwargs):
            self.put_calls.append({"url": url, "data": data})
            resp = unittest.mock.MagicMock()
            resp.status_code = self.put_status
            return resp

        patchers = [
            patch.object(self.provider, "_resolve_drive_id", side_effect=fake_resolve_drive_id),
            patch.object(self.provider, "_ensure_access_token", side_effect=fake_ensure_token),
            patch.object(self.provider, "_api_post", side_effect=fake_api_post),
            patch.object(self.provider, "throttle"),
            patch("app.providers.aliyun.requests.put", side_effect=fake_put),
        ]
        for patcher in patchers:
            patcher.start()
        self.addCleanup(self._stop_all)

    def _stop_all(self):
        # patch 由 addCleanup 自动停止；此处占位保持结构清晰
        pass

    # ---- 响应构造 ----
    @staticmethod
    def _create_resp(rapid=False, file_id="F-1", upload_id="U-1", part_urls=None, part_count=1):
        urls = part_urls or [f"https://oss.example.com/part{i + 1}" for i in range(part_count)]
        part_info_list = [
            {"part_number": i + 1, "upload_url": urls[i]}
            for i in range(part_count)
        ]
        resp = {"file_id": file_id, "upload_id": upload_id}
        if rapid:
            resp["rapid_upload"] = True
        else:
            resp["part_info_list"] = part_info_list
        return resp


class TestAliyunUploadHappyPath(AliyunUploadTestBase):
    def test_rapid_upload_single_round(self):
        """第一段即秒传命中（rapid_upload=true）。"""
        self.create_response = self._create_resp(rapid=True)
        data = b"r" * 2048
        result = self.provider.upload_file(
            "rt-1", _MemSink(data), "t.mkv", "dir-1",
            sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.file_id, "F-1")
        self.assertEqual(len(self.api_calls), 1)
        body = self.api_calls[0]["body"]
        self.assertIn("pre_hash", body)
        self.assertNotIn("content_hash", body)
        self.assertEqual(body["parent_file_id"], "dir-1")
        self.assertEqual(body["check_name_mode"], "overwrite")
        self.assertFalse(self.put_calls)

    def test_rapid_upload_two_round(self):
        """两段式秒传：PreHashMatched → content_hash + proof_code → rapid。"""
        self.create_response = RuntimeError("阿里云盘 API 错误: PreHashMatched - pre hash matched")
        data = b"t" * 5000
        sha1 = hashlib.sha1(data).hexdigest()
        # 第一段抛错后第二段返回 rapid
        original = self.create_response

        def side_effect(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            if isinstance(original, Exception) and "pre_hash" in body:
                raise original
            return self._create_resp(rapid=True)

        with patch.object(self.provider, "_api_post", side_effect=side_effect):
            result = self.provider.upload_file(
                "rt-1", _MemSink(data, sha1=sha1.upper()), "t.mkv", "root", size=len(data),
            )
        self.assertEqual(result.status, "rapid")
        second = self.api_calls[-1]["body"]
        self.assertEqual(second["content_hash"], sha1)
        self.assertEqual(second["content_hash_name"], "sha1")
        self.assertEqual(second["proof_version"], "v1")
        self.assertIn("proof_code", second)
        # proof_code 内容校验
        offset = int(hashlib.md5(b"fake-access-token").hexdigest()[:16], 16) % len(data)
        self.assertEqual(
            second["proof_code"], base64.b64encode(data[offset:offset + 8]).decode()
        )
        self.assertFalse(self.put_calls)

    def test_upload_after_rapid_miss(self):
        """秒传未命中：分片 PUT → complete → uploaded。"""
        part_urls = ["https://oss.example.com/p1", "https://oss.example.com/p2"]
        self.create_response = self._create_resp(
            file_id="F-9", upload_id="U-9", part_urls=part_urls, part_count=2
        )
        data = b"u" * (PART_SIZE + 777)
        result = self.provider.upload_file(
            "rt-1", _MemSink(data), "big.mkv", "root",
            sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.file_id, "F-9")
        self.assertEqual(len(self.put_calls), 2)
        self.assertEqual(len(self.put_calls[0]["data"]), PART_SIZE)
        self.assertEqual(len(self.put_calls[1]["data"]), 777)
        # complete 调用
        complete_call = self.api_calls[-1]
        self.assertEqual(complete_call["url"], COMPLETE_URL)
        self.assertEqual(complete_call["body"], {
            "drive_id": "drive-1", "file_id": "F-9", "upload_id": "U-9",
        })

    def test_part_count_for_large_file(self):
        """25MB 文件 → 3 个分片（10/10/5）。"""
        self.create_response = self._create_resp(part_count=3)
        data = b"L" * (25 * 1024 * 1024)
        result = self.provider.upload_file(
            "rt-1", _MemSink(data), "huge.mkv", "root",
            sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertEqual(result.status, "uploaded")
        sizes = [len(c["data"]) for c in self.put_calls]
        self.assertEqual(sizes, [PART_SIZE, PART_SIZE, 5 * 1024 * 1024])
        # 第一段 body 的 part_info_list 有 3 项
        first_body = self.api_calls[0]["body"]
        self.assertEqual(len(first_body["part_info_list"]), 3)
        self.assertEqual(first_body["size"], 25 * 1024 * 1024)

    def test_pre_hash_is_first_1kb_sha1(self):
        """pre_hash 必须是文件前 1024 字节的 SHA1。"""
        self.create_response = self._create_resp(rapid=True)
        data = b"p" * 4096
        self.provider.upload_file(
            "rt-1", _MemSink(data), "t.bin", "root",
            sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        body = self.api_calls[0]["body"]
        self.assertEqual(body["pre_hash"], hashlib.sha1(data[:1024]).hexdigest())

    def test_sha1_computed_when_missing(self):
        """未传 sha1 时二段式自动计算完整哈希。"""
        original_exc = RuntimeError("阿里云盘 API 错误: PreHashMatched - pre hash matched")
        data = b"c" * 3000
        sha1 = hashlib.sha1(data).hexdigest()

        def side_effect(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            if "pre_hash" in body and "content_hash" not in body:
                raise original_exc
            return self._create_resp(rapid=True)

        with patch.object(self.provider, "_api_post", side_effect=side_effect):
            result = self.provider.upload_file("rt-1", _MemSink(data), "t.bin", "root", size=len(data))
        self.assertEqual(result.status, "rapid")
        second = self.api_calls[-1]["body"]
        self.assertEqual(second["content_hash"], sha1)
        self.assertEqual(result.sha1, sha1.upper())

    def test_progress_callback(self):
        self.create_response = self._create_resp(part_count=2)
        data = b"g" * (PART_SIZE + 512)
        progress = []
        self.provider.upload_file(
            "rt-1", _MemSink(data), "t.mkv", "root",
            sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
            progress_cb=lambda sent, total: progress.append((sent, total)),
        )
        self.assertEqual(progress, [(PART_SIZE, len(data)), (len(data), len(data))])


class TestAliyunUploadCancel(AliyunUploadTestBase):
    def test_cancel_before_parts(self):
        self.create_response = self._create_resp(part_count=3)
        data = b"x" * (PART_SIZE * 3)
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file(
                "rt-1", _MemSink(data), "t.mkv", "root",
                sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
                cancel_check=lambda: True,
            )
        self.assertIn("取消", str(ctx.exception))
        self.assertFalse(self.put_calls)

    def test_cancel_between_parts(self):
        self.create_response = self._create_resp(part_count=3)
        data = b"x" * (PART_SIZE * 3)
        checks = {"n": 0}

        def cancel_check():
            checks["n"] += 1
            return checks["n"] >= 2

        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file(
                "rt-1", _MemSink(data), "t.mkv", "root",
                sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
                cancel_check=cancel_check,
            )
        self.assertIn("取消", str(ctx.exception))
        self.assertEqual(len(self.put_calls), 1)


class TestAliyunUploadErrors(AliyunUploadTestBase):
    def test_missing_refresh_token(self):
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("", _MemSink(b"x"), "t.mkv", "root", size=1)
        self.assertIn("refresh_token", str(ctx.exception))

    def test_missing_filename(self):
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", _MemSink(b"x"), "", "root", size=1)
        self.assertIn("文件名", str(ctx.exception))

    def test_empty_file(self):
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", _MemSink(b""), "t.mkv", "root", size=0)
        self.assertIn("空文件", str(ctx.exception))

    def test_unknown_size(self):
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", io.BytesIO(b"x"), "t.mkv", "root")

    def test_first_round_login_expired(self):
        self.create_response = RuntimeError("阿里云盘 API 错误: AccessTokenExpired - token expired")
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", _MemSink(b"x" * 1024), "t.mkv", "root", size=1024)
        self.assertIn("登录", str(ctx.exception))

    def test_first_round_space_full(self):
        self.create_response = RuntimeError("阿里云盘 API 错误: ForbiddenNoSpace - no space")
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", _MemSink(b"x" * 1024), "t.mkv", "root", size=1024)
        self.assertIn("空间不足", str(ctx.exception))

    def test_second_round_prehash_matched_again(self):
        """二段仍返回 PreHashMatched → 秒传校验失败文案（不死循环）。"""

        def side_effect(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            raise RuntimeError("阿里云盘 API 错误: PreHashMatched - pre hash matched")

        with patch.object(self.provider, "_api_post", side_effect=side_effect):
            with self.assertRaises(UploadError) as ctx:
                self.provider.upload_file("rt-1", _MemSink(b"x" * 1024), "t.mkv", "root", size=1024)
        self.assertIn("秒传校验", str(ctx.exception))

    def test_put_failure_retriable(self):
        self.create_response = self._create_resp(part_count=1)
        self.put_status = 403
        data = b"e" * 2048
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file(
                "rt-1", _MemSink(data), "t.mkv", "root",
                sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
            )
        self.assertIn("上传失败", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_complete_file_id_mismatch(self):
        self.create_response = self._create_resp(file_id="F-1", part_count=1)
        self.complete_response = {"file_id": "F-OTHER"}
        data = b"m" * 2048
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file(
                "rt-1", _MemSink(data), "t.mkv", "root",
                sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
            )
        self.assertIn("提交结果异常", str(ctx.exception))

    def test_complete_api_error(self):
        self.create_response = self._create_resp(part_count=1)

        def side_effect(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            if url == COMPLETE_URL:
                raise RuntimeError("阿里云盘 API 错误: TooManyRequests - slow down")
            return self.create_response

        with patch.object(self.provider, "_api_post", side_effect=side_effect):
            with self.assertRaises(UploadError) as ctx:
                self.provider.upload_file(
                    "rt-1", _MemSink(b"m" * 2048), "t.mkv", "root",
                    sha1=hashlib.sha1(b"m" * 2048).hexdigest().upper(), size=2048,
                )
        self.assertIn("频繁", str(ctx.exception))

    def test_missing_part_info(self):
        self.create_response = {"file_id": "F-1", "upload_id": "U-1"}  # 无 part_info_list
        with self.assertRaises(UploadError) as ctx:
            self.provider.upload_file("rt-1", _MemSink(b"x" * 1024), "t.mkv", "root", size=1024)
        self.assertIn("分片信息", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()


OPEN_API = "https://openapi.alipan.com"
OPEN_CREATE_URL = f"{OPEN_API}/adrive/v1.0/openFile/create"
OPEN_COMPLETE_URL = f"{OPEN_API}/adrive/v1.0/openFile/complete"
OPEN_LIST_URL = f"{OPEN_API}/adrive/v1.0/openFile/list"


class TestAliyunOpenApiMode(unittest.TestCase):
    """扫码授权（access_token）模式：所有文件操作必须打到 openapi.alipan.com。

    网页会话模式的 token 与 OpenAPI token 不通用，打错域名会 401 AccessTokenInvalid。
    """

    def setUp(self):
        self.provider = AliyunProvider()
        self.api_calls = []
        self.put_calls = []
        self.responses = {}  # url -> dict 或 Exception

        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            item = self.responses.get(url)
            if isinstance(item, Exception):
                raise item
            return item if item is not None else {}

        def fake_put(url, data=None, timeout=None, **kwargs):
            self.put_calls.append({"url": url, "data": data})
            resp = unittest.mock.MagicMock()
            resp.status_code = 200
            return resp

        def fake_post(url, headers=None, json=None, timeout=None, **kwargs):
            self.api_calls.append({"url": url, "body": json})
            resp = unittest.mock.MagicMock()
            resp.status_code = 204
            resp.text = ""
            return resp

        for patcher in [
            patch.object(self.provider, "_is_open_access_mode", return_value=True),
            patch.object(self.provider, "_resolve_drive_id", return_value="drive-1"),
            patch.object(self.provider, "_ensure_access_token", return_value="at-1"),
            patch.object(self.provider, "_api_post", side_effect=fake_api_post),
            patch.object(self.provider, "throttle"),
            patch("app.providers.aliyun.requests.put", side_effect=fake_put),
            patch("app.providers.aliyun.requests.post", side_effect=fake_post),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _upload(self, data):
        return self.provider.upload_file(
            "at-1",
            _MemSink(data),
            "probe.bin",
            "root",
            sha1=hashlib.sha1(data).hexdigest().upper(),
            size=len(data),
        )

    def test_upload_rapid_uses_open_create_and_complete(self):
        self.responses[OPEN_CREATE_URL] = {
            "file_id": "F-9", "upload_id": "U-9", "rapid_upload": True,
        }
        result = self._upload(b"q" * 2048)
        self.assertEqual(result.status, "rapid")
        urls = [c["url"] for c in self.api_calls]
        self.assertIn(OPEN_CREATE_URL, urls)
        self.assertIn(OPEN_COMPLETE_URL, urls)
        create_body = next(c["body"] for c in self.api_calls if c["url"] == OPEN_CREATE_URL)
        # OpenAPI 重名策略只支持 ignore/auto_rename/refuse
        self.assertEqual(create_body["check_name_mode"], "ignore")

    def test_upload_normal_parts_then_complete(self):
        self.responses[OPEN_CREATE_URL] = {
            "file_id": "F-9",
            "upload_id": "U-9",
            "part_info_list": [{"part_number": 1, "upload_url": "https://oss.example.com/p1"}],
        }
        self.responses[OPEN_COMPLETE_URL] = {"file_id": "F-9"}
        result = self._upload(b"w" * 2048)
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(len(self.put_calls), 1)
        self.assertEqual(self.api_calls[-1]["url"], OPEN_COMPLETE_URL)

    def test_list_uses_open_endpoint_with_marker_pagination(self):
        pages = [
            {
                "items": [{"file_id": "d1", "name": "a", "type": "folder", "size": 0}],
                "next_marker": "m2",
            },
            {
                "items": [{"file_id": "f1", "name": "b.mkv", "type": "file", "size": 10}],
                "next_marker": "",
            },
        ]

        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.api_calls.append({"url": url, "body": body})
            return pages.pop(0)

        with patch.object(self.provider, "_api_post", side_effect=fake_api_post):
            payload = self.provider.list_entries_payload("at-1", "root")
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["summary"], {"folder_count": 1, "file_count": 1})
        self.assertTrue(all(c["url"] == OPEN_LIST_URL for c in self.api_calls))
        self.assertEqual(self.api_calls[0]["body"]["marker"], "")
        self.assertEqual(self.api_calls[1]["body"]["marker"], "m2")

    def test_create_folder_uses_open_endpoint(self):
        self.responses[OPEN_CREATE_URL] = {"file_id": "D-1"}
        result = self.provider.create_folder("at-1", "root", "跨盘复制自测")
        self.assertEqual(result["cid"], "D-1")
        self.assertEqual(self.api_calls[0]["url"], OPEN_CREATE_URL)
        self.assertEqual(self.api_calls[0]["body"]["type"], "folder")

    def test_file_management_uses_open_endpoints(self):
        self.provider.rename_entry("at-1", "F-1", "new.mkv")
        self.provider.move_entries("at-1", ["F-1"], "D-1")
        self.provider.copy_entries("at-1", ["F-1"], "D-1")
        self.provider.delete_entries("at-1", ["F-1"])
        urls = [c["url"] for c in self.api_calls]
        self.assertIn(f"{OPEN_API}/adrive/v1.0/openFile/update", urls)
        self.assertIn(f"{OPEN_API}/adrive/v1.0/openFile/move", urls)
        self.assertIn(f"{OPEN_API}/adrive/v1.0/openFile/copy", urls)
        self.assertIn(f"{OPEN_API}/adrive/v1.0/openFile/delete", urls)
        self.assertFalse(any("api.alipan.com" in u and "openapi" not in u for u in urls))

    def test_share_receive_blocked_in_open_mode(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.provider.prepare_share_receive("at-1", {"share_id": "s1"})
        self.assertIn("分享转存", str(ctx.exception))
        with self.assertRaises(RuntimeError):
            self.provider.submit_share_receive("at-1", {"share_id": "s1"}, [{"id": "f1"}])

    def test_probe_connectivity_checks_file_access(self):
        self.responses[OPEN_LIST_URL] = {"items": [], "next_marker": ""}
        self.assertTrue(self.provider.probe_connectivity("at-1"))
        self.assertIn(OPEN_LIST_URL, [c["url"] for c in self.api_calls])

    def test_probe_connectivity_false_when_list_forbidden(self):
        self.responses[OPEN_LIST_URL] = RuntimeError("PermissionDenied - scope")
        self.assertFalse(self.provider.probe_connectivity("at-1"))


class TestAliyunErrorBodyPreserved(unittest.TestCase):
    """4xx 响应体必须保留（code/message），否则两段式秒传与错误提示都会失效。"""

    def setUp(self):
        self.provider = AliyunProvider()
        patch.object(self.provider, "throttle").start()
        self.addCleanup(patch.stopall)

    def _fake_response(self, status_code, payload, text=""):
        resp = unittest.mock.MagicMock()
        resp.status_code = status_code
        resp.text = text or (str(payload) if payload is not None else "")
        resp.reason = "Bad Request"
        if payload is None:
            resp.json.side_effect = ValueError("no json")
        else:
            resp.json.return_value = payload
        return resp

    def test_post_json_raises_with_code_and_message(self):
        resp = self._fake_response(
            400, {"code": "PreHashMatched", "message": "pre hash matched"}
        )
        with patch("app.providers.aliyun.requests.post", return_value=resp):
            with self.assertRaises(RuntimeError) as ctx:
                self.provider._post_json("https://x/y", {}, {})
        msg = str(ctx.exception)
        self.assertIn("PreHashMatched", msg)
        self.assertIn("pre hash matched", msg)

    def test_post_json_handles_non_json_error_body(self):
        resp = self._fake_response(403, None, text="Forbidden")
        with patch("app.providers.aliyun.requests.post", return_value=resp):
            with self.assertRaises(RuntimeError) as ctx:
                self.provider._post_json("https://x/y", {}, {})
        self.assertIn("403", str(ctx.exception))
        self.assertIn("Forbidden", str(ctx.exception))


class TestAliyunTwoStageRapidUpload(unittest.TestCase):
    """第一段 pre_hash 被 PreHashMatched 拒绝后，第二段 content_hash + proof_code 秒传命中。"""

    def setUp(self):
        self.provider = AliyunProvider()
        self.calls = []
        for patcher in [
            patch.object(self.provider, "_is_open_access_mode", return_value=True),
            patch.object(self.provider, "_resolve_drive_id", return_value="drive-1"),
            patch.object(self.provider, "_ensure_access_token", return_value="at-1"),
            patch.object(self.provider, "throttle"),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_second_stage_uses_content_hash_then_completes(self):
        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.calls.append({"url": url, "body": body})
            if url.endswith("/openFile/create") and "pre_hash" in body:
                raise RuntimeError("阿里云盘 API 400: PreHashMatched - pre hash matched")
            if url.endswith("/openFile/create"):
                return {"file_id": "F-1", "upload_id": "U-1", "rapid_upload": True}
            return {"file_id": "F-1"}

        with patch.object(self.provider, "_api_post", side_effect=fake_api_post):
            data = b"probe"
            result = self.provider.upload_file(
                "at-1",
                _MemSink(data),
                "probe.bin",
                "root",
                sha1=hashlib.sha1(data).hexdigest().upper(),
                size=len(data),
            )

        self.assertEqual(result.status, "rapid")
        create_calls = [c for c in self.calls if c["url"].endswith("/openFile/create")]
        self.assertEqual(len(create_calls), 2)
        second = create_calls[1]["body"]
        self.assertEqual(second["content_hash_name"], "sha1")
        self.assertIn("content_hash", second)
        self.assertIn("proof_code", second)
        self.assertEqual(second["proof_version"], "v1")
        # 秒传命中且有 upload_id 时仍要 complete
        self.assertTrue(self.calls[-1]["url"].endswith("/openFile/complete"))


class AliyunGetFileHashTest(unittest.TestCase):
    """源端元信息 → SHA1（跨盘复制秒传探测用，不下载文件）。"""

    HASH = "abcdef0123456789abcdef0123456789abcdef01"

    def setUp(self):
        self.provider = AliyunProvider()
        self.calls = []
        self.open_mode = False
        self.response = {
            "type": "file",
            "name": "a.mkv",
            "size": 2048,
            "content_hash": self.HASH,
            "content_hash_name": "sha1",
        }

        def fake_api_post(refresh_token, url, body, timeout=30, share_token=""):
            self.calls.append({"url": url, "body": body})
            if isinstance(self.response, Exception):
                raise self.response
            return self.response

        patchers = [
            patch.object(self.provider, "_resolve_drive_id", side_effect=lambda token: "drive-1"),
            patch.object(self.provider, "_is_open_access_mode", side_effect=lambda: self.open_mode),
            patch.object(self.provider, "_api_post", side_effect=fake_api_post),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_web_session_reads_content_hash(self):
        info = self.provider.get_file_hash("rt-1", "file-1")
        self.assertEqual(info["sha1"], self.HASH.upper())
        self.assertEqual(info["size"], 2048)
        self.assertEqual(info["name"], "a.mkv")
        self.assertEqual(self.calls[0]["url"], "https://api.alipan.com/v2/file/get")
        self.assertEqual(self.calls[0]["body"], {"drive_id": "drive-1", "file_id": "file-1"})

    def test_open_mode_uses_openapi_endpoint(self):
        self.open_mode = True
        info = self.provider.get_file_hash("oauth-token", "file-1")
        self.assertEqual(info["sha1"], self.HASH.upper())
        self.assertEqual(self.calls[0]["url"], "https://openapi.alipan.com/adrive/v1.0/openFile/get")

    def test_non_sha1_hash_returns_empty(self):
        self.response = {"type": "file", "size": 10, "content_hash_name": "crc64", "content_hash": "x"}
        self.assertEqual(self.provider.get_file_hash("rt-1", "file-1"), {})

    def test_folder_returns_empty(self):
        self.response = {"type": "folder", "size": 0, "content_hash": self.HASH}
        self.assertEqual(self.provider.get_file_hash("rt-1", "folder-1"), {})

    def test_empty_file_returns_empty(self):
        self.response = {"type": "file", "size": 0, "content_hash": self.HASH}
        self.assertEqual(self.provider.get_file_hash("rt-1", "file-1"), {})
