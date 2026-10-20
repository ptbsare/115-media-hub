"""夸克网盘上传能力单元测试（mock 全部 HTTP，无真实网络请求）。

覆盖：秒传命中/未命中、分片上传、哈希计算、取消、错误映射、参数校验。
"""
import hashlib
import io
import unittest
from unittest.mock import patch

from app.providers import quark
from app.providers.base import UploadError, UploadResult


def _ok(payload):
    body = {"code": 0, "status": 200, "success": True}
    body.update(payload)
    return body


def _pre_resp(task_id="T-1", part_size=4 * 1024 * 1024):
    return _ok(
        {
            "data": {
                "task_id": task_id,
                "bucket": "bkt-prod",
                "obj_key": "upload/obj/test.mkv",
                "upload_id": "UID-1",
                "upload_url": "https://upload.quark.cn",
                "auth_info": "AI-1",
                "callback": {"callbackUrl": "https://cb.quark.cn", "callbackBody": "{}"},
            },
            "metadata": {"part_size": part_size},
        }
    )


def _hash_resp(finished):
    return _ok({"data": {"finish": bool(finished)}})


def _auth_resp(auth_key="OSS-AK-1"):
    return _ok({"data": {"auth_key": auth_key}})


class _FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = int(status_code)
        self.headers = headers if headers is not None else {"ETag": '"etag-1"'}


class _FakeSession:
    """模拟 _get_quark_http_session()：记录 OSS PUT/POST。"""

    def __init__(self):
        self.put_calls = []
        self.post_calls = []
        self.put_status = 200
        self.put_headers = {"ETag": '"etag-1"'}
        self.post_status = 200

    def put(self, url, params=None, data=None, headers=None, timeout=None):
        self.put_calls.append(
            {"url": url, "params": params, "data": data, "headers": headers}
        )
        return _FakeResponse(self.put_status, self.put_headers)

    def post(self, url, params=None, data=None, headers=None, timeout=None):
        self.post_calls.append(
            {"url": url, "params": params, "data": data, "headers": headers}
        )
        return _FakeResponse(self.post_status)


class _MemSink(io.BytesIO):
    """带 size 属性的内存数据源（模拟复制引擎的 UploadSink）。"""

    def __init__(self, data: bytes, sha1: str = ""):
        super().__init__(data)
        self.size = len(data)
        if sha1:
            self.sha1 = sha1


class QuarkUploadTestBase(unittest.TestCase):
    def setUp(self):
        self.fake_session = _FakeSession()
        self.api_calls = []

        def fake_request_payload(url, payload, headers, timeout=45, method="POST", fallback=""):
            self.api_calls.append({"url": url, "payload": payload, "method": method})
            if "/file/upload/pre" in url:
                return self.pre_resp()
            if "/file/update/hash" in url:
                return self.hash_resp()
            if "/file/upload/auth" in url:
                return _auth_resp()
            if "/file/upload/finish" in url:
                return _ok({"data": {}})
            raise AssertionError(f"未预期的接口调用: {url}")

        self.fake_request_payload = fake_request_payload
        self._patchers = [
            patch("app.providers.quark._request_quark_json_payload", side_effect=fake_request_payload),
            patch("app.providers.quark._get_quark_http_session", return_value=self.fake_session),
            patch.object(quark, "mark_cookie_health_success"),
            patch.object(quark, "mark_cookie_health_failure"),
        ]
        for patcher in self._patchers:
            patcher.start()
        self.addCleanup(self._stop_patchers)

    def _stop_patchers(self):
        for patcher in self._patchers:
            patcher.stop()

    def pre_resp(self):
        return _pre_resp()

    def hash_resp(self):
        return _hash_resp(False)


class TestQuarkUploadHappyPath(QuarkUploadTestBase):
    def test_rapid_upload_hit(self):
        """秒传命中：零上传流量，不触发 OSS。"""
        self.hash_resp = lambda: _hash_resp(True)
        data = b"x" * 2048
        sink = io.BytesIO(data)
        result = quark.upload_quark_file(
            "cookie-1", sink, "test.mkv", "dir-1",
            md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertIsInstance(result, UploadResult)
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.sha1, hashlib.sha1(data).hexdigest().upper())
        self.assertFalse(self.fake_session.put_calls)
        self.assertFalse(self.fake_session.post_calls)
        # pre 与 hash 各一次
        urls = [c["url"] for c in self.api_calls]
        self.assertTrue(any("/file/upload/pre" in u for u in urls))
        self.assertTrue(any("/file/update/hash" in u for u in urls))
        # pre 载荷包含目录与大小
        pre_call = next(c for c in self.api_calls if "/file/upload/pre" in c["url"])
        self.assertEqual(pre_call["payload"]["pdir_fid"], "dir-1")
        self.assertEqual(pre_call["payload"]["size"], 2048)

    def test_upload_after_rapid_miss(self):
        """秒传未命中：分片 PUT → commit POST → finish → uploaded。"""
        data = b"y" * (4 * 1024 * 1024 + 123)
        sink = io.BytesIO(data)
        result = quark.upload_quark_file(
            "cookie-1", sink, "test.mkv", "0",
            md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertEqual(result.status, "uploaded")
        # 4MB/片 + 尾部 123 字节 = 2 片
        self.assertEqual(len(self.fake_session.put_calls), 2)
        first_put = self.fake_session.put_calls[0]
        self.assertEqual(first_put["params"], {"partNumber": "1", "uploadId": "UID-1"})
        self.assertIn("Authorization", first_put["headers"])
        self.assertIn("x-oss-date", first_put["headers"])
        self.assertEqual(first_put["headers"]["Referer"], "https://pan.quark.cn/")
        self.assertEqual(len(first_put["data"]), 4 * 1024 * 1024)
        # auth 每片一次 + commit 一次 = 3 次
        auth_calls = [c for c in self.api_calls if "/file/upload/auth" in c["url"]]
        self.assertEqual(len(auth_calls), 3)
        # commit POST 带 XML 体与 Content-MD5
        commit_call = self.fake_session.post_calls[0]
        body = commit_call["data"].decode("utf-8") if isinstance(commit_call["data"], bytes) else commit_call["data"]
        self.assertIn("<CompleteMultipartUpload>", body)
        self.assertIn("<PartNumber>1</PartNumber>", body)
        self.assertIn("<PartNumber>2</PartNumber>", body)
        self.assertIn("Content-MD5", commit_call["headers"])
        self.assertIn("x-oss-callback", commit_call["headers"])
        # finish 收尾被调用
        self.assertTrue(any("/file/upload/finish" in c["url"] for c in self.api_calls))

    def test_hashes_computed_when_missing(self):
        """未传哈希时函数自行读取计算（一遍同时算 md5+sha1）。"""
        self.hash_resp = lambda: _hash_resp(True)
        data = b"z" * 4096
        sink = _MemSink(data)
        result = quark.upload_quark_file("cookie-1", sink, "t.bin", "0")
        self.assertEqual(result.status, "rapid")
        hash_call = next(c for c in self.api_calls if "/file/update/hash" in c["url"])
        self.assertEqual(hash_call["payload"]["md5"], hashlib.md5(data).hexdigest())
        self.assertEqual(hash_call["payload"]["sha1"], hashlib.sha1(data).hexdigest().lower())
        self.assertEqual(result.sha1, hashlib.sha1(data).hexdigest().upper())

    def test_sink_sha1_reused(self):
        """sink 自带 sha1 时直接复用，不重复读取。"""
        self.hash_resp = lambda: _hash_resp(True)
        data = b"w" * 1024
        sink_sha1 = hashlib.sha1(data).hexdigest().upper()
        sink = _MemSink(data, sha1=sink_sha1)
        result = quark.upload_quark_file("cookie-1", sink, "t.bin", "0")
        hash_call = next(c for c in self.api_calls if "/file/update/hash" in c["url"])
        self.assertEqual(hash_call["payload"]["sha1"], sink_sha1.lower())
        self.assertEqual(result.sha1, sink_sha1)

    def test_hash_payload_always_lowercase(self):
        """秒传检测的两个哈希必须是小写。

        真机 2026-10-20：夸克 ``/file/update/hash`` 遇到大写 sha1 且云端已有
        同一文件时返回 400 ``invalid file [sha1 非法]``，改成小写立即 200 并
        ``finish=true``。这里锁住大小写，避免以后再"顺手 upper 一下"。
        """
        self.hash_resp = lambda: _hash_resp(True)
        data = b"L" * 2048
        upper_sha1 = hashlib.sha1(data).hexdigest().upper()
        upper_md5 = hashlib.md5(data).hexdigest().upper()
        quark.upload_quark_file(
            "cookie-1", _MemSink(data), "t.bin", "0",
            md5=upper_md5, sha1=upper_sha1, size=len(data),
        )
        hash_call = next(c for c in self.api_calls if "/file/update/hash" in c["url"])
        self.assertEqual(hash_call["payload"]["sha1"], upper_sha1.lower())
        self.assertEqual(hash_call["payload"]["md5"], upper_md5.lower())

    def test_progress_callback(self):
        """进度回调按分片递增上报。"""
        data = b"p" * (4 * 1024 * 1024 + 1024)
        progress = []
        quark.upload_quark_file(
            "cookie-1", io.BytesIO(data), "t.mkv", "0",
            md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(),
            size=len(data), progress_cb=lambda sent, total: progress.append((sent, total)),
        )
        self.assertEqual(progress, [(4 * 1024 * 1024, len(data)), (len(data), len(data))])


class TestQuarkUploadCancel(QuarkUploadTestBase):
    def test_cancel_before_parts(self):
        data = b"c" * (4 * 1024 * 1024 * 2)
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file(
                "cookie-1", io.BytesIO(data), "t.mkv", "0",
                md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(),
                size=len(data), cancel_check=lambda: True,
            )
        self.assertIn("取消", str(ctx.exception))
        self.assertFalse(self.fake_session.put_calls)

    def test_cancel_between_parts(self):
        data = b"c" * (4 * 1024 * 1024 * 3)
        checks = {"n": 0}

        def cancel_check():
            checks["n"] += 1
            return checks["n"] >= 2  # 第二片边界取消

        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file(
                "cookie-1", io.BytesIO(data), "t.mkv", "0",
                md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(),
                size=len(data), cancel_check=cancel_check,
            )
        self.assertIn("取消", str(ctx.exception))
        self.assertEqual(len(self.fake_session.put_calls), 1)  # 只传了第一片


class TestQuarkUploadPartRetry(QuarkUploadTestBase):
    """分片抖动只在分片粒度重试，不能退回整文件（否则要从源盘重下一遍）。"""

    def test_flaky_part_is_retried_without_restarting_file(self):
        data = b"q" * (4 * 1024 * 1024 + 10)  # 2 片
        real_part = quark._quark_upload_part
        failures = {"n": 0}

        def flaky(cookie, pre, mime_type, part_number, chunk):
            if part_number == 1 and failures["n"] < 2:
                failures["n"] += 1
                raise UploadError(
                    "夸克分片 1 上传失败（网络错误）", retriable=True, provider="quark"
                )
            return real_part(cookie, pre, mime_type, part_number, chunk)

        with patch.object(quark, "_quark_upload_part", side_effect=flaky), patch("time.sleep") as sleeper:
            result = quark.upload_quark_file(
                "cookie-1", io.BytesIO(data), "t.mkv", "0",
                md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(),
                size=len(data),
            )

        self.assertEqual(result.status, "uploaded")
        self.assertEqual(failures["n"], 2)  # 重试了 2 次
        self.assertTrue(sleeper.called)  # 走了退避
        # 只有 2 片真正发出去，说明没有整文件重来
        self.assertEqual(len(self.fake_session.put_calls), 2)

    def test_part_gives_up_after_attempts_with_readable_message(self):
        data = b"q" * (4 * 1024 * 1024 + 10)

        def always_fail(cookie, pre, mime_type, part_number, chunk):
            raise UploadError("夸克分片 1 上传失败（网络错误）", retriable=True, provider="quark")

        with patch.object(quark, "_quark_upload_part", side_effect=always_fail), patch("time.sleep"):
            with self.assertRaises(UploadError) as ctx:
                quark.upload_quark_file(
                    "cookie-1", io.BytesIO(data), "t.mkv", "0",
                    md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(),
                    size=len(data),
                )
        message = str(ctx.exception)
        self.assertIn("分片 1", message)
        self.assertIn("重试", message)


class TestQuarkUploadErrors(QuarkUploadTestBase):
    def test_missing_cookie(self):
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file("", io.BytesIO(b"x"), "t.mkv", "0", size=1)
        self.assertIn("Cookie", str(ctx.exception))

    def test_missing_filename(self):
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file("cookie-1", io.BytesIO(b"x"), "", "0", size=1)
        self.assertIn("文件名", str(ctx.exception))

    def test_empty_file(self):
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file("cookie-1", io.BytesIO(b""), "t.mkv", "0", size=0)
        self.assertIn("空文件", str(ctx.exception))

    def test_unknown_size(self):
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file("cookie-1", io.BytesIO(b"x"), "t.mkv", "0")

    def test_pre_login_expired(self):
        def pre_fail(url, payload, headers, timeout=45, method="POST", fallback=""):
            if "/file/upload/pre" in url:
                return {"code": 32011, "message": "invalid cookie", "status": 500}
            raise AssertionError("pre 失败后不应继续")

        with patch("app.providers.quark._request_quark_json_payload", side_effect=pre_fail):
            with self.assertRaises(UploadError) as ctx:
                quark.upload_quark_file("cookie-1", io.BytesIO(b"x" * 1024), "t.mkv", "0", size=1024)
        self.assertIn("登录", str(ctx.exception))

    def test_pre_missing_task_id(self):
        def pre_no_task(url, payload, headers, timeout=45, method="POST", fallback=""):
            if "/file/upload/pre" in url:
                return _ok({"data": {"bucket": "b"}, "metadata": {}})
            raise AssertionError("pre 无 task_id 后不应继续")

        with patch("app.providers.quark._request_quark_json_payload", side_effect=pre_no_task):
            with self.assertRaises(UploadError) as ctx:
                quark.upload_quark_file("cookie-1", io.BytesIO(b"x" * 1024), "t.mkv", "0", size=1024)
        self.assertIn("任务 ID", str(ctx.exception))

    def test_hash_api_failure(self):
        def hash_fail(url, payload, headers, timeout=45, method="POST", fallback=""):
            if "/file/upload/pre" in url:
                return _pre_resp()
            if "/file/update/hash" in url:
                return {"code": 40001, "message": "需要安全验证", "status": 500}
            raise AssertionError("hash 失败后不应继续")

        with patch("app.providers.quark._request_quark_json_payload", side_effect=hash_fail):
            with self.assertRaises(UploadError) as ctx:
                quark.upload_quark_file("cookie-1", io.BytesIO(b"x" * 1024), "t.mkv", "0", size=1024)
        self.assertIn("验证", str(ctx.exception))

    def test_oss_put_failure(self):
        self.fake_session.put_status = 403
        data = b"e" * (4 * 1024 * 1024 + 1)
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file(
                "cookie-1", io.BytesIO(data), "t.mkv", "0",
                md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
            )
        self.assertIn("上传失败", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_oss_commit_failure(self):
        self.fake_session.post_status = 500
        data = b"e" * (4 * 1024 * 1024 + 1)
        with self.assertRaises(UploadError) as ctx:
            quark.upload_quark_file(
                "cookie-1", io.BytesIO(data), "t.mkv", "0",
                md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
            )
        self.assertIn("提交失败", str(ctx.exception))

    def test_small_file_single_put(self):
        """小文件（< 分片大小）只需一次 PUT。"""
        data = b"s" * 1024
        result = quark.upload_quark_file(
            "cookie-1", io.BytesIO(data), "small.txt", "0",
            md5=hashlib.md5(data).hexdigest(), sha1=hashlib.sha1(data).hexdigest().upper(), size=len(data),
        )
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(len(self.fake_session.put_calls), 1)
        self.assertEqual(self.fake_session.put_calls[0]["params"]["partNumber"], "1")


class TestQuarkDownloadUrl(unittest.TestCase):
    """夸克下载直链（复制引擎的源取数能力）。"""

    def test_resolve_download_url_ok(self):
        resp = _ok({"data": [{"fid": "f-1", "download_url": "https://cdn.example.com/a", "size": 123, "file_name": "a.mkv"}]})
        with patch("app.providers.quark._request_quark_json_payload", return_value=resp), \
             patch.object(quark, "mark_cookie_health_success"):
            info = quark.resolve_quark_download_url("cookie-1", "f-1")
        self.assertEqual(info["url"], "https://cdn.example.com/a")
        self.assertEqual(info["size"], 123)
        self.assertEqual(info["file_name"], "a.mkv")

    def test_resolve_download_url_empty(self):
        resp = _ok({"data": [{"fid": "f-1", "download_url": ""}]})
        with patch("app.providers.quark._request_quark_json_payload", return_value=resp), \
             patch.object(quark, "mark_cookie_health_success"):
            with self.assertRaises(RuntimeError) as ctx:
                quark.resolve_quark_download_url("cookie-1", "f-1")
        self.assertIn("返回为空", str(ctx.exception))

    def test_resolve_download_url_api_error(self):
        with patch("app.providers.quark._request_quark_json_payload", return_value={"code": 32011, "message": "invalid cookie", "status": 500}):
            with self.assertRaises(RuntimeError) as ctx:
                quark.resolve_quark_download_url("cookie-1", "f-1")
        self.assertIn("夸克登录已失效", str(ctx.exception))

    def test_provider_delegates(self):
        p = quark.QuarkProvider()
        with patch.object(quark, "resolve_quark_download_url", return_value={"url": "https://x/1"}):
            self.assertEqual(p.resolve_download_url("cookie-1", "f-1"), "https://x/1")
            self.assertEqual(p.resolve_download_info("cookie-1", "f-1")["url"], "https://x/1")


if __name__ == "__main__":
    unittest.main()


class TestQuarkCreateFolderRetry(unittest.TestCase):
    """父目录刚创建后立刻建子目录，夸克会瞬时假报「file is doloading[同名冲突]」。

    真机复现：跨盘复制准备目录时必然连建两级，不重试会直接中断任务。
    """

    def setUp(self):
        for name in ("mark_cookie_health_success", "mark_cookie_health_failure"):
            patcher = patch.object(quark, name)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_retries_then_succeeds(self):
        calls = []

        def fake_request(url, payload, headers, timeout=45, method="POST", fallback=""):
            calls.append(payload["file_name"])
            if len(calls) == 1:
                raise RuntimeError("HTTP 400: file is doloading[同名冲突]")
            return {"code": 0, "status": 200, "data": {"fid": "NEW-ID"}}

        with patch.object(quark, "_request_quark_json_payload", side_effect=fake_request), \
             patch.object(quark, "list_quark_entries_payload", return_value={"entries": []}), \
             patch.object(quark, "time") as fake_time:
            result = quark.create_quark_folder("cookie", "0", "跨盘复制自测")

        self.assertEqual(result["id"], "NEW-ID")
        self.assertTrue(result["created"])
        self.assertEqual(len(calls), 2)
        self.assertTrue(fake_time.sleep.called)

    def test_conflict_reuses_existing_folder(self):
        def fake_request(*args, **kwargs):
            raise RuntimeError("HTTP 400: file is doloading[同名冲突]")

        existing = {"entries": [{"is_dir": True, "name": "跨盘复制自测", "id": "EXIST-1"}]}
        with patch.object(quark, "_request_quark_json_payload", side_effect=fake_request), \
             patch.object(quark, "list_quark_entries_payload", return_value=existing), \
             patch.object(quark, "time"):
            result = quark.create_quark_folder("cookie", "0", "跨盘复制自测")

        self.assertEqual(result["id"], "EXIST-1")
        self.assertFalse(result["created"])

    def test_other_errors_not_retried(self):
        calls = []

        def fake_request(*args, **kwargs):
            calls.append(1)
            raise RuntimeError("HTTP 403: 登录已失效")

        with patch.object(quark, "_request_quark_json_payload", side_effect=fake_request):
            with self.assertRaises(RuntimeError):
                quark.create_quark_folder("cookie", "0", "任一目录")

        self.assertEqual(len(calls), 1)


class TestQuarkCookieRotation(unittest.TestCase):
    """夸克每次 API 响应都会轮换 __puus；下载直链的 CDN 回调按它校验登录态。

    真机复现：不合并轮换值，下载 403 RequestDeniedByCallback / auth expired。
    """

    COOKIE = "__pus=old-pus; __puus=old-puus; tfstk=t1"

    class _Resp:
        def __init__(self, cookies):
            self.cookies = cookies

    def test_rotation_merges_new_puus(self):
        quark._QUARK_COOKIE_ROTATED.clear()
        with patch.object(quark, "_persist_rotated_quark_cookie") as persist:
            merged = quark._rotate_quark_cookie(
                self.COOKIE, self._Resp({"__puus": "new-puus"})
            )
        self.assertIn("__puus=new-puus", merged)
        self.assertIn("__pus=old-pus", merged)
        self.assertIn("tfstk=t1", merged)
        self.assertTrue(persist.called)
        # 旧值映射到最新值，后续请求自动用新 cookie
        self.assertEqual(quark._current_quark_cookie(self.COOKIE), merged)

    def test_rotation_keeps_pus_when_present(self):
        quark._QUARK_COOKIE_ROTATED.clear()
        with patch.object(quark, "_persist_rotated_quark_cookie"):
            merged = quark._rotate_quark_cookie(
                self.COOKIE, self._Resp({"__puus": "n1", "__pus": "n2"})
            )
        self.assertIn("__pus=n2", merged)
        self.assertIn("__puus=n1", merged)

    def test_no_set_cookie_keeps_original(self):
        quark._QUARK_COOKIE_ROTATED.clear()
        merged = quark._rotate_quark_cookie(self.COOKIE, self._Resp({}))
        self.assertEqual(merged, self.COOKIE)

    def test_download_info_carries_rotated_cookie(self):
        quark._QUARK_COOKIE_ROTATED.clear()
        with patch.object(quark, "_persist_rotated_quark_cookie"), \
             patch.object(quark, "_request_quark_json_payload") as req, \
             patch.object(quark, "mark_cookie_health_success"):
            def fake_request(*args, **kwargs):
                # 模拟服务端轮换
                quark._QUARK_COOKIE_ROTATED[self.COOKIE] = "__puus=fresh; __pus=old-pus"
                return {"status": 200, "data": [{"download_url": "https://dl/x", "size": 10}]}

            req.side_effect = fake_request
            info = quark.resolve_quark_download_url(self.COOKIE, "fid-1")

        self.assertEqual(info["url"], "https://dl/x")
        self.assertEqual(info["cookie"], "__puus=fresh; __pus=old-pus")


class TestQuarkDownloadSizeLimit(unittest.TestCase):
    """真机复现：2.6GB 视频取直链时夸克返回 400 / code 23018「download file size limit」。

    根因是网关按 UA 判身份：浏览器 UA 受非会员单文件限额，客户端 UA 不受限。
    """

    COOKIE = "b-user-id=u1; __pus=old-pus; __puus=old-puus"

    def setUp(self):
        quark._QUARK_COOKIE_ROTATED.clear()

    def test_download_request_uses_client_user_agent(self):
        captured = {}

        def fake_request(url, payload, headers, **kwargs):
            captured["headers"] = dict(headers)
            return {"status": 200, "data": [{"download_url": "https://dl/x", "size": 10}]}

        with patch.object(quark, "_request_quark_json_payload", side_effect=fake_request), \
             patch.object(quark, "mark_cookie_health_success"):
            quark.resolve_quark_download_url(self.COOKIE, "fid-1")

        ua = captured["headers"]["User-Agent"]
        self.assertIn("quark-cloud-drive", ua)
        self.assertIn("Channel/pckk_other_ch", ua)
        self.assertNotIn("Sec-Ch-Ua", captured["headers"])
        # 其余接口仍用浏览器 UA
        self.assertIn("Edg/", quark._build_quark_headers(self.COOKIE)["User-Agent"])

    def test_size_limit_http_error_becomes_actionable_hint(self):
        import json

        import requests

        resp = requests.Response()
        resp.status_code = 400
        resp._content = json.dumps(
            {
                "status": 400,
                "code": 23018,
                "message": "download file size limit[5e5d0bb3b0294f49a31ccb2ef261bb9c]",
            }
        ).encode("utf-8")
        exc = requests.HTTPError(response=resp)

        with self.assertRaises(RuntimeError) as ctx:
            quark._raise_quark_http_error(exc)
        message = str(ctx.exception)
        self.assertIn("会员", message)
        self.assertNotIn("过于频繁", message)
        self.assertNotIn("HTTP 400", message)

    def test_map_api_error_keeps_rate_limit_wording(self):
        self.assertIn(
            "会员",
            quark._map_quark_api_error({"code": 23018, "message": "download file size limit[x]"}),
        )
        self.assertIn(
            "频繁",
            quark._map_quark_api_error({"code": 0, "message": "request rate limit exceeded"}),
        )
