"""115 上传 / 秒传能力单元测试（P0-A）。

覆盖：
    - pan115_cipher：AES-128-CBC（FIPS-197 向量）、LZ4 解压、载荷编解码往返
    - upload_115_file：秒传命中、小文件单传、大文件分片、405 退避、
      二次验证、取消、错误人话映射、userinfo 缓存、SHA1 自动计算
"""
import hashlib
import io
import json
import unittest
from unittest.mock import patch

from app.providers import pan115
from app.providers.base import UploadError, UploadResult
from app.providers.pan115_cipher import (
    AES_IV,
    AES_KEY,
    aes128_cbc_decrypt,
    aes128_cbc_encrypt,
    decode_upload_response,
    lz4_block_decompress,
    lz4_decompress,
    make_upload_payload,
)


class MemSink(io.BytesIO):
    """带 size/sha1 属性的内存数据源（模拟复制引擎的 UploadSink）。"""

    def __init__(self, data: bytes, sha1: str = ""):
        super().__init__(data)
        self._data = data
        if sha1:
            self.sha1 = sha1


class TrackingSink(MemSink):
    """记录 seek 位置的内存数据源（断言续传跳过了已完成分片）。"""

    def __init__(self, data: bytes, sha1: str = ""):
        super().__init__(data, sha1=sha1)
        self.seeks: list = []

    def seek(self, offset, whence=0):
        self.seeks.append(offset)
        return super().seek(offset, whence)


def _make_encrypted_response(payload: dict) -> bytes:
    """按服务端格式伪造加密响应：分块 LZ4（纯字面量）+ AES-CBC。"""
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    lit_len = len(raw)
    ext = bytearray()
    n = lit_len - 15
    while n >= 255:
        ext.append(255)
        n -= 255
    ext.append(n)
    block = bytes([0xF0]) + bytes(ext) + raw
    frame = len(block).to_bytes(2, "little") + block
    return aes128_cbc_encrypt(frame, AES_KEY, AES_IV)


def _ok_upload_info(*args, **kwargs):
    return (12345, "test-userkey")


FAKE_TOKEN = {
    "AccessKeyId": "fake-ak",
    "AccessKeySecret": "fake-sk",
    "SecurityToken": "fake-st",
}


class Pan115CipherTest(unittest.TestCase):
    def test_aes128_fips197_vector(self):
        key = bytes.fromhex("000102030405060708090a0b0c0d0e0f")
        pt = bytes.fromhex("00112233445566778899aabbccddeeff")
        iv0 = b"\x00" * 16
        ct = aes128_cbc_encrypt(pt, key, iv0)
        self.assertEqual(ct.hex(), "69c4e0d86a7b0430d8cdb78070b4c55a")
        self.assertEqual(aes128_cbc_decrypt(ct, key, iv0), pt)

    def test_cbc_multiblock_roundtrip_with_padding(self):
        key = bytes(range(16))
        iv = bytes(range(16, 32))
        for size in (1, 15, 16, 17, 100):
            data = bytes((i * 7) % 256 for i in range(size))
            enc = aes128_cbc_encrypt(data, key, iv)
            self.assertEqual(len(enc) % 16, 0)
            self.assertEqual(aes128_cbc_decrypt(enc, key, iv), data)

    def test_lz4_block_pure_literal_and_match(self):
        self.assertEqual(lz4_block_decompress(b"\xa0" + b"A" * 10), b"A" * 10)
        self.assertEqual(lz4_block_decompress(b"\x40" + b"abcd" + b"\x04\x00"), b"abcdabcd")

    def test_lz4_frame_multiblock(self):
        # literal "xy" + match(offset=2, len=4) -> "xyxyxy"
        block = b"\x20" + b"xy" + b"\x02\x00"
        frame = len(block).to_bytes(2, "little") + block
        self.assertEqual(lz4_decompress(frame), b"xyxyxy")

    def test_initupload_payload_roundtrip(self):
        """make_upload_payload 产出的请求可被 decode_upload_response 反向解读。"""
        payload = {
            "userid": 12345,
            "userkey": "test-userkey",
            "fileid": "A" * 40,
            "filename": "test.mkv",
            "filesize": 2048,
            "target": "U_1_0",
            "path": "",
        }
        packed = make_upload_payload(payload)
        self.assertIn("k_ec", packed["params"])
        self.assertEqual(len(packed["data"]) % 16, 0)
        # 伪造服务端加密响应并解密
        fake = _make_encrypted_response({"state": True, "status": 2, "pickcode": "pc123"})
        decoded = decode_upload_response(fake)
        self.assertEqual(decoded["status"], 2)
        self.assertEqual(decoded["pickcode"], "pc123")


class Upload115FileTest(unittest.TestCase):
    def setUp(self):
        self._patches = [
            patch.object(pan115, "get_115_user_upload_info", side_effect=_ok_upload_info),
            patch.object(pan115, "mark_cookie_health_success"),
            patch.object(pan115, "mark_cookie_health_failure"),
        ]
        for p in self._patches:
            p.start()
        # 清空 userinfo 缓存，避免用例间污染
        pan115._115_upload_user_info_cache.clear()

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def _init_resp(self, status, **extra):
        data = {"status": status}
        data.update(extra)
        return {"state": True, "data": data}

    def test_rapid_upload_hit(self):
        data = b"x" * 1024
        sha1 = hashlib.sha1(data).hexdigest().upper()
        with patch.object(
            pan115,
            "_upload_115_initupload",
            return_value={"state": True, "data": {"status": 2}, "pickcode": "pc-rapid"},
        ) as m_init:
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "test.mkv", "42", size=len(data), sha1=sha1
            )
        self.assertIsInstance(result, UploadResult)
        self.assertEqual(result.status, "rapid")
        self.assertTrue(result.is_rapid)
        self.assertEqual(result.pick_code, "pc-rapid")
        self.assertEqual(result.size, len(data))
        # 秒传命中不得触发 OSS 链路
        sent_payload = m_init.call_args[0][3]
        self.assertEqual(sent_payload["target"], "U_1_42")
        self.assertEqual(sent_payload["fileid"], sha1)

    def test_small_file_single_put(self):
        data = b"y" * (512 * 1024)
        sha1 = hashlib.sha1(data).hexdigest().upper()
        init_resp = self._init_resp(
            1,
            bucket="fhnfile",
            object="obj-key-1",
            pick_code="pc-small",
            callback={"callback": '{"url":"cb"}', "callback_var": '{"k":"v"}'},
        )
        progress = []
        with patch.object(pan115, "_upload_115_initupload", return_value=init_resp), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_upload_whole", return_value=None) as m_put:
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "small.bin", "7",
                size=len(data), sha1=sha1, part_size_mb=10,
                progress_cb=lambda s, t: progress.append((s, t)),
            )
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.pick_code, "pc-small")
        m_put.assert_called_once()
        self.assertEqual(progress[-1], (len(data), len(data)))

    def test_large_file_multipart(self):
        data = b"z" * (10 * 1024 * 1024 + 1024)  # > 10MB，确保走分片路径（2 片）
        sha1 = hashlib.sha1(data).hexdigest().upper()
        init_resp = self._init_resp(
            1,
            bucket="fhnfile",
            object="obj-key-2",
            callback={"callback": "{}", "callback_var": "{}"},
        )
        with patch.object(pan115, "_upload_115_initupload", return_value=init_resp), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_multipart_init", return_value="up-1") as m_init, \
             patch.object(pan115, "_oss_upload_part", side_effect=["e1", "e2", "e3"]) as m_part, \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done:
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "big.mkv", "0",
                size=len(data), sha1=sha1, part_size_mb=10,
            )
        self.assertEqual(result.status, "uploaded")
        m_init.assert_called_once()
        self.assertEqual(m_part.call_count, 2)
        # 分片编号与 ETag 按序传递
        numbers = [c[0][5] for c in m_part.call_args_list]
        self.assertEqual(numbers, [1, 2])
        parts = m_done.call_args[0][5]
        self.assertEqual([p["PartNumber"] for p in parts], [1, 2])
        self.assertEqual([p["ETag"] for p in parts], ["e1", "e2"])

    def test_flaky_part_is_retried_without_restarting_file(self):
        """分片抖动只在分片粒度重试，不退回整文件（否则要从源盘重下一遍）。"""
        data = b"f" * (10 * 1024 * 1024 + 1024)  # 2 片
        sha1 = hashlib.sha1(data).hexdigest().upper()
        init_resp = self._init_resp(
            1,
            bucket="fhnfile",
            object="obj-key-retry",
            callback={"callback": "{}", "callback_var": "{}"},
        )
        flaky = [
            UploadError("115 分片 1 上传失败（OSS HTTP 500）", retriable=True, provider="115"),
            UploadError("115 分片 1 上传失败（OSS HTTP 500）", retriable=True, provider="115"),
            "e1",
            "e2",
        ]
        with patch.object(pan115, "_upload_115_initupload", return_value=init_resp), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_multipart_init", return_value="up-3"), \
             patch.object(pan115, "_oss_upload_part", side_effect=flaky) as m_part, \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done, \
             patch("time.sleep") as sleeper:
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "big.mkv", "0",
                size=len(data), sha1=sha1, part_size_mb=10,
            )
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(m_part.call_count, 4)  # 第 1 片试了 3 次 + 第 2 片 1 次
        self.assertTrue(sleeper.called)
        parts = m_done.call_args[0][5]
        self.assertEqual([p["ETag"] for p in parts], ["e1", "e2"])

    def test_rate_limit_backoff_then_success(self):
        data = b"r" * 100
        sha1 = hashlib.sha1(data).hexdigest().upper()
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] < 3:
                raise pan115._UploadRateLimited("限流")
            return {"state": True, "data": {"status": 2}, "pickcode": "pc"}

        with patch.object(pan115, "_upload_115_initupload", side_effect=flaky), \
             patch("app.providers.pan115.time.sleep") as m_sleep:
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "f.bin", "0", size=len(data), sha1=sha1
            )
        self.assertEqual(result.status, "rapid")
        self.assertEqual(calls["n"], 3)
        self.assertEqual(m_sleep.call_count, 2)  # 两次退避

    def test_rate_limit_exhausted(self):
        data = b"r" * 100
        sha1 = hashlib.sha1(data).hexdigest().upper()
        with patch.object(
            pan115, "_upload_115_initupload", side_effect=pan115._UploadRateLimited("限流")
        ), patch("app.providers.pan115.time.sleep"):
            with self.assertRaises(UploadError) as ctx:
                pan115.upload_115_file(
                    "cookie", MemSink(data, sha1=sha1), "f.bin", "0", size=len(data), sha1=sha1
                )
        self.assertIn("限流", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_sign_check_retry(self):
        data = b"s" * 4096
        sha1 = hashlib.sha1(data).hexdigest().upper()
        sign_val = hashlib.sha1(data[100:200]).hexdigest().upper()
        responses = [
            self._init_resp(7, sign_key="sk", sign_check="100-199"),
            {"state": True, "data": {"status": 2}, "pickcode": "pc-sign"},
        ]

        def side_effect(cookie, userid, userkey, payload):
            resp = responses.pop(0)
            if "sign_val" in payload:
                self.assertEqual(payload["sign_val"], sign_val)
                self.assertEqual(payload["sign_key"], "sk")
            return resp

        with patch.object(pan115, "_upload_115_initupload", side_effect=side_effect):
            result = pan115.upload_115_file(
                "cookie", MemSink(data, sha1=sha1), "sign.mkv", "0", size=len(data), sha1=sha1
            )
        self.assertEqual(result.status, "rapid")

    def test_sign_check_repeated_fails(self):
        data = b"s" * 4096
        sha1 = hashlib.sha1(data).hexdigest().upper()
        with patch.object(
            pan115,
            "_upload_115_initupload",
            return_value=self._init_resp(7, sign_key="sk", sign_check="0-99"),
        ):
            with self.assertRaises(UploadError) as ctx:
                pan115.upload_115_file(
                    "cookie", MemSink(data, sha1=sha1), "sign.mkv", "0", size=len(data), sha1=sha1
                )
        self.assertIn("二次验证", str(ctx.exception))

    def test_cancel_between_parts(self):
        data = b"c" * (10 * 1024 * 1024 + 1024)  # > 10MB，确保走分片路径
        sha1 = hashlib.sha1(data).hexdigest().upper()
        init_resp = self._init_resp(
            1, bucket="b", object="k", callback={"callback": "{}", "callback_var": "{}"}
        )
        checks = {"n": 0}

        def cancel_check():
            checks["n"] += 1
            return checks["n"] >= 2  # 第二片边界取消

        with patch.object(pan115, "_upload_115_initupload", return_value=init_resp), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_multipart_init", return_value="up-2"), \
             patch.object(pan115, "_oss_upload_part", return_value="e1"), \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done:
            with self.assertRaises(UploadError) as ctx:
                pan115.upload_115_file(
                    "cookie", MemSink(data, sha1=sha1), "c.mkv", "0",
                    size=len(data), sha1=sha1, part_size_mb=10, cancel_check=cancel_check,
                )
        self.assertIn("取消", str(ctx.exception))
        m_done.assert_not_called()

    def test_error_mapping_login_expired(self):
        data = b"e" * 10
        sha1 = hashlib.sha1(data).hexdigest().upper()
        with patch.object(
            pan115,
            "_upload_115_initupload",
            return_value={"state": False, "errcode": 20000, "error_msg": "未登录"},
        ):
            with self.assertRaises(UploadError) as ctx:
                pan115.upload_115_file(
                    "cookie", MemSink(data, sha1=sha1), "e.bin", "0", size=len(data), sha1=sha1
                )
        self.assertIn("Cookie", str(ctx.exception))

    def test_sha1_computed_when_missing(self):
        """上层未提供 sha1 时，自动读一遍计算。"""
        data = b"h" * 2048
        expected = hashlib.sha1(data).hexdigest().upper()
        captured = {}

        def capture(cookie, userid, userkey, payload):
            captured.update(payload)
            return {"state": True, "data": {"status": 2}, "pickcode": "pc"}

        with patch.object(pan115, "_upload_115_initupload", side_effect=capture):
            result = pan115.upload_115_file(
                "cookie", MemSink(data), "h.bin", "0", size=len(data)
            )
        self.assertEqual(captured["fileid"], expected)
        self.assertEqual(result.sha1, expected)

    def test_user_upload_info_cache(self):
        # 本用例测真实的 get_115_user_upload_info，先停掉 setUp 的同名 patch
        self._patches[0].stop()
        pan115._115_upload_user_info_cache.clear()
        payload = {"state": True, "data": {"user_id": 999, "userkey": "k-abc"}}
        with patch("app.providers.pan115.http_request_json", return_value=payload) as m_http:
            first = pan115.get_115_user_upload_info("cookie-x")
            second = pan115.get_115_user_upload_info("cookie-x")
        self.assertEqual(first, (999, "k-abc"))
        self.assertEqual(second, (999, "k-abc"))
        self.assertEqual(m_http.call_count, 1)  # 第二次走缓存

    def test_user_upload_info_login_expired(self):
        # 同上：停掉 setUp 的同名 patch，测真实实现
        self._patches[0].stop()
        pan115._115_upload_user_info_cache.clear()
        with patch("app.providers.pan115.http_request_json", return_value={"state": False, "data": {}}), \
             patch.object(pan115, "mark_cookie_health_failure"):
            with self.assertRaises(UploadError) as ctx:
                pan115.get_115_user_upload_info("cookie-y")
        self.assertIn("Cookie", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()


class TestPan115ProviderUploadDelegation(unittest.TestCase):
    """复制引擎按统一接口传 md5，115 只用 sha1；参数不能因此炸掉。"""

    def test_provider_accepts_md5_and_forwards(self):
        provider = pan115.Pan115Provider()
        captured = {}

        def fake_upload(cookie, sink, filename, target_dir_id="0", **kwargs):
            captured.update(kwargs)
            return UploadResult(status="rapid", pick_code="PC-1", name=filename, size=4)

        with patch.object(pan115, "upload_115_file", side_effect=fake_upload):
            result = provider.upload_file(
                "cookie",
                io.BytesIO(b"blat"),
                "probe.bin",
                "dir-1",
                sha1="ABCDEF",
                md5="0123456789abcdef0123456789abcdef",
                size=4,
            )

        self.assertEqual(result.status, "rapid")
        self.assertEqual(captured["sha1"], "ABCDEF")
        self.assertEqual(captured["md5"], "0123456789abcdef0123456789abcdef")
        self.assertEqual(captured["size"], 4)


class TestPan115UploadResume(unittest.TestCase):
    """115 OSS 分片续传（设计文档 §3.4）：首run 记会话、续run 对账跳分片。"""

    PART_SIZE = 10 * 1024 * 1024
    # 10MB + 1024B → part_size_mb=10 时恰好 2 片
    BIG_DATA = b"z" * (10 * 1024 * 1024 + 1024)

    def setUp(self):
        self._patches = [
            patch.object(pan115, "get_115_user_upload_info", side_effect=_ok_upload_info),
            patch.object(pan115, "mark_cookie_health_success"),
            patch.object(pan115, "mark_cookie_health_failure"),
        ]
        for p in self._patches:
            p.start()
        pan115._115_upload_user_info_cache.clear()

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def _init_resp(self, **extra):
        data = {"status": 1, "bucket": "fhnfile", "object": "obj-key", "callback": {"callback": "{}", "callback_var": "{}"}}
        data.update(extra)
        return {"state": True, "data": data}

    def _sha1(self):
        return hashlib.sha1(self.BIG_DATA).hexdigest().upper()

    def _session(self, upload_id="up-r1", parts=None):
        return {
            "provider": "115",
            "fields": {
                "bucket": "fhnfile",
                "object": "obj-key",
                "callback": {"callback": "{}", "callback_var": "{}"},
                "pickcode": "pc-r",
            },
            "upload_id": upload_id,
            "parts": parts if parts is not None else [],
        }

    def test_first_run_records_session_and_reports_parts(self):
        """首run：分片初始化后把会话写回 upload_resume；每片完成回调上报。"""
        done_calls = []
        upload_resume = {"upload": {}, "on_part_done": lambda n, e: done_calls.append((n, e))}
        with patch.object(pan115, "_upload_115_initupload", return_value=self._init_resp()), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_multipart_init", return_value="up-new") as m_init, \
             patch.object(pan115, "_oss_upload_part", side_effect=["e1", "e2"]), \
             patch.object(pan115, "_oss_multipart_complete", return_value=None):
            result = pan115.upload_115_file(
                "cookie", TrackingSink(self.BIG_DATA, sha1=self._sha1()), "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        self.assertEqual(result.status, "uploaded")
        m_init.assert_called_once()
        session = upload_resume["upload"]
        self.assertEqual(session["provider"], "115")
        self.assertEqual(session["upload_id"], "up-new")
        self.assertEqual(session["fields"]["object"], "obj-key")
        # 有引擎回调时分片进度由引擎记录，provider 只上报
        self.assertEqual(session["parts"], [])
        self.assertEqual(done_calls, [(1, "e1"), (2, "e2")])

    def test_first_run_without_callback_accumulates_parts(self):
        """没有引擎回调时，provider 自己把分片进度写回会话（终态可续传）。"""
        upload_resume = {"upload": {}}
        with patch.object(pan115, "_upload_115_initupload", return_value=self._init_resp()), \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_multipart_init", return_value="up-new"), \
             patch.object(pan115, "_oss_upload_part", side_effect=["e1", "e2"]), \
             patch.object(pan115, "_oss_multipart_complete", return_value=None):
            pan115.upload_115_file(
                "cookie", TrackingSink(self.BIG_DATA, sha1=self._sha1()), "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        self.assertEqual(
            upload_resume["upload"]["parts"],
            [{"PartNumber": 1, "ETag": "e1"}, {"PartNumber": 2, "ETag": "e2"}],
        )

    def test_resume_skips_completed_parts(self):
        """续run：跳过 initupload/init，OSS 实测分片对账，只传剩余分片。"""
        listed = [{"PartNumber": 1, "ETag": "e1", "Size": self.PART_SIZE}]
        recorded = [{"PartNumber": 1, "ETag": "e-old"}]
        upload_resume = {"upload": self._session(parts=recorded)}
        part_calls = []
        sink = TrackingSink(self.BIG_DATA, sha1=self._sha1())
        with patch.object(pan115, "_upload_115_initupload") as m_init, \
             patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN) as m_token, \
             patch.object(pan115, "_oss_multipart_init") as m_mp_init, \
             patch.object(pan115, "_oss_list_parts", return_value=listed) as m_list, \
             patch.object(
                 pan115, "_oss_upload_part",
                 side_effect=lambda *a: part_calls.append((a[5], len(a[6]))) or "e2",
             ) as m_part, \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done:
            result = pan115.upload_115_file(
                "cookie", sink, "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        self.assertEqual(result.status, "uploaded")
        # 秒传探测与分片初始化都不该发生；token 必须重取（短时效）
        m_init.assert_not_called()
        m_mp_init.assert_not_called()
        m_list.assert_called_once()
        self.assertTrue(m_token.called)
        # 只传了第 2 片，且从正确偏移读（跳过第 1 片的 10MB）
        self.assertEqual(m_part.call_count, 1)
        self.assertEqual(part_calls, [(2, 1024)])
        self.assertIn(self.PART_SIZE, sink.seeks)
        # complete 的 parts 完整有序，第 1 片 ETag 以 OSS 实测为准
        parts = m_done.call_args[0][5]
        self.assertEqual(parts, [
            {"PartNumber": 1, "ETag": "e1"},
            {"PartNumber": 2, "ETag": "e2"},
        ])
        # 对账后的分片列表写回会话
        self.assertEqual(upload_resume["upload"]["parts"], parts)

    def test_resume_falls_back_to_recorded_parts_when_list_fails(self):
        """_oss_list_parts 拉不到（返回空）时退回本地记录，已记的分片仍跳过。"""
        listed = []
        recorded = [{"PartNumber": 1, "ETag": "e-old"}]
        upload_resume = {"upload": self._session(parts=recorded)}
        part_calls = []
        sink = TrackingSink(self.BIG_DATA, sha1=self._sha1())
        with patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_list_parts", return_value=listed), \
             patch.object(
                 pan115, "_oss_upload_part",
                 side_effect=lambda *a: part_calls.append(a[5]) or "e2",
             ) as m_part, \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done:
            pan115.upload_115_file(
                "cookie", sink, "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        self.assertEqual(part_calls, [2])
        parts = m_done.call_args[0][5]
        self.assertEqual(parts, [
            {"PartNumber": 1, "ETag": "e-old"},
            {"PartNumber": 2, "ETag": "e2"},
        ])

    def test_resume_all_parts_done_completes_immediately(self):
        """全部片已在 OSS：不再上传任何分片，直接合并。"""
        listed = [
            {"PartNumber": 1, "ETag": "e1", "Size": self.PART_SIZE},
            {"PartNumber": 2, "ETag": "e2", "Size": 1024},
        ]
        upload_resume = {"upload": self._session(parts=[])}
        with patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_list_parts", return_value=listed), \
             patch.object(pan115, "_oss_upload_part") as m_part, \
             patch.object(pan115, "_oss_multipart_complete", return_value=None) as m_done:
            result = pan115.upload_115_file(
                "cookie", TrackingSink(self.BIG_DATA, sha1=self._sha1()), "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        self.assertEqual(result.status, "uploaded")
        m_part.assert_not_called()
        parts = m_done.call_args[0][5]
        self.assertEqual([p["ETag"] for p in parts], ["e1", "e2"])

    def test_resume_reports_new_parts_via_callback(self):
        """续run 新完成的分片同样走 on_part_done 上报。"""
        listed = [{"PartNumber": 1, "ETag": "e1", "Size": self.PART_SIZE}]
        done_calls = []
        upload_resume = {
            "upload": self._session(parts=[{"PartNumber": 1, "ETag": "e1"}]),
            "on_part_done": lambda n, e: done_calls.append((n, e)),
        }
        with patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN), \
             patch.object(pan115, "_oss_list_parts", return_value=listed), \
             patch.object(pan115, "_oss_upload_part", return_value="e2"), \
             patch.object(pan115, "_oss_multipart_complete", return_value=None):
            pan115.upload_115_file(
                "cookie", TrackingSink(self.BIG_DATA, sha1=self._sha1()), "big.mkv", "0",
                size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                upload_resume=upload_resume,
            )
        # 只上报新传的第 2 片；第 1 片是上次传的，不重复上报
        self.assertEqual(done_calls, [(2, "e2")])

    def test_resume_missing_upload_id_raises(self):
        """会话字段不全（缺 upload_id）按明确错误抛出，不静默重传。"""
        upload_resume = {"upload": {"fields": {"bucket": "b", "object": "k"}}}
        with patch.object(pan115, "_upload_115_get_token", return_value=FAKE_TOKEN):
            with self.assertRaises(UploadError) as ctx:
                pan115.upload_115_file(
                    "cookie", TrackingSink(self.BIG_DATA, sha1=self._sha1()), "big.mkv", "0",
                    size=len(self.BIG_DATA), sha1=self._sha1(), part_size_mb=10,
                    upload_resume=upload_resume,
                )
        self.assertIn("upload_id", str(ctx.exception))

    def test_provider_forwards_upload_resume(self):
        """Pan115Provider.upload_file 把 upload_resume 透传给 upload_115_file。"""
        provider = pan115.Pan115Provider()
        captured = {}
        handle = {"upload": {"upload_id": "up-x"}}

        def fake_upload(cookie, sink, filename, target_dir_id="0", **kwargs):
            captured.update(kwargs)
            return UploadResult(status="uploaded", name=filename, size=4)

        with patch.object(pan115, "upload_115_file", side_effect=fake_upload):
            provider.upload_file(
                "cookie", io.BytesIO(b"blat"), "probe.bin", "dir-1",
                sha1="ABCDEF", size=4, upload_resume=handle,
            )
        self.assertIs(captured["upload_resume"], handle)
