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
