"""跨盘复制「零下载流量秒传」单测（mock 全部 HTTP 与 provider，无真实网络）。

背景：跨盘复制原本必须先完整下载一遍才能算 SHA1/MD5（秒传探测只要 sha1+size）。
115（文件对象带 ``sha1``）与阿里云盘（``content_hash`` 就是 SHA1）可以直接读元信息，
所以源端是这两家时应当「元信息 → 目标端秒传探测」，命中即零下载流量。

覆盖：已知哈希不发下载请求、秒传命中文案、秒传未命中惰性下载 + 哈希校验、
哈希不一致、拿不到哈希时降级、惰性 sink 读写/定位/关闭边界、临时文件清理、
源哈希缓存命中与按账号隔离、UploadError 映射不被破坏。
"""
import hashlib
import os
import tempfile
import unittest
from unittest.mock import patch

import requests

from app.providers.base import UploadError, UploadResult
from app.services import cross_cloud_copy as xcc


DATA = bytes(range(256)) * 16  # 4096 字节
SHA1 = hashlib.sha1(DATA).hexdigest().upper()
MD5 = hashlib.md5(DATA).hexdigest()
OTHER_DATA = b"q" * len(DATA)


class _FakeResponse:
    def __init__(self, status_code=200, body=b""):
        self.status_code = int(status_code)
        self._body = body
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, chunk_size=1):
        for start in range(0, len(self._body), chunk_size):
            yield self._body[start:start + chunk_size]

    def close(self):
        self.closed = True


class _Source:
    """源端 provider（鸭子类型）：115/阿里都能 get_file_hash，夸克只给直链。"""

    label = "源网盘"

    def __init__(self, name="115", file_hash=None, download_info=None):
        self.name = name
        self.file_hash = file_hash
        self.download_info = (
            download_info
            if download_info is not None
            else {
                "url": "https://dl.example.com/f",
                "size": len(DATA),
                "file_name": "movie.mkv",
            }
        )
        self.hash_calls = 0
        self.download_info_calls = 0

    def resolve_download_info(self, cookie, file_id):
        self.download_info_calls += 1
        if isinstance(self.download_info, Exception):
            raise self.download_info
        return dict(self.download_info)

    def resolve_download_url(self, cookie, file_id):
        return str(self.resolve_download_info(cookie, file_id).get("url", "") or "")

    def get_file_hash(self, cookie, file_id):
        self.hash_calls += 1
        if isinstance(self.file_hash, Exception):
            raise self.file_hash
        return dict(self.file_hash or {})


class _Target:
    """目标端 provider：秒传命中时**完全不读 sink**（与 115 秒传实现一致）。"""

    supports_upload = True
    label = "目标网盘"

    def __init__(self, name="aliyun", rapid=True):
        self.name = name
        self.rapid = rapid
        self.calls = []

    def upload_file(
        self,
        cookie,
        sink,
        filename,
        target_dir_id="0",
        *,
        sha1="",
        md5="",
        size=-1,
        progress_cb=None,
        cancel_check=None,
        **kwargs,
    ):
        record = {
            "filename": filename,
            "sha1": sha1,
            "md5": md5,
            "size": size,
            "target_dir_id": target_dir_id,
            "sink_size": getattr(sink, "size", -1),
            "data": None,
        }
        if not self.rapid:
            # 秒传未命中：真实 provider 此时才逐块读 sink
            record["data"] = sink.read()
            sink.seek(0)
        record["downloaded"] = getattr(sink, "downloaded", None)
        self.calls.append(record)
        if self.rapid:
            return UploadResult(
                status="rapid", name=filename, size=size, sha1=sha1, message="秒传命中"
            )
        return UploadResult(
            status="uploaded", name=filename, size=size, sha1=sha1, message="上传完成"
        )


class _FailingTarget(_Target):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def upload_file(self, *args, **kwargs):
        raise self.error


class ZeroTrafficTestBase(unittest.TestCase):
    def setUp(self):
        xcc.clear_source_hash_cache()
        self.get_calls = []
        self.get_error = None
        self.body = DATA

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            self.get_calls.append({"url": url, "headers": dict(headers or {})})
            if self.get_error is not None:
                raise self.get_error
            return _FakeResponse(200, self.body)

        patchers = [
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch("app.services.cross_cloud_copy.time.sleep"),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _copy(self, source=None, target=None, **overrides):
        source = source if source is not None else _Source(
            file_hash={"sha1": SHA1.lower(), "size": len(DATA), "name": "movie.mkv"}
        )
        target = target if target is not None else _Target()
        kwargs = {
            "source_provider": source,
            "source_cookie": "ck-115",
            "source_file_id": "fid-1",
            "target_provider": target,
            "target_cookie": "ck-ali",
            "target_dir_id": "dir-1",
        }
        kwargs.update(overrides)
        return xcc.copy_file_across_clouds(**kwargs)


class TestZeroTrafficEngine(ZeroTrafficTestBase):
    def test_known_hash_makes_no_download_request(self):
        source = _Source(
            name="115", file_hash={"sha1": SHA1.lower(), "size": len(DATA), "name": "movie.mkv"}
        )
        target = _Target(name="aliyun")
        result = self._copy(source=source, target=target)

        self.assertEqual(self.get_calls, [])  # 一个下载请求都没发
        self.assertEqual(source.download_info_calls, 0)  # 连直链都不用取
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.message, "秒传命中，未产生下载流量")
        self.assertEqual(result.size, len(DATA))
        self.assertEqual(result.sha1, SHA1)
        self.assertEqual(result.name, "movie.mkv")
        call = target.calls[0]
        self.assertEqual(call["sha1"], SHA1)
        self.assertEqual(call["size"], len(DATA))
        self.assertEqual(call["filename"], "movie.mkv")
        self.assertEqual(call["sink_size"], len(DATA))  # size 属性不触发下载
        self.assertFalse(call["downloaded"])  # 惰性 sink 从未落地

    def test_explicit_source_hash_skips_provider_probe(self):
        source = _Source(name="aliyun", file_hash=RuntimeError("不该被调用"))
        result = self._copy(
            source=source,
            target=_Target(name="115"),
            source_sha1=SHA1.lower(),
            source_size=len(DATA),
            filename="直接给名字.mkv",
        )
        self.assertEqual(source.hash_calls, 0)
        self.assertEqual(self.get_calls, [])
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.message, "秒传命中，未产生下载流量")

    def test_progress_skips_fake_download_phase(self):
        progress = []
        self._copy(progress_cb=lambda sent, total, phase: progress.append((phase, sent, total)))
        phases = [item[0] for item in progress]
        self.assertNotIn("download", phases)
        self.assertIn("upload", phases)

    def test_rapid_miss_downloads_lazily_and_verifies_hash(self):
        target = _Target(name="aliyun", rapid=False)
        result = self._copy(target=target)

        self.assertTrue(self.get_calls)  # 未命中才真正下载
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.message, "上传完成")
        self.assertEqual(result.sha1, SHA1)
        call = target.calls[0]
        self.assertEqual(call["data"], DATA)
        self.assertTrue(call["downloaded"])

    def test_hash_mismatch_reports_source_changed(self):
        source = _Source(
            name="115",
            file_hash={
                "sha1": hashlib.sha1(OTHER_DATA).hexdigest().upper(),
                "size": len(DATA),
                "name": "movie.mkv",
            },
        )
        target = _Target(name="aliyun", rapid=False)
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(source=source, target=target)
        self.assertIn("源文件已变更，请重试", str(ctx.exception))
        self.assertNotIn("上传到目标网盘失败", str(ctx.exception))
        self.assertFalse(ctx.exception.retriable)
        # 校验失败要丢掉缓存，避免同一份过期元信息反复失败
        calls_before = source.hash_calls
        xcc.resolve_source_hash(source, "ck-115", "fid-1")
        self.assertEqual(source.hash_calls, calls_before + 1)

    def test_missing_source_hash_falls_back_to_download(self):
        source = _Source(name="quark", file_hash={})
        target = _Target(name="115", rapid=True)
        result = self._copy(source=source, target=target)

        self.assertTrue(self.get_calls)
        self.assertEqual(result.status, "rapid")
        # 回退路径沿用原文案，不宣称零流量
        self.assertEqual(result.message, "秒传命中")
        self.assertEqual(result.sha1, SHA1)
        self.assertEqual(target.calls[0]["sha1"], SHA1)
        self.assertEqual(target.calls[0]["md5"], MD5)

    def test_hash_probe_failure_falls_back_to_download(self):
        source = _Source(name="aliyun", file_hash=RuntimeError("元信息接口 500"))
        result = self._copy(source=source, target=_Target(name="115"))
        self.assertTrue(self.get_calls)
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.sha1, SHA1)

    def test_download_failure_bubbles_as_copy_error(self):
        self.get_error = requests.ConnectionError("boom")
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(target=_Target(name="aliyun", rapid=False))
        self.assertIn("源文件下载失败", str(ctx.exception))
        self.assertNotIn("上传到目标网盘失败", str(ctx.exception))

    def test_upload_error_mapping_still_applies(self):
        error = UploadError("115 上传接口持续限流，请稍后再试", retriable=True, provider="115")
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(target=_FailingTarget(error))
        self.assertIn("限流", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_unknown_size_is_filled_from_hash(self):
        result = self._copy(size=-1)
        self.assertEqual(result.size, len(DATA))

    def test_target_rapid_without_hash_keeps_original_path(self):
        """没有源哈希时仍先下载再秒传：确认原路径行为未变。"""
        source = _Source(name="quark", file_hash={})
        target = _Target(name="aliyun", rapid=True)
        self._copy(source=source, target=target)
        self.assertEqual(target.calls[0]["downloaded"], None)  # 普通 BytesIO 没有该属性


class TestLazySink(unittest.TestCase):
    def setUp(self):
        xcc.clear_source_hash_cache()
        self.get_calls = []
        self.body = DATA

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            self.get_calls.append(url)
            return _FakeResponse(200, self.body)

        for patcher in (
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch("app.services.cross_cloud_copy.time.sleep"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _sink(self, size=None, expected_sha1=SHA1, temp_dir=""):
        return xcc._LazyDownloadSink(
            source_provider=_Source(name="115"),
            source_cookie="ck-115",
            source_file_id="fid-1",
            size=len(DATA) if size is None else size,
            expected_sha1=expected_sha1,
            temp_dir=temp_dir,
        )

    def test_size_seek_zero_tell_do_not_download(self):
        sink = self._sink()
        self.assertEqual(sink.size, len(DATA))
        self.assertEqual(sink.seek(0), 0)
        self.assertEqual(sink.seek(0, 0), 0)
        self.assertEqual(sink.tell(), 0)
        self.assertFalse(sink.downloaded)
        self.assertEqual(self.get_calls, [])
        sink.close()

    def test_seek_to_nonzero_position_triggers_download(self):
        sink = self._sink()
        try:
            self.assertEqual(sink.seek(0, 2), len(DATA))
            self.assertTrue(sink.downloaded)
            self.assertTrue(self.get_calls)
            self.assertEqual(sink.read(4), b"")  # 已在末尾
        finally:
            sink.close()

    def test_read_is_progressive_and_seek_back_does_not_redownload(self):
        sink = self._sink()
        try:
            self.assertEqual(sink.read(10), DATA[:10])
            self.assertEqual(sink.tell(), 10)
            self.assertEqual(sink.read(5), DATA[10:15])
            calls_after_first = len(self.get_calls)
            self.assertEqual(sink.seek(0), 0)
            self.assertEqual(sink.read(len(DATA)), DATA)
            self.assertEqual(len(self.get_calls), calls_after_first)
        finally:
            sink.close()

    def test_negative_seek_from_end_triggers_download(self):
        sink = self._sink()
        try:
            self.assertEqual(sink.seek(-8, 2), len(DATA) - 8)
            self.assertEqual(sink.read(8), DATA[-8:])
        finally:
            sink.close()

    def test_close_without_download_is_safe_and_idempotent(self):
        sink = self._sink()
        sink.close()
        sink.close()
        self.assertFalse(sink.downloaded)

    def test_read_after_close_raises(self):
        sink = self._sink()
        sink.close()
        with self.assertRaises(xcc.CrossCloudCopyError):
            sink.read(1)
        with self.assertRaises(xcc.CrossCloudCopyError):
            sink.seek(1)

    def test_read_after_close_when_already_downloaded_raises(self):
        sink = self._sink()
        sink.read(4)
        sink.close()
        with self.assertRaises(xcc.CrossCloudCopyError):
            sink.read(1)

    def test_download_failure_raises_copy_error(self):
        with patch(
            "app.services.cross_cloud_copy.requests.get",
            side_effect=requests.ConnectionError("boom"),
        ):
            sink = self._sink()
            try:
                with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
                    sink.read(1)
                self.assertIn("源文件下载失败", str(ctx.exception))
            finally:
                sink.close()

    def test_temp_file_channel_is_cleaned_up(self):
        temp_root = tempfile.mkdtemp(prefix="xcopy-zero-")
        try:
            with patch.object(xcc, "MEMORY_CHANNEL_THRESHOLD_BYTES", 1024):
                sink = self._sink(temp_dir=temp_root)
                self.assertEqual(sink.read(4), DATA[:4])
                self.assertTrue(sink.temp_path)
                self.assertTrue(os.path.exists(sink.temp_path))
                sink.close()
                self.assertFalse(os.path.exists(sink.temp_path))
                self.assertEqual(os.listdir(temp_root), [])
        finally:
            import shutil

            shutil.rmtree(temp_root, ignore_errors=True)

    def test_close_without_download_leaves_temp_dir_empty(self):
        temp_root = tempfile.mkdtemp(prefix="xcopy-zero-")
        try:
            with patch.object(xcc, "MEMORY_CHANNEL_THRESHOLD_BYTES", 1024):
                sink = self._sink(temp_dir=temp_root)
                sink.close()
                self.assertEqual(os.listdir(temp_root), [])
        finally:
            import shutil

            shutil.rmtree(temp_root, ignore_errors=True)


class TestSourceHashCache(unittest.TestCase):
    def setUp(self):
        xcc.clear_source_hash_cache()

    def _source(self):
        return _Source(name="115", file_hash={"sha1": SHA1, "size": len(DATA), "name": "m.mkv"})

    def test_cache_hit_skips_repeat_probe(self):
        source = self._source()
        first = xcc.resolve_source_hash(source, "ck-a", "fid-1")
        second = xcc.resolve_source_hash(source, "ck-a", "fid-1")
        self.assertEqual(first, second)
        self.assertEqual(source.hash_calls, 1)
        self.assertEqual(first["sha1"], SHA1)

    def test_cache_is_scoped_by_account(self):
        source = self._source()
        xcc.resolve_source_hash(source, "ck-a", "fid-1")
        xcc.resolve_source_hash(source, "ck-b", "fid-1")
        self.assertEqual(source.hash_calls, 2)

    def test_cache_expires_after_ttl(self):
        source = self._source()
        with patch.object(xcc.time, "monotonic", return_value=1000.0):
            xcc.resolve_source_hash(source, "ck-a", "fid-1")
        with patch.object(
            xcc.time,
            "monotonic",
            return_value=1000.0 + xcc.SOURCE_HASH_CACHE_TTL_SECONDS + 1,
        ):
            xcc.resolve_source_hash(source, "ck-a", "fid-1")
        self.assertEqual(source.hash_calls, 2)

    def test_invalid_hash_is_not_cached(self):
        source = _Source(name="115", file_hash={"sha1": "not-a-sha1", "size": len(DATA)})
        self.assertEqual(xcc.resolve_source_hash(source, "ck-a", "fid-1"), {})
        self.assertEqual(xcc.resolve_source_hash(source, "ck-a", "fid-1"), {})
        self.assertEqual(source.hash_calls, 2)

    def test_provider_without_get_file_hash_returns_empty(self):
        class _Legacy:
            name = "quark"

        self.assertEqual(xcc.resolve_source_hash(_Legacy(), "ck", "fid"), {})


if __name__ == "__main__":
    unittest.main()
