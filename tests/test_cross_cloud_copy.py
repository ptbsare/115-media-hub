"""跨盘复制引擎单元测试（mock 全部 HTTP 与 provider，无真实网络请求）。

覆盖：内存/磁盘双通道、秒传命中/未命中、哈希同步计算、取消、换链重试、
下载不完整、幂等跳过、目标能力校验、孤儿清理、进度回调。
"""
import hashlib
import io
import itertools
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests

from app import db
from app.services import cross_cloud_copy as xcc
from app.services import media_hash_cache
from app.providers.base import UploadError, UploadResult


class _FakeProvider:
    """鸭子类型 provider（引擎只用 name/label/supports_upload/下载/列表/上传）。"""

    def __init__(
        self,
        name,
        label="",
        *,
        supports_upload=False,
        download_info=None,
        upload_result=None,
        entries=None,
    ):
        self.name = name
        self.label = label or name
        self.supports_upload = supports_upload
        self.download_info = download_info
        self.upload_result = upload_result
        self.entries = entries or []
        self.upload_calls = []

    def resolve_download_info(self, cookie, file_id):
        if isinstance(self.download_info, Exception):
            raise self.download_info
        return self.download_info

    def resolve_download_url(self, cookie, file_id):
        info = self.resolve_download_info(cookie, file_id)
        if isinstance(info, dict):
            return str(info.get("url", "") or "")
        return ""

    def list_entries_payload(self, cookie, cid="0", folders_only=False):
        return {"entries": self.entries}

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
        self.upload_calls.append(
            {
                "filename": filename,
                "target_dir_id": target_dir_id,
                "sha1": sha1,
                "md5": md5,
                "size": size,
                "data": sink.read(),
            }
        )
        sink.seek(0)
        # 模拟真实 provider 的上报：秒传命中也报一次完成进度
        if progress_cb is not None:
            try:
                progress_cb(size if size >= 0 else len(self.upload_calls[-1]["data"]), size)
            except Exception:
                pass
        if isinstance(self.upload_result, Exception):
            raise self.upload_result
        return self.upload_result


class _FakeResponse:
    def __init__(self, status_code=200, chunks=None):
        self.status_code = int(status_code)
        self._chunks = chunks or []
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def iter_content(self, chunk_size=1):
        for chunk in self._chunks:
            yield chunk

    def close(self):
        self.closed = True


def _rapid_result(name="t.mkv"):
    return UploadResult(status="rapid", name=name, message="秒传命中")


def _uploaded_result(name="t.mkv"):
    return UploadResult(status="uploaded", name=name, message="上传完成")


def _chunked(data, chunk_size=1024 * 1024):
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]


class CrossCloudCopyTestBase(unittest.TestCase):
    def setUp(self):
        # 复制成功会把哈希写进「备忘录」，这里换成临时库：测试之间互不串味，
        # 也不会写到真实数据目录。
        self._tmp_db_dir = tempfile.mkdtemp(prefix="xcopy-db-")
        self._orig_db_path = db.DB_PATH
        self._orig_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self._tmp_db_dir, "data.db")
        db._DB_ENSURED = False
        db.ensure_db()
        xcc.clear_source_hash_cache()
        self.addCleanup(self._restore_db)

        self.get_calls = []
        self.get_responses = []  # 按调用次序消费；单个则重复返回

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            self.get_calls.append({"url": url, "headers": headers})
            if not self.get_responses:
                raise AssertionError("未配置下载响应")
            if len(self.get_responses) == 1:
                return self.get_responses[0]
            return self.get_responses.pop(0)

        self._patchers = [
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch("app.services.cross_cloud_copy.time.sleep"),
        ]
        for patcher in self._patchers:
            patcher.start()
        self.addCleanup(self._stop_all)

    def _restore_db(self):
        db.DB_PATH = self._orig_db_path
        db._DB_ENSURED = self._orig_db_ensured
        shutil.rmtree(self._tmp_db_dir, ignore_errors=True)

    def _stop_all(self):
        for patcher in self._patchers:
            patcher.stop()

    def _copy(self, **overrides):
        kwargs = {
            "source_provider": self.source,
            "source_cookie": "cookie-q",
            "source_file_id": "fid-1",
            "target_provider": self.target,
            "target_cookie": "cookie-t",
            "target_dir_id": "dir-1",
        }
        kwargs.update(overrides)
        return xcc.copy_file_across_clouds(**kwargs)


class TestHappyPath(CrossCloudCopyTestBase):
    def setUp(self):
        super().setUp()
        self.data = b"m" * 3000
        self.source = _FakeProvider(
            "quark",
            download_info={"url": "https://cdn.example.com/f1", "size": 3000, "file_name": "movie.mkv"},
            supports_upload=True,
        )
        self.target = _FakeProvider("aliyun", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]

    def test_rapid_hit_full_flow(self):
        result = self._copy()
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.source_provider, "quark")
        self.assertEqual(result.target_provider, "aliyun")
        self.assertEqual(result.size, 3000)
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        # 上传收到的哈希与数据正确
        call = self.target.upload_calls[0]
        self.assertEqual(call["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(call["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(call["data"], self.data)
        self.assertEqual(call["target_dir_id"], "dir-1")

    def test_filename_from_source_meta(self):
        """未传 filename 时用源文件元信息的名字。"""
        result = self._copy()
        self.assertEqual(result.name, "movie.mkv")
        self.assertEqual(self.target.upload_calls[0]["filename"], "movie.mkv")

    def test_explicit_filename_overrides(self):
        result = self._copy(filename="自定义名字.mkv")
        self.assertEqual(result.name, "自定义名字.mkv")

    def test_uploaded_status(self):
        self.target.upload_result = _uploaded_result()
        result = self._copy()
        self.assertEqual(result.status, "uploaded")

    def test_progress_phases(self):
        progress = []
        self._copy(progress_cb=lambda sent, total, phase: progress.append((phase, sent, total)))
        phases = [p[0] for p in progress]
        self.assertIn("download", phases)
        self.assertIn("upload", phases)
        # 下载进度单调递增
        download_progress = [p for p in progress if p[0] == "download"]
        self.assertEqual([p[1] for p in download_progress], sorted(p[1] for p in download_progress))

    def test_download_headers_carry_cookie(self):
        self._copy()
        self.assertEqual(self.get_calls[0]["headers"]["Cookie"], "cookie-q")

    def test_download_headers_follow_provider_bound_user_agent(self):
        """115 直链按 UA 签名：provider 返回的下载头必须原样生效，不能被默认 UA 顶掉。"""
        provider_headers = {
            "User-Agent": "Mozilla/5.0 115-media-hub",
            "Referer": "https://115.com/",
            "Cookie": "UID=abc; acw_tc=1",
        }
        source = _FakeProvider(
            "115",
            download_info={"url": "https://x/115", "size": 0, "headers": dict(provider_headers)},
        )
        target = _FakeProvider("quark", supports_upload=True, upload_result=_rapid_result())
        self._copy(source_provider=source, target_provider=target)
        sent = self.get_calls[0]["headers"]
        self.assertEqual(sent["User-Agent"], "Mozilla/5.0 115-media-hub")
        self.assertEqual(sent["Referer"], "https://115.com/")
        self.assertEqual(sent["Cookie"], "UID=abc; acw_tc=1")


class TestChannels(CrossCloudCopyTestBase):
    def setUp(self):
        super().setUp()
        self.source = _FakeProvider("quark", download_info={"url": "https://x/0", "size": 0})
        self.target = _FakeProvider("115", supports_upload=True, upload_result=_rapid_result())

    def test_memory_channel_small_file(self):
        data = b"s" * (2 * 1024 * 1024)
        source = _FakeProvider("quark", download_info={"url": "https://x/1", "size": len(data)})
        target = _FakeProvider("115", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(data, 512 * 1024))]
        result = self._copy(source_provider=source, target_provider=target)
        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.size, len(data))

    def test_temp_file_channel_large_file(self):
        """大文件走磁盘临时文件，且用完即清。"""
        data = b"L" * (xcc.MEMORY_CHANNEL_THRESHOLD_BYTES + 1024)
        source = _FakeProvider("quark", download_info={"url": "https://x/2", "size": len(data)})
        target = _FakeProvider("aliyun", supports_upload=True, upload_result=_rapid_result())
        temp_root = tempfile.mkdtemp(prefix="xcopy-test-")
        try:
            self.get_responses = [_FakeResponse(200, _chunked(data, 4 * 1024 * 1024))]
            result = self._copy(
                source_provider=source, target_provider=target, temp_dir=temp_root
            )
            self.assertEqual(result.status, "rapid")
            self.assertEqual(result.size, len(data))
            # 临时目录无残留
            leftovers = [n for n in os.listdir(temp_root) if n.startswith("xcopy-")]
            self.assertEqual(leftovers, [])
        finally:
            for name in os.listdir(temp_root):
                os.remove(os.path.join(temp_root, name))
            os.rmdir(temp_root)

    def test_temp_dir_created_when_missing(self):
        data = b"L" * (xcc.MEMORY_CHANNEL_THRESHOLD_BYTES + 512)
        source = _FakeProvider("quark", download_info={"url": "https://x/3", "size": len(data)})
        target = _FakeProvider("aliyun", supports_upload=True, upload_result=_rapid_result())
        base = tempfile.mkdtemp(prefix="xcopy-base-")
        temp_root = os.path.join(base, "nested", "tmp")
        try:
            self.get_responses = [_FakeResponse(200, _chunked(data, 4 * 1024 * 1024))]
            result = self._copy(
                source_provider=source, target_provider=target, temp_dir=temp_root
            )
            self.assertEqual(result.status, "rapid")
            self.assertTrue(os.path.isdir(temp_root))
        finally:
            import shutil

            shutil.rmtree(base, ignore_errors=True)

    def test_unknown_size_still_works(self):
        """不知道大小时按 -1 走系统临时通道（下载后按实际大小）。"""
        data = b"u" * 4096
        source = _FakeProvider("quark", download_info={"url": "https://x/4", "size": 0})
        target = _FakeProvider("115", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(data))]
        result = self._copy(source_provider=source, target_provider=target)
        self.assertEqual(result.size, 4096)


class TestCancel(CrossCloudCopyTestBase):
    def setUp(self):
        super().setUp()
        self.data = b"c" * (4 * 1024 * 1024)
        self.source = _FakeProvider("quark", download_info={"url": "https://x/c", "size": len(self.data)})
        self.target = _FakeProvider("115", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(self.data, 1024 * 1024))]

    def test_cancel_during_download(self):
        checks = {"n": 0}

        def cancel_check():
            checks["n"] += 1
            return checks["n"] >= 2

        with self.assertRaises(xcc.CrossCloudCopyCancelled):
            self._copy(cancel_check=cancel_check)
        self.assertFalse(self.target.upload_calls)

    def test_cancel_during_upload(self):
        """上传阶段取消：provider 抛 UploadError("上传已取消")，引擎透传为人话错误。"""

        def uploading(cookie, sink, filename, target_dir_id="0", *, sha1="", md5="", size=-1, progress_cb=None, cancel_check=None, **kwargs):
            self.target.upload_calls.append({"filename": filename})
            if cancel_check and cancel_check():
                raise UploadError("上传已取消")
            return _uploaded_result()

        self.target.upload_file = uploading
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(cancel_check=lambda: True)
        self.assertIn("取消", str(ctx.exception))


class TestRetryAndErrors(CrossCloudCopyTestBase):
    def setUp(self):
        super().setUp()
        self.data = b"r" * 2048
        self.source = _FakeProvider("quark", download_info={"url": "https://x/r", "size": 2048})
        self.target = _FakeProvider("115", supports_upload=True, upload_result=_rapid_result())

    def test_retry_refreshes_link(self):
        """第一次下载 403（直链过期）→ 换链重试成功。"""
        self.get_responses = [
            _FakeResponse(403, [b"forbidden"]),
            _FakeResponse(200, _chunked(self.data)),
        ]
        result = self._copy()
        self.assertEqual(result.status, "rapid")
        self.assertEqual(len(self.get_calls), 2)

    def test_retry_exhausted(self):
        self.get_responses = [_FakeResponse(500, [b"err"])]
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy()
        self.assertIn("下载失败", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)
        self.assertEqual(len(self.get_calls), xcc.DOWNLOAD_MAX_RETRIES)

    def test_incomplete_download(self):
        data = b"i" * 100
        self.source.download_info = {"url": "https://x/i", "size": 200}
        self.get_responses = [_FakeResponse(200, _chunked(data))]
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy()
        self.assertIn("下载不完整", str(ctx.exception))

    def test_empty_download(self):
        self.source.download_info = {"url": "https://x/e", "size": 0}
        self.get_responses = [_FakeResponse(200, [b""])]
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy()
        self.assertIn("为空", str(ctx.exception))

    def test_source_resolve_failure(self):
        self.source.download_info = RuntimeError("夸克下载地址获取失败")
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy()
        self.assertIn("下载失败", str(ctx.exception))

    def test_target_upload_error_retriable(self):
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]
        self.target.upload_result = UploadError("115 上传接口持续限流，请稍后再试", retriable=True, provider="115")
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy()
        self.assertIn("限流", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_target_no_upload_capability(self):
        plain = _FakeProvider("quark")
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(target_provider=plain)
        self.assertIn("不支持上传", str(ctx.exception))

    def test_same_provider_rejected(self):
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]
        same = _FakeProvider("quark", supports_upload=True, upload_result=_rapid_result())
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(target_provider=same)
        self.assertIn("同一个网盘", str(ctx.exception))

    def test_missing_source_file_id(self):
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._copy(source_file_id="")
        self.assertIn("源文件 ID", str(ctx.exception))

    def test_empty_download_url(self):
        self.source.download_info = {"url": "", "size": 2048}
        with self.assertRaises(xcc.CrossCloudCopyError):
            self._copy()


class TestIdempotency(CrossCloudCopyTestBase):
    def setUp(self):
        super().setUp()
        self.data = b"k" * 1500
        self.source = _FakeProvider("quark", download_info={"url": "https://x/k", "size": 1500})
        self.target = _FakeProvider(
            "115",
            supports_upload=True,
            upload_result=_rapid_result(),
            entries=[
                {"id": "1", "name": "exists.mkv", "is_dir": False, "size": 1500},
                {"id": "2", "name": "other.mkv", "is_dir": False, "size": 999},
            ],
        )
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]

    def test_existing_same_name_size_skipped(self):
        result = self._copy(filename="exists.mkv", size=1500, check_existing=True)
        self.assertEqual(result.status, "skipped")
        self.assertFalse(self.target.upload_calls)
        self.assertFalse(self.get_calls)  # 未发起下载

    def test_existing_check_off_by_default(self):
        result = self._copy(filename="exists.mkv", size=1500)
        self.assertEqual(result.status, "rapid")

    def test_different_size_not_skipped(self):
        result = self._copy(filename="other.mkv", size=1500, check_existing=True)
        self.assertEqual(result.status, "rapid")

    def test_list_failure_does_not_block(self):
        def failing_list(cookie, cid="0", folders_only=False):
            raise RuntimeError("列目录失败")

        self.target.list_entries_payload = failing_list
        result = self._copy(filename="exists.mkv", size=1500, check_existing=True)
        self.assertEqual(result.status, "rapid")


class _HashSource(_FakeProvider):
    """带可选哈希能力的源网盘（用来观察是否真的问过 provider）。"""

    def __init__(self, *args, file_hash=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_hash = {} if file_hash is None else file_hash
        self.hash_calls = 0

    def get_file_hash(self, cookie, file_id):
        self.hash_calls += 1
        if isinstance(self.file_hash, Exception):
            raise self.file_hash
        return self.file_hash


class _RapidTarget(_FakeProvider):
    """秒传命中即返回，绝不读取 sink（读一下就会触发惰性下载）。"""

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
        self.upload_calls.append({"filename": filename, "sha1": sha1, "md5": md5, "size": size})
        if progress_cb is not None:
            try:
                progress_cb(size, size)
            except Exception:
                pass
        if isinstance(self.upload_result, Exception):
            raise self.upload_result
        return self.upload_result


class TestMediaHashCacheIntegration(CrossCloudCopyTestBase):
    """哈希备忘录接进复制引擎后的行为（命中 / 沉淀 / 脏缓存自愈 / 故障降级）。"""

    def setUp(self):
        super().setUp()
        self.data = b"h" * 5000
        self.sha1 = hashlib.sha1(self.data).hexdigest().upper()
        # 每个用例自己传 source/target，这里只占位（_copy 的默认值会读这两个属性）
        self.source = None
        self.target = None

    def _source(self, file_id):
        return _HashSource(
            "quark",
            download_info={
                "url": f"https://cdn.example.com/{file_id}",
                "size": len(self.data),
                "file_name": f"{file_id}.mkv",
            },
        )

    def test_cache_hit_skips_provider_hash_and_download(self):
        # 小写写入也应命中（内部统一大写）
        media_hash_cache.remember_file_hash("quark", "fid-cache", len(self.data), self.sha1.lower())
        source = self._source("fid-cache")
        target = _RapidTarget("115", supports_upload=True, upload_result=_rapid_result())

        result = self._copy(
            source_provider=source,
            target_provider=target,
            source_file_id="fid-cache",
            size=len(self.data),
            filename="cached.mkv",
        )

        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.sha1, self.sha1)
        self.assertEqual(result.message, "秒传命中，未产生下载流量")
        self.assertEqual(source.hash_calls, 0)  # 没问 provider
        self.assertEqual(self.get_calls, [])  # 一个下载请求都没发
        self.assertEqual(target.upload_calls[0]["sha1"], self.sha1)
        self.assertEqual(target.upload_calls[0]["size"], len(self.data))

    def test_successful_copy_remembers_hash(self):
        source = self._source("fid-write")
        target = _RapidTarget("115", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]

        result = self._copy(
            source_provider=source,
            target_provider=target,
            source_file_id="fid-write",
            size=len(self.data),
            filename="w.mkv",
        )

        self.assertEqual(result.sha1, self.sha1)
        self.assertEqual(
            media_hash_cache.lookup_file_hash("quark", "fid-write", len(self.data)),
            self.sha1,
        )

    def test_dirty_cache_heals_and_recomputes(self):
        """缓存里的 sha1 是错的：先清缓存，再下载重算，最终成功。"""
        wrong_sha1 = hashlib.sha1(b"stale-content").hexdigest().upper()
        media_hash_cache.remember_file_hash("quark", "fid-dirty", len(self.data), wrong_sha1)
        source = self._source("fid-dirty")
        target = _FakeProvider("115", supports_upload=True, upload_result=_uploaded_result())
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]

        forget_spy = MagicMock(wraps=media_hash_cache.forget_file_hash)
        with patch.object(media_hash_cache, "forget_file_hash", forget_spy):
            result = self._copy(
                source_provider=source,
                target_provider=target,
                source_file_id="fid-dirty",
                size=len(self.data),
                filename="d.mkv",
            )

        # 自愈分支必须真的执行过：清过一次脏缓存
        forget_spy.assert_called_once_with("quark", "fid-dirty")
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.sha1, self.sha1)
        # 第一次走零流量路径（上传时才下载）校验失败 → 必须重新下载算哈希
        self.assertTrue(self.get_calls)
        self.assertEqual(target.upload_calls[-1]["sha1"], self.sha1)
        self.assertEqual(target.upload_calls[-1]["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(source.hash_calls, 0)  # 夸克没哈希能力，不该去问
        # 脏缓存被替换成重算后的正确值
        self.assertEqual(
            media_hash_cache.lookup_file_hash("quark", "fid-dirty", len(self.data)),
            self.sha1,
        )

    def test_clean_cache_hit_needs_no_download(self):
        """对照组：缓存正确时一点下载都不该发生（防止自愈逻辑误伤正常路径）。"""
        media_hash_cache.remember_file_hash("quark", "fid-clean", len(self.data), self.sha1)
        source = self._source("fid-clean")
        target = _RapidTarget("115", supports_upload=True, upload_result=_rapid_result())

        self._copy(
            source_provider=source,
            target_provider=target,
            source_file_id="fid-clean",
            size=len(self.data),
            filename="c.mkv",
        )

        self.assertEqual(self.get_calls, [])

    def test_cache_failures_do_not_break_copy(self):
        source = self._source("fid-boom")
        target = _RapidTarget("115", supports_upload=True, upload_result=_rapid_result())
        self.get_responses = [_FakeResponse(200, _chunked(self.data))]

        with patch.object(media_hash_cache, "lookup_file_hash", side_effect=RuntimeError("db down")), patch.object(
            media_hash_cache, "remember_file_hash", side_effect=RuntimeError("db down")
        ):
            result = self._copy(
                source_provider=source,
                target_provider=target,
                source_file_id="fid-boom",
                size=len(self.data),
                filename="b.mkv",
            )

        self.assertEqual(result.status, "rapid")
        self.assertEqual(result.sha1, self.sha1)


class TestOrphanCleanup(unittest.TestCase):
    def test_cleanup_old_files(self):
        temp_root = tempfile.mkdtemp(prefix="xcopy-orphan-")
        try:
            old = os.path.join(temp_root, "xcopy-old.part")
            new = os.path.join(temp_root, "xcopy-new.part")
            other = os.path.join(temp_root, "keep.txt")
            for path, mtime in ((old, 0), (new, None), (other, 0)):
                with open(path, "w") as fh:
                    fh.write("x")
                if mtime == 0:
                    os.utime(path, (0, 0))
            removed = xcc.cleanup_orphan_temp_files(temp_root, max_age_hours=1)
            self.assertEqual(removed, 1)
            self.assertFalse(os.path.exists(old))
            self.assertTrue(os.path.exists(new))
            self.assertTrue(os.path.exists(other))
        finally:
            import shutil

            shutil.rmtree(temp_root, ignore_errors=True)

    def test_cleanup_missing_dir(self):
        self.assertEqual(xcc.cleanup_orphan_temp_files("/nonexistent-dir-xyz"), 0)


if __name__ == "__main__":
    unittest.main()


class TestIdempotencyUsesFreshListing(unittest.TestCase):
    """115 目录列表有本地缓存：刚上传完立刻复跑，命中缓存会看不到新文件。

    幂等预检必须带 force_refresh=True，否则同一次批量里会重复上传同一文件。
    """

    class _Provider:
        name = "target"
        label = "target"
        supports_upload = True

        def __init__(self):
            self.calls = []

        def list_entries_payload(self, cookie, cid="0", folders_only=False, force_refresh=False):
            self.calls.append(force_refresh)
            return {"entries": [{"name": "a.mkv", "size": 10, "is_dir": False}]}

        def upload_file(self, *args, **kwargs):  # pragma: no cover - 命中跳过不会上传
            raise AssertionError("已存在同名文件时不应上传")

    def test_force_refresh_passed_and_skips(self):
        target = self._Provider()
        result = xcc.copy_file_across_clouds(
            source_provider=_FakeProvider("src", download_info={"url": "https://x/y"}),
            source_cookie="c",
            source_file_id="fid",
            target_provider=target,
            target_cookie="c",
            target_dir_id="dir",
            filename="a.mkv",
            size=10,
            check_existing=True,
        )
        self.assertEqual(result.status, "skipped")
        self.assertEqual(target.calls, [True])


class TestResumePipeline(CrossCloudCopyTestBase):
    """F2 引擎侧 resume 管道（0.15.1）：跳过下载 / 前缀续下 / upload_resume / 临时文件保留。"""

    SEGMENT = 64 * 1024

    def setUp(self):
        super().setUp()
        self.data = bytes(range(256)) * 1024 * 8  # 2MB，32 段
        self.range_calls = []
        # 基座的 get mock 按 get_responses 次序消费；这里换成线程安全的 Range 服务
        patcher = patch(
            "app.services.cross_cloud_copy.requests.get", side_effect=self._fake_get
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        for extra in (
            patch.object(xcc, "DOWNLOAD_SEGMENT_BYTES", self.SEGMENT),
            patch.object(xcc, "MEMORY_CHANNEL_THRESHOLD_BYTES", 1024),
        ):
            extra.start()
            self.addCleanup(extra.stop)
        self.temp_dir = tempfile.mkdtemp(prefix="xcopy-resume-")
        self.addCleanup(shutil.rmtree, self.temp_dir, ignore_errors=True)
        self.source = _FakeProvider(
            "quark",
            download_info={
                "url": "https://x/r",
                "size": len(self.data),
                "file_name": "movie.mkv",
            },
        )
        self.target = _FakeProvider(
            "115", supports_upload=True, upload_result=_uploaded_result()
        )

    def _fake_get(self, url, stream=False, headers=None, timeout=None, **kwargs):
        range_header = str((headers or {}).get("Range", "") or "")
        if not range_header:
            return _FakeResponse(200, _chunked(self.data))
        start, _, end = range_header.replace("bytes=", "").partition("-")
        start_i, end_i = int(start), int(end)
        self.range_calls.append((start_i, end_i))
        return _FakeResponse(206, _chunked(self.data[start_i:end_i + 1]))

    def _copy_resume(self, resume, **overrides):
        kwargs = {
            "source_provider": self.source,
            "target_provider": self.target,
            "filename": "movie.mkv",
            "size": len(self.data),
            "temp_dir": self.temp_dir,
            "resume": resume,
        }
        kwargs.update(overrides)
        return self._copy(**kwargs)

    def _write_temp_file(self, name, payload):
        path = os.path.join(self.temp_dir, name)
        with open(path, "wb") as fh:
            fh.write(payload)
        return path

    def _resume_state(self, temp_path, *, sha1="", md5="", downloaded=0, upload=None):
        return {
            "temp_path": temp_path,
            "size": len(self.data),
            "sha1": sha1,
            "md5": md5,
            "downloaded_bytes": downloaded,
            "upload": upload if upload is not None else {},
        }

    def _leftover_parts(self):
        return [n for n in os.listdir(self.temp_dir) if n.startswith("xcopy-")]

    # ---- 分支一：哈希已算 + 文件完整 → 完全跳过下载 ----

    def test_resume_complete_skips_download(self):
        temp_path = self._write_temp_file("xcopy-done.part", self.data)
        resume = self._resume_state(
            temp_path,
            sha1=hashlib.sha1(self.data).hexdigest().upper(),
            md5=hashlib.md5(self.data).hexdigest(),
            downloaded=len(self.data),
        )
        with patch.object(
            xcc, "_download_to_sink", side_effect=AssertionError("不应发生下载")
        ) as spy:
            result = self._copy_resume(resume)
        spy.assert_not_called()
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)
        self.assertEqual(self.range_calls, [])  # 一个 Range 请求都没发
        self.assertFalse(os.path.exists(temp_path))  # 成功即删

    def test_resume_complete_with_stale_sha1_redownloads(self):
        """sha1 非空但文件不完整（状态损坏）：按无哈希续传处理，重下后算哈希。"""
        temp_path = self._write_temp_file("xcopy-stale.part", self.data[: self.SEGMENT])
        resume = self._resume_state(
            temp_path,
            sha1=hashlib.sha1(b"stale-content").hexdigest().upper(),
            downloaded=self.SEGMENT,
        )
        result = self._copy_resume(resume)
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)
        # 断点之后的段重下了（前缀不重下）
        self.assertFalse([call for call in self.range_calls if call[0] < self.SEGMENT])
        self.assertFalse(os.path.exists(temp_path))  # 成功即删

    # ---- 分支二：无 sha1 → 从连续前缀续下剩余段 ----

    def test_resume_without_sha1_continues_from_offset(self):
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-part.part", self.data[:prefix])
        updates = []
        resume = self._resume_state(temp_path, downloaded=prefix)
        real_download = xcc._download_to_sink
        seen = {}

        def spy(**kwargs):
            seen.update(kwargs)
            return real_download(**kwargs)

        with patch.object(xcc, "_download_to_sink", side_effect=spy):
            result = self._copy_resume(resume, on_resume_update=updates.append)
        self.assertEqual(seen["start_offset"], prefix)
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)
        # 前缀区间没有重下（mock 计数断言），剩余段一个不少
        self.assertFalse([call for call in self.range_calls if call[0] < prefix])
        self.assertEqual(len(self.range_calls), (len(self.data) - prefix) // self.SEGMENT)
        # 时机②：下载完成即把哈希写进 resume 并触发回调
        self.assertEqual(resume["sha1"], result.sha1)
        self.assertEqual(resume["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(resume["downloaded_bytes"], len(self.data))
        self.assertTrue(updates)
        self.assertFalse(os.path.exists(temp_path))  # 成功即删

    def test_resume_download_reports_prefix_progress(self):
        """续传时 on_segment_done 上报的是「连续前缀」，downloaded_bytes 只增不减。"""
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-prog.part", self.data[:prefix])
        snapshots = []

        def capture(state):
            snapshots.append(dict(state))

        resume = self._resume_state(temp_path, downloaded=prefix)
        # 节流 ≥1s：用每次 +2s 的假时钟让每段完成都触发一次回调
        ticks = itertools.count(step=2.0)
        with patch(
            "app.services.cross_cloud_copy.time.monotonic", side_effect=lambda: next(ticks)
        ):
            self._copy_resume(resume, on_resume_update=capture)
        # 24 个剩余段 + 哈希完成的一次
        self.assertEqual(len(snapshots), (len(self.data) - prefix) // self.SEGMENT + 1)
        self.assertEqual(snapshots[0]["downloaded_bytes"], prefix + self.SEGMENT)
        self.assertEqual(snapshots[-1]["downloaded_bytes"], len(self.data))
        self.assertEqual(snapshots[-1]["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(
            [item["downloaded_bytes"] for item in snapshots],
            sorted(item["downloaded_bytes"] for item in snapshots),
        )

    def test_resume_non_aligned_offset_rewinds_to_segment_boundary(self):
        """历史坏断点（段中间位置）自动回退到段边界，不再判死。

        真实故障：downloaded_bytes=3828076306（16MB 的 228.17 段）+ sha1 为空，
        引擎直接抛「续传起始偏移未按分段边界对齐」。这种状态必须向下回退到
        floor 边界照常续完，并把状态改写成合法值——连续前缀包含对齐后的全部
        字节，重下不足一段是安全代价。
        """
        aligned = 8 * self.SEGMENT
        stale = aligned + 1234  # 落在第 8 段中间
        temp_path = self._write_temp_file("xcopy-misaligned.part", self.data[:stale])
        resume = self._resume_state(temp_path, downloaded=stale)
        seen = {}
        real_download = xcc._download_to_sink

        def spy(**kwargs):
            seen.update(kwargs)
            return real_download(**kwargs)

        with patch.object(xcc, "_download_to_sink", side_effect=spy):
            result = self._copy_resume(resume)
        self.assertEqual(seen["start_offset"], aligned)  # 回退到段边界
        self.assertEqual(resume["downloaded_bytes"], len(self.data))  # 已完成 → 全量
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)

    def test_note_downloaded_snaps_to_segment_boundary(self):
        """断点只记分段栅格值：末段/文件尾位置不能被原样写进 downloaded_bytes。"""
        state = {"downloaded_bytes": 0}
        tracker = xcc._ResumeTracker(state, None)
        tracker.note_downloaded(3 * self.SEGMENT + 777)
        self.assertEqual(state["downloaded_bytes"], 3 * self.SEGMENT)
        tracker.note_downloaded(2 * self.SEGMENT)
        self.assertEqual(state["downloaded_bytes"], 2 * self.SEGMENT)

    # ---- 分支三：resume 无效 → 按 None 处理 ----

    def test_resume_invalid_fields_treated_as_none(self):
        resume = {"temp_path": "", "size": "abc", "downloaded_bytes": None, "sha1": 5}
        result = self._copy_resume(resume)
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(result.sha1, hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)
        self.assertEqual(len(self.range_calls), len(self.data) // self.SEGMENT)

    def test_resume_non_dict_treated_as_none(self):
        result = self._copy_resume("not-a-dict")
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)

    def test_resume_missing_temp_file_treated_as_none(self):
        resume = self._resume_state(
            os.path.join(self.temp_dir, "xcopy-gone.part"), downloaded=0
        )
        result = self._copy_resume(resume)
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)

    def test_resume_size_mismatch_treated_as_none(self):
        temp_path = self._write_temp_file("xcopy-mm.part", self.data)
        resume = self._resume_state(temp_path, downloaded=0)
        resume["size"] = len(self.data) + 1
        result = self._copy_resume(resume)
        self.assertEqual(result.status, "uploaded")
        self.assertEqual(self.target.upload_calls[0]["data"], self.data)

    # ---- 上传会话透传与分片记账 ----

    def test_upload_resume_passthrough_and_part_tracking(self):
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-up.part", self.data[:prefix])
        part_lengths = []  # 回调当时的 parts 长度（list 会被后续 mutate，不能延迟求值）

        def capture(state):
            upload = state.get("upload") or {}
            part_lengths.append(len(upload.get("parts") or []))

        resume = self._resume_state(
            temp_path,
            downloaded=prefix,
            upload={"provider": "115", "upload_id": "oss-1", "parts": []},
        )

        def uploading(
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
            upload_resume=None,
            **kwargs,
        ):
            self.target.upload_calls.append(
                {"filename": filename, "upload_resume": upload_resume}
            )
            callback = (upload_resume or {}).get("on_part_done")
            callback(1, "etag-1")
            callback(2, "etag-2")
            callback(1, "etag-1")  # OSS 重试同一 part：按 PartNumber 去重
            return _uploaded_result()

        self.target.upload_file = uploading
        self._copy_resume(resume, on_resume_update=capture)
        # upload_resume 原样透传，on_part_done 由引擎挂上
        self.assertEqual(self.target.upload_calls[0]["upload_resume"], resume["upload"])
        self.assertTrue(callable(resume["upload"]["on_part_done"]))
        # parts 按 PartNumber 去重记账，每次分片完成都触发一次续传更新
        self.assertEqual(
            resume["upload"]["parts"],
            [{"PartNumber": 1, "ETag": "etag-1"}, {"PartNumber": 2, "ETag": "etag-2"}],
        )
        self.assertEqual(part_lengths[-3:], [1, 2, 2])  # 新增、新增、去重

    def test_upload_resume_not_passed_without_resume(self):
        """resume 为 None 时不能传 upload_resume 关键字参数（provider 旧签名无此参）。"""
        captured = {}

        def uploading(cookie, sink, filename, target_dir_id="0", **kwargs):
            captured.update(kwargs)
            return _uploaded_result()

        self.target.upload_file = uploading
        self._copy(
            source_provider=self.source,
            target_provider=self.target,
            filename="movie.mkv",
            size=len(self.data),
            temp_dir=self.temp_dir,
        )
        self.assertNotIn("upload_resume", captured)

    def test_precheck_uses_remaining_bytes_for_resume(self):
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-pre.part", self.data[:prefix])
        resume = self._resume_state(temp_path, downloaded=prefix)
        with patch.object(xcc, "_precheck_disk_space") as precheck:
            self._copy_resume(resume)
        precheck.assert_called_once_with(self.temp_dir, len(self.data) - prefix)

    # ---- 临时文件保留规则（设计 §3.3） ----

    def test_resume_run_failure_keeps_temp_file(self):
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-keep.part", self.data[:prefix])
        resume = self._resume_state(temp_path, downloaded=prefix)
        self.target.upload_result = UploadError(
            "115 上传接口持续限流，请稍后再试", retriable=True, provider="115"
        )
        with self.assertRaises(xcc.CrossCloudCopyError):
            self._copy_resume(resume)
        # 失败保留（本地数据 + 上传会话都有继承价值）
        self.assertTrue(os.path.exists(temp_path))
        with open(temp_path, "rb") as fh:
            self.assertEqual(fh.read(), self.data)

    def test_cancel_keeps_temp_file(self):
        prefix = 8 * self.SEGMENT
        temp_path = self._write_temp_file("xcopy-cancel.part", self.data[:prefix])
        resume = self._resume_state(temp_path, downloaded=prefix)

        def cancel_check():
            return len(self.range_calls) >= 3

        with self.assertRaises(xcc.CrossCloudCopyCancelled):
            self._copy_resume(resume, cancel_check=cancel_check)
        self.assertTrue(os.path.exists(temp_path))

    def test_fresh_run_zero_byte_failure_deletes_temp_file(self):
        """全新运行：还没下到任何字节就失败 → 照旧删（并行预分配会让文件虚大，
        保留判断必须按真实下载水位，不能只看文件大小）。"""

        def failing_get(url, stream=False, headers=None, timeout=None, **kwargs):
            raise requests.ConnectionError("boom")

        with patch(
            "app.services.cross_cloud_copy.requests.get", side_effect=failing_get
        ):
            with self.assertRaises(xcc.CrossCloudCopyError):
                self._copy(
                    source_provider=self.source,
                    target_provider=self.target,
                    filename="movie.mkv",
                    size=len(self.data),
                    temp_dir=self.temp_dir,
                )
        self.assertEqual(self._leftover_parts(), [])

    def test_fresh_run_partial_failure_keeps_temp_file(self):
        """全新运行（任务层形态：空骨架 + 落库回调）：下到一部分字节后失败 → 保留。"""
        state = {"n": 0}

        def flaky_get(url, stream=False, headers=None, timeout=None, **kwargs):
            state["n"] += 1
            if state["n"] <= 3:
                return self._fake_get(url, stream=stream, headers=headers, timeout=timeout)
            raise requests.ConnectionError("boom")

        # 空骨架 = 还没有任何落盘数据，但有人记账（任务层能落库）
        skeleton = {
            "temp_path": "",
            "size": len(self.data),
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 0,
            "upload": {},
        }
        with patch("app.services.cross_cloud_copy.requests.get", side_effect=flaky_get):
            with self.assertRaises(xcc.CrossCloudCopyError):
                self._copy_resume(skeleton, on_resume_update=lambda s: None)
        self.assertEqual(len(self._leftover_parts()), 1)
        # 引擎把本次新建的临时文件认领进状态：重试才有路径可继承
        self.assertTrue(skeleton["temp_path"].startswith(self.temp_dir))
        self.assertGreater(skeleton["downloaded_bytes"], 0)

    def test_fresh_run_without_recorder_deletes_temp_file(self):
        """全新运行但没人记账（无 resume、无回调）：下了字节也不留（没人能继承）。"""
        state = {"n": 0}

        def flaky_get(url, stream=False, headers=None, timeout=None, **kwargs):
            state["n"] += 1
            if state["n"] <= 3:
                return self._fake_get(url, stream=stream, headers=headers, timeout=timeout)
            raise requests.ConnectionError("boom")

        with patch("app.services.cross_cloud_copy.requests.get", side_effect=flaky_get):
            with self.assertRaises(xcc.CrossCloudCopyError):
                self._copy(
                    source_provider=self.source,
                    target_provider=self.target,
                    filename="movie.mkv",
                    size=len(self.data),
                    temp_dir=self.temp_dir,
                )
        self.assertEqual(self._leftover_parts(), [])
