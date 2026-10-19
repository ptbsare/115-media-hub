"""文件哈希备忘录（media_hash_cache）单元测试。

用临时目录建一个独立 SQLite 库（参考 test_local_storage.py 的 tempfile.mkdtemp 写法），
不碰真实数据目录，也不依赖全局 tempfile 探测。
"""

import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from app import db
from app.services import media_hash_cache as mhc


class MediaHashCacheTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_hash_cache_")
        self._orig_db_path = db.DB_PATH
        self._orig_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "data.db")
        db._DB_ENSURED = False
        db.ensure_db()

    def tearDown(self):
        db.DB_PATH = self._orig_db_path
        db._DB_ENSURED = self._orig_db_ensured
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _age_row(self, provider, file_id, seconds):
        """把 updated_ts 往前拨，模拟过期。"""
        with db.db_connection() as conn:
            conn.execute(
                "UPDATE media_hash_cache SET updated_ts = ? WHERE provider = ? AND file_id = ?",
                (time.time() - seconds, provider, file_id),
            )
            conn.commit()


class TestRememberAndLookup(MediaHashCacheTestBase):
    def test_remember_then_lookup(self):
        sha1 = "a" * 40
        mhc.remember_file_hash("quark", "fid-1", 1024, sha1)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-1", 1024), sha1.upper())

    def test_sha1_normalized_to_upper(self):
        mhc.remember_file_hash("quark", "fid-2", 2048, "b" * 40)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-2", 2048), "B" * 40)

    def test_provider_name_is_case_insensitive(self):
        mhc.remember_file_hash("Quark", "fid-3", 10, "c" * 40)
        self.assertEqual(mhc.lookup_file_hash("QUARK", "fid-3", 10), "C" * 40)

    def test_invalid_sha1_rejected(self):
        mhc.remember_file_hash("quark", "fid-4", 10, "not-a-sha1")
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-4", 10), "")

    def test_invalid_size_ignored(self):
        mhc.remember_file_hash("quark", "fid-5", 0, "d" * 40)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-5", 0), "")


class TestSizeValidation(MediaHashCacheTestBase):
    def test_size_mismatch_is_miss(self):
        mhc.remember_file_hash("quark", "fid-6", 1024, "e" * 40)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-6", 1025), "")
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-6", 1024), "E" * 40)

    def test_unknown_size_is_miss(self):
        mhc.remember_file_hash("quark", "fid-7", 1024, "f" * 40)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-7", -1), "")
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-7", 0), "")


class TestExpiry(MediaHashCacheTestBase):
    def test_expired_entry_is_miss(self):
        mhc.remember_file_hash("quark", "fid-8", 999, "1" * 40)
        self._age_row("quark", "fid-8", mhc.HASH_CACHE_TTL_SECONDS + 60)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-8", 999), "")

    def test_entry_within_ttl_still_valid(self):
        mhc.remember_file_hash("quark", "fid-9", 999, "2" * 40)
        self._age_row("quark", "fid-9", mhc.HASH_CACHE_TTL_SECONDS - 60)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-9", 999), "2" * 40)


class TestForget(MediaHashCacheTestBase):
    def test_forget_removes_entry(self):
        mhc.remember_file_hash("quark", "fid-10", 512, "3" * 40)
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-10", 512), "3" * 40)
        mhc.forget_file_hash("quark", "fid-10")
        self.assertEqual(mhc.lookup_file_hash("quark", "fid-10", 512), "")

    def test_forget_missing_entry_is_noop(self):
        mhc.forget_file_hash("quark", "not-exist")  # 不应抛异常


class TestFailuresAreSwallowed(MediaHashCacheTestBase):
    def test_lookup_swallows_db_error(self):
        with patch.object(mhc, "db_connection", side_effect=RuntimeError("db down")):
            self.assertEqual(mhc.lookup_file_hash("quark", "fid-11", 100), "")

    def test_remember_swallows_db_error(self):
        with patch.object(mhc, "db_connection", side_effect=RuntimeError("db down")):
            mhc.remember_file_hash("quark", "fid-12", 100, "4" * 40)  # 不应抛异常

    def test_forget_swallows_db_error(self):
        with patch.object(mhc, "db_connection", side_effect=RuntimeError("db down")):
            mhc.forget_file_hash("quark", "fid-13")  # 不应抛异常


class TestConcurrentWrites(MediaHashCacheTestBase):
    def test_same_key_written_concurrently(self):
        """复制任务在线程池里跑，同一 key 可能被多个线程同时写（INSERT OR REPLACE）。"""
        errors = []

        def worker(index):
            try:
                for _ in range(5):
                    mhc.remember_file_hash("quark", "fid-14", 4096, f"{index:040X}")
            except Exception as exc:  # pragma: no cover - 失败时才走这里
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        # 最终留下的是其中某一个线程写的值，格式与大小都必须正确
        cached = mhc.lookup_file_hash("quark", "fid-14", 4096)
        self.assertEqual(len(cached), 40)
        self.assertEqual(cached, cached.upper())

        with db.db_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM media_hash_cache WHERE provider = ? AND file_id = ?",
                ("quark", "fid-14"),
            ).fetchone()[0]
        self.assertEqual(int(count), 1)


if __name__ == "__main__":
    unittest.main()
