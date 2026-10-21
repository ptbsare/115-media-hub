"""任务断点续传单测（F2）：状态校验矩阵 / 生命周期落库 / 孤儿清理联动。

引擎 ``cross_cloud_copy.copy_file_across_clouds`` 由并行实施的另一方负责，
本文件凡涉及引擎行为一律 monkeypatch，不依赖其实现细节。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app import db
from app.services import cross_cloud_copy as xcc
from app.services import cross_copy_jobs as ccj
from app.services import cross_copy_resume as ccr


def _uploaded_result():
    return xcc.CopyFileResult(
        status="uploaded",
        source_provider="quark",
        target_provider="115",
        file_id="F-1",
        name="movie.mkv",
        size=1024,
        sha1="ABC",
        message="已完成",
    )


class _FakeResponse:
    """最小下载响应假件（iter_content / raise_for_status / close）。"""

    def __init__(self, status_code, chunks):
        self.status_code = int(status_code)
        self._chunks = chunks or []

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected HTTP {self.status_code}")

    def iter_content(self, chunk_size=1):
        for chunk in self._chunks:
            yield chunk

    def close(self):
        pass


class FakeProvider:
    def __init__(self, name, label, cookie="cookie-x", supports_upload=True):
        self.name = name
        self.label = label
        self._cookie = cookie
        self.supports_upload = supports_upload
        self.supports_delete = True
        self.delete_calls = []

    def get_cookie(self, cfg):
        return self._cookie

    def is_configured(self, cfg):
        return bool(self._cookie)

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        self.delete_calls.append(list(entry_ids))
        return {"ok": True}


class ResumeTestBase(unittest.TestCase):
    """临时库 + 临时目录 + 配置；任务层外围依赖全部 patch 掉。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ccr_test_")
        self.original_db_path = db.DB_PATH
        self.original_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "test.db")
        db._DB_ENSURED = False
        db.ensure_db()
        with db.db_connection() as conn:
            conn.execute("DELETE FROM cross_copy_jobs")
            conn.commit()
        # 模块级内存状态：不清会串味（进度节流 / 断点写库节流 / 排队名额）
        ccj._job_progress.clear()
        ccj._last_db_progress.clear()
        ccj._last_resume_persist.clear()
        ccj._cancel_requested.clear()
        ccj._job_detail.clear()
        with ccj._sched_lock:
            ccj._queued.clear()
            ccj._active.clear()
        with ccj._running_lock:
            ccj._running.clear()

        self.cfg = {
            "cross_copy_notify_subscription": False,
            "cross_copy_notify_manual": False,
            "cross_copy_delete_source_after": True,
            "provider_enabled": {"quark": True, "115": True},
        }
        self.source_provider = FakeProvider("quark", "夸克网盘")
        self.target_provider = FakeProvider("115", "115网盘")
        patchers = [
            patch.object(ccj, "get_config", return_value=self.cfg),
            patch.object(ccj, "_get_provider_or_none", side_effect=self._fake_get_provider),
            patch.object(ccj, "_send_notify_content", MagicMock()),
            patch.object(ccj, "notify_channel_ready", return_value=False),
            patch.object(ccj, "submit_background", MagicMock()),
        ]
        self._patchers = patchers
        for p in patchers:
            p.start()

    def tearDown(self):
        for p in self._patchers:
            p.stop()
        db.DB_PATH = self.original_db_path
        db._DB_ENSURED = self.original_db_ensured
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_get_provider(self, name):
        if name == "quark":
            return self.source_provider
        if name == "115":
            return self.target_provider
        return None

    def _create_job(self, **overrides):
        params = dict(
            title="测试电影",
            source_provider="quark",
            target_provider="115",
            source_file_id="f-quark-1",
            target_dir_id="dir-115",
            filename="movie.mkv",
            file_size=1024,
        )
        params.update(overrides)
        return ccj.create_cross_copy_job(**params)

    def _make_temp_file(self, name, size):
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as fh:
            fh.write(b"\0" * size)
        return path

    def _job_with_resume(self, resume_state, **overrides):
        job_id = self._create_job(**overrides)
        ccj.update_cross_copy_job(job_id, resume=resume_state)
        return ccj.get_cross_copy_job(job_id)

    def _run(self, job_id):
        return asyncio.run(ccj.run_cross_copy_job(job_id))


class TestBuildResumeState(ResumeTestBase):
    """校验矩阵：文件缺失 / 大小不符 / 源哈希变化 / 探测不到时接受。"""

    def test_no_resume_returns_none(self):
        job = ccj.get_cross_copy_job(self._create_job())
        # 新任务 resume 键存在但为空
        self.assertEqual(job["resume"], {})
        self.assertIsNone(ccr.build_resume_state(job, self.cfg))

    def test_missing_temp_file_invalidates_and_clears(self):
        state = {
            "temp_path": os.path.join(self.tmp, "gone.part"),
            "size": 1024,
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 512,
            "upload": {},
        }
        job = self._job_with_resume(state)
        self.assertIsNone(ccr.build_resume_state(job, self.cfg))
        # 作废要落库：再读一行 resume 已被清空
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_size_mismatch_invalidates_and_clears(self):
        # 磁盘上只有 100 字节，断点声称前缀 512：前缀不完整，数据不可信
        temp_path = self._make_temp_file("short.part", 100)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 512,
            "upload": {},
        }
        job = self._job_with_resume(state)
        self.assertIsNone(ccr.build_resume_state(job, self.cfg))
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_file_larger_than_expected_invalidates(self):
        # 比预期还大：不是同一份文件
        temp_path = self._make_temp_file("big.part", 2048)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        self.assertIsNone(ccr.build_resume_state(job, self.cfg))
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_complete_file_accepted(self):
        temp_path = self._make_temp_file("full.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        built = ccr.build_resume_state(job, self.cfg)
        self.assertIsNotNone(built)
        self.assertEqual(built["temp_path"], temp_path)
        self.assertEqual(built["size"], 1024)
        self.assertEqual(built["downloaded_bytes"], 1024)
        self.assertEqual(built["upload"], {})

    def test_prefix_accepted_and_normalized(self):
        temp_path = self._make_temp_file("prefix.part", 500)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "ABCDEF",
            "md5": "MD5LOWER",
            "downloaded_bytes": 500,
            "upload": "not-a-dict",
        }
        job = self._job_with_resume(state)
        built = ccr.build_resume_state(job, self.cfg)
        self.assertIsNotNone(built)
        self.assertEqual(built["sha1"], "ABCDEF")
        self.assertEqual(built["md5"], "md5lower")  # 归一化为小写
        self.assertEqual(built["upload"], {})  # 非 dict 会话按空处理
        # 合法状态不清库
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"]["size"], 1024)

    def test_source_hash_changed_invalidates_and_clears(self):
        """源文件被替换（哈希变了）：旧内容绝不能传上去，作废重传。"""
        temp_path = self._make_temp_file("stale.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "OLDOLDSHA1",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        with patch.object(
            ccr, "_probe_source_hash", return_value={"sha1": "NEWNEWSHA1", "size": 1024}
        ):
            self.assertIsNone(ccr.build_resume_state(job, self.cfg))
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_source_size_changed_invalidates_and_clears(self):
        temp_path = self._make_temp_file("resized.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "SAMESHA1",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        with patch.object(
            ccr, "_probe_source_hash", return_value={"sha1": "SAMESHA1", "size": 2048}
        ):
            self.assertIsNone(ccr.build_resume_state(job, self.cfg))
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_hash_probe_unavailable_accepts(self):
        """夸克给不出哈希（探测不到）：接受续传，不因探测失败作废。"""
        temp_path = self._make_temp_file("quark.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "WHATEVER",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        with patch.object(ccr, "_probe_source_hash", return_value={}):
            built = ccr.build_resume_state(job, self.cfg)
        self.assertIsNotNone(built)
        self.assertEqual(built["sha1"], "WHATEVER")

    def test_hash_probe_match_accepts(self):
        temp_path = self._make_temp_file("same.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "MATCHSHA1",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        job = self._job_with_resume(state)
        with patch.object(
            ccr, "_probe_source_hash", return_value={"sha1": "MATCHSHA1", "size": 1024}
        ):
            built = ccr.build_resume_state(job, self.cfg)
        self.assertIsNotNone(built)

    def test_probe_wrapper_without_cookie_returns_empty(self):
        """拿不到源网盘 cookie 时按探测不到处理（接受续传），不炸。"""
        job = ccj.get_cross_copy_job(self._create_job())
        self.assertEqual(ccr._probe_source_hash(job, self.cfg, 1024), {})


class TestProbeSourceHashWiring(ResumeTestBase):
    """_probe_source_hash 与 resolve_source_hash 的接线（cookie → 复探调用）。"""

    def test_probe_calls_resolve_source_hash_with_source_identity(self):
        from app.providers.registry import _providers

        job = ccj.get_cross_copy_job(self._create_job())
        self.cfg["cookie_quark"] = "ck-real"
        captured = {}

        def fake_resolve(provider, cookie, file_id, size=-1):
            captured["provider"] = getattr(provider, "name", "")
            captured["cookie"] = cookie
            captured["file_id"] = file_id
            captured["size"] = size
            return {"sha1": "PROBE123", "size": 1024, "origin": "provider"}

        # 注册一个假源 provider（quark 真身在本测试进程里未必已注册）；
        # 用完必须还原，否则污染同进程后续用例（label / 订阅源归一化都查注册表）
        fake = FakeProvider("quark", "夸克网盘", cookie="ck-real")
        previous = _providers.get("quark")
        _providers["quark"] = fake
        try:
            with patch.object(xcc, "resolve_source_hash", side_effect=fake_resolve):
                probed = ccr._probe_source_hash(job, self.cfg, 1024)
        finally:
            if previous is None:
                _providers.pop("quark", None)
            else:
                _providers["quark"] = previous

        self.assertEqual(probed["sha1"], "PROBE123")
        self.assertEqual(captured["provider"], "quark")
        self.assertEqual(captured["cookie"], "ck-real")
        self.assertEqual(captured["file_id"], "f-quark-1")
        self.assertEqual(captured["size"], 1024)


class TestClearResumeState(ResumeTestBase):
    def test_clear_empties_state(self):
        job_id = self._create_job()
        ccj.update_cross_copy_job(job_id, resume={"temp_path": "/x", "size": 1})
        ccr.clear_resume_state(job_id)
        self.assertEqual(ccj.get_cross_copy_job(job_id)["resume"], {})

    def test_corrupted_json_reads_as_empty(self):
        """resume_json 损坏（手改库/历史脏数据）：按无断点处理，不炸。"""
        job_id = self._create_job()
        with db.db_connection() as conn:
            conn.execute(
                "UPDATE cross_copy_jobs SET resume_json = ? WHERE id = ?",
                ("{not json", job_id),
            )
            conn.commit()
        self.assertEqual(ccj.get_cross_copy_job(job_id)["resume"], {})


class TestPruneResumeForMissingFiles(ResumeTestBase):
    def test_prunes_only_missing_temp_files(self):
        alive_path = self._make_temp_file("alive.part", 16)
        gone_path = os.path.join(self.tmp, "gone.part")
        job_alive = self._job_with_resume(
            {"temp_path": alive_path, "size": 16, "downloaded_bytes": 16, "upload": {}}
        )
        job_gone = self._job_with_resume(
            {"temp_path": gone_path, "size": 16, "downloaded_bytes": 8, "upload": {}}
        )
        cleaned = ccr.prune_resume_for_missing_files()
        self.assertEqual(cleaned, 1)
        self.assertEqual(
            ccj.get_cross_copy_job(job_alive["id"])["resume"]["temp_path"], alive_path
        )
        self.assertEqual(ccj.get_cross_copy_job(job_gone["id"])["resume"], {})

    def test_empty_and_corrupted_rows_ignored(self):
        self._create_job()  # '{}'
        job_id = self._create_job()
        with db.db_connection() as conn:
            conn.execute(
                "UPDATE cross_copy_jobs SET resume_json = ? WHERE id = ?",
                ("[1,2,3]", job_id),
            )
            conn.commit()
        self.assertEqual(ccr.prune_resume_for_missing_files(), 0)


class TestResumeStateForDb(ResumeTestBase):
    """落库净化：on_part_done 闭包等运行时对象必须剔除，其余字段原样保留。"""

    def _state_with_callback(self):
        return {
            "temp_path": "/tmp/x.part",
            "size": 1024,
            "sha1": "ABC",
            "md5": "def",
            "downloaded_bytes": 512,
            "upload": {
                "provider": "115",
                "fields": {"bucket": "b"},
                "upload_id": "uid-1",
                "parts": [{"PartNumber": 1, "ETag": "e1"}],
                "on_part_done": lambda part_number, etag: None,
            },
        }

    def test_strips_on_part_done_and_keeps_everything_else(self):
        cleaned = ccr.resume_state_for_db(self._state_with_callback())
        self.assertNotIn("on_part_done", cleaned["upload"])
        self.assertEqual(cleaned["upload"]["upload_id"], "uid-1")
        self.assertEqual(cleaned["upload"]["parts"], [{"PartNumber": 1, "ETag": "e1"}])
        self.assertEqual(cleaned["downloaded_bytes"], 512)
        # 净化结果必须能直接 json.dumps（这就是本函数的存在意义）
        db.safe_json_dumps(cleaned)

    def test_does_not_mutate_original_state(self):
        """引擎持原对象就地更新，净化必须是深拷贝。"""
        state = self._state_with_callback()
        cleaned = ccr.resume_state_for_db(state)
        cleaned["downloaded_bytes"] = 999
        cleaned["upload"]["parts"].append({"PartNumber": 2})
        self.assertEqual(state["downloaded_bytes"], 512)
        self.assertEqual(len(state["upload"]["parts"]), 1)
        self.assertIn("on_part_done", state["upload"])

    def test_unknown_non_serializable_value_nullified_not_crashing(self):
        cleaned = ccr.resume_state_for_db({"temp_path": "/x", "weird": object()})
        self.assertEqual(cleaned["temp_path"], "/x")
        self.assertIsNone(cleaned["weird"])
        db.safe_json_dumps(cleaned)

    def test_non_dict_returns_empty(self):
        self.assertEqual(ccr.resume_state_for_db(None), {})
        self.assertEqual(ccr.resume_state_for_db("nope"), {})

    def test_db_write_with_callback_state_persists(self):
        """回归钉：带闭包的状态经 update_cross_copy_job 落库后读回来是完整 JSON。

        修复前 json.dumps 遇函数对象抛 TypeError，被落库处的 try/except 吞掉，
        断点状态静默停在旧值——本用例直接钉住写库通路。
        """
        job_id = self._create_job()
        ccj.update_cross_copy_job(job_id, resume=self._state_with_callback())
        stored = ccj.get_cross_copy_job(job_id)["resume"]
        self.assertEqual(stored["downloaded_bytes"], 512)
        self.assertEqual(stored["upload"]["upload_id"], "uid-1")
        self.assertEqual(stored["upload"]["parts"], [{"PartNumber": 1, "ETag": "e1"}])
        self.assertNotIn("on_part_done", stored["upload"])


class TestResumeLifecycle(ResumeTestBase):
    """run_cross_copy_job 单文件分支：构造/传入 resume、成功清除、取消失败落库。"""

    def _seed_valid_state(self, **overrides):
        temp_path = self._make_temp_file("seed.part", 1024)
        state = {
            "temp_path": temp_path,
            "size": 1024,
            "sha1": "",
            "md5": "",
            "downloaded_bytes": 1024,
            "upload": {},
        }
        return self._job_with_resume(state, **overrides)

    def test_engine_receives_resume_state_and_persister(self):
        job = self._seed_valid_state()
        captured = {}
        resume_writes = []
        real_update = ccj.update_cross_copy_job

        def recording_update(job_id, **fields):
            if "resume" in fields:
                resume_writes.append(fields["resume"])
            return real_update(job_id, **fields)

        def fake_engine(**kwargs):
            captured.update(kwargs)
            # 引擎就地更新状态并触发回调（契约行为）
            kwargs["resume"]["downloaded_bytes"] = 2048
            kwargs["on_resume_update"](kwargs["resume"])
            return _uploaded_result()

        with patch.object(xcc, "copy_file_across_clouds", side_effect=fake_engine), \
             patch.object(ccj, "update_cross_copy_job", side_effect=recording_update):
            self._run(job["id"])

        self.assertEqual(captured["resume"]["temp_path"], job["resume"]["temp_path"])
        self.assertTrue(callable(captured["on_resume_update"]))
        # 回调落库（节流首次立即写）：引擎改过的状态确实写进了 DB
        self.assertTrue(
            any(state.get("downloaded_bytes") == 2048 for state in resume_writes),
            f"resume writes: {resume_writes}",
        )
        # 成功路径最后把断点清掉
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})

    def test_no_state_passes_skeleton(self):
        """没有可继承断点时也传空骨架 + 落库回调（bootstrap 续传链）。

        只在「已有断点」时才传状态的话，首跑失败留下的临时文件没有任何
        状态指向它，重试接不上——续传链永远走不到第二步。
        """
        job_id = self._create_job()
        captured = {}

        def fake_engine(**kwargs):
            captured.update(kwargs)
            return _uploaded_result()

        with patch.object(xcc, "copy_file_across_clouds", side_effect=fake_engine):
            self._run(job_id)
        resume = captured.get("resume")
        self.assertIsInstance(resume, dict)
        self.assertEqual(resume.get("temp_path"), "")
        self.assertEqual(resume.get("downloaded_bytes"), 0)
        self.assertTrue(callable(captured.get("on_resume_update")))

    def test_success_clears_resume_state(self):
        job = self._seed_valid_state()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job["id"])
        final = ccj.get_cross_copy_job(job["id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["resume"], {})

    def test_cancel_persists_state_and_appends_suffix(self):
        job = self._seed_valid_state()

        def cancelled_engine(**kwargs):
            raise xcc.CrossCloudCopyCancelled("已取消")

        with patch.object(xcc, "copy_file_across_clouds", side_effect=cancelled_engine):
            self._run(job["id"])
        final = ccj.get_cross_copy_job(job["id"])
        self.assertEqual(final["status"], "cancelled")
        # 断点被保留：重试可续传
        self.assertEqual(final["resume"]["downloaded_bytes"], 1024)
        self.assertIn("已保留断点，重试可续传", final["status_detail"])

    def test_cancel_without_state_has_no_suffix(self):
        job_id = self._create_job()

        def cancelled_engine(**kwargs):
            raise xcc.CrossCloudCopyCancelled("已取消")

        with patch.object(xcc, "copy_file_across_clouds", side_effect=cancelled_engine):
            self._run(job_id)
        final = ccj.get_cross_copy_job(job_id)
        self.assertEqual(final["status"], "cancelled")
        self.assertNotIn("已保留断点", final["status_detail"])
        self.assertEqual(final["resume"], {})

    def test_failure_persists_state(self):
        job = self._seed_valid_state()

        def failing_engine(**kwargs):
            # 模拟中断在下载中：状态推进后抛错
            kwargs["resume"]["downloaded_bytes"] = 700
            kwargs["on_resume_update"](kwargs["resume"])
            raise xcc.CrossCloudCopyError("115 操作过于频繁")

        with patch.object(xcc, "copy_file_across_clouds", side_effect=failing_engine):
            self._run(job["id"])
        final = ccj.get_cross_copy_job(job["id"])
        self.assertEqual(final["status"], "failed")
        # 终态保底：节流没写到的最后状态也落库了
        self.assertEqual(final["resume"]["downloaded_bytes"], 700)

    def test_folder_job_passes_no_resume(self):
        from app.services import cross_cloud_copy_folder as xccf

        job_id = self._create_job(source_is_dir=True, filename="剧集文件夹")
        captured = {}

        def fake_folder(**kwargs):
            captured.update(kwargs)
            from types import SimpleNamespace

            return SimpleNamespace(
                status="completed",
                folder_name="剧集文件夹",
                total_bytes=10,
                message="ok",
                to_dict=lambda: {"status": "completed"},
            )

        with patch.object(xccf, "copy_folder_across_clouds", side_effect=fake_folder):
            self._run(job_id)
        self.assertNotIn("resume", captured)
        self.assertNotIn("on_resume_update", captured)

    def test_startup_cleanup_prunes_resume(self):
        """孤儿清理联动：临时文件没了，断点状态作废。"""
        # 指向一个从不存在的路径（等价于临时文件已被清理掉的情形），
        # 不在测试里做真实删除——本机沙箱会拦 unlink。
        gone_path = os.path.join(self.tmp, "never-existed.part")
        job = self._job_with_resume(
            {"temp_path": gone_path, "size": 64, "downloaded_bytes": 64, "upload": {}}
        )
        ccj.cleanup_temp_files_on_startup()
        self.assertEqual(ccj.get_cross_copy_job(job["id"])["resume"], {})


class _RangeSourceProvider:
    """带 Range 分段服务的假源 provider（quark 角色，拿不出哈希）。"""

    name = "quark"
    label = "夸克网盘"
    supports_upload = False

    def __init__(self, data):
        self.data = data
        self.range_calls = []

    def get_cookie(self, cfg):
        return "cookie-q"

    def is_configured(self, cfg):
        return True

    def resolve_download_info(self, cookie, file_id):
        return {"url": "https://x/r", "size": len(self.data), "file_name": "movie.mkv"}


class _FlakyTargetProvider:
    """假目标 provider（115 角色）：第一次上传失败，第二次成功。"""

    name = "115"
    label = "115网盘"
    supports_upload = True

    def __init__(self):
        self.upload_calls = []
        self.fail_next = True

    def get_cookie(self, cfg):
        return "cookie-t"

    def is_configured(self, cfg):
        return True

    def upload_file(
        self, cookie, sink, filename, target_dir_id="0", *,
        sha1="", md5="", size=-1, progress_cb=None, cancel_check=None,
        upload_resume=None, **kwargs,
    ):
        data = sink.read()
        sink.seek(0)
        self.upload_calls.append(
            {"sha1": sha1, "md5": md5, "size": size, "bytes": len(data),
             "upload_resume": upload_resume}
        )
        if progress_cb is not None:
            try:
                progress_cb(size if size >= 0 else len(data), size)
            except Exception:
                pass
        if self.fail_next:
            self.fail_next = False
            raise xcc.UploadError("上传到一半失败", retriable=True)
        from app.providers.base import UploadResult

        return UploadResult(status="uploaded", name=filename, size=size, sha1=sha1)


class TestResumeBootstrapChain(ResumeTestBase):
    """集成：首跑失败留断点 → 重试真续传（任务层 + 真引擎 + 假 provider）。

    这是 bootstrap 修复的回归钉：任务层对单文件任务总是传骨架 + 落库回调，
    引擎 tracking-only 记账，失败后断点落库；第二次运行 build_resume_state
    校验通过，引擎跳过下载直接上传。任何一环退回「无状态不传」，这条链就断。
    """

    SEGMENT = 64 * 1024

    def setUp(self):
        super().setUp()
        self.data = bytes(range(256)) * 1024 * 8  # 2MB
        self.cfg["cross_copy_temp_dir"] = self.tmp
        self.source = _RangeSourceProvider(self.data)
        self.target = _FlakyTargetProvider()

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            range_header = str((headers or {}).get("Range", "") or "")
            if not range_header:
                return _FakeResponse(200, [self.data])
            start, _, end = range_header.replace("bytes=", "").partition("-")
            start_i, end_i = int(start), int(end)
            self.source.range_calls.append((start_i, end_i))
            return _FakeResponse(206, [self.data[start_i:end_i + 1]])

        extra = [
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch.object(xcc, "DOWNLOAD_SEGMENT_BYTES", self.SEGMENT),
            patch.object(xcc, "MEMORY_CHANNEL_THRESHOLD_BYTES", 1024),
            patch("app.services.cross_cloud_copy.time.sleep"),
        ]
        self._patchers.extend(extra)
        for patcher in extra:
            patcher.start()

    def _fake_get_provider(self, name):
        if name == "quark":
            return self.source
        if name == "115":
            return self.target
        return None

    def test_first_failure_keeps_breakpoint_and_retry_resumes(self):
        job_id = self._create_job(file_size=len(self.data))

        # ---- 第一run：下载完成，上传失败 ----
        self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")
        state = job["resume"]
        self.assertTrue(state.get("temp_path"), "首跑失败必须留下断点状态")
        self.assertTrue(os.path.isfile(state["temp_path"]), "临时文件必须保留")
        self.assertEqual(state["downloaded_bytes"], len(self.data))
        self.assertTrue(state["sha1"], "下载完成必须算过哈希")
        self.assertIn("已保留断点", job["status_detail"])
        range_calls_after_first = len(self.source.range_calls)
        self.assertGreater(range_calls_after_first, 0)
        self.assertEqual(len(self.target.upload_calls), 1)

        # ---- 第二run：重试后跳过下载，直接上传 ----
        ccj.update_cross_copy_job(job_id, status="pending", error_message="")
        self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["resume"], {}, "成功后断点必须清除")
        self.assertEqual(
            len(self.source.range_calls),
            range_calls_after_first,
            "续传run 不应再发任何 Range 请求（下载整体跳过）",
        )
        self.assertEqual(len(self.target.upload_calls), 2)
        second = self.target.upload_calls[1]
        self.assertEqual(second["bytes"], len(self.data), "上传读到的必须是完整本地文件")
        self.assertTrue(second["sha1"])


if __name__ == "__main__":
    unittest.main()
