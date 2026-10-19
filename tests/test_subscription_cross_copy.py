"""订阅跨盘补源测试（#10）。"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app import core
from app import db
from app.services import cross_copy_jobs as ccj
from app.services import subscription_cross_copy as sc


class FakeQuarkProvider:
    """夸克 provider 的最小假实现（分享转存 + 目录操作）。"""

    name = "quark"
    label = "夸克网盘"
    supports_upload = True
    supports_delete = True

    def __init__(self, fail_save=False, empty_share=False, staged=None):
        self.fail_save = fail_save
        self.empty_share = empty_share
        self.staged = staged if staged is not None else [
            {"id": "fid-staged-1", "name": "movie.mkv", "size": 2048},
        ]
        self.calls = []

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        self.calls.append(("resolve_share", share_url))
        return {"url": share_url, "pwd_id": "pwd-1", "stoken": "st-1", "receive_code": receive_code}

    def list_share_entries(self, cookie, share_payload, cid="0", offset=0, limit=200):
        self.calls.append(("list_share",))
        if self.empty_share:
            return []
        return [{"id": "share-fid-1", "name": "movie.mkv", "size": 2048}]

    def ensure_folder_id_by_path(self, cookie, relative_path):
        self.calls.append(("ensure_folder", relative_path))
        return "staging-dir-1"

    def prepare_share_receive(self, cookie, share_payload, cid="0"):
        self.calls.append(("prepare",))
        return {**share_payload, "target_cid": str(cid or "0")}

    def submit_share_receive(self, cookie, receive_payload, files):
        self.calls.append(("submit",))
        if self.fail_save:
            raise RuntimeError("夸克转存被风控")
        return {"response": {"code": 0}, "selection": {}}

    def list_entries(self, cookie, cid="0"):
        self.calls.append(("list_entries",))
        return list(self.staged)


class FakeTargetProvider:
    name = "115"
    label = "115网盘"
    supports_upload = True
    supports_delete = True

    def is_configured(self, cfg):
        return True

    def get_cookie(self, cfg):
        return "cookie-115"


class SubscriptionCrossCopyTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_scc_test_")
        self.original_db_path = db.DB_PATH
        self.original_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "test.db")
        db._DB_ENSURED = False
        db.ensure_db()
        with db.db_connection() as conn:
            conn.execute("DELETE FROM cross_copy_jobs")
            conn.commit()

        self.quark = FakeQuarkProvider()
        self.target = FakeTargetProvider()
        self.cfg = {"cookie_quark": "cookie-quark"}

        def fake_get_provider(name):
            if name == "quark":
                return self.quark
            if name == "115":
                return self.target
            return None

        patchers = [
            patch.object(sc, "_get_provider_or_none", side_effect=fake_get_provider),
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

    def _task(self, **overrides):
        task = {
            "name": "测试剧集",
            "title": "测试剧集",
            "provider": "115",
            "savepath": "/影视库/测试剧集",
            "cross_copy_mode": "fallback",
            "cross_copy_sources": ["quark"],
        }
        task.update(overrides)
        return task

    def _candidate(self, url="https://pan.quark.cn/s/abc123", link_type="quark", score=90):
        return {
            "score": score,
            "item": {"id": 1, "link_url": url, "link_type": link_type},
        }


class TestModeGating(SubscriptionCrossCopyTestBase):
    def test_mode_off_skips(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(cross_copy_mode="off"),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(result["registered"], [])
        self.assertEqual(result["skipped"], ["补源未开启"])

    def test_task_provider_quark_skips(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(provider="quark", cross_copy_mode="fallback", cross_copy_sources=["quark"]),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(result["registered"], [])
        self.assertIn("任务网盘即夸克，无需补源", result["skipped"])

    def test_missing_quark_cookie(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg={"cookie_quark": ""},
            candidates=[self._candidate()],
        )
        self.assertEqual(result["registered"], [])
        self.assertIn("夸克 Cookie 未配置", result["skipped"])

    def test_no_quark_candidates(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate(link_type="115")],
        )
        self.assertEqual(result["registered"], [])
        self.assertIn("候选中没有夸克分享", result["skipped"])

    def test_empty_candidates(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[],
        )
        self.assertEqual(result["registered"], [])


class TestRegistration(SubscriptionCrossCopyTestBase):
    def test_register_success(self):
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(len(result["registered"]), 1)
        job_id = result["registered"][0]
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["source_provider"], "quark")
        self.assertEqual(job["target_provider"], "115")
        self.assertEqual(job["source_file_id"], "fid-staged-1")
        self.assertEqual(job["origin"], "subscription")
        self.assertEqual(job["origin_ref"], "测试剧集")
        self.assertEqual(job["source_mode"], "save_first")
        self.assertEqual(job["filename"], "movie.mkv")
        self.assertEqual(job["file_size"], 2048)
        # 分享转存全链路被调用
        call_names = [c[0] for c in self.quark.calls]
        self.assertEqual(
            call_names,
            ["resolve_share", "list_share", "ensure_folder", "prepare", "submit", "list_entries"],
        )
        # 中转目录带任务子目录
        ensure_call = next(c for c in self.quark.calls if c[0] == "ensure_folder")
        self.assertIn("云下载/跨盘中转/测试剧集", ensure_call[1])
        ccj.submit_background.assert_called_once()
        submit_args = ccj.submit_background.call_args[0]
        self.assertEqual(submit_args[0], ccj.run_cross_copy_job)
        self.assertEqual(submit_args[1], job_id)

    def test_dedupe_active_job(self):
        first = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(len(first["registered"]), 1)
        second = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(second["registered"], [])
        self.assertTrue(any("已在补源队列中" in s for s in second["skipped"]))

    def test_dedupe_only_for_same_task(self):
        sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        result = sc.register_cross_copy_from_candidates(
            task=self._task(name="另一个任务", title="另一个任务"),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(len(result["registered"]), 1)

    def test_save_failure_skips(self):
        self.quark.fail_save = True
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(result["registered"], [])
        self.assertTrue(any("转存失败" in s for s in result["skipped"]))
        self.assertTrue(any("转存失败" in msg for msg, _ in result["logs"]))

    def test_empty_share_skips(self):
        self.quark.empty_share = True
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(result["registered"], [])
        self.assertTrue(any("转存失败" in s for s in result["skipped"]))

    def test_staged_lookup_fallback_first_entry(self):
        # 分享名与落盘名不一致时退回目录第一条
        self.quark.staged = [{"id": "fid-other", "name": "renamed.mkv", "size": 999}]
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(len(result["registered"]), 1)
        job = ccj.get_cross_copy_job(result["registered"][0])
        self.assertEqual(job["source_file_id"], "fid-other")
        self.assertEqual(job["filename"], "renamed.mkv")

    def test_max_registered_per_run(self):
        candidates = [
            self._candidate(url=f"https://pan.quark.cn/s/share{i}", score=90 - i)
            for i in range(5)
        ]
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=candidates,
        )
        self.assertEqual(len(result["registered"]), sc.MAX_REGISTERED_PER_RUN)

    def test_duplicate_urls_deduped(self):
        candidates = [self._candidate(), self._candidate()]
        result = sc.register_cross_copy_from_candidates(
            task=self._task(),
            cfg=self.cfg,
            candidates=candidates,
        )
        self.assertEqual(len(result["registered"]), 1)


class TestConfigNormalization(unittest.TestCase):
    def test_mode_values(self):
        self.assertEqual(core.normalize_subscription_cross_copy_mode("off"), "off")
        self.assertEqual(core.normalize_subscription_cross_copy_mode("fallback"), "fallback")
        self.assertEqual(core.normalize_subscription_cross_copy_mode("prefer"), "prefer")
        self.assertEqual(core.normalize_subscription_cross_copy_mode(""), "off")
        self.assertEqual(core.normalize_subscription_cross_copy_mode("junk"), "off")

    def test_sources_normalization(self):
        self.assertEqual(core.normalize_subscription_cross_copy_sources(["quark"]), ["quark"])
        self.assertEqual(core.normalize_subscription_cross_copy_sources(["quark", "quark"]), ["quark"])
        self.assertEqual(core.normalize_subscription_cross_copy_sources(["aliyun"]), [])
        self.assertEqual(
            core.normalize_subscription_cross_copy_sources(["quark"], task_provider="quark"),
            [],
        )

    def test_task_normalization_keeps_fields(self):
        task = core.normalize_subscription_task(
            {
                "name": "x",
                "provider": "115",
                "cross_copy_mode": "fallback",
                "cross_copy_sources": ["quark"],
            }
        )
        self.assertEqual(task["cross_copy_mode"], "fallback")
        self.assertEqual(task["cross_copy_sources"], ["quark"])

    def test_task_normalization_defaults_off(self):
        task = core.normalize_subscription_task({"name": "x", "provider": "115"})
        self.assertEqual(task["cross_copy_mode"], "off")
        self.assertEqual(task["cross_copy_sources"], [])


if __name__ == "__main__":
    unittest.main()
