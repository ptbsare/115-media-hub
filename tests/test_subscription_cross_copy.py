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
    """夸克 provider 的最小假实现（分享转存 + 目录操作）。

    中转目录用 dirs 模拟成一份会变的真实目录：转存成功后条目真的会
    出现在目录里。这样「转存前已有的不重复注册」才测得出来（如果 list_entries
    永远返回同一份固定列表，前快照和后快照一样，什么都注册不出来）。
    """

    name = "quark"
    label = "夸克网盘"
    supports_upload = True
    supports_delete = True

    def __init__(self, fail_save=False, empty_share=False, existing=None, staged=None, share_entries=None):
        self.fail_save = fail_save
        self.empty_share = empty_share
        self.default_share = (
            list(share_entries)
            if share_entries is not None
            else [{"id": "share-fid-1", "name": "movie.mkv", "size": 2048, "is_dir": False}]
        )
        self.shares = {}          # 按分享链接指定内容：{url: [条目]}
        self.existing = list(existing or [])   # 每个中转目录的初始内容
        self.dirs = {}            # {中转目录 id: [条目]}
        self.staged = staged      # 显式指定「转存后目录里出现的条目」；None 表示自动生成
        self.last_share_url = ""
        self.received = []        # 每次转存提交的条目
        self.calls = []

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        self.calls.append(("resolve_share", share_url))
        self.last_share_url = share_url
        return {"url": share_url, "pwd_id": "pwd-1", "stoken": "st-1", "receive_code": receive_code}

    def list_share_entries(self, cookie, share_payload, cid="0", offset=0, limit=200):
        self.calls.append(("list_share",))
        if self.empty_share:
            # 与真实 provider 一致：返回 dict（见 base.list_share_entries 契约）
            return {"entries": [], "summary": {"folder_count": 0, "file_count": 0}}
        url = str(share_payload.get("url", "") or "")
        entries = list(self.shares.get(url, self.default_share))
        return {
            "entries": entries,
            "summary": {
                "folder_count": sum(1 for e in entries if e.get("is_dir")),
                "file_count": sum(1 for e in entries if not e.get("is_dir")),
            },
        }

    def ensure_folder_id_by_path(self, cookie, relative_path):
        self.calls.append(("ensure_folder", relative_path))
        folder_id = f"staging::{relative_path}"
        self.dirs.setdefault(folder_id, list(self.existing))
        return folder_id

    def prepare_share_receive(self, cookie, share_payload, cid="0"):
        self.calls.append(("prepare",))
        return {**share_payload, "target_cid": str(cid or "0")}

    def submit_share_receive(self, cookie, receive_payload, files):
        self.calls.append(("submit",))
        if self.fail_save:
            raise RuntimeError("夸克转存被风控")
        self.received.append(list(files))
        bucket = self.dirs.setdefault(str(receive_payload.get("target_cid", "") or ""), [])
        if self.staged is not None:
            bucket.extend(self.staged)
        else:
            # 模拟夸克：转存成功后目录里多出一份同名副本，但 id 是新的
            bucket.extend(
                [
                    {
                        "id": f"fid-{item.get('id', '')}",
                        "name": item.get("name", ""),
                        "size": int(item.get("size", 0) or 0),
                        "is_dir": bool(item.get("is_dir")),
                    }
                    for item in files
                ]
            )
        return {"response": {"code": 0}, "selection": {}}

    def list_entries(self, cookie, cid="0"):
        self.calls.append(("list_entries",))
        return list(self.dirs.setdefault(str(cid or ""), []))


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
        self._reset_scheduler_state()

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
        self._reset_scheduler_state()
        db.DB_PATH = self.original_db_path
        db._DB_ENSURED = self.original_db_ensured
        shutil.rmtree(self.tmp, ignore_errors=True)

    @staticmethod
    def _reset_scheduler_state():
        """清掉调度器的内存队列/在跑集合。

        任务表每个用例都会清空，id 从头开始；调度器的内存状态却跨用例留着，
        不清的话上个用例排过的 id 会让这个用例的 submit 被当成重复而跳过。
        """
        for name in ("_queued", "_active", "_running", "_cancel_requested"):
            state = getattr(ccj, name, None)
            if hasattr(state, "clear"):
                try:
                    state.clear()
                except Exception:  # noqa: BLE001 - 状态结构变了也不该拖垮用例
                    pass

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

    def _register(self, **kwargs):
        kwargs.setdefault("task", self._task())
        kwargs.setdefault("cfg", self.cfg)
        return sc.register_cross_copy_from_candidates(**kwargs)


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
        result = self._register(candidates=[self._candidate()])
        self.assertEqual(len(result["registered"]), 1)
        job_id = result["registered"][0]
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["source_provider"], "quark")
        self.assertEqual(job["target_provider"], "115")
        self.assertEqual(job["source_file_id"], "fid-share-fid-1")
        self.assertEqual(job["origin"], "subscription")
        self.assertEqual(job["origin_ref"], "测试剧集")
        self.assertEqual(job["source_mode"], "save_first")
        self.assertEqual(job["filename"], "movie.mkv")
        self.assertEqual(job["file_size"], 2048)
        self.assertEqual(job["source_is_dir"], 0)
        self.assertEqual(job["source_path"], "/云下载/跨盘中转/测试剧集/movie.mkv")
        # 分享转存全链路被调用：转存前列目录 → 转存 → 转存后再列目录
        call_names = [c[0] for c in self.quark.calls]
        self.assertEqual(
            call_names,
            ["resolve_share", "list_share", "ensure_folder", "list_entries", "prepare", "submit", "list_entries"],
        )
        # 中转目录带任务子目录
        ensure_call = next(c for c in self.quark.calls if c[0] == "ensure_folder")
        self.assertIn("云下载/跨盘中转/测试剧集", ensure_call[1])
        ccj.submit_background.assert_called_once()
        submit_args = ccj.submit_background.call_args[0]
        self.assertEqual(submit_args[0], ccj.run_cross_copy_job)
        self.assertEqual(submit_args[1], job_id)

    def test_dedupe_active_job(self):
        first = self._register(candidates=[self._candidate()])
        self.assertEqual(len(first["registered"]), 1)
        # 第二轮：同一个分享的内容已经躺在中转目录里，不再重复转存、不再注册
        second = self._register(candidates=[self._candidate()])
        self.assertEqual(second["registered"], [])
        self.assertTrue(any("上次已经转存过" in s for s in second["skipped"]))
        # 第二次连转存都不该发起
        self.assertEqual(len(self.quark.received), 1)

    def test_dedupe_only_for_same_task(self):
        self._register(candidates=[self._candidate()])
        result = sc.register_cross_copy_from_candidates(
            task=self._task(name="另一个任务", title="另一个任务"),
            cfg=self.cfg,
            candidates=[self._candidate()],
        )
        self.assertEqual(len(result["registered"]), 1)

    def test_save_failure_skips(self):
        self.quark.fail_save = True
        result = self._register(candidates=[self._candidate()])
        self.assertEqual(result["registered"], [])
        self.assertTrue(any("转存失败" in s for s in result["skipped"]))
        self.assertTrue(any("转存失败" in msg for msg, _ in result["logs"]))

    def test_empty_share_skips(self):
        self.quark.empty_share = True
        result = self._register(candidates=[self._candidate()])
        self.assertEqual(result["registered"], [])
        self.assertTrue(any("转存失败" in s for s in result["skipped"]))

    def test_staged_name_differs_from_share_entry(self):
        # 落盘时名字和分享里的不一样（分享重命名）：以中转目录里实际的名字/大小为准
        self.quark.staged = [{"id": "fid-other", "name": "renamed.mkv", "size": 999, "is_dir": False}]
        result = self._register(candidates=[self._candidate()])
        self.assertEqual(len(result["registered"]), 1)
        job = ccj.get_cross_copy_job(result["registered"][0])
        self.assertEqual(job["source_file_id"], "fid-other")
        self.assertEqual(job["filename"], "renamed.mkv")
        self.assertEqual(job["file_size"], 999)

    def test_max_registered_per_run(self):
        candidates = [
            self._candidate(url=f"https://pan.quark.cn/s/share{i}", score=90 - i)
            for i in range(5)
        ]
        # 每个分享一个不同的文件，避免「内容重复」干扰计数
        for i in range(5):
            self.quark.shares[f"https://pan.quark.cn/s/share{i}"] = [
                {"id": f"share-{i}", "name": f"ep{i}.mkv", "size": 100 + i, "is_dir": False}
            ]
        result = self._register(candidates=candidates)
        # 上限数的是「分享」，不是任务
        self.assertEqual(len(result["registered"]), sc.MAX_REGISTERED_PER_RUN)

    def test_duplicate_urls_deduped(self):
        candidates = [self._candidate(), self._candidate()]
        result = self._register(candidates=candidates)
        self.assertEqual(len(result["registered"]), 1)


class TestMultiEntryShare(SubscriptionCrossCopyTestBase):
    """一个分享里有多个条目：文件夹、散文件、混合。"""

    URL = "https://pan.quark.cn/s/abc123"

    def test_folder_share_registers_one_folder_job(self):
        # 分享是一个 12 集的文件夹
        self.quark.default_share = [{"id": "share-folder-1", "name": "剧集 12 集", "size": 0, "is_dir": True}]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 1, result)
        job = ccj.get_cross_copy_job(result["registered"][0])
        self.assertEqual(job["source_is_dir"], 1)
        self.assertEqual(job["source_file_id"], "fid-share-folder-1")
        self.assertEqual(job["filename"], "剧集 12 集")
        self.assertEqual(job["source_path"], "/云下载/跨盘中转/测试剧集/剧集 12 集")
        self.assertTrue(any("1 个文件夹" in msg for msg, _ in result["logs"]))

        # 转存时把整个文件夹作为一项提交（不是只取第一个）
        self.assertEqual(len(self.quark.received[0]), 1)
        self.assertTrue(self.quark.received[0][0]["is_dir"])

    def test_twelve_loose_files_register_twelve_jobs(self):
        self.quark.default_share = [
            {"id": f"share-ep{i}", "name": f"第{i:02d}集.mkv", "size": 1000 + i, "is_dir": False}
            for i in range(1, 13)
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 12, result)
        names = sorted(ccj.get_cross_copy_job(job_id)["filename"] for job_id in result["registered"])
        self.assertEqual(names, sorted(f"第{i:02d}集.mkv" for i in range(1, 13)))
        # 每个任务都是单文件任务
        for job_id in result["registered"]:
            self.assertEqual(ccj.get_cross_copy_job(job_id)["source_is_dir"], 0)

    def test_three_loose_files_register_three_jobs(self):
        self.quark.default_share = [
            {"id": "share-a", "name": "a.mkv", "size": 100, "is_dir": False},
            {"id": "share-b", "name": "b.mkv", "size": 200, "is_dir": False},
            {"id": "share-c", "name": "c.mkv", "size": 300, "is_dir": False},
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 3, result)
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["a.mkv", "b.mkv", "c.mkv"])
        self.assertEqual([job["file_size"] for job in jobs], [100, 200, 300])
        self.assertEqual({job["source_share_url"] for job in jobs}, {self.URL})
        self.assertEqual({job["target_savepath"] for job in jobs}, {"/影视库/测试剧集"})
        self.assertEqual(len({job["source_file_id"] for job in jobs}), 3)

    def test_mixed_folder_and_files(self):
        self.quark.default_share = [
            {"id": "share-folder", "name": "正片", "size": 0, "is_dir": True},
            {"id": "share-file-1", "name": "花絮.mkv", "size": 500, "is_dir": False},
            {"id": "share-file-2", "name": "字幕.ass", "size": 30, "is_dir": False},
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 3, result)
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        dir_jobs = [job for job in jobs if job["source_is_dir"] == 1]
        file_jobs = [job for job in jobs if job["source_is_dir"] == 0]
        self.assertEqual([job["filename"] for job in dir_jobs], ["正片"])
        self.assertEqual(sorted(job["filename"] for job in file_jobs), ["字幕.ass", "花絮.mkv"])
        self.assertTrue(any("1 个文件夹、2 个文件" in msg for msg, _ in result["logs"]))

    def test_history_files_in_staging_not_registered_again(self):
        # 中转目录里有前几轮转存的文件（同名同大小），这轮只补新的 2 个
        self.quark.existing = [
            {"id": "old-1", "name": "第01集.mkv", "size": 1000, "is_dir": False},
            {"id": "old-2", "name": "第02集.mkv", "size": 1001, "is_dir": False},
        ]
        self.quark.default_share = [
            {"id": "share-ep1", "name": "第01集.mkv", "size": 1000, "is_dir": False},
            {"id": "share-ep2", "name": "第02集.mkv", "size": 1001, "is_dir": False},
            {"id": "share-ep3", "name": "第03集.mkv", "size": 1002, "is_dir": False},
            {"id": "share-ep4", "name": "第04集.mkv", "size": 1003, "is_dir": False},
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 2, result)
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(sorted(job["filename"] for job in jobs), ["第03集.mkv", "第04集.mkv"])
        skipped_text = " ".join(result["skipped"])
        self.assertIn("中转目录已有同名文件，跳过：第01集.mkv", skipped_text)
        self.assertIn("中转目录已有同名文件，跳过：第02集.mkv", skipped_text)
        # 只把缺的两个发给夸克转存，老文件不会再多存一份出来
        self.assertEqual([item["name"] for item in self.quark.received[0]], ["第03集.mkv", "第04集.mkv"])

    def test_all_entries_already_staged(self):
        self.quark.existing = [{"id": "old-1", "name": "movie.mkv", "size": 2048, "is_dir": False}]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(result["registered"], [])
        self.assertTrue(any("上次已经转存过" in s for s in result["skipped"]))
        self.assertEqual(self.quark.received, [])

    def test_max_files_per_share_via_env(self):
        self.quark.default_share = [
            {"id": f"share-ep{i}", "name": f"第{i:02d}集.mkv", "size": 1000 + i, "is_dir": False}
            for i in range(1, 6)
        ]
        with patch.dict(os.environ, {"CROSS_COPY_SUBSCRIPTION_MAX_FILES": "2"}):
            result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 2, result)
        skipped_text = " ".join(result["skipped"])
        self.assertIn("这轮只补前 2 个", skipped_text)
        self.assertIn("其余 3 个下次再补", skipped_text)

    def test_no_dir_flag_falls_back_to_files(self):
        # 列表项里没有 is_dir 标记：全部按文件注册，并在报告里说明
        self.quark.staged = [
            {"id": "fid-1", "name": "a.mkv", "size": 100},
            {"id": "fid-2", "name": "b.mkv", "size": 200},
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 2, result)
        for job_id in result["registered"]:
            self.assertEqual(ccj.get_cross_copy_job(job_id)["source_is_dir"], 0)
        self.assertTrue(any("全部按文件注册" in s for s in result["skipped"]))


class TestDedupeGranularity(SubscriptionCrossCopyTestBase):
    def test_same_share_multiple_jobs_not_killed(self):
        """同一分享注册多个任务时，去重不能把第二个及以后的误杀。"""
        self.quark.default_share = [
            {"id": "share-a", "name": "a.mkv", "size": 100, "is_dir": False},
            {"id": "share-b", "name": "b.mkv", "size": 200, "is_dir": False},
            {"id": "share-c", "name": "c.mkv", "size": 300, "is_dir": False},
        ]
        result = self._register(candidates=[self._candidate()])
        self.assertEqual(len(result["registered"]), 3, result)

        # 旧调用方式（不带 source_file_id）行为不变：同一分享能查到任务
        active = sc.find_active_cross_copy_job("测试剧集", TestMultiEntryShare.URL)
        self.assertIsNotNone(active)
        # 带条目 id 时精确命中
        matched = sc.find_active_cross_copy_job("测试剧集", TestMultiEntryShare.URL, "fid-share-b")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["filename"], "b.mkv")
        self.assertIsNone(sc.find_active_cross_copy_job("测试剧集", TestMultiEntryShare.URL, "fid-nope"))

    def test_entry_already_queued_is_skipped(self):
        """某个条目已在补源队列里 → 只跳过它，同一分享的其他条目照常注册。"""
        existing_job = ccj.create_cross_copy_job(
            title="测试剧集",
            source_provider="quark",
            target_provider="115",
            source_file_id="fid-share-a",
            source_share_url=TestMultiEntryShare.URL,
            origin="subscription",
            origin_ref="测试剧集",
        )
        self.quark.default_share = [
            {"id": "share-a", "name": "a.mkv", "size": 100, "is_dir": False},
            {"id": "share-b", "name": "b.mkv", "size": 200, "is_dir": False},
        ]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 1, result)
        self.assertNotIn(existing_job, result["registered"])
        job = ccj.get_cross_copy_job(result["registered"][0])
        self.assertEqual(job["filename"], "b.mkv")
        self.assertTrue(any("已在补源队列中，跳过：a.mkv" in s for s in result["skipped"]))


class TestStagingResultShape(SubscriptionCrossCopyTestBase):
    def test_stage_returns_folders_files_skipped(self):
        self.quark.existing = [{"id": "old-1", "name": "old.mkv", "size": 10, "is_dir": False}]
        self.quark.default_share = [
            {"id": "share-folder", "name": "正片", "size": 0, "is_dir": True},
            {"id": "share-file", "name": "花絮.mkv", "size": 500, "is_dir": False},
            {"id": "share-old", "name": "old.mkv", "size": 10, "is_dir": False},
        ]
        staged = sc._stage_share_to_quark(
            self.quark,
            "cookie-quark",
            TestMultiEntryShare.URL,
            "",
            "云下载/跨盘中转/测试剧集",
        )
        self.assertIn("云下载/跨盘中转/测试剧集", staged["folder_id"])
        self.assertEqual([item["name"] for item in staged["folders"]], ["正片"])
        self.assertEqual([item["name"] for item in staged["files"]], ["花絮.mkv"])
        self.assertEqual(staged["folders"][0]["id"], "fid-share-folder")
        self.assertEqual(staged["files"][0]["size"], 500)
        self.assertEqual(staged["skipped"], ["中转目录已有同名文件，跳过：old.mkv"])
        self.assertFalse(staged["already_staged"])


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
