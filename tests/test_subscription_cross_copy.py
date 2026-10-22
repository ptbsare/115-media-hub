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
        self.share_dirs = {}      # 分享内的子目录：{父条目 id: [条目]}，模拟真实树
        self.existing = list(existing or [])   # 每个中转目录的初始内容
        self.dirs = {}            # {中转目录 id: [条目]}
        self.staged = staged      # 显式指定「转存后目录里出现的条目」；None 表示自动生成
        self.last_share_url = ""
        self.received = []        # 每次转存提交的条目
        self.calls = []
        self.deleted = set()      # delete_entries 真删掉的 id

    def resolve_share_payload(self, cookie, share_url, raw_text="", receive_code=""):
        self.calls.append(("resolve_share", share_url))
        self.last_share_url = share_url
        return {"url": share_url, "pwd_id": "pwd-1", "stoken": "st-1", "receive_code": receive_code}

    def list_share_entries(self, cookie, share_payload, cid="0", offset=0, limit=200):
        self.calls.append(("list_share", str(cid)))
        if self.empty_share:
            # 与真实 provider 一致：返回 dict（见 base.list_share_entries 契约）
            return {"entries": [], "summary": {"folder_count": 0, "file_count": 0}}
        normalized_cid = str(cid or "0")
        if normalized_cid != "0" and normalized_cid in self.share_dirs:
            entries = list(self.share_dirs[normalized_cid])
        else:
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

    def resolve_folder_id_by_path(self, cookie, relative_path):
        """只解析不创建（清理逻辑靠它定位中转目录，建目录的副作用不能有）。"""
        self.calls.append(("resolve_folder", relative_path))
        folder_id = f"staging::{relative_path}"
        return folder_id if folder_id in self.dirs else ""

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        self.calls.append(("delete", list(entry_ids)))
        targets = {str(item or "").strip() for item in (entry_ids or [])}
        self.deleted.update(targets)
        for bucket in self.dirs.values():
            bucket[:] = [entry for entry in bucket if str(entry.get("id", "")) not in targets]
        return {"ids": sorted(targets)}


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
            "year": "2024",
            "media_type": "movie",
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

    def _stage_season_folder(self, entries, folder_name="某剧 全季"):
        """模拟「分享里只有一个整季文件夹」转存进中转目录。"""
        folder = {"id": "share-folder-1", "name": folder_name, "size": 0, "is_dir": True}
        self.quark.default_share = [folder]
        self.quark.existing = []
        # 中转目录里「转存」出来的文件夹，id 由 Fake 在 submit 时生成
        self.quark.dirs["fid-share-folder-1"] = list(entries)


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
        self.assertEqual(
            {job["target_savepath"] for job in jobs},
            {"影视库/测试剧集/测试剧集 2024"},
        )
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

    def test_all_entries_already_staged_registers_missing_job(self):
        # 中转目录里已有这个条目，但库里没有任何补源任务（上一轮转存成功、
        # 注册/复制没走完），这轮要把它补注册上，不能当成「已经补过」跳过。
        self.quark.existing = [{"id": "old-1", "name": "movie.mkv", "size": 2048, "is_dir": False}]
        result = self._register(candidates=[self._candidate()])

        self.assertEqual(len(result["registered"]), 1, result)
        job = ccj.get_cross_copy_job(result["registered"][0])
        self.assertEqual(job["filename"], "movie.mkv")
        self.assertEqual(job["source_file_id"], "old-1")
        self.assertIn("上次已经转存过", " ".join(result["skipped"]))
        # 没有再往夸克发一次转存
        self.assertEqual(self.quark.received, [])

    def test_all_entries_already_staged_skips_completed_job(self):
        # 上次已经复制完成，中转文件还在：不能再注册一份，否则目标目录会多一份
        self.quark.existing = [{"id": "old-1", "name": "movie.mkv", "size": 2048, "is_dir": False}]
        done_id = ccj.create_cross_copy_job(
            title="测试剧集",
            source_provider="quark",
            target_provider="115",
            source_file_id="old-1",
            target_savepath="/影视库/测试剧集",
            filename="movie.mkv",
            file_size=2048,
            source_mode="save_first",
            source_share_url=self.URL,
            origin="subscription",
            origin_ref="测试剧集",
        )
        ccj.update_cross_copy_job(done_id, status="completed")

        result = self._register(candidates=[self._candidate()])

        self.assertEqual(result["registered"], [])
        self.assertIn("以前已补过，跳过：movie.mkv", " ".join(result["skipped"]))
        self.assertEqual(self.quark.received, [])

    def test_all_entries_already_staged_skips_skipped_job(self):
        # 上次结果是 skipped（目标已有同名文件），中转文件还在：不能再注册，
        # 否则每轮都重复注册一条、再 skipped 一次，日志持续膨胀、中转副本一直占配额
        self.quark.existing = [{"id": "old-1", "name": "movie.mkv", "size": 2048, "is_dir": False}]
        skipped_id = ccj.create_cross_copy_job(
            title="测试剧集",
            source_provider="quark",
            target_provider="115",
            source_file_id="old-1",
            target_savepath="/影视库/测试剧集",
            filename="movie.mkv",
            file_size=2048,
            source_mode="save_first",
            source_share_url=self.URL,
            origin="subscription",
            origin_ref="测试剧集",
        )
        ccj.update_cross_copy_job(skipped_id, status="skipped")

        result = self._register(candidates=[self._candidate()])

        self.assertEqual(result["registered"], [])
        self.assertIn("以前已补过，跳过：movie.mkv", " ".join(result["skipped"]))
        self.assertEqual(self.quark.received, [])


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


class TestTargetSavepath(unittest.TestCase):
    """补源落点必须和订阅导入一致：电影进「片名 年份」，剧集进 Season 目录。"""

    @staticmethod
    def _resolve(task, candidate=None):
        return sc._cross_copy_target_savepath(task, candidate or {})

    def test_movie_goes_into_title_year_folder(self):
        task = {"savepath": "/影视库/电影", "media_type": "movie", "title": "让子弹飞", "year": "2010"}
        self.assertEqual(self._resolve(task), "影视库/电影/让子弹飞 2010")

    def test_movie_without_year(self):
        task = {"savepath": "/影视库/电影", "media_type": "movie", "title": "让子弹飞"}
        self.assertEqual(self._resolve(task), "影视库/电影/让子弹飞")

    def test_movie_without_title_falls_back(self):
        task = {"savepath": "/影视库/电影", "media_type": "movie"}
        self.assertEqual(self._resolve(task), "影视库/电影/未命名电影")

    def test_movie_with_illegal_chars_sanitized(self):
        task = {"savepath": "/影视库/电影", "media_type": "movie", "title": "A/B:C", "year": "2024"}
        self.assertEqual(self._resolve(task), "影视库/电影/A B C 2024")

    def test_tv_uses_task_season(self):
        task = {"savepath": "/影视库/剧集", "media_type": "tv", "title": "某剧", "season": 2}
        self.assertEqual(self._resolve(task), "影视库/剧集/Season 02")

    def test_tv_prefers_candidate_season(self):
        task = {"savepath": "/影视库/剧集", "media_type": "tv", "title": "某剧", "season": 1}
        self.assertEqual(self._resolve(task, {"season": 3}), "影视库/剧集/Season 03")

    def test_tv_season_mismatch_deferred_uses_task_season(self):
        task = {"savepath": "/影视库/剧集", "media_type": "tv", "title": "某剧", "season": 3}
        candidate = {"season": 2, "season_mismatch_deferred": True}
        self.assertEqual(self._resolve(task, candidate), "影视库/剧集/Season 03")

    def test_tv_does_not_double_season_folder(self):
        # savepath 本身已经指到 Season 目录时不能再套一层
        task = {"savepath": "/影视库/剧集/Season 01", "media_type": "tv", "title": "某剧", "season": 1}
        self.assertEqual(self._resolve(task), "影视库/剧集/Season 01")

    def test_empty_savepath_keeps_title_folder(self):
        task = {"savepath": "", "media_type": "movie", "title": "让子弹飞", "year": "2010"}
        self.assertEqual(self._resolve(task), "让子弹飞 2010")

    def test_registered_job_target_matches_import_for_tv(self):
        """端到端：剧集补源任务的目标目录带 Season 层。"""
        base = SubscriptionCrossCopyTestBase()
        base.setUp()
        try:
            task = base._task(media_type="tv", season=1, savepath="/影视库/剧集")
            result = sc.register_cross_copy_from_candidates(
                task=task,
                cfg=self._cfg(base),
                candidates=[base._candidate()],
            )
            self.assertEqual(len(result["registered"]), 1, result)
            job = ccj.get_cross_copy_job(result["registered"][0])
            self.assertEqual(job["target_savepath"], "影视库/剧集/Season 01")
        finally:
            base.tearDown()

    @staticmethod
    def _cfg(base):
        return base.cfg


class TestSourceGating(unittest.TestCase):
    """补源只接通了夸克作源，其余源必须显式识别为「跳过」而不是静默忽略。"""

    def test_quark_is_usable(self):
        self.assertEqual(sc.split_cross_copy_sources(["quark"]), (["quark"], []))

    def test_other_providers_are_skipped(self):
        usable, skipped = sc.split_cross_copy_sources(["aliyun"])
        self.assertEqual(usable, [])
        self.assertEqual(skipped, ["aliyun"])

        usable, skipped = sc.split_cross_copy_sources(["115"])
        self.assertEqual(usable, [])
        self.assertEqual(skipped, ["115"])

    def test_mixed_sources_split(self):
        self.assertEqual(
            sc.split_cross_copy_sources(["aliyun", "quark", "115"]),
            (["quark"], ["aliyun", "115"]),
        )

    def test_duplicates_and_blanks_removed(self):
        self.assertEqual(
            sc.split_cross_copy_sources(["quark", "quark", " ", "aliyun", "aliyun"]),
            (["quark"], ["aliyun"]),
        )

    def test_empty_inputs(self):
        for value in ([], None, ""):
            self.assertEqual(sc.split_cross_copy_sources(value), ([], []))

    def test_set_input(self):
        self.assertEqual(sc.split_cross_copy_sources({"quark"}), (["quark"], []))

    def test_gating_matches_core_whitelist(self):
        # 判定名单必须就是任务归一化用的那份，避免两处漂移
        for name in core.SUBSCRIPTION_CROSS_COPY_SOURCE_PROVIDERS:
            self.assertEqual(sc.split_cross_copy_sources([name])[0], [name])


class TestShareUrlSanity(unittest.TestCase):
    """TG 长文本会把下一条 URL 粘到上一条尾部，这种链接转存必然失败。"""

    def test_normal_quark_url_ok(self):
        self.assertTrue(sc._is_plausible_share_url("https://pan.quark.cn/s/c0335c85e399"))

    def test_polluted_url_rejected(self):
        # 实测踩到的形态：末尾被拼上了下一条消息的 https
        self.assertFalse(sc._is_plausible_share_url("https://pan.quark.cn/s/b1dfae35a04bhttps"))
        self.assertFalse(
            sc._is_plausible_share_url("https://pan.quark.cn/s/abc https://pan.quark.cn/s/def")
        )

    def test_blank_and_non_http_rejected(self):
        for value in ("", None, "pan.quark.cn/s/abc", "magnet:?xt=urn:btih:abc"):
            self.assertFalse(sc._is_plausible_share_url(value))

    def test_pick_skips_polluted_and_keeps_good(self):
        candidates = [
            {"item": {"link_type": "quark", "link_url": "https://pan.quark.cn/s/badhttps"}},
            {"item": {"link_type": "quark", "link_url": "https://pan.quark.cn/s/good1"}},
        ]
        picked = sc._pick_share_candidates(candidates, 5)
        self.assertEqual([sc._extract_share_url(c) for c in picked], ["https://pan.quark.cn/s/good1"])


class TestCrossCopyTitleHit(unittest.TestCase):
    """补源候选池是全频道的，排序必须让「标题命中本剧」的压过纯高分别的剧。"""

    def test_title_hit(self):
        from app.services import subscription as subscription_module

        task = {"title": "光阴之外", "media_type": "tv"}
        self.assertEqual(
            subscription_module._cross_copy_title_hit(
                task, {"item": {"title": "名称：光阴之外（2025）4K HQ S01E01 - E44"}}
            ),
            1,
        )

    def test_other_show_not_hit(self):
        from app.services import subscription as subscription_module

        task = {"title": "光阴之外", "media_type": "tv"}
        self.assertEqual(
            subscription_module._cross_copy_title_hit(
                task, {"item": {"title": "#凡人修仙传 #年番4 #幕兰之战 (2026) #4K 194集"}}
            ),
            0,
        )

    def test_raw_text_is_ignored(self):
        # 日报贴正文里堆着一串剧名，拿 raw_text 判断会让所有日报都算命中
        from app.services import subscription as subscription_module

        task = {"title": "光阴之外"}
        self.assertEqual(
            subscription_module._cross_copy_title_hit(
                task, {"item": {"title": "国漫更新目录", "raw_text": "光阴之外 更新至44集"}}
            ),
            0,
        )

    def test_aliases_are_considered(self):
        from app.services import subscription as subscription_module

        task = {"title": "光阴之外", "tmdb_original_title": "Sob o Olhar do Tempo"}
        self.assertEqual(
            subscription_module._cross_copy_title_hit(
                task, {"item": {"title": "Sob o Olhar do Tempo (2025) 4K"}}
            ),
            1,
        )


class TestMissingEpisodeSelection(SubscriptionCrossCopyTestBase):
    """整季文件夹分享 + 本盘已有进度：只补缺的集，且拍平到目标目录。"""

    def _tv_folder_task(self, **overrides):
        return self._task(media_type="tv", season=1, **overrides)

    def test_only_missing_episodes_registered_and_flattened(self):
        self._stage_season_folder(
            [
                {"id": f"f{i}", "name": f"S01E{i:02d}.mkv", "size": 100 + i, "is_dir": False}
                for i in range(1, 9)
            ]
        )
        result = self._register(task=self._tv_folder_task(), candidates=[self._candidate()], last_episode=5)

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(sorted(job["filename"] for job in jobs), ["S01E06.mkv", "S01E07.mkv", "S01E08.mkv"])
        # 每个文件单独立任务，不再是「一个整目录任务」，这样才不会多一层分享文件夹名
        for job in jobs:
            self.assertFalse(bool(job.get("source_is_dir")))
            self.assertEqual(job["target_savepath"], "影视库/测试剧集/Season 01")
            self.assertIn("云下载/跨盘中转/测试剧集/某剧 全季/", job["source_path"])
            self.assertIn(job["filename"], job["source_path"])

    def test_no_last_episode_keeps_whole_folder(self):
        # 本盘一集都没有时仍整目录复制（一个任务搞定，比逐集注册快）
        self._stage_season_folder(
            [{"id": "f1", "name": "S01E01.mkv", "size": 100, "is_dir": False}]
        )
        result = self._register(task=self._tv_folder_task(), candidates=[self._candidate()], last_episode=0)

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(len(jobs), 1)
        self.assertTrue(bool(jobs[0].get("source_is_dir")))

    def test_duplicate_episode_across_shares_registered_once(self):
        # 两个「全季」包都含 E08：先到先得，不重复注册
        self._stage_season_folder(
            [
                {"id": "a7", "name": "S01E07.mkv", "size": 107, "is_dir": False},
                {"id": "a8", "name": "S01E08.mkv", "size": 108, "is_dir": False},
            ]
        )
        result = self._register(
            task=self._tv_folder_task(),
            candidates=[
                self._candidate(url="https://pan.quark.cn/s/aaa"),
                self._candidate(url="https://pan.quark.cn/s/bbb"),
            ],
            last_episode=6,
        )

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(sorted(job["filename"] for job in jobs), ["S01E07.mkv", "S01E08.mkv"])
        self.assertIn("本轮其它分享已补过第 7 集", " ".join(result["skipped"]))

    def test_files_without_episode_are_not_copied(self):
        self._stage_season_folder(
            [
                {"id": "n1", "name": "预告片.mp4", "size": 10, "is_dir": False},
                {"id": "n2", "name": "S01E09.mkv", "size": 109, "is_dir": False},
            ]
        )
        result = self._register(task=self._tv_folder_task(), candidates=[self._candidate()], last_episode=8)

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E09.mkv"])
        self.assertIn("认不出集数，没复制：预告片.mp4", " ".join(result["skipped"]))

    def test_folder_with_nothing_new_is_skipped(self):
        self._stage_season_folder(
            [{"id": "old", "name": "S01E05.mkv", "size": 105, "is_dir": False}]
        )
        result = self._register(task=self._tv_folder_task(), candidates=[self._candidate()], last_episode=5)

        self.assertEqual(result["registered"], [])
        self.assertIn("里没有本盘缺的集", " ".join(result["skipped"]))


class TestScatteredFileEpisodeFilter(SubscriptionCrossCopyTestBase):
    """分享里直接躺着的散文件也要按缺集筛。

    以前只筛「整季文件夹」，散文件无脑全注册：本盘已有的集会被再下一遍。
    """

    def _tv_task(self, **overrides):
        return self._task(media_type="tv", season=1, **overrides)

    def test_existing_episode_file_skipped_but_missing_one_kept(self):
        self.quark.default_share = [
            {"id": "a42", "name": "S01E42.mkv", "size": 142, "is_dir": False},
            {"id": "a43", "name": "S01E43.mkv", "size": 143, "is_dir": False},
        ]
        result = self._register(task=self._tv_task(), candidates=[self._candidate()], last_episode=42)

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E43.mkv"])
        self.assertEqual(jobs[0]["target_savepath"], "影视库/测试剧集/Season 01")

    def test_all_episodes_present_means_nothing_registered(self):
        self.quark.default_share = [
            {"id": "a43", "name": "S01E43.mkv", "size": 143, "is_dir": False},
            {"id": "a44", "name": "S01E44.mkv", "size": 144, "is_dir": False},
        ]
        result = self._register(task=self._tv_task(), candidates=[self._candidate()], last_episode=44)

        self.assertEqual(result["registered"], [])

    def test_movie_still_registers_everything(self):
        # 电影没有集数概念，不该被集数过滤误伤
        self.quark.default_share = [
            {"id": "m1", "name": "movie.mkv", "size": 2048, "is_dir": False},
        ]
        result = self._register(task=self._task(media_type="movie"), candidates=[self._candidate()], last_episode=9)

        self.assertEqual(len(result["registered"]), 1)

    def test_polluted_name_is_not_copied(self):
        # 认不出集数的脏名（全角「4！2」）不复制，避免往季目录里塞杂物
        self.quark.default_share = [
            {"id": "bad", "name": "4！2.mp4", "size": 42, "is_dir": False},
            {"id": "ok", "name": "S01E43.mkv", "size": 143, "is_dir": False},
        ]
        result = self._register(task=self._tv_task(), candidates=[self._candidate()], last_episode=42)

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E43.mkv"])
        self.assertIn("认不出集数，没复制：4！2.mp4", " ".join(result["skipped"]))


class TestExistingEpisodeSetSelection(SubscriptionCrossCopyTestBase):
    """传了目标目录已有集集合时，按集合精确判缺——能识别中间缺集。"""

    def _tv_task(self, **overrides):
        return self._task(media_type="tv", season=1, **overrides)

    def test_middle_gap_is_detected(self):
        # 本盘 E1~E41 齐全但没有 E30，last_episode=41。
        # 只看最高集会认为 E30 早就有了；按集合判才知道要补。
        existing = set(range(1, 42)) - {30}
        self.quark.default_share = [
            {"id": "a30", "name": "S01E30.mkv", "size": 130, "is_dir": False},
            {"id": "a40", "name": "S01E40.mkv", "size": 140, "is_dir": False},
        ]
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=41,
            existing_episodes=existing,
        )

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E30.mkv"])

    def test_empty_set_means_copy_everything(self):
        # 空集合 = 本盘一集都没有（不是「不确定」），该全补
        self.quark.default_share = [
            {"id": "a1", "name": "S01E01.mkv", "size": 101, "is_dir": False},
        ]
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=0,
            existing_episodes=set(),
        )

        self.assertEqual(len(result["registered"]), 1)

    def test_folder_expansion_uses_the_set(self):
        self._stage_season_folder(
            [
                {"id": "f30", "name": "S01E30.mkv", "size": 130, "is_dir": False},
                {"id": "f41", "name": "S01E41.mkv", "size": 141, "is_dir": False},
            ]
        )
        existing = set(range(1, 42)) - {30}
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=41,
            existing_episodes=existing,
        )

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E30.mkv"])


class TestStageOnlyMissingEpisodes(SubscriptionCrossCopyTestBase):
    """转存这一步就只转存缺的集，而不是把整季包塞进夸克中转。

    以前是整包转存、再在中转目录里筛：下载是筛过了，但夸克配额已经被整包占住。
    """

    def _tv_task(self, **overrides):
        return self._task(media_type="tv", season=1, **overrides)

    def _share_season(self, episodes, folder_name="某剧 全季", sizes=None):
        """分享根是一个整季文件夹，文件夹里是各集（真实夸克的常见形态）。"""
        self.quark.default_share = [
            {"id": "share-folder-1", "name": folder_name, "size": 0, "is_dir": True}
        ]
        sizes = sizes or {}
        self.quark.share_dirs["share-folder-1"] = [
            {
                "id": f"share-ep{e}",
                "name": f"S01E{e:02d}.mp4",
                "size": int(sizes.get(e, 1000 + e)),
                "is_dir": False,
                "parent_id": "share-folder-1",
                "fid_token": f"tok-{e}",
            }
            for e in episodes
        ]

    def test_only_missing_episode_is_saved_to_staging(self):
        # 本盘有 E1~E34，分享含 E1~E36：只该把 E35/E36 转存进夸克中转
        self._share_season(range(1, 37))
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=34,
            existing_episodes=set(range(1, 35)),
        )

        self.assertEqual(len(self.quark.received), 1, result)
        staged_names = [item["name"] for item in self.quark.received[0]]
        self.assertEqual(staged_names, ["S01E35.mp4", "S01E36.mp4"])
        # 整季文件夹没有被当成一个条目提交（否则又是整包转存）
        self.assertFalse(any(item.get("is_dir") for item in self.quark.received[0]))
        # 每个文件都带 fid_token，夸克按文件转存要靠它
        self.assertTrue(all(item.get("fid_token") for item in self.quark.received[0]))

        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(sorted(job["filename"] for job in jobs), ["S01E35.mp4", "S01E36.mp4"])
        # 文件直接落在中转任务目录下，不再多一层分享文件夹名
        for job in jobs:
            self.assertEqual(job["source_path"], f"/云下载/跨盘中转/测试剧集/{job['filename']}")
            self.assertEqual(job["target_savepath"], "影视库/测试剧集/Season 01")

    def test_middle_gap_only_saves_the_missing_episode(self):
        # 缺的是 E35，本盘 E1~E36 其它都在
        existing = set(range(1, 37)) - {35}
        self._share_season(range(1, 37))
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=36,
            existing_episodes=existing,
        )

        self.assertEqual(
            [item["name"] for item in self.quark.received[0]], ["S01E35.mp4"]
        )
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E35.mp4"])

    def test_nothing_missing_means_no_staging_at_all(self):
        self._share_season(range(1, 9))
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=8,
            existing_episodes=set(range(1, 9)),
        )

        self.assertEqual(result["registered"], [])
        # 关键：一次转存都不该发，否则夸克配额被白占
        self.assertEqual(self.quark.received, [])
        self.assertNotIn("ensure_folder", [call[0] for call in self.quark.calls])
        self.assertIn("没有本盘缺的集", " ".join(result["skipped"]))
        self.assertTrue(any("未转存" in msg for msg, _ in result["logs"]))

    def test_quality_duplicates_keep_the_larger_file(self):
        # 同一集有 4K / 1080p 两份：只转存大的那份，别白占双倍配额
        self._share_season([35], sizes={35: 800})
        self.quark.share_dirs["share-folder-1"] = [
            {
                "id": "ep35-1080",
                "name": "S01E35.mp4",
                "size": 800,
                "is_dir": False,
                "parent_id": "share-folder-1",
            },
            {
                "id": "ep35-4k",
                "name": "S01E35.4K.mp4",
                "size": 8000,
                "is_dir": False,
                "parent_id": "share-folder-1",
            },
        ]
        self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=34,
            existing_episodes=set(range(1, 35)),
        )

        self.assertEqual([item["name"] for item in self.quark.received[0]], ["S01E35.4K.mp4"])

    def test_second_share_does_not_restage_claimed_episodes(self):
        # 分享 A 有 E35，分享 B 有 E35+E36：B 只该转存 E36
        self._share_season([35])
        self.quark.shares["https://pan.quark.cn/s/bbb"] = [
            {"id": "share-folder-b", "name": "另一份全季", "size": 0, "is_dir": True}
        ]
        self.quark.share_dirs["share-folder-b"] = [
            {
                "id": f"b-ep{e}",
                "name": f"S01E{e:02d}.mp4",
                "size": 2000 + e,
                "is_dir": False,
                "parent_id": "share-folder-b",
                "fid_token": f"btok-{e}",
            }
            for e in (35, 36)
        ]
        result = self._register(
            task=self._tv_task(),
            candidates=[
                self._candidate(url="https://pan.quark.cn/s/abc123", score=98),
                self._candidate(url="https://pan.quark.cn/s/bbb", score=90),
            ],
            last_episode=34,
            existing_episodes=set(range(1, 35)),
        )

        self.assertEqual(len(self.quark.received), 2, result)
        self.assertEqual([item["name"] for item in self.quark.received[0]], ["S01E35.mp4"])
        self.assertEqual([item["name"] for item in self.quark.received[1]], ["S01E36.mp4"])
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual(sorted(job["filename"] for job in jobs), ["S01E35.mp4", "S01E36.mp4"])

    def test_movie_task_still_stages_whole_share(self):
        self.quark.default_share = [
            {"id": "share-folder-1", "name": "电影合集", "size": 0, "is_dir": True}
        ]
        result = self._register(
            task=self._task(media_type="movie"),
            candidates=[self._candidate()],
            last_episode=9,
        )

        self.assertEqual(len(result["registered"]), 1)
        self.assertTrue(self.quark.received[0][0]["is_dir"])

    def test_unscannable_share_falls_back_to_whole_staging(self):
        # 分享子目录读不出来（扫不到文件）时退回整包转存，至少不会一集都不补
        self.quark.default_share = [
            {"id": "share-folder-1", "name": "某剧 全季", "size": 0, "is_dir": True}
        ]
        self.quark.dirs["fid-share-folder-1"] = [
            {"id": "f35", "name": "S01E35.mp4", "size": 1035, "is_dir": False}
        ]
        original_list = self.quark.list_share_entries

        def flaky(cookie, share_payload, cid="0", offset=0, limit=200):
            if str(cid or "0") != "0":
                raise RuntimeError("子目录接口 403")
            return original_list(cookie, share_payload, cid, offset, limit)

        self.quark.list_share_entries = flaky
        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=34,
            existing_episodes=set(range(1, 35)),
        )

        # 退回整包：提交的是分享文件夹本身
        self.assertTrue(self.quark.received)
        self.assertTrue(self.quark.received[0][0]["is_dir"])
        self.assertIn("本轮按整包转存", " ".join(result["skipped"]))
        # 后面的按集筛选照旧生效，不会把已有集也复制一遍
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E35.mp4"])

    def test_nested_season_folder_not_mistaken_for_task_season(self):
        # 分享根内嵌 `Season 2/` 子目录，里面文件的裸名是 S01E01.mp4（S01 前缀）。
        # 单季任务追 S01，绝不能让这些 S02 的文件落进 Season 01 污染库结构。
        self.quark.default_share = [
            {"id": "d-s1", "name": "Season 1", "size": 0, "is_dir": True},
            {"id": "d-s2", "name": "Season 2", "size": 0, "is_dir": True},
        ]
        self.quark.share_dirs["d-s1"] = [
            {
                "id": "s1e1", "name": "S01E01.mp4", "size": 1001,
                "is_dir": False, "parent_id": "d-s1", "fid_token": "tok-s1e1",
            },
            {
                "id": "s1e2", "name": "S01E02.mp4", "size": 1002,
                "is_dir": False, "parent_id": "d-s1", "fid_token": "tok-s1e2",
            },
        ]
        self.quark.share_dirs["d-s2"] = [
            {
                "id": "s2e1", "name": "S01E01.mp4", "size": 2001,
                "is_dir": False, "parent_id": "d-s2", "fid_token": "tok-s2e1",
            },
        ]

        result = self._register(
            task=self._tv_task(),
            candidates=[self._candidate()],
            last_episode=1,
            existing_episodes={1},
        )

        # 只补 Season 1 缺的 E02；Season 2 里的 S01E01 被季号识别挡掉
        jobs = [ccj.get_cross_copy_job(job_id) for job_id in result["registered"]]
        self.assertEqual([job["filename"] for job in jobs], ["S01E02.mp4"])
        for job in jobs:
            self.assertEqual(job["target_savepath"], "影视库/测试剧集/Season 01")
        # 转存只发了 Season 1 缺的那一集
        staged_names = [item["name"] for item in self.quark.received[0]]
        self.assertEqual(staged_names, ["S01E02.mp4"])



    """复制成功后清理中转目录里的空文件夹（只删确实为空的）。"""

    def _prepare_staging(self):
        root = f"staging::{sc.CROSS_COPY_STAGING_ROOT}/测试剧集"
        self.quark.dirs[root] = [
            {"id": "d-empty", "name": "空的分享文件夹", "size": 0, "is_dir": True},
            {"id": "d-keep", "name": "还有文件的", "size": 0, "is_dir": True},
        ]
        self.quark.dirs["d-empty"] = []
        self.quark.dirs["d-keep"] = [
            {"id": "x1", "name": "S01E44.mkv", "size": 1, "is_dir": False},
        ]
        return root

    def test_only_empty_folders_removed(self):
        self._prepare_staging()
        removed = sc.cleanup_staging_empty_folders(self.quark, "cookie-quark", "测试剧集")

        self.assertEqual(removed, 1)
        self.assertIn("d-empty", self.quark.deleted)
        self.assertNotIn("d-keep", self.quark.deleted)

    def test_task_staging_root_is_never_removed(self):
        root = self._prepare_staging()
        sc.cleanup_staging_empty_folders(self.quark, "cookie-quark", "测试剧集")

        self.assertNotIn(root, self.quark.deleted)

    def test_missing_staging_dir_is_noop(self):
        # 没跑过补源的任务没有中转目录，不能顺手建一个出来
        removed = sc.cleanup_staging_empty_folders(self.quark, "cookie-quark", "没跑过补源的任务")
        self.assertEqual(removed, 0)
        self.assertNotIn(("ensure_folder", sc._staging_relative_path("没跑过补源的任务")), self.quark.calls)

    def test_nested_empty_chain_removed_bottom_up(self):
        root = f"staging::{sc.CROSS_COPY_STAGING_ROOT}/测试剧集"
        self.quark.dirs[root] = [
            {"id": "d-outer", "name": "Season 1", "size": 0, "is_dir": True},
        ]
        self.quark.dirs["d-outer"] = [
            {"id": "d-inner", "name": "某剧 4K 全季", "size": 0, "is_dir": True},
        ]
        self.quark.dirs["d-inner"] = []

        removed = sc.cleanup_staging_empty_folders(self.quark, "cookie-quark", "测试剧集")

        self.assertEqual(removed, 2)
        self.assertIn("d-inner", self.quark.deleted)
        self.assertIn("d-outer", self.quark.deleted)

    def test_provider_without_delete_is_noop(self):
        self._prepare_staging()
        self.quark.supports_delete = False
        removed = sc.cleanup_staging_empty_folders(self.quark, "cookie-quark", "测试剧集")
        self.assertEqual(removed, 0)


class TestStagingFallbackVolumeLimit(unittest.TestCase):
    """扫不出分享文件时要退回整包，但整包不能无限大。

    退回整包是为了「至少不会一集都补不上」，代价是把不付费的夸克账号一次塞满；
    一旦塞满，后面所有补源都会失败，而多转进来的部分没有任何路径会删。
    """

    @staticmethod
    def _provider(per_file_size):
        return FakeQuarkProvider(
            share_entries=[
                {"id": "share-fid-1", "name": "S01E01.mkv", "size": per_file_size, "is_dir": False},
                {"id": "share-fid-2", "name": "S01E02.mkv", "size": per_file_size, "is_dir": False},
            ]
        )

    def test_oversized_fallback_is_refused(self):
        provider = self._provider(3 * 1024 ** 3)  # 两集共 6GB
        with patch.object(sc, "_scan_share_media_files", return_value=([], ["扫描失败"])), patch.object(
            sc, "staging_fallback_max_bytes", return_value=2 * 1024 ** 3
        ):
            staged = sc._stage_share_to_quark(
                provider,
                "cookie-quark",
                "https://pan.quark.cn/s/abc",
                "",
                "云下载/跨盘中转/测试剧集",
                task={"media_type": "tv", "name": "测试剧集"},
                last_episode=5,
            )
        self.assertEqual(staged["files"], [])
        self.assertEqual(staged["folders"], [])
        self.assertFalse(staged["already_staged"])
        self.assertTrue(any("超过整包上限" in item for item in staged["skipped"]))
        # 关键：一次转存都没提交
        self.assertEqual(provider.received, [])

    def test_small_fallback_still_staged(self):
        provider = self._provider(1024)
        with patch.object(sc, "_scan_share_media_files", return_value=([], ["扫描失败"])), patch.object(
            sc, "staging_fallback_max_bytes", return_value=2 * 1024 ** 3
        ):
            staged = sc._stage_share_to_quark(
                provider,
                "cookie-quark",
                "https://pan.quark.cn/s/abc",
                "",
                "云下载/跨盘中转/测试剧集",
                task={"media_type": "tv", "name": "测试剧集"},
                last_episode=5,
            )
        self.assertTrue(any("按整包转存" in item for item in staged["skipped"]))
        self.assertTrue(provider.received)

    def test_limit_zero_means_unlimited(self):
        with patch.dict(os.environ, {"CROSS_COPY_STAGING_FALLBACK_MAX_GB": "0"}):
            self.assertEqual(sc.staging_fallback_max_bytes(), 0)
        with patch.dict(os.environ, {"CROSS_COPY_STAGING_FALLBACK_MAX_GB": "junk"}):
            self.assertEqual(sc.staging_fallback_max_bytes(), 10 * 1024 ** 3)


class TestExistingEpisodeScanFailure(unittest.TestCase):
    """扫描失败必须返回 None（未知），不能返回空集合。

    空集合在下游是「本盘一集都没有」的强断言，一次扫描失败就会让整季被判缺、
    整季重下。这两者必须在类型上分开。
    """

    @staticmethod
    def _scan():
        from app.services import subscription_task_runner as runner

        return asyncio.run(
            runner._scan_subscription_existing_episodes_quietly({"provider": "115", "savepath": "剧"}, {})
        )

    def test_missing_provider_returns_none(self):
        from app.services import subscription_task_runner as runner

        with patch.object(runner, "_get_provider_or_none", return_value=None):
            self.assertIsNone(self._scan())

    def test_empty_cookie_returns_none(self):
        from app.services import subscription_task_runner as runner

        provider = MagicMock()
        provider.get_cookie.return_value = ""
        with patch.object(runner, "_get_provider_or_none", return_value=provider):
            self.assertIsNone(self._scan())

    def test_scan_exception_returns_none(self):
        from app.services import subscription_task_runner as runner

        provider = MagicMock()
        provider.get_cookie.return_value = "cookie"
        provider.resolve_folder_id_by_path.side_effect = RuntimeError("boom")
        with patch.object(runner, "_get_provider_or_none", return_value=provider):
            self.assertIsNone(self._scan())

    def test_missing_folder_is_really_empty(self):
        """目录不存在 = 这部剧本盘还没有，是真空集合，不能当成「没扫成」。"""
        from app.services import subscription_task_runner as runner

        provider = MagicMock()
        provider.get_cookie.return_value = "cookie"
        provider.resolve_folder_id_by_path.return_value = ""
        with patch.object(runner, "_get_provider_or_none", return_value=provider):
            self.assertEqual(self._scan(), set())


if __name__ == "__main__":
    unittest.main()
