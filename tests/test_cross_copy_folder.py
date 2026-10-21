"""跨盘复制「整个文件夹」测试。

覆盖：多层嵌套清单、空文件夹、超文件数/超深度、单文件失败继续、
全部失败、取消中断、目标目录复用、字节加权进度，以及任务层
source_is_dir 分支与「文件夹任务不删源」。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app import db
from app.routes import cross_copy as cross_copy_routes
from app.services import cross_cloud_copy as xcc
from app.services import cross_cloud_copy_folder as xcf
from app.services import cross_copy_jobs as ccj


def entry(item_id, name, is_dir=False, size=0):
    return {"id": item_id, "name": name, "is_dir": is_dir, "size": size}


class FakeFolderProvider:
    """最小可用的网盘替身：一棵用 cid 索引的目录树。"""

    def __init__(self, tree=None, name="quark", label="夸克网盘", supports_upload=True):
        self.name = name
        self.label = label
        self.supports_upload = supports_upload
        self.supports_delete = True
        self.cookie = "cookie-x"
        self.tree = {cid: [dict(item) for item in items] for cid, items in (tree or {}).items()}
        self.list_calls = []
        self.create_calls = []
        self.delete_calls = []
        self._next_id = 0

    def get_cookie(self, cfg):
        return self.cookie

    def is_configured(self, cfg):
        return bool(self.cookie)

    def list_entries_payload(self, cookie, cid="0", folders_only=False):
        self.list_calls.append((str(cid), bool(folders_only)))
        entries = [dict(item) for item in self.tree.get(str(cid), [])]
        if folders_only:
            entries = [item for item in entries if item.get("is_dir")]
        return {"entries": entries}

    def create_folder(self, cookie, cid="0", folder_name=""):
        self._next_id += 1
        folder_id = f"new-{cid}-{folder_name}-{self._next_id}"
        self.create_calls.append((str(cid), folder_name))
        self.tree.setdefault(str(cid), []).append(entry(folder_id, folder_name, is_dir=True))
        self.tree.setdefault(folder_id, [])
        return {"id": folder_id, "name": folder_name, "cid": str(cid), "created": True}

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        self.delete_calls.append(list(entry_ids))
        return {"ok": True}


def uploaded(name="", size=0, status="uploaded"):
    return xcc.CopyFileResult(
        status=status,
        source_provider="quark",
        target_provider="115",
        file_id="F-1",
        name=name,
        size=size,
        message="已完成",
    )


class FolderTestCase(unittest.TestCase):
    def setUp(self):
        self._orig_max_files = xcf.MANIFEST_MAX_FILES
        self._orig_max_depth = xcf.MANIFEST_MAX_DEPTH

    def tearDown(self):
        xcf.MANIFEST_MAX_FILES = self._orig_max_files
        xcf.MANIFEST_MAX_DEPTH = self._orig_max_depth

    # 源目录树：
    #   根 ── a.mkv (100)
    #        b.mkv (300)
    #        子目录 ── c.mkv (50)
    #                空目录
    def source_tree(self):
        return {
            "root": [
                entry("f-a", "a.mkv", size=100),
                entry("d-sub", "子目录", is_dir=True),
                entry("f-b", "b.mkv", size=300),
            ],
            "d-sub": [
                entry("f-c", "c.mkv", size=50),
                entry("d-empty", "空目录", is_dir=True),
            ],
            "d-empty": [],
        }

    def source(self):
        return FakeFolderProvider(self.source_tree(), name="quark", label="夸克网盘")

    def target(self):
        provider = FakeFolderProvider({"0": []}, name="115", label="115网盘")
        provider.cookie = "cookie-115"
        return provider


class TestBuildManifest(FolderTestCase):
    def test_nested_manifest(self):
        manifest = xcf.build_folder_manifest(self.source(), "c", "root")
        rel_paths = [item["rel_path"] for item in manifest["files"]]
        self.assertEqual(rel_paths, ["a.mkv", "b.mkv", "子目录/c.mkv"])
        self.assertEqual(manifest["dirs"], ["子目录", "子目录/空目录"])
        self.assertEqual(manifest["total_bytes"], 450)
        self.assertFalse(manifest["truncated"])
        self.assertEqual(manifest["skipped"], [])
        self.assertEqual(manifest["files"][2]["file_id"], "f-c")

    def test_empty_folder_keeps_dirs_and_zero_bytes(self):
        provider = FakeFolderProvider({"root": [entry("d-1", "空的", is_dir=True)], "d-1": []})
        manifest = xcf.build_folder_manifest(provider, "c", "root")
        self.assertEqual(manifest["files"], [])
        self.assertEqual(manifest["dirs"], ["空的"])
        self.assertEqual(manifest["total_bytes"], 0)
        self.assertFalse(manifest["truncated"])

    def test_file_count_limit_sets_truncated(self):
        xcf.MANIFEST_MAX_FILES = 2
        manifest = xcf.build_folder_manifest(self.source(), "c", "root")
        self.assertTrue(manifest["truncated"])
        self.assertEqual(len(manifest["files"]), 2)
        self.assertTrue(any("上限" in item["reason"] for item in manifest["skipped"]))

    def test_depth_limit_sets_truncated(self):
        xcf.MANIFEST_MAX_DEPTH = 1
        provider = FakeFolderProvider(
            {
                "root": [entry("d-1", "一", is_dir=True)],
                "d-1": [entry("d-2", "二", is_dir=True)],
                "d-2": [entry("f-deep", "deep.mkv", size=1)],
            }
        )
        manifest = xcf.build_folder_manifest(provider, "c", "root")
        self.assertTrue(manifest["truncated"])
        self.assertIn("一/二", manifest["dirs"])
        self.assertEqual(manifest["files"], [])
        self.assertTrue(any("层级" in item["reason"] for item in manifest["skipped"]))

    def test_list_failure_raises_instead_of_empty_manifest(self):
        class Broken(FakeFolderProvider):
            def list_entries_payload(self, cookie, cid="0", folders_only=False):
                raise RuntimeError("cookie 过期")

        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            xcf.build_folder_manifest(Broken(), "c", "root")
        self.assertIn("读取源文件夹", str(ctx.exception))

    def test_cancel_check_before_descend(self):
        with self.assertRaises(xcc.CrossCloudCopyCancelled):
            xcf.build_folder_manifest(self.source(), "c", "root", cancel_check=lambda: True)


class TestCopyFolder(FolderTestCase):
    def _run(self, copy_impl, **overrides):
        source = self.source()
        target = self.target()
        kwargs = dict(
            source_provider=source,
            source_cookie="c",
            source_folder_id="root",
            target_provider=target,
            target_cookie="c115",
            target_dir_id="0",
            target_folder_name="电影合集",
            check_existing=True,
        )
        kwargs.update(overrides)
        with patch.object(xcc, "copy_file_across_clouds", side_effect=copy_impl):
            result = xcf.copy_folder_across_clouds(**kwargs)
        return result, source, target

    def test_copies_tree_and_reports_counts(self):
        calls = []

        def fake_copy(**kwargs):
            calls.append((kwargs["filename"], kwargs["target_dir_id"], kwargs["size"]))
            return uploaded(kwargs["filename"], kwargs["size"])

        result, _, target = self._run(fake_copy)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.files_total, 3)
        self.assertEqual(result.files_done, 3)
        self.assertEqual(result.files_failed, 0)
        self.assertEqual(result.total_bytes, 450)
        self.assertEqual(result.folder_name, "电影合集")
        # 根目录 + 两个子目录
        self.assertEqual(result.dirs_created, 3)
        root_id = next(item["id"] for item in target.tree["0"] if item["name"] == "电影合集")
        sub_id = next(item["id"] for item in target.tree[root_id] if item["name"] == "子目录")
        # 文件进了对应层级的目录
        self.assertEqual(
            sorted(calls),
            [("a.mkv", root_id, 100), ("b.mkv", root_id, 300), ("c.mkv", sub_id, 50)],
        )
        self.assertIn("3 个文件", result.message)

    def test_reuses_existing_target_folder(self):
        target = self.target()
        target.tree["0"] = [entry("existing-root", "电影合集", is_dir=True)]
        target.tree["existing-root"] = []
        calls = []

        def fake_copy(**kwargs):
            calls.append(kwargs["target_dir_id"])
            return uploaded(kwargs["filename"], kwargs["size"])

        with patch.object(xcc, "copy_file_across_clouds", side_effect=fake_copy):
            result = xcf.copy_folder_across_clouds(
                source_provider=self.source(),
                source_cookie="c",
                source_folder_id="root",
                target_provider=target,
                target_cookie="c115",
                target_dir_id="0",
                target_folder_name="电影合集",
            )
        self.assertEqual(result.target_folder_id, "existing-root")
        # 根目录没有重复新建
        self.assertNotIn(("0", "电影合集"), target.create_calls)
        self.assertTrue(all(cid != "" for cid in calls))

    def test_single_failure_continues_and_marks_partial(self):
        def fake_copy(**kwargs):
            if kwargs["filename"] == "b.mkv":
                raise xcc.CrossCloudCopyError("下载失败：链接过期")
            return uploaded(kwargs["filename"], kwargs["size"])

        result, _, _ = self._run(fake_copy)
        self.assertEqual(result.status, "partial")
        self.assertEqual(result.files_done, 2)
        self.assertEqual(result.files_failed, 1)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(result.failures[0]["rel_path"], "b.mkv")
        self.assertIn("链接过期", result.failures[0]["message"])
        self.assertIn("部分完成", result.message)

    def test_all_files_failed_marks_failed(self):
        def fake_copy(**kwargs):
            raise xcc.CrossCloudCopyError("上传被限流")

        result, _, _ = self._run(fake_copy)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.files_done, 0)
        self.assertEqual(result.files_failed, 3)
        self.assertIn("复制失败", result.message)

    def test_skipped_files_count_as_success(self):
        def fake_copy(**kwargs):
            return uploaded(kwargs["filename"], kwargs["size"], status="skipped")

        result, _, _ = self._run(fake_copy)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.files_skipped, 3)
        self.assertEqual(result.files_done, 0)

    def test_cancel_stops_midway(self):
        copied = []

        def fake_copy(**kwargs):
            copied.append(kwargs["filename"])
            return uploaded(kwargs["filename"], kwargs["size"])

        with self.assertRaises(xcc.CrossCloudCopyCancelled):
            self._run(fake_copy, cancel_check=lambda: len(copied) >= 1)
        self.assertEqual(copied, ["a.mkv"])

    def test_progress_is_byte_weighted_and_monotonic(self):
        events = []

        def fake_copy(**kwargs):
            cb = kwargs["progress_cb"]
            size = kwargs["size"]
            cb(size // 2, size, "download")
            cb(size, size, "download")
            cb(size // 2, size, "upload")
            cb(size, size, "upload")
            return uploaded(kwargs["filename"], size)

        statuses = []
        result, _, _ = self._run(
            fake_copy,
            progress_cb=lambda sent, total, phase: events.append((sent, total, phase)),
            status_cb=statuses.append,
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(events[-1][0], 450)
        self.assertEqual(events[-1][1], 450)
        sent_values = [item[0] for item in events]
        self.assertEqual(sent_values, sorted(sent_values))  # 进度条不回退
        self.assertIn(100 + 150, sent_values)  # 第一个文件 100 字节 + 第二个文件下载到一半
        self.assertEqual({item[2] for item in events}, {"download", "upload"})
        self.assertIn("文件 1/3", statuses)
        self.assertIn("文件 3/3", statuses)

    def test_truncated_manifest_refuses_to_copy(self):
        xcf.MANIFEST_MAX_FILES = 1
        with patch.object(xcc, "copy_file_across_clouds") as m_copy:
            with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
                self._run(lambda **kwargs: uploaded())
        self.assertIn("一次复制不完", str(ctx.exception))
        self.assertIn("CROSS_COPY_FOLDER_MAX_FILES", str(ctx.exception))
        m_copy.assert_not_called()

    def test_empty_folder_only_creates_dirs(self):
        provider = FakeFolderProvider({"root": [entry("d-1", "空的", is_dir=True)], "d-1": []})
        target = self.target()
        with patch.object(xcc, "copy_file_across_clouds") as m_copy:
            result = xcf.copy_folder_across_clouds(
                source_provider=provider,
                source_cookie="c",
                source_folder_id="root",
                target_provider=target,
                target_cookie="c115",
                target_dir_id="0",
                target_folder_name="空合集",
            )
        m_copy.assert_not_called()
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.files_total, 0)
        self.assertEqual(result.dirs_created, 2)  # 根目录 + 空的
        self.assertIn("没有文件", result.message)

    def test_target_folder_create_failure_raises(self):
        target = self.target()

        def boom(cookie, cid="0", folder_name=""):
            raise RuntimeError("目标空间已满")

        target.create_folder = boom
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            xcf.copy_folder_across_clouds(
                source_provider=self.source(),
                source_cookie="c",
                source_folder_id="root",
                target_provider=target,
                target_cookie="c115",
                target_dir_id="0",
                target_folder_name="电影合集",
            )
        self.assertIn("空间已满", str(ctx.exception))

    def test_same_provider_rejected(self):
        provider = self.source()
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            xcf.copy_folder_across_clouds(
                source_provider=provider,
                source_cookie="c",
                source_folder_id="root",
                target_provider=provider,
                target_cookie="c",
                target_dir_id="0",
                target_folder_name="x",
            )
        self.assertIn("同一个网盘", str(ctx.exception))


class FolderJobsTestBase(unittest.TestCase):
    """任务层：用 mock provider 跑 run_cross_copy_job，验证 source_is_dir 分支。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ccf_test_")
        self.original_db_path = db.DB_PATH
        self.original_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "test.db")
        db._DB_ENSURED = False
        db.ensure_db()
        with db.db_connection() as conn:
            conn.execute("DELETE FROM cross_copy_jobs")
            conn.commit()
        ccj._job_progress.clear()
        ccj._last_db_progress.clear()
        ccj._job_detail.clear()
        ccj._cancel_requested.clear()

        self.cfg = {
            "cross_copy_notify_subscription": True,
            "cross_copy_notify_manual": True,
            "cross_copy_delete_source_after": True,
            "provider_enabled": {"quark": True, "115": True, "aliyun": True},
        }
        self.source_provider = FakeFolderProvider({}, name="quark", label="夸克网盘")
        self.target_provider = FakeFolderProvider({"0": []}, name="115", label="115网盘")

        self._patchers = [
            patch.object(ccj, "get_config", return_value=self.cfg),
            patch.object(ccj, "_get_provider_or_none", side_effect=self._get_provider),
            patch.object(ccj, "_send_notify_content", MagicMock()),
            patch.object(ccj, "notify_channel_ready", return_value=True),
            patch.object(ccj, "submit_background", MagicMock()),
            patch.object(
                ccj,
                "build_notify_runtime_config",
                return_value={"enabled": True, "channel": "wecom_bot", "wecom_webhook": "https://example.invalid/hook"},
            ),
            patch.object(cross_copy_routes, "get_config", return_value=self.cfg),
            patch.object(cross_copy_routes, "_get_provider_or_none", side_effect=self._get_provider),
        ]
        for patcher in self._patchers:
            patcher.start()

    def tearDown(self):
        for patcher in self._patchers:
            patcher.stop()
        db.DB_PATH = self.original_db_path
        db._DB_ENSURED = self.original_db_ensured
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _get_provider(self, name):
        if name == "quark":
            return self.source_provider
        if name == "115":
            return self.target_provider
        return None

    def _create_folder_job(self, **overrides):
        params = dict(
            title="电影合集",
            source_provider="quark",
            target_provider="115",
            source_file_id="d-root",
            target_dir_id="0",
            filename="电影合集",
            file_size=0,
            source_is_dir=True,
        )
        params.update(overrides)
        return ccj.create_cross_copy_job(**params)

    def _folder_result(self, status="completed", **overrides):
        kwargs = dict(
            status=status,
            source_provider="quark",
            target_provider="115",
            folder_name="电影合集",
            target_folder_id="dir-root",
            files_total=3,
            files_done=3,
            files_skipped=0,
            files_failed=0,
            failures=[],
            total_bytes=450,
            message="复制完成：共 3 个文件，3 个已复制",
        )
        kwargs.update(overrides)
        return xcf.CopyFolderResult(**kwargs)

    def _run(self, job_id):
        return asyncio.run(ccj.run_cross_copy_job(job_id))


class TestFolderJobBranch(FolderJobsTestBase):
    def test_source_is_dir_defaults_to_zero(self):
        job_id = ccj.create_cross_copy_job(
            title="单文件",
            source_provider="quark",
            target_provider="115",
            source_file_id="f-1",
            filename="movie.mkv",
            file_size=10,
        )
        self.assertEqual(ccj.get_cross_copy_job(job_id)["source_is_dir"], 0)

    def test_folder_job_calls_folder_engine_and_writes_result(self):
        job_id = self._create_folder_job()
        with patch.object(xcf, "copy_folder_across_clouds", return_value=self._folder_result()) as m_copy:
            self._run(job_id)
        m_copy.assert_called_once()
        kwargs = m_copy.call_args.kwargs
        self.assertEqual(kwargs["source_folder_id"], "d-root")
        self.assertEqual(kwargs["target_folder_name"], "电影合集")
        self.assertEqual(kwargs["target_dir_id"], "0")
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["progress_percent"], 100)
        self.assertEqual(job["source_is_dir"], 1)
        self.assertEqual(job["file_size"], 450)
        self.assertEqual(job["target_path"], "/电影合集")
        self.assertEqual(job["result"]["files_total"], 3)
        self.assertEqual(job["result"]["files_done"], 3)
        self.assertEqual(job["result"]["failures"], [])

    def test_file_job_does_not_call_folder_engine(self):
        job_id = ccj.create_cross_copy_job(
            title="单文件",
            source_provider="quark",
            target_provider="115",
            source_file_id="f-1",
            filename="movie.mkv",
            file_size=10,
        )
        with patch.object(xcc, "copy_file_across_clouds", return_value=uploaded("movie.mkv", 10)):
            with patch.object(xcf, "copy_folder_across_clouds") as m_folder:
                self._run(job_id)
        m_folder.assert_not_called()
        self.assertEqual(ccj.get_cross_copy_job(job_id)["status"], "completed")

    def test_partial_result_keeps_job_completed_with_detail(self):
        job_id = self._create_folder_job()
        result = self._folder_result(
            status="partial",
            files_done=2,
            files_failed=1,
            failures=[{"rel_path": "b.mkv", "message": "下载失败"}],
            message="部分完成：共 3 个文件，2 个已复制、1 个失败",
        )
        with patch.object(xcf, "copy_folder_across_clouds", return_value=result):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        # 任务状态机不变：任务算完成，明细里带 partial 与失败清单
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["result"]["status"], "partial")
        self.assertEqual(job["result"]["files_failed"], 1)
        self.assertEqual(job["result"]["failures"][0]["rel_path"], "b.mkv")
        self.assertIn("部分完成", job["status_detail"])

    def test_all_failed_result_marks_job_failed(self):
        job_id = self._create_folder_job()
        result = self._folder_result(
            status="failed",
            files_done=0,
            files_failed=3,
            failures=[{"rel_path": "a.mkv", "message": "上传被限流"}],
            message="复制失败：3 个文件都没成功（上传被限流）",
        )
        with patch.object(xcf, "copy_folder_across_clouds", return_value=result):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")
        self.assertIn("复制失败", job["error_message"])
        self.assertEqual(job["result"]["files_failed"], 3)
        ccj._send_notify_content.assert_called_once()

    def test_folder_job_never_deletes_source(self):
        # 即使是订阅补源（origin=subscription），文件夹任务也不删源
        job_id = self._create_folder_job(origin="subscription")
        with patch.object(xcf, "copy_folder_across_clouds", return_value=self._folder_result()):
            self._run(job_id)
        self.assertEqual(self.source_provider.delete_calls, [])

    def test_folder_job_cancel(self):
        job_id = self._create_folder_job()
        with patch.object(xcf, "copy_folder_across_clouds", side_effect=xcc.CrossCloudCopyCancelled()):
            self._run(job_id)
        self.assertEqual(ccj.get_cross_copy_job(job_id)["status"], "cancelled")

    def test_progress_detail_shows_file_index(self):
        job_id = self._create_folder_job()
        ccj._report_file_detail(job_id, "文件 2/3")
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status_detail"], "文件 2/3")
        # 进度写库有节流，模拟真实间隔后再来一次回调
        with ccj._progress_lock:
            ccj._last_db_progress[job_id] = float(ccj._last_db_progress.get(job_id, 0)) - 1.0
        ccj._report_progress(job_id, 100, 400, "download")
        job = ccj.get_cross_copy_job(job_id)
        self.assertIn("文件 2/3", job["status_detail"])
        self.assertIn("下载", job["status_detail"])


class TestFolderRoutes(FolderJobsTestBase):
    class FakeJsonRequest:
        def __init__(self, payload):
            self.payload = payload
            self.query_params = {}

        async def json(self):
            return self.payload

    def test_route_passes_source_is_dir(self):
        with patch.object(ccj, "submit_cross_copy_job"):
            result = asyncio.run(
                cross_copy_routes.create_cross_copy_job_endpoint(
                    self.FakeJsonRequest(
                        {
                            "source_provider": "quark",
                            "target_provider": "115",
                            "source_file_id": "d-root",
                            "source_is_dir": True,
                            "filename": "电影合集",
                        }
                    )
                )
            )
        self.assertTrue(result["ok"])
        job = ccj.get_cross_copy_job(result["job_id"])
        self.assertEqual(job["source_is_dir"], 1)

    def test_route_accepts_folder_id_without_rejecting(self):
        with patch.object(ccj, "submit_cross_copy_job"):
            result = asyncio.run(
                cross_copy_routes.create_cross_copy_job_endpoint(
                    self.FakeJsonRequest(
                        {
                            "source_provider": "quark",
                            "target_provider": "115",
                            "source_file_id": "folder-123",
                            "source_is_dir": "1",
                            "filename": "某个文件夹",
                        }
                    )
                )
            )
        self.assertTrue(result["ok"])
        self.assertEqual(ccj.get_cross_copy_job(result["job_id"])["source_file_id"], "folder-123")

    def test_default_is_file_job(self):
        with patch.object(ccj, "submit_cross_copy_job"):
            result = asyncio.run(
                cross_copy_routes.create_cross_copy_job_endpoint(
                    self.FakeJsonRequest(
                        {"source_provider": "quark", "target_provider": "115", "source_file_id": "f-1"}
                    )
                )
            )
        self.assertEqual(ccj.get_cross_copy_job(result["job_id"])["source_is_dir"], 0)

    def test_115_and_aliyun_are_source_capable(self):
        aliyun = FakeFolderProvider({}, name="aliyun", label="阿里云盘")
        original = self._get_provider

        def get_provider(name):
            if name == "aliyun":
                return aliyun
            return original(name)

        with patch.object(cross_copy_routes, "_get_provider_or_none", side_effect=get_provider):
            capabilities = asyncio.run(
                cross_copy_routes.get_cross_copy_capabilities(self.FakeJsonRequest({}))
            )
        states = {item["name"]: item for item in capabilities["providers"]}
        self.assertTrue(states["115"]["can_be_source"])
        self.assertTrue(states["aliyun"]["can_be_source"])
        combos = {(item["source"], item["target"]) for item in capabilities["combinations"]}
        self.assertIn(("115", "quark"), combos)


class TestSchemaMigration(unittest.TestCase):
    def test_cross_copy_jobs_has_source_is_dir_column(self):
        tmp = tempfile.mkdtemp(prefix="wb_ccf_db_")
        original_path = db.DB_PATH
        original_ensured = db._DB_ENSURED
        try:
            db.DB_PATH = os.path.join(tmp, "test.db")
            db._DB_ENSURED = False
            db.ensure_db()
            with db.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(cross_copy_jobs)")
                columns = {row[1] for row in cursor.fetchall()}
            self.assertIn("source_is_dir", columns)
        finally:
            db.DB_PATH = original_path
            db._DB_ENSURED = original_ensured
            shutil.rmtree(tmp, ignore_errors=True)

    def test_old_table_gets_column_added_with_default_zero(self):
        """老库升级：已有任务表补列，历史任务默认按「单文件任务」处理。"""
        import sqlite3

        tmp = tempfile.mkdtemp(prefix="wb_ccf_db_old_")
        original_path = db.DB_PATH
        original_ensured = db._DB_ENSURED
        try:
            path = os.path.join(tmp, "old.db")
            conn = sqlite3.connect(path)
            # 加 source_is_dir 之前的真实建表语句（少这一列）
            conn.execute(
                """
                CREATE TABLE cross_copy_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL DEFAULT '',
                    source_provider TEXT NOT NULL DEFAULT '',
                    target_provider TEXT NOT NULL DEFAULT '',
                    source_file_id TEXT NOT NULL DEFAULT '',
                    source_mode TEXT NOT NULL DEFAULT 'save_first',
                    source_share_url TEXT NOT NULL DEFAULT '',
                    target_dir_id TEXT NOT NULL DEFAULT '',
                    target_savepath TEXT NOT NULL DEFAULT '',
                    source_path TEXT NOT NULL DEFAULT '',
                    target_path TEXT NOT NULL DEFAULT '',
                    filename TEXT NOT NULL DEFAULT '',
                    file_size INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    status_detail TEXT NOT NULL DEFAULT '',
                    progress_percent INTEGER NOT NULL DEFAULT 0,
                    phase TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error_message TEXT NOT NULL DEFAULT '',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    origin TEXT NOT NULL DEFAULT 'manual',
                    origin_ref TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "INSERT INTO cross_copy_jobs(title, created_at, updated_at) VALUES ('老任务', 'x', 'x')"
            )
            conn.commit()
            conn.close()

            db.DB_PATH = path
            db._DB_ENSURED = False
            db.ensure_db()
            with db.db_connection() as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(cross_copy_jobs)").fetchall()}
                row = conn.execute(
                    "SELECT source_is_dir FROM cross_copy_jobs WHERE title = '老任务'"
                ).fetchone()
            self.assertIn("source_is_dir", columns)
            self.assertEqual(row[0], 0)
        finally:
            db.DB_PATH = original_path
            db._DB_ENSURED = original_ensured
            shutil.rmtree(tmp, ignore_errors=True)


class TestMessageWording(unittest.TestCase):
    """用户可见文案不能出现「回源」「会员」这类黑话。"""

    def test_no_jargon_in_folder_module(self):
        text = open(xcf.__file__, encoding="utf-8").read()
        for word in ("回源", "会员"):
            self.assertNotIn(word, text)


if __name__ == "__main__":
    unittest.main()
