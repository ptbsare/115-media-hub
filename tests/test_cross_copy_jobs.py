"""跨盘复制任务测试（#9 任务接入与手动入口）。

注意：本会话环境 tempfile 自检不稳定，统一用 tempfile.mkdtemp()
显式建目录并在 tearDown 手动清理。
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
from app.routes import cross_copy as cross_copy_routes


class FakeJsonRequest:
    def __init__(self, payload):
        self.payload = payload
        self.query_params = {}

    async def json(self):
        return self.payload


class FakeQueryRequest:
    def __init__(self, params):
        self.payload = {}
        self.query_params = params

    async def json(self):
        return self.payload


class FakeProvider:
    def __init__(self, name, label, cookie="cookie-x", supports_upload=True, supports_delete=True):
        self.name = name
        self.label = label
        self._cookie = cookie
        self.supports_upload = supports_upload
        self.supports_delete = supports_delete
        self.delete_calls = []

    def get_cookie(self, cfg):
        return self._cookie

    def is_configured(self, cfg):
        return bool(self._cookie)

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        self.delete_calls.append(list(entry_ids))
        return {"ok": True}


def _uploaded_result(status="uploaded"):
    return xcc.CopyFileResult(
        status=status,
        source_provider="quark",
        target_provider="115",
        file_id="F-1" if status == "uploaded" else "",
        name="movie.mkv",
        size=1024,
        sha1="ABC",
        message="已完成" if status == "uploaded" else "目标已存在同名同大小文件，跳过",
    )


class CrossCopyJobsTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ccj_test_")
        self.original_db_path = db.DB_PATH
        self.original_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "test.db")
        db._DB_ENSURED = False
        db.ensure_db()
        # 清理跨用例残留
        with db.db_connection() as conn:
            conn.execute("DELETE FROM cross_copy_jobs")
            conn.commit()
        # 模块级内存状态也要清：job id 从 1 重新开始，残留会串味（进度节流/取消标记）
        ccj._job_progress.clear()
        ccj._last_db_progress.clear()
        ccj._cancel_requested.clear()

        self.cfg = {
            "cross_copy_notify": True,
            "cross_copy_delete_source_after": True,
            "provider_enabled": {"quark": True, "115": True, "aliyun": True},
            "local_storages": [],
        }
        self.source_provider = FakeProvider("quark", "夸克网盘")
        self.target_provider = FakeProvider("115", "115网盘")

        patchers = [
            patch.object(ccj, "get_config", return_value=self.cfg),
            patch.object(ccj, "_get_provider_or_none", side_effect=self._fake_get_provider),
            patch.object(ccj, "_send_notify_content", MagicMock()),
            patch.object(
                ccj,
                "build_notify_runtime_config",
                return_value={"enabled": True, "channel": "wecom_bot", "wecom_webhook": "https://example.invalid/hook"},
            ),
            patch.object(ccj, "submit_background", MagicMock()),
            patch.object(cross_copy_routes, "get_config", return_value=self.cfg),
            patch.object(cross_copy_routes, "_get_provider_or_none", side_effect=self._fake_get_provider),
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

    def _run(self, job_id):
        return asyncio.run(ccj.run_cross_copy_job(job_id))


class TestJobCrud(CrossCopyJobsTestBase):
    def test_create_and_get(self):
        job_id = self._create_job()
        job = ccj.get_cross_copy_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["source_provider"], "quark")
        self.assertEqual(job["target_provider"], "115")
        self.assertEqual(job["file_size"], 1024)
        self.assertEqual(job["origin"], "manual")

    def test_get_missing(self):
        self.assertIsNone(ccj.get_cross_copy_job(9999))

    def test_list_jobs(self):
        self._create_job(title="A")
        self._create_job(title="B")
        jobs = ccj.list_cross_copy_jobs(limit=10)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["title"], "B")  # 倒序

    def test_list_filter_status(self):
        self._create_job(title="A")
        jobs = ccj.list_cross_copy_jobs(limit=10, status="completed")
        self.assertEqual(jobs, [])

    def test_update_normalizes_status(self):
        job_id = self._create_job()
        ccj.update_cross_copy_job(job_id, status="COMPLETED", progress_percent=100)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["progress_percent"], 100)


class TestJobExecution(CrossCopyJobsTestBase):
    def test_success_uploaded(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result("uploaded")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["progress_percent"], 100)
        self.assertEqual(job["result"]["file_id"], "F-1")
        ccj._send_notify_content.assert_called_once()

    def test_success_rapid(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result("rapid")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "completed")

    def test_skipped(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result("skipped")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "skipped")
        ccj._send_notify_content.assert_called_once()

    def test_failure(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", side_effect=xcc.CrossCloudCopyError("115 操作过于频繁")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")
        self.assertIn("频繁", job["error_message"])
        ccj._send_notify_content.assert_called_once()

    def test_unexpected_exception_does_not_crash(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", side_effect=ValueError("boom")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")

    def test_cancel_pending(self):
        job_id = self._create_job()
        result = ccj.cancel_cross_copy_job(job_id)
        self.assertTrue(result["ok"])
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "cancelled")

    def test_cancel_running_at_checkpoint(self):
        job_id = self._create_job()

        def fake_copy(**kwargs):
            # 模拟运行中取消：检查点第一次调用即命中
            raise xcc.CrossCloudCopyCancelled("已取消")

        with patch.object(xcc, "copy_file_across_clouds", side_effect=fake_copy):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "cancelled")

    def test_cancel_finished_job_rejected(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job_id)
        result = ccj.cancel_cross_copy_job(job_id)
        self.assertFalse(result["ok"])
        self.assertIn("已结束", result["msg"])

    def test_retry_after_failure(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", side_effect=xcc.CrossCloudCopyError("网络错误")):
            self._run(job_id)
        result = ccj.retry_cross_copy_job(job_id)
        self.assertTrue(result["ok"])
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["retry_count"], 1)
        ccj.submit_background.assert_called_once()

    def test_retry_running_rejected(self):
        job_id = self._create_job()
        ccj.update_cross_copy_job(job_id, status="running")
        result = ccj.retry_cross_copy_job(job_id)
        self.assertFalse(result["ok"])

    def test_delete_source_after_success(self):
        # 订阅补源：源文件是本任务自己转存到中转目录的副本，完成后可清理
        job_id = self._create_job(origin="subscription")
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job_id)
        self.assertEqual(self.source_provider.delete_calls, [["f-quark-1"]])

    def test_manual_job_never_deletes_source(self):
        # 手动复制的源文件是用户自己网盘里的原片，无论配置如何都不能删
        job_id = self._create_job(origin="manual")
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job_id)
        self.assertEqual(self.source_provider.delete_calls, [])

    def test_no_delete_when_disabled(self):
        self.cfg["cross_copy_delete_source_after"] = False
        job_id = self._create_job(origin="subscription")
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job_id)
        self.assertEqual(self.source_provider.delete_calls, [])

    def test_no_delete_on_failure(self):
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", side_effect=xcc.CrossCloudCopyError("失败")):
            self._run(job_id)
        self.assertEqual(self.source_provider.delete_calls, [])

    def test_notify_disabled(self):
        self.cfg["cross_copy_notify"] = False
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result()):
            self._run(job_id)
        ccj._send_notify_content.assert_not_called()

    def test_missing_cookie_fails(self):
        self.target_provider._cookie = ""
        job_id = self._create_job()
        with patch.object(xcc, "copy_file_across_clouds") as m_copy:
            self._run(job_id)
        m_copy.assert_not_called()
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")
        self.assertIn("认证信息", job["error_message"])

    def test_same_provider_rejected(self):
        job_id = self._create_job(source_provider="115", target_provider="115")
        with patch.object(xcc, "copy_file_across_clouds") as m_copy:
            self._run(job_id)
        m_copy.assert_not_called()
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["status"], "failed")
        self.assertIn("同一个网盘", job["error_message"])


class TestStartupRequeue(CrossCopyJobsTestBase):
    def test_requeue_running_and_pending(self):
        self._create_job(title="A")
        running_id = self._create_job(title="B")
        ccj.update_cross_copy_job(running_id, status="running")
        done_id = self._create_job(title="C")
        ccj.update_cross_copy_job(done_id, status="completed")
        result = ccj.requeue_cross_copy_jobs_on_startup()
        self.assertEqual(result["requeued"], 2)
        ccj.submit_background.assert_called()


class TestRoutes(CrossCopyJobsTestBase):
    def test_capabilities(self):
        result = asyncio.run(cross_copy_routes.get_cross_copy_capabilities(FakeQueryRequest({})))
        self.assertTrue(result["ok"])
        names = {p["name"] for p in result["providers"]}
        self.assertIn("quark", names)
        self.assertIn("115", names)
        combos = {(c["source"], c["target"]) for c in result["combinations"]}
        self.assertIn(("quark", "115"), combos)
        self.assertNotIn(("115", "115"), combos)

    def test_create_job_endpoint(self):
        with patch.object(ccj, "submit_cross_copy_job") as m_submit:
            result = asyncio.run(
                cross_copy_routes.create_cross_copy_job_endpoint(
                    FakeJsonRequest(
                        {
                            "source_provider": "quark",
                            "target_provider": "115",
                            "source_file_id": "f-1",
                            "filename": "movie.mkv",
                        }
                    )
                )
            )
        self.assertTrue(result["ok"])
        self.assertGreater(result["job_id"], 0)
        m_submit.assert_called_once_with(result["job_id"])

    def test_create_job_same_provider_rejected(self):
        result = asyncio.run(
            cross_copy_routes.create_cross_copy_job_endpoint(
                FakeJsonRequest({"source_provider": "115", "target_provider": "115", "source_file_id": "f-1"})
            )
        )
        self.assertEqual(result.status_code, 400)

    def test_create_job_unsupported_source_rejected(self):
        # 115 / 阿里都已能作为源；这里用一个不在能力清单里的网盘验证拦截
        result = asyncio.run(
            cross_copy_routes.create_cross_copy_job_endpoint(
                FakeJsonRequest({"source_provider": "baidu", "target_provider": "115", "source_file_id": "f-1"})
            )
        )
        self.assertEqual(result.status_code, 400)

    def test_create_job_empty_file_id_rejected(self):
        result = asyncio.run(
            cross_copy_routes.create_cross_copy_job_endpoint(
                FakeJsonRequest({"source_provider": "quark", "target_provider": "115"})
            )
        )
        self.assertEqual(result.status_code, 400)

    def test_job_detail_endpoint(self):
        job_id = self._create_job()
        result = asyncio.run(cross_copy_routes.get_cross_copy_job_endpoint(FakeQueryRequest({}), job_id))
        self.assertTrue(result["ok"])
        self.assertEqual(result["job"]["id"], job_id)

    def test_job_detail_missing(self):
        result = asyncio.run(cross_copy_routes.get_cross_copy_job_endpoint(FakeQueryRequest({}), 9999))
        self.assertEqual(result.status_code, 404)

    def test_list_endpoint(self):
        self._create_job()
        result = asyncio.run(cross_copy_routes.list_cross_copy_jobs_endpoint(FakeQueryRequest({})))
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["jobs"]), 1)


class TestJobPaths(CrossCopyJobsTestBase):
    """任务卡片要显示"源文件在哪、会放到哪"，所以路径必须随任务落库。"""

    def test_paths_persisted(self):
        job_id = self._create_job(
            source_path="/电影/沙丘2.mkv",
            target_path="/云下载/电影/沙丘2.mkv",
        )
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["source_path"], "/电影/沙丘2.mkv")
        self.assertEqual(job["target_path"], "/云下载/电影/沙丘2.mkv")

    def test_target_path_falls_back_to_savepath_plus_filename(self):
        job_id = self._create_job(target_savepath="云下载/电影")
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["target_path"], "/云下载/电影/movie.mkv")

    def test_completion_rewrites_target_path_with_final_name(self):
        job_id = self._create_job(target_savepath="云下载/电影", filename="")
        with patch.object(xcc, "copy_file_across_clouds", return_value=_uploaded_result("uploaded")):
            self._run(job_id)
        job = ccj.get_cross_copy_job(job_id)
        self.assertEqual(job["filename"], "movie.mkv")
        self.assertEqual(job["target_path"], "/云下载/电影/movie.mkv")

    def test_create_endpoint_accepts_paths(self):
        with patch.object(ccj, "submit_cross_copy_job"):
            result = asyncio.run(
                cross_copy_routes.create_cross_copy_job_endpoint(
                    FakeJsonRequest(
                        {
                            "source_provider": "quark",
                            "target_provider": "115",
                            "source_file_id": "f-1",
                            "filename": "movie.mkv",
                            "source_path": "/电影/movie.mkv",
                            "target_path": "/云下载/电影/movie.mkv",
                        }
                    )
                )
            )
        job = ccj.get_cross_copy_job(result["job_id"])
        self.assertEqual(job["source_path"], "/电影/movie.mkv")
        self.assertEqual(job["target_path"], "/云下载/电影/movie.mkv")


class TestTransferProgress(CrossCopyJobsTestBase):
    """进度除了百分比，还要有「已完成 / 总大小 · 速度」，下载与上传都要。"""

    def _age_progress(self, job_id, seconds):
        """把上一次回调的时间往前拨，模拟真实间隔（否则算不出速度/会被写库节流）。"""
        with ccj._progress_lock:
            entry = ccj._job_progress.get(job_id)
            if entry:
                entry["updated_ts"] -= seconds
            last_persist = ccj._last_db_progress.get(job_id)
            if last_persist is not None:
                ccj._last_db_progress[job_id] = last_persist - seconds

    def test_progress_exposes_bytes_and_percent_in_list(self):
        job_id = self._create_job(file_size=200 * 1024 * 1024)
        ccj._report_progress(job_id, 100 * 1024 * 1024, 200 * 1024 * 1024, "download")
        jobs = ccj.list_cross_copy_jobs(limit=10)
        progress = jobs[0]["progress"]
        self.assertEqual(progress["sent"], 100 * 1024 * 1024)
        self.assertEqual(progress["total"], 200 * 1024 * 1024)
        self.assertEqual(progress["percent"], 50)
        self.assertEqual(jobs[0]["progress_percent"], 50)

    def test_download_detail_contains_size_and_speed(self):
        job_id = self._create_job(file_size=1024 * 1024)
        ccj._report_progress(job_id, 256 * 1024, 1024 * 1024, "download")
        self._age_progress(job_id, 2.0)
        ccj._report_progress(job_id, 1024 * 1024, 1024 * 1024, "download")
        job = ccj.get_cross_copy_job(job_id)
        self.assertIn("下载", job["status_detail"])
        self.assertIn("1.00MB", job["status_detail"])
        self.assertIn("/s", job["status_detail"])
        self.assertGreater(job["progress"]["speed"], 0)

    def test_upload_detail_uses_upload_wording(self):
        job_id = self._create_job(file_size=8 * 1024 * 1024)
        ccj._report_progress(job_id, 1024 * 1024, 8 * 1024 * 1024, "upload")
        self._age_progress(job_id, 1.0)
        ccj._report_progress(job_id, 4 * 1024 * 1024, 8 * 1024 * 1024, "upload")
        job = ccj.get_cross_copy_job(job_id)
        self.assertIn("上传", job["status_detail"])
        self.assertIn("4.00MB / 8.00MB", job["status_detail"])

    def test_speed_resets_when_phase_changes(self):
        job_id = self._create_job(file_size=1024 * 1024)
        ccj._report_progress(job_id, 512 * 1024, 1024 * 1024, "download")
        self._age_progress(job_id, 1.0)
        ccj._report_progress(job_id, 1024 * 1024, 1024 * 1024, "download")
        self.assertGreater(ccj.get_cross_copy_job(job_id)["progress"]["speed"], 0)
        self._age_progress(job_id, 1.0)
        ccj._report_progress(job_id, 128 * 1024, 1024 * 1024, "upload")
        progress = ccj.get_cross_copy_job(job_id)["progress"]
        self.assertEqual(progress["phase"], "upload")
        self.assertEqual(progress["speed"], 0)

    def test_progress_write_is_throttled_but_never_drops_final_chunk(self):
        job_id = self._create_job(file_size=4 * 1024 * 1024)
        ccj._report_progress(job_id, 1024 * 1024, 4 * 1024 * 1024, "download")
        ccj._report_progress(job_id, 2 * 1024 * 1024, 4 * 1024 * 1024, "download")
        # 中间块被节流：DB 里还是第一次的 25%
        self.assertEqual(ccj.get_cross_copy_job(job_id)["progress_percent"], 25)
        # 最后一块必须落库
        ccj._report_progress(job_id, 4 * 1024 * 1024, 4 * 1024 * 1024, "download")
        self.assertEqual(ccj.get_cross_copy_job(job_id)["progress_percent"], 100)

    def test_format_transfer_size(self):
        self.assertEqual(ccj.format_transfer_size(0), "0B")
        self.assertEqual(ccj.format_transfer_size(1024), "1.00KB")
        self.assertEqual(ccj.format_transfer_size(2797571732), "2.61GB")


if __name__ == "__main__":
    unittest.main()
