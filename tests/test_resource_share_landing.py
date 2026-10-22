"""资源页「分享转存」等落地再标完成的测试。

背景：夸克 save 只提交异步任务，文件过几秒才出现在目标目录。以前提交完立刻标
「已完成」，用户去看目录是空的，只能当成失败。这里覆盖两端：

1. `_wait_share_receive_landing` 本身的判定（成功 / 对方报失败 / 超时但目录已落地 /
   超时且目录没有 / 同步网盘不需要等 / 取消能及时打断）；
2. `run_resource_job` 的三态收口（completed / failed / submitted）。
"""

import asyncio
import json
import unittest
from unittest import mock

from app import core
from app.services import resource as resource_service


class ScriptedWaitProvider:
    """只实现等落地用到的方法；outcomes 按调用顺序出队。"""

    name = "quark"
    label = "夸克网盘"
    supports_share_receive = True
    supports_monitor = False

    def __init__(self, outcomes=None, entries=None, with_wait=True):
        self.outcomes = list(outcomes or [])
        self.entries = list(entries or [])
        self.calls = []
        if not with_wait:
            # 模拟 115：同步接口，压根没有 wait_share_save 这个方法
            return
        self.wait_share_save = self._wait

    def get_cookie(self, _cfg=None):
        return "quark-cookie"

    def resolve_folder_id_by_path(self, cookie, savepath):
        return "target-cid"

    def list_entries(self, cookie, cid="0"):
        return list(self.entries)

    def _wait(self, cookie, save_result, timeout=120.0, interval=1.0):
        self.calls.append(timeout)
        if self.outcomes:
            return self.outcomes.pop(0)
        return {"ok": False, "status": 0, "reason": "任务还在跑"}


EXPECTED = [{"id": "f1", "name": "S01E42.mp4", "size": 1024, "is_dir": False}]


class ShareLandingWaitTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.patcher = mock.patch.object(
            resource_service, "RESOURCE_SHARE_LANDING_POLL_CHUNK_SECONDS", 0.05
        )
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    async def test_sync_provider_is_skipped(self):
        provider = ScriptedWaitProvider(with_wait=False)
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=5,
        )

        self.assertFalse(result["supported"])
        self.assertTrue(result["confirmed"])
        self.assertTrue(result["skipped"])

    async def test_success_returns_confirmed(self):
        provider = ScriptedWaitProvider(outcomes=[{"ok": True, "status": 2, "reason": ""}])
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={"response": {"data": {"task_id": "t1"}}},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=5,
        )

        self.assertTrue(result["supported"])
        self.assertTrue(result["confirmed"])
        self.assertFalse(result.get("failed"))
        self.assertEqual(result["status"], 2)
        self.assertEqual(len(provider.calls), 1)

    async def test_provider_failure_is_reported(self):
        provider = ScriptedWaitProvider(
            outcomes=[{"ok": False, "status": 3, "reason": "空间不足"}]
        )
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=5,
        )

        self.assertTrue(result["failed"])
        self.assertFalse(result["confirmed"])
        self.assertIn("空间不足", result["reason"])

    async def test_timeout_confirmed_by_directory(self):
        # 任务接口一直说还在跑，但目录里已经有文件 → 按落地成功收口
        provider = ScriptedWaitProvider(
            entries=[{"id": "f1", "name": "S01E42.mp4", "size": 1024, "is_dir": False}]
        )
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=0.05,
        )

        self.assertTrue(result["confirmed"])
        self.assertTrue(result.get("directory_confirmed"))
        self.assertFalse(result.get("failed"))

    async def test_timeout_without_directory_evidence_is_unconfirmed(self):
        provider = ScriptedWaitProvider(entries=[])
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=0.05,
        )

        self.assertFalse(result["confirmed"])
        self.assertFalse(result.get("failed"))

    async def test_directory_match_requires_same_size(self):
        provider = ScriptedWaitProvider(
            entries=[{"id": "f9", "name": "S01E42.mp4", "size": 999, "is_dir": False}]
        )
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=0.05,
        )

        self.assertFalse(result["confirmed"])

    async def test_cancel_during_wait_propagates(self):
        provider = ScriptedWaitProvider()

        def cancel(stage=""):
            raise resource_service.ResourceJobCancelledError("已取消")

        with self.assertRaises(resource_service.ResourceJobCancelledError):
            await resource_service._wait_share_receive_landing(
                provider=provider,
                cookie="c",
                bundle={},
                target_cid="target-cid",
                expected=EXPECTED,
                total_timeout=5,
                ensure_not_cancelled=cancel,
            )
        self.assertEqual(provider.calls, [])

    async def test_directory_check_errors_are_not_fatal(self):
        provider = ScriptedWaitProvider(outcomes=[{"ok": False, "status": 0, "reason": "接口异常"}])
        provider.list_entries = mock.MagicMock(side_effect=RuntimeError("403"))
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="c",
            bundle={},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=0.05,
        )

        self.assertFalse(result["confirmed"])


class ShareReceiveLandingJobTest(unittest.IsolatedAsyncioTestCase):
    """run_resource_job 的收口：确认才 completed，没确认就 submitted。"""

    def setUp(self):
        resource_service.resource_job_running.clear()
        resource_service.resource_job_cancel_requested.clear()

    def _job(self, job_id=900):
        return {
            "id": job_id,
            "resource_id": 0,
            "title": "光阴之外",
            "link_url": "https://pan.quark.cn/s/abc123",
            "link_type": "quark",
            "folder_id": "target-cid",
            "savepath": "115连载中/光阴之外 (2025)/Season 01",
            "monitor_task_name": "",
            "refresh_delay_seconds": 0,
            "auto_refresh": False,
            "extra": {},
            "extra_json": json.dumps(
                {
                    "selected_entries": [
                        {"id": "f1", "name": "S01E42.mp4", "size": 1024, "is_dir": False}
                    ],
                    "selected_ids": ["f1"],
                }
            ),
        }

    async def _run(self, landing, job_id=900):
        provider = ScriptedWaitProvider()
        updates = []
        with mock.patch.object(resource_service, "get_resource_job", return_value=self._job(job_id)), \
                mock.patch.object(resource_service, "get_resource_item", return_value={}), \
                mock.patch.object(
                    resource_service, "get_config", return_value={"provider_enabled": {"quark": True}}
                ), \
                mock.patch.object(
                    resource_service, "_get_share_receive_provider_by_link_type", return_value=provider
                ), \
                mock.patch.object(
                    resource_service,
                    "_submit_provider_share_receive_job",
                    return_value={
                        "response": {"data": {"task_id": "t1"}},
                        "selection": {"selected_entries": [{"id": "f1", "name": "S01E42.mp4", "size": 1024}]},
                    },
                ), \
                mock.patch.object(
                    resource_service,
                    "update_resource_job",
                    side_effect=lambda job_id, **fields: updates.append((job_id, fields)),
                ), \
                mock.patch.object(
                    resource_service, "_wait_share_receive_landing", new=mock.AsyncMock(return_value=landing)
                ), \
                mock.patch.object(resource_service, "release_process_memory"):
            await resource_service.run_resource_job(job_id)
        return updates

    async def test_confirmed_landing_marks_completed(self):
        updates = await self._run(
            {"supported": True, "confirmed": True, "skipped": False, "status": 2, "elapsed": 3.2}
        )
        final = [fields for _job_id, fields in updates if "status" in fields][-1]
        self.assertEqual(final["status"], "completed")
        self.assertIn("已确认落地", final["status_detail"])
        self.assertIn("finished_at", final)
        self.assertIn("share_landing", json.loads(final["extra_json"]))

    async def test_unconfirmed_landing_stays_submitted(self):
        updates = await self._run(
            {
                "supported": True,
                "confirmed": False,
                "skipped": False,
                "status": 0,
                "elapsed": 120.0,
                "reason": "等待落地超时",
            }
        )
        final = [fields for _job_id, fields in updates if "status" in fields][-1]
        self.assertEqual(final["status"], "submitted")
        self.assertIn("未确认落地", final["status_detail"])
        self.assertNotIn("finished_at", final)

    async def test_directory_confirmation_is_written_to_detail(self):
        updates = await self._run(
            {
                "supported": True,
                "confirmed": True,
                "skipped": False,
                "status": 0,
                "elapsed": 45.0,
                "directory_confirmed": True,
            }
        )
        final = [fields for _job_id, fields in updates if "status" in fields][-1]
        self.assertEqual(final["status"], "completed")
        self.assertIn("已在目标目录确认落地", final["status_detail"])

    async def test_provider_failure_marks_failed(self):
        updates = await self._run(
            {
                "supported": True,
                "confirmed": False,
                "failed": True,
                "skipped": False,
                "status": 3,
                "elapsed": 2.0,
                "reason": "空间不足",
            },
            job_id=901,
        )
        final = [fields for _job_id, fields in updates if "status" in fields][-1]
        self.assertEqual(final["status"], "failed")
        self.assertIn("空间不足", final["status_detail"])
        self.assertIn("finished_at", final)

    async def test_sync_provider_keeps_previous_behavior(self):
        updates = await self._run({"supported": False, "confirmed": True, "skipped": True}, job_id=902)
        final = [fields for _job_id, fields in updates if "status" in fields][-1]
        self.assertEqual(final["status"], "completed")
        # 115 那条路文案保持原样，不出现「已确认落地」
        self.assertNotIn("已确认落地", final["status_detail"])
        self.assertNotIn("share_landing", json.loads(final["extra_json"]))


class ShareLandingTimeoutConstantTest(unittest.TestCase):
    def test_constant_is_clamped(self):
        self.assertGreaterEqual(core.RESOURCE_SHARE_LANDING_TIMEOUT_SECONDS, 10)
        self.assertLessEqual(core.RESOURCE_SHARE_LANDING_TIMEOUT_SECONDS, 600)


class ShareLandingAmbiguousTaskTest(unittest.IsolatedAsyncioTestCase):
    """夸克没回 task_id 时不能当「同步完成」。

    那是本功能最隐蔽的假成功路径：以前返回 ok=True，调用方直接判「已确认落地」，
    连目录都不查，于是空目录也标完成。现在必须落到「按目录确认」。
    """

    def setUp(self):
        self.patcher = mock.patch.object(
            resource_service, "RESOURCE_SHARE_LANDING_POLL_CHUNK_SECONDS", 0.05
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_wait_share_save_without_task_id_is_ambiguous(self):
        from app.providers.quark import QuarkProvider

        outcome = QuarkProvider().wait_share_save("cookie", {"response": {"data": {}}})
        self.assertFalse(outcome["ok"])
        self.assertTrue(outcome["ambiguous"])
        self.assertEqual(outcome["status"], 0)

    async def test_ambiguous_outcome_confirmed_by_directory(self):
        provider = ScriptedWaitProvider(
            outcomes=[{"ok": False, "status": 0, "ambiguous": True, "reason": "没有 task_id"}],
            entries=EXPECTED,
        )
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="quark-cookie",
            bundle={"ok": True},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=1.0,
        )
        self.assertTrue(result["confirmed"])
        self.assertTrue(result.get("directory_confirmed"))
        self.assertFalse(result.get("failed"))

    async def test_ambiguous_with_empty_target_stays_unconfirmed(self):
        provider = ScriptedWaitProvider(
            outcomes=[{"ok": False, "status": 0, "ambiguous": True, "reason": "没有 task_id"}] * 8,
            entries=[],
        )
        result = await resource_service._wait_share_receive_landing(
            provider=provider,
            cookie="quark-cookie",
            bundle={"ok": True},
            target_cid="target-cid",
            expected=EXPECTED,
            total_timeout=0.2,
        )
        self.assertFalse(result["confirmed"])
        self.assertFalse(result.get("failed"))


if __name__ == "__main__":
    unittest.main()
