"""跨盘补源成功 = 订阅更新成功，通知文案与去重的测试。

补源以前每个文件发一条「跨盘复制完成」，正文的「文件」还是片名，补 5 集
收到 5 条一模一样的消息。现在并入订阅那一条，这里把口径钉住：
- 标题与同盘导入一致（订阅更新成功），多一行「来源：跨盘补源（夸克 → 115）」
- 集数走同一张去重表（任务 + 保存路径 + 集数），重试不会重复提醒
- 整包补源解析不出集数时不编数字，写「整包补入 N 个文件」
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest import mock

from app import db
from app.services import notify


TV_TASK = {
    "name": "光阴之外",
    "title": "光阴之外",
    "media_type": "tv",
    "provider": "115",
}


class CrossCopySubscriptionNotifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_cc_notify_test_")
        self.original_db_path = db.DB_PATH
        self.original_db_ensured = db._DB_ENSURED
        db.DB_PATH = os.path.join(self.tmp, "test.db")
        db._DB_ENSURED = False
        db.ensure_db()
        self.cfg = {
            "notify_push_enabled": True,
            "notify_channel": "wecom_bot",
            "notify_wecom_webhook": "https://example.invalid/hook",
        }
        self.send_patcher = mock.patch.object(notify, "_send_notify_content", mock.MagicMock())
        self.send_mock = self.send_patcher.start()

    def tearDown(self):
        self.send_patcher.stop()
        db.DB_PATH = self.original_db_path
        db._DB_ENSURED = self.original_db_ensured
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _push(self, task=None, **overrides):
        payload = dict(
            episodes=[42, 43, 44],
            savepath="影视/光阴之外 (2025)/Season 01",
            job_ids=[11, 12, 13],
            file_count=3,
            source_label="夸克网盘",
            target_label="115网盘",
            next_episode=44,
            total_episodes=60,
        )
        payload.update(overrides)
        return asyncio.run(
            notify.push_cross_copy_subscription_notification(
                cfg=self.cfg,
                task=dict(task or TV_TASK),
                **payload,
            )
        )

    def _content(self):
        self.assertTrue(self.send_mock.called)
        return self.send_mock.call_args[0][1]

    def test_markdown_reads_like_subscription_success(self):
        result = self._push()

        self.assertTrue(result["pushed"])
        content = self._content()
        self.assertIn("### 订阅更新成功", content)
        self.assertIn("任务：光阴之外（电视剧）", content)
        self.assertIn("来源：跨盘补源（夸克网盘 → 115网盘）", content)
        self.assertIn("E42、E43、E44", content)
        self.assertIn("共 3 集", content)
        self.assertIn("当前进度：E44 / 60", content)
        self.assertIn("影视/光阴之外 (2025)/Season 01", content)
        self.assertIn("#11、#12、#13", content)
        # 不能再出现每文件一条那种「跨盘复制完成」的标题
        self.assertNotIn("跨盘复制完成", content)

    def test_episodes_are_deduped_across_runs(self):
        first = self._push()
        second = self._push()

        self.assertTrue(first["pushed"])
        self.assertFalse(second["pushed"])
        self.assertEqual(second["reason"], "deduped")
        self.assertEqual(self.send_mock.call_count, 1)

    def test_partial_overlap_only_reports_new_episodes(self):
        self._push(episodes=[42])
        self.send_mock.reset_mock()

        result = self._push(episodes=[42, 43])

        self.assertTrue(result["pushed"])
        self.assertEqual(result["episodes"], [43])
        content = self._content()
        self.assertIn("E43", content)
        self.assertNotIn("E42", content)
        self.assertIn("共 1 集", content)

    def test_disabled_push_switch_sends_nothing(self):
        self.cfg["notify_push_enabled"] = False

        result = self._push()

        self.assertFalse(result["pushed"])
        self.assertEqual(result["reason"], "disabled")
        self.send_mock.assert_not_called()

    def test_folder_copy_without_episodes_does_not_invent_numbers(self):
        result = self._push(episodes=[], job_ids=[11], file_count=5, next_episode=0, total_episodes=0)

        self.assertTrue(result["pushed"])
        content = self._content()
        self.assertIn("整包补入 5 个条目", content)
        self.assertIn("进度以目录扫描为准", content)
        self.assertNotIn("当前进度", content)

    def test_movie_copy_is_stated_as_movie(self):
        movie_task = {"name": "流浪地球", "title": "流浪地球", "media_type": "movie"}

        self._push(task=movie_task, episodes=[], file_count=1, next_episode=0, total_episodes=0)

        content = self._content()
        self.assertIn("流浪地球（电影）", content)
        self.assertIn("电影资源已通过跨盘补源入库", content)


if __name__ == "__main__":
    unittest.main()
