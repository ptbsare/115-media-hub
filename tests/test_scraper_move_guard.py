"""跨网盘移动禁用：/scraper/{provider}/move 的来源网盘校验。

跨网盘移动等于「复制 + 删源」，删源一旦出错无法回滚，所以只开放同盘移动。
前端点「移动」跨盘时按钮已经禁用，这里测后端也拦得住（脚本或旧页面绕过前端
直接打接口的情况）。
"""

import unittest
from unittest import mock

from fastapi.testclient import TestClient

from app.core import require_auth
from app.main import app
from app.routes import scraper as scraper_routes


class ScraperMoveGuardTest(unittest.TestCase):
    def setUp(self):
        app.dependency_overrides[require_auth] = lambda: True
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(require_auth, None)

    def test_cross_cloud_move_is_rejected_before_service_call(self):
        with mock.patch.object(scraper_routes, "move_scraper_entries") as move_mock:
            response = self.client.post(
                "/scraper/115/move",
                json={
                    "entry_ids": ["abc123"],
                    "source_cid": "100",
                    "source_provider": "quark",
                    "target_cid": "200",
                },
            )
        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertIn("移动只能在同一个网盘内使用", body["msg"])
        self.assertIn("跨网盘请用复制", body["msg"])
        # 关键：必须在调用移动服务之前就拦下，不能先动了文件再报错
        move_mock.assert_not_called()

    def test_same_cloud_move_still_works(self):
        with mock.patch.object(
            scraper_routes, "move_scraper_entries", return_value={"ok": True, "moved": []}
        ) as move_mock:
            response = self.client.post(
                "/scraper/115/move",
                json={
                    "entry_ids": ["abc123"],
                    "source_cid": "100",
                    "source_provider": "115",
                    "target_cid": "200",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        move_mock.assert_called_once()

    def test_missing_source_provider_keeps_old_behaviour(self):
        """老调用方（CLI / 旧页面）不带 source_provider 时按原来的方式放行。"""
        with mock.patch.object(
            scraper_routes, "move_scraper_entries", return_value={"ok": True, "moved": []}
        ) as move_mock:
            response = self.client.post(
                "/scraper/115/move",
                json={"entry_ids": ["abc123"], "source_cid": "100", "target_cid": "200"},
            )
        self.assertEqual(response.status_code, 200)
        move_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
