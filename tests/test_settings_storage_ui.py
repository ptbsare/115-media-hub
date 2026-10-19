"""设置页「本地存储位置」接口测试。

覆盖设置页存储位置分区用到的路由：列表、新增（非法路径/重复路径）、删除后
清空暂存引用、路径探测失败、暂存位置写回。配置读写全部 mock，不碰真实
config.json；磁盘探测用临时目录，避免依赖运行环境。
"""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from contextlib import contextmanager

from fastapi.testclient import TestClient

from app.core import require_auth
from app.main import app
from app.routes import settings as settings_routes
from app.services import local_storage as ls


class StorageLocationsApiTest(unittest.TestCase):
    def setUp(self):
        # 仓库里其它测试也用 mkdtemp 显式指定 dir，避免 tempfile 自检问题
        self.tmp = tempfile.mkdtemp(prefix="wb_settings_storage_")
        self.storage = ls.normalize_local_storage_entry(
            {"path": self.tmp, "label": "测试盘", "enabled": True}
        )
        self.cfg = {
            "local_storages": [self.storage],
            "cross_copy_temp_storage": self.storage["id"],
            "cross_copy_temp_subdir": "cross_copy_tmp",
        }
        # 默认位置指到同一个目录，列表里就不会额外冒出一条不可用的 /app/config
        self.default_path_patch = mock.patch.object(ls, "default_storage_path", return_value=self.tmp)
        self.default_path_patch.start()
        app.dependency_overrides[require_auth] = lambda: True
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.pop(require_auth, None)
        self.default_path_patch.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    @contextmanager
    def _patched(self, cfg=None):
        """把 get_config/save_config 换成内存实现，产出 save_config 的 mock。"""
        current = cfg if isinstance(cfg, dict) else self.cfg
        with mock.patch.object(settings_routes, "get_config", return_value=current), \
                mock.patch.object(settings_routes, "save_config") as save_mock:
            yield save_mock

    def test_list_returns_locations_and_temp_fields(self):
        with self._patched() as save_mock:
            resp = self.client.get("/storage/locations")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["cross_copy_temp_storage"], self.storage["id"])
        self.assertEqual(data["cross_copy_temp_subdir"], "cross_copy_tmp")
        self.assertEqual(len(data["locations"]), 1)
        location = data["locations"][0]
        for key in (
            "id",
            "label",
            "path",
            "enabled",
            "exists",
            "writable",
            "free_bytes",
            "total_bytes",
            "ok",
            "error",
        ):
            self.assertIn(key, location)
        self.assertEqual(location["path"], self.tmp)
        self.assertTrue(location["exists"])
        self.assertTrue(location["writable"])
        self.assertGreater(location["free_bytes"], 0)
        self.assertTrue(save_mock.call_count == 0)

    def test_add_rejects_invalid_path(self):
        with self._patched() as save_mock:
            resp = self.client.post("/storage/locations/add", json={"path": "relative/dir", "label": "坏路径"})
        self.assertEqual(resp.status_code, 400)
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertIn("路径不合法", data["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_add_rejects_duplicate_path(self):
        with self._patched() as save_mock:
            resp = self.client.post("/storage/locations/add", json={"path": self.tmp, "label": "重复"})
        self.assertEqual(resp.status_code, 400)
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertIn("已添加过", data["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_add_rejects_missing_dir(self):
        missing = os.path.join(self.tmp, "no-such-dir")
        with self._patched() as save_mock:
            resp = self.client.post("/storage/locations/add", json={"path": missing, "label": "不存在"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("不存在", resp.json()["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_delete_clears_temp_reference(self):
        with self._patched() as save_mock:
            resp = self.client.post("/storage/locations/delete", json={"id": self.storage["id"]})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])
        self.assertEqual(save_mock.call_count, 1)
        saved = save_mock.call_args[0][0]
        self.assertEqual(saved["local_storages"], [])
        # 暂存位置正指向被删的位置时引用必须清空，否则复制会指向不存在的 id
        self.assertEqual(saved["cross_copy_temp_storage"], "")
        self.assertEqual(saved["cross_copy_temp_subdir"], "cross_copy_tmp")

    def test_probe_failure_reports_reason(self):
        missing = os.path.join(self.tmp, "no-such-dir")
        with self._patched():
            resp = self.client.post("/storage/locations/probe", json={"path": missing})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertFalse(data["probe"]["exists"])
        self.assertIn("不存在", data["probe"]["error"])

    def test_probe_invalid_path_returns_400(self):
        with self._patched():
            resp = self.client.post("/storage/locations/probe", json={"path": "relative"})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()["ok"])

    def test_temp_endpoint_saves_location_and_subdir(self):
        with self._patched() as save_mock:
            resp = self.client.post(
                "/storage/locations/temp",
                json={"storage_id": self.storage["id"], "subdir": "big_tmp"},
            )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["cross_copy_temp_storage"], self.storage["id"])
        self.assertEqual(data["cross_copy_temp_subdir"], "big_tmp")
        saved = save_mock.call_args[0][0]
        self.assertEqual(saved["cross_copy_temp_storage"], self.storage["id"])
        self.assertEqual(saved["cross_copy_temp_subdir"], "big_tmp")
        # 只写这两个字段，位置列表原样带过去
        self.assertEqual(saved["local_storages"], [self.storage])

    def test_temp_endpoint_empty_subdir_falls_back_to_default(self):
        with self._patched() as save_mock:
            resp = self.client.post(
                "/storage/locations/temp",
                json={"storage_id": self.storage["id"], "subdir": ""},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["cross_copy_temp_subdir"], "cross_copy_tmp")
        self.assertEqual(
            save_mock.call_args[0][0]["cross_copy_temp_subdir"], "cross_copy_tmp"
        )

    def test_temp_endpoint_rejects_traversal_subdir(self):
        with self._patched() as save_mock:
            resp = self.client.post(
                "/storage/locations/temp",
                json={"storage_id": self.storage["id"], "subdir": "../escape"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("子目录不合法", resp.json()["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_temp_endpoint_rejects_unknown_storage(self):
        with self._patched() as save_mock:
            resp = self.client.post(
                "/storage/locations/temp",
                json={"storage_id": "st-not-exist", "subdir": "cross_copy_tmp"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("不存在", resp.json()["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_temp_endpoint_allows_empty_storage_for_default(self):
        with self._patched() as save_mock:
            resp = self.client.post(
                "/storage/locations/temp",
                json={"storage_id": "", "subdir": "cross_copy_tmp"},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["cross_copy_temp_storage"], "")
        self.assertEqual(save_mock.call_args[0][0]["cross_copy_temp_storage"], "")


if __name__ == "__main__":
    unittest.main()
