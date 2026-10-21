"""设置页「本地存储位置」接口测试（2026-10-21 简化为单暂存目录模型）。

覆盖 GET/POST /storage/temp-dir：读取（含旧配置迁移）、保存合法路径、
拒绝相对路径、拒绝不存在目录、恢复默认。配置读写全部 mock，不碰真实
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


class StorageTempDirApiTest(unittest.TestCase):
    def setUp(self):
        # 仓库里其它测试也用 mkdtemp 显式指定 dir，避免 tempfile 自检问题
        self.tmp = tempfile.mkdtemp(prefix="wb_settings_storage_")
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
        current = cfg if isinstance(cfg, dict) else {}
        with mock.patch.object(settings_routes, "get_config", return_value=current), \
                mock.patch.object(settings_routes, "save_config") as save_mock:
            yield save_mock

    def test_get_returns_default_dir_and_probe(self):
        with self._patched({}) as save_mock:
            resp = self.client.get("/storage/temp-dir")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["dir"], os.path.join(self.tmp, "cross_copy_tmp"))
        self.assertEqual(data["default_dir"], os.path.join(self.tmp, "cross_copy_tmp"))
        self.assertTrue(data["is_default"])
        probe = data["probe"]
        for key in ("ok", "exists", "writable", "free_bytes", "total_bytes", "error"):
            self.assertIn(key, probe)
        # 默认目录不存在时探测应报不存在，但不影响接口返回
        self.assertFalse(probe["exists"])
        self.assertEqual(save_mock.call_count, 0)

    def test_get_returns_configured_dir(self):
        cfg = {"cross_copy_temp_dir": self.tmp}
        with self._patched(cfg):
            resp = self.client.get("/storage/temp-dir")
        data = resp.json()
        self.assertEqual(data["dir"], self.tmp)
        self.assertFalse(data["is_default"])
        self.assertTrue(data["probe"]["ok"])

    def test_get_migrates_legacy_config(self):
        """旧配置（位置 id + 子目录）读取时迁移成实际路径（GET 不落盘）。"""
        cfg = {
            "local_storages": [{"id": "st-x", "path": self.tmp, "label": "大盘", "enabled": True}],
            "cross_copy_temp_storage": "st-x",
            "cross_copy_temp_subdir": "legacy_tmp",
        }
        with self._patched(cfg) as save_mock:
            resp = self.client.get("/storage/temp-dir")
        data = resp.json()
        self.assertEqual(data["dir"], os.path.join(self.tmp, "legacy_tmp"))
        # GET 只读不写：不触发迁移落盘，is_default 反映新字段是否为空
        self.assertTrue(data["is_default"])
        self.assertEqual(save_mock.call_count, 0)

    def test_save_valid_path(self):
        with self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": self.tmp})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["dir"], self.tmp)
        self.assertFalse(data["is_default"])
        self.assertTrue(data["probe"]["ok"])
        saved = save_mock.call_args[0][0]
        self.assertEqual(saved["cross_copy_temp_dir"], self.tmp)

    def test_save_normalizes_path(self):
        with self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": self.tmp + "///"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["dir"], self.tmp)
        self.assertEqual(save_mock.call_args[0][0]["cross_copy_temp_dir"], self.tmp)

    def test_save_rejects_relative_path(self):
        with self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": "relative/dir"})
        self.assertEqual(resp.status_code, 400)
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertIn("路径不合法", data["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_save_rejects_missing_dir(self):
        missing = os.path.join(self.tmp, "no-such-dir")
        with self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": missing})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("不存在", resp.json()["msg"])
        self.assertEqual(save_mock.call_count, 0)

    def test_save_empty_path_restores_default(self):
        cfg = {"cross_copy_temp_dir": self.tmp}
        with self._patched(cfg) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": ""})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertTrue(data["is_default"])
        self.assertEqual(data["dir"], os.path.join(self.tmp, "cross_copy_tmp"))
        saved = save_mock.call_args[0][0]
        self.assertEqual(saved["cross_copy_temp_dir"], "")

    def test_save_empty_path_with_legacy_config_restores_default(self):
        """旧配置在恢复默认时也必须回到 config 卷默认目录，而不是迁移值。"""
        cfg = {
            "local_storages": [{"id": "st-x", "path": self.tmp, "enabled": True}],
            "cross_copy_temp_storage": "st-x",
        }
        with self._patched(cfg) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": ""})
        data = resp.json()
        self.assertTrue(data["is_default"])
        self.assertEqual(data["dir"], os.path.join(self.tmp, "cross_copy_tmp"))
        self.assertEqual(save_mock.call_args[0][0]["cross_copy_temp_dir"], "")

    def test_old_location_endpoints_gone(self):
        """旧的位置管理端点已下线，应返回 404/405。"""
        with self._patched({}):
            self.assertIn(self.client.get("/storage/locations").status_code, (404, 405))
            self.assertIn(
                self.client.post("/storage/locations/add", json={"path": self.tmp}).status_code,
                (404, 405),
            )


if __name__ == "__main__":
    unittest.main()
