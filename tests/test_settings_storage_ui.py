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
from pathlib import Path

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

    def test_get_includes_candidates_and_scan_flag(self):
        """读取接口要带上候选目录列表，前端才有的可选。"""
        with self._patched({}):
            resp = self.client.get("/storage/temp-dir")
        data = resp.json()
        self.assertIsInstance(data["candidates"], list)
        self.assertIn("scan_supported", data)
        self.assertIn("tier", data)
        self.assertIn("warning", data)

    def _fake_mounts(self, mount_root):
        return [
            {"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True},
            {"path": mount_root, "fs_type": "ext4", "device": "/dev/sdb1", "is_container_layer": False},
        ]

    def test_save_with_create_builds_subdir_inside_mount(self):
        """挂载点是空盘时，create=true 直接把子目录建出来。"""
        mount_root = os.path.join(self.tmp, "disk")
        os.makedirs(mount_root)
        target = os.path.join(mount_root, "cross_copy_tmp")
        with mock.patch.object(ls, "read_mount_points", return_value=self._fake_mounts(mount_root)), \
                self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": target, "create": True})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(os.path.isdir(target))
        self.assertTrue(resp.json()["probe"]["created"])
        self.assertEqual(save_mock.call_args[0][0]["cross_copy_temp_dir"], target)

    def test_save_without_create_flag_rejects_missing_dir(self):
        mount_root = os.path.join(self.tmp, "disk2")
        os.makedirs(mount_root)
        target = os.path.join(mount_root, "cross_copy_tmp")
        with mock.patch.object(ls, "read_mount_points", return_value=self._fake_mounts(mount_root)), \
                self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": target})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(os.path.exists(target))
        self.assertEqual(save_mock.call_count, 0)

    def test_save_refuses_to_create_in_container_layer(self):
        """手填一个容器临时层里的不存在的路径，不能被悄悄建出来。"""
        target = os.path.join(self.tmp, "stray", "cross_copy_tmp")
        # 只挂载了 /（容器可写层），tmp 下面的目录都不算挂载盘
        mounts = [{"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True}]
        with mock.patch.object(ls, "read_mount_points", return_value=mounts), \
                self._patched({}) as save_mock:
            resp = self.client.post("/storage/temp-dir", json={"path": target, "create": True})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("不是挂载进来的磁盘", resp.json()["msg"])
        self.assertFalse(os.path.exists(target))
        self.assertEqual(save_mock.call_count, 0)

    def test_save_container_layer_returns_warning(self):
        """存到容器临时层能成功，但必须回一句风险提示。"""
        stray = os.path.join(self.tmp, "stray2")
        os.makedirs(stray)
        mounts = [{"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True}]
        with mock.patch.object(ls, "read_mount_points", return_value=mounts), self._patched({}):
            resp = self.client.post("/storage/temp-dir", json={"path": stray})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["tier"], "container_layer")
        self.assertIn("临时层", data["warning"])

    def test_old_location_endpoints_gone(self):
        """旧的位置管理端点已下线，应返回 404/405。"""
        with self._patched({}):
            self.assertIn(self.client.get("/storage/locations").status_code, (404, 405))
            self.assertIn(
                self.client.post("/storage/locations/add", json={"path": self.tmp}).status_code,
                (404, 405),
            )


class StorageCandidateUiTest(unittest.TestCase):
    """暂存目录从「手填路径」改成「选挂载点」的前端结构回归。"""

    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        self.template = (root / "templates/partials/pages/settings.html").read_text(encoding="utf-8")
        self.module = (root / "static/js/modules/tabs/settings.js").read_text(encoding="utf-8")
        self.css = (root / "static/css/index.css").read_text(encoding="utf-8")

    def test_template_has_candidate_list_and_warning(self):
        self.assertIn('id="settings-storage-candidates"', self.template)
        self.assertIn('id="settings-storage-warning"', self.template)
        self.assertIn('onclick="rescanStorageCandidates()"', self.template)

    def test_manual_input_kept_as_folded_fallback(self):
        """手填还在，但收进「扫描不到」的折叠区，不再是主路径。"""
        self.assertIn("settings-storage-manual", self.template)
        self.assertIn("扫描不到要用的目录", self.template)
        self.assertIn('onclick="saveStorageTempDirFromInput()"', self.template)

    def test_module_renders_and_selects_candidates(self):
        self.assertIn("function renderStorageCandidates", self.module)
        self.assertIn("window.selectStorageCandidate = selectStorageCandidate", self.module)
        self.assertIn("window.rescanStorageCandidates = rescanStorageCandidates", self.module)
        self.assertIn("window.saveStorageTempDirFromInput = saveStorageTempDirFromInput", self.module)

    def test_module_sends_create_flag(self):
        """保存要带 create，挂载点是空盘时后端才能建子目录。"""
        self.assertIn("create: createMissing", self.module)

    def test_module_degrades_gracefully_without_mount_scan(self):
        self.assertIn("scanSupported", self.module)

    def test_css_covers_candidates_and_day_theme(self):
        self.assertIn(".settings-storage-candidate {", self.css)
        self.assertIn("html.theme-day .settings-storage-candidate {", self.css)
        self.assertIn("html.theme-day .settings-storage-warning", self.css)


if __name__ == "__main__":
    unittest.main()
