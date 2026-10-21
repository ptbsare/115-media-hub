"""本地暂存目录管理测试（跨盘复制暂存目录，2026-10-21 起为单目录模型）。

注意：本会话环境的 tempfile 自检被破坏，测试统一用 tempfile.mkdtemp()
显式指定 dir（mkdtemp 本身不触发自检），并在 tearDown 手动清理。
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from app.services import local_storage as ls


class TestPathNormalization(unittest.TestCase):
    def test_absolute_path_kept(self):
        self.assertEqual(ls._normalize_container_path("/app/storage"), "/app/storage")

    def test_trailing_slash_stripped(self):
        self.assertEqual(ls._normalize_container_path("/app/storage///"), "/app/storage")

    def test_duplicate_slashes_collapsed(self):
        self.assertEqual(ls._normalize_container_path("/app//storage"), "/app/storage")

    def test_backslash_converted(self):
        self.assertEqual(ls._normalize_container_path("\\app\\storage"), "/app/storage")

    def test_relative_path_rejected(self):
        self.assertEqual(ls._normalize_container_path("app/storage"), "")
        self.assertEqual(ls._normalize_container_path("./storage"), "")

    def test_empty_rejected(self):
        self.assertEqual(ls._normalize_container_path(""), "")
        self.assertEqual(ls._normalize_container_path(None), "")

    def test_root_kept(self):
        self.assertEqual(ls._normalize_container_path("/"), "/")


class TestDefaultTempDir(unittest.TestCase):
    def test_default_dir_under_config_volume(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            self.assertEqual(ls.default_temp_dir(), "/app/config/cross_copy_tmp")

    def test_config_dir_env_override(self):
        with patch.dict(os.environ, {"MEDIA_HUB_CONFIG_DIR": "/data/config"}):
            self.assertEqual(ls.default_storage_path(), "/data/config")
            self.assertEqual(ls.default_temp_dir(), "/data/config/cross_copy_tmp")


class TestResolveTempDir(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_empty_cfg_falls_back_to_default(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            self.assertEqual(ls.resolve_temp_dir({}), "/app/config/cross_copy_tmp")

    def test_new_field_wins(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            cfg = {
                "cross_copy_temp_dir": "/app/storage/cross_copy_tmp",
                # 旧字段同时存在时以新字段为准
                "local_storages": [{"id": "st-x", "path": "/app/storage", "enabled": True}],
                "cross_copy_temp_storage": "st-x",
                "cross_copy_temp_subdir": "my_tmp",
            }
            self.assertEqual(ls.resolve_temp_dir(cfg), "/app/storage/cross_copy_tmp")

    def test_new_field_normalized(self):
        cfg = {"cross_copy_temp_dir": "/app/storage//tmp///"}
        self.assertEqual(ls.resolve_temp_dir(cfg), "/app/storage/tmp")

    def test_invalid_new_field_ignored(self):
        """新字段是相对路径等非法值时忽略，走默认。"""
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            self.assertEqual(ls.resolve_temp_dir({"cross_copy_temp_dir": "relative/dir"}), "/app/config/cross_copy_tmp")

    def test_legacy_storage_id_migrated(self):
        """旧配置：位置 id + 子目录合成实际路径。"""
        cfg = {
            "local_storages": [{"id": "st-x", "path": "/app/storage", "enabled": True}],
            "cross_copy_temp_storage": "st-x",
            "cross_copy_temp_subdir": "my_tmp",
        }
        self.assertEqual(ls.resolve_temp_dir(cfg), "/app/storage/my_tmp")

    def test_legacy_default_subdir(self):
        cfg = {
            "local_storages": [{"id": "st-x", "path": "/app/storage", "enabled": True}],
            "cross_copy_temp_storage": "st-x",
        }
        self.assertEqual(ls.resolve_temp_dir(cfg), "/app/storage/cross_copy_tmp")

    def test_legacy_unknown_id_falls_back_default(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            cfg = {"local_storages": [], "cross_copy_temp_storage": "st-gone"}
            self.assertEqual(ls.resolve_temp_dir(cfg), "/app/config/cross_copy_tmp")

    def test_legacy_list_not_a_list(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            cfg = {"local_storages": {"bad": True}, "cross_copy_temp_storage": "st-x"}
            self.assertEqual(ls.resolve_temp_dir(cfg), "/app/config/cross_copy_tmp")

    def test_create_makes_dir(self):
        target = os.path.join(self.tmp, "cross_copy_tmp")
        ls.resolve_cross_copy_temp_dir({"cross_copy_temp_dir": target}, create=True)
        self.assertTrue(os.path.isdir(target))


class TestProbe(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_existing_writable_dir(self):
        probe = ls.probe_storage_path(self.tmp)
        self.assertTrue(probe["ok"])
        self.assertTrue(probe["exists"])
        self.assertTrue(probe["writable"])
        self.assertGreater(probe["free_bytes"], 0)
        self.assertEqual(probe["error"], "")

    def test_missing_dir(self):
        probe = ls.probe_storage_path(os.path.join(self.tmp, "nope"))
        self.assertFalse(probe["ok"])
        self.assertFalse(probe["exists"])
        self.assertIn("不存在", probe["error"])

    def test_empty_path(self):
        probe = ls.probe_storage_path("")
        self.assertFalse(probe["ok"])
        self.assertIn("不能为空", probe["error"])

    def test_probe_file_cleaned_up(self):
        ls.probe_storage_path(self.tmp)
        leftovers = [n for n in os.listdir(self.tmp) if n.startswith(".wb_storage_probe")]
        self.assertEqual(leftovers, [])

    def test_readonly_dir_not_writable(self):
        ro = os.path.join(self.tmp, "ro")
        os.makedirs(ro)
        os.chmod(ro, 0o500)
        try:
            probe = ls.probe_storage_path(ro)
            self.assertFalse(probe["ok"])
            self.assertIn("不可写", probe["error"])
        finally:
            os.chmod(ro, 0o700)


class TestEnsureFreeSpace(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_ok(self):
        ls.ensure_free_space(self.tmp, 1024)

    def test_insufficient(self):
        with self.assertRaises(ValueError) as ctx:
            ls.ensure_free_space(self.tmp, 10 ** 18)
        msg = str(ctx.exception)
        self.assertIn("空间不足", msg)
        self.assertIn(self.tmp, msg)

    def test_zero_noop(self):
        ls.ensure_free_space(self.tmp, 0)


class TestConfigNormalization(unittest.TestCase):
    """core 的配置归一化：只保留 cross_copy_temp_dir，旧字段一次性迁移后清除。"""

    def _normalize(self, cfg):
        from app import core

        merged = dict(cfg)
        core._normalize_cross_copy_temp_dir_config(merged)
        return merged

    def test_new_field_kept(self):
        merged = self._normalize({"cross_copy_temp_dir": "/app/storage/cross_copy_tmp"})
        self.assertEqual(merged["cross_copy_temp_dir"], "/app/storage/cross_copy_tmp")
        self.assertNotIn("local_storages", merged)
        self.assertNotIn("cross_copy_temp_storage", merged)
        self.assertNotIn("cross_copy_temp_subdir", merged)

    def test_legacy_migrated_and_cleared(self):
        merged = self._normalize({
            "local_storages": [{"id": "st-x", "path": "/app/storage", "enabled": True}],
            "cross_copy_temp_storage": "st-x",
            "cross_copy_temp_subdir": "my_tmp",
        })
        self.assertEqual(merged["cross_copy_temp_dir"], "/app/storage/my_tmp")
        self.assertNotIn("local_storages", merged)
        self.assertNotIn("cross_copy_temp_storage", merged)
        self.assertNotIn("cross_copy_temp_subdir", merged)

    def test_empty_stays_empty(self):
        merged = self._normalize({})
        self.assertEqual(merged["cross_copy_temp_dir"], "")

    def test_invalid_new_field_rejected(self):
        merged = self._normalize({"cross_copy_temp_dir": "relative/dir"})
        self.assertEqual(merged["cross_copy_temp_dir"], "")

    def test_invalid_new_field_with_legacy_falls_to_legacy(self):
        merged = self._normalize({
            "cross_copy_temp_dir": "relative/dir",
            "local_storages": [{"id": "st-x", "path": "/app/storage", "enabled": True}],
            "cross_copy_temp_storage": "st-x",
        })
        self.assertEqual(merged["cross_copy_temp_dir"], "/app/storage/cross_copy_tmp")


class TestEngineTempDirHook(unittest.TestCase):
    """复制引擎的临时目录解析与空间预检钩子。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_resolve_temp_dir_from_cfg(self):
        from app.services import cross_cloud_copy as xcc

        cfg = {"cross_copy_temp_dir": self.tmp}
        self.assertEqual(xcc.resolve_temp_dir(cfg), self.tmp)

    def test_resolve_temp_dir_empty_cfg_falls_back_default(self):
        """空 cfg（非 None）按默认位置解析。"""
        from app.services import cross_cloud_copy as xcc

        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            self.assertEqual(xcc.resolve_temp_dir({}), "/app/config/cross_copy_tmp")

    def test_resolve_temp_dir_none_cfg_returns_empty(self):
        """cfg=None 表示调用方未接入配置，保持空（用系统临时目录）。"""
        from app.services import cross_cloud_copy as xcc

        self.assertEqual(xcc.resolve_temp_dir(None), "")

    def test_precheck_small_file_skipped(self):
        from app.services import cross_cloud_copy as xcc

        # 小文件（低于内存阈值）不做磁盘预检
        xcc._precheck_disk_space("/nonexistent-dir-xyz", 1024)

    def test_precheck_large_file_insufficient_space(self):
        from app.services import cross_cloud_copy as xcc

        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            xcc._precheck_disk_space(self.tmp, 10 ** 18)
        self.assertIn("空间不足", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
