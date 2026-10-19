"""本地存储位置管理测试（#8 存储位置管理）。

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


class TestSubdirNormalization(unittest.TestCase):
    def test_normal_subdir(self):
        self.assertEqual(ls._normalize_subdir("cross_copy_tmp"), "cross_copy_tmp")

    def test_nested_subdir(self):
        self.assertEqual(ls._normalize_subdir("a/b"), "a/b")

    def test_trailing_slash_stripped(self):
        self.assertEqual(ls._normalize_subdir("a/b/"), "a/b")

    def test_absolute_rejected(self):
        self.assertEqual(ls._normalize_subdir("/etc"), "")

    def test_traversal_rejected(self):
        self.assertEqual(ls._normalize_subdir("../etc"), "")
        self.assertEqual(ls._normalize_subdir("a/../../b"), "")

    def test_dot_segments_cleaned(self):
        self.assertEqual(ls._normalize_subdir("./a/./b"), "a/b")

    def test_empty_returns_empty(self):
        self.assertEqual(ls._normalize_subdir(""), "")


class TestEntryNormalization(unittest.TestCase):
    def test_basic_entry(self):
        entry = ls.normalize_local_storage_entry({"path": "/app/storage", "label": "机械硬盘"})
        self.assertIsNotNone(entry)
        self.assertEqual(entry["label"], "机械硬盘")
        self.assertTrue(entry["enabled"])
        self.assertTrue(entry["id"].startswith("st-"))

    def test_id_stable_for_same_path(self):
        e1 = ls.normalize_local_storage_entry({"path": "/app/storage"})
        e2 = ls.normalize_local_storage_entry({"path": "/app/storage/"})
        self.assertEqual(e1["id"], e2["id"])

    def test_invalid_path_dropped(self):
        self.assertIsNone(ls.normalize_local_storage_entry({"path": "relative"}))
        self.assertIsNone(ls.normalize_local_storage_entry({}))
        self.assertIsNone(ls.normalize_local_storage_entry(None))

    def test_dedup_by_id(self):
        storages = ls.normalize_local_storages(
            [
                {"path": "/app/storage", "label": "A"},
                {"path": "/app/storage", "label": "B"},
                {"path": "/app/storage2", "label": "C"},
            ]
        )
        self.assertEqual(len(storages), 2)
        self.assertEqual(storages[0]["label"], "A")

    def test_legacy_dict_form(self):
        storages = ls.normalize_local_storages({"path": "/app/storage"})
        self.assertEqual(len(storages), 1)

    def test_default_storage_injected(self):
        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            storages = ls.ensure_default_storage([])
            self.assertEqual(len(storages), 1)
            self.assertEqual(storages[0]["path"], "/app/config")

    def test_default_storage_not_duplicated(self):
        existing = [{"id": "st-x", "label": "x", "path": "/app/storage", "enabled": True}]
        with patch.object(ls, "default_storage_path", return_value="/app/storage"):
            storages = ls.ensure_default_storage(existing)
            self.assertEqual(len(storages), 1)


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


class TestResolveAndSpace(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _cfg_with_storage(self, path):
        entry = ls.normalize_local_storage_entry({"path": path, "label": "测试盘"})
        return {"local_storages": [entry], "cross_copy_temp_storage": entry["id"]}

    def test_resolve_creates_subdir(self):
        cfg = self._cfg_with_storage(self.tmp)
        target = ls.resolve_storage_dir(cfg, cfg["local_storages"][0]["id"], "cross_copy_tmp")
        self.assertEqual(target, os.path.join(self.tmp, "cross_copy_tmp"))
        self.assertTrue(os.path.isdir(target))

    def test_resolve_without_create(self):
        cfg = self._cfg_with_storage(self.tmp)
        target = ls.resolve_storage_dir(cfg, cfg["local_storages"][0]["id"], "xcopy", create=False)
        self.assertFalse(os.path.exists(target))

    def test_resolve_unknown_storage_raises(self):
        with self.assertRaises(ValueError):
            ls.resolve_storage_dir({"local_storages": []}, "st-nope", "sub")

    def test_resolve_empty_id_uses_default(self):
        with patch.object(ls, "default_storage_path", return_value=self.tmp):
            target = ls.resolve_storage_dir({}, "", "sub")
            self.assertEqual(target, os.path.join(self.tmp, "sub"))

    def test_resolve_cross_copy_temp_dir(self):
        cfg = self._cfg_with_storage(self.tmp)
        cfg["cross_copy_temp_subdir"] = "my_tmp"
        target = ls.resolve_cross_copy_temp_dir(cfg)
        self.assertEqual(target, os.path.join(self.tmp, "my_tmp"))

    def test_ensure_free_space_ok(self):
        ls.ensure_free_space(self.tmp, 1024)

    def test_ensure_free_space_insufficient(self):
        with self.assertRaises(ValueError) as ctx:
            ls.ensure_free_space(self.tmp, 10 ** 18)
        msg = str(ctx.exception)
        self.assertIn("空间不足", msg)
        self.assertIn(self.tmp, msg)

    def test_ensure_free_space_zero_noop(self):
        ls.ensure_free_space(self.tmp, 0)

    def test_list_volumes_with_state(self):
        cfg = self._cfg_with_storage(self.tmp)
        with patch.object(ls, "default_storage_path", return_value=self.tmp):
            volumes = ls.list_storage_volumes(cfg)
        # 配置里的路径即默认路径 → 不重复注入，仅一条
        self.assertEqual(len(volumes), 1)
        self.assertTrue(volumes[0]["ok"])
        self.assertEqual(volumes[0]["label"], "测试盘")


class TestConfigIntegration(unittest.TestCase):
    def test_normalize_via_core(self):
        from app import core

        with patch.object(ls, "default_storage_path", return_value="/app/config"):
            normalized = core._normalize_local_storages_config(
                [{"path": "/app/storage", "label": "HDD"}, {"path": "bad"}]
            )
        self.assertEqual([e["path"] for e in normalized], ["/app/config", "/app/storage"])

    def test_cross_copy_subdir_normalize(self):
        from app import core

        self.assertEqual(core._normalize_cross_copy_temp_subdir("x/y"), "x/y")
        self.assertEqual(core._normalize_cross_copy_temp_subdir("../evil"), "cross_copy_tmp")
        self.assertEqual(core._normalize_cross_copy_temp_subdir(""), "cross_copy_tmp")


class TestEngineTempDirHook(unittest.TestCase):
    """复制引擎的临时目录解析与空间预检钩子。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_test_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_resolve_temp_dir_from_cfg(self):
        from app.services import cross_cloud_copy as xcc

        entry = ls.normalize_local_storage_entry({"path": self.tmp})
        cfg = {"local_storages": [entry], "cross_copy_temp_storage": entry["id"]}
        self.assertEqual(xcc.resolve_temp_dir(cfg), os.path.join(self.tmp, "cross_copy_tmp"))

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
