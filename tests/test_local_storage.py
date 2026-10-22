"""本地暂存目录管理测试（跨盘复制暂存目录，2026-10-21 起为单目录模型）。

临时目录用 ``tempfile.mkdtemp()`` 建、tearDown 里清；根目录由
``scripts/run_tests.py`` 指到本轮会话目录（系统临时区），不落进仓库。
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


class TestMountPoints(unittest.TestCase):
    """挂载点枚举：只列能存大文件的目录，系统挂载和单文件挂载要剔掉。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_mount_")
        self.config_dir = os.path.join(self.tmp, "config")
        self.storage_dir = os.path.join(self.tmp, "storage")
        os.makedirs(self.config_dir)
        os.makedirs(self.storage_dir)
        # 单文件挂载（模拟 /etc/hosts），必须被 isdir 过滤掉
        self.host_file = os.path.join(self.tmp, "hosts")
        with open(self.host_file, "w", encoding="utf-8") as fh:
            fh.write("127.0.0.1 localhost\n")
        self.mounts_file = os.path.join(self.tmp, "mounts")
        lines = [
            "overlay / overlay rw,relatime 0 0",
            "proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0",
            "sysfs /sys sysfs ro,nosuid,nodev,noexec 0 0",
            "tmpfs /tmp tmpfs rw,nosuid,nodev 0 0",
            f"/dev/sda1 {self.config_dir} ext4 rw,relatime 0 0",
            f"/dev/sdb1 {self.storage_dir} ext4 rw,relatime 0 0",
            f"/dev/vda1 {self.host_file} ext4 rw,relatime 0 0",
        ]
        with open(self.mounts_file, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _paths(self):
        return [item["path"] for item in ls.read_mount_points(self.mounts_file)]

    def test_system_filesystems_filtered(self):
        paths = self._paths()
        self.assertNotIn("/proc", paths)
        self.assertNotIn("/sys", paths)
        self.assertNotIn("/tmp", paths)

    def test_single_file_mount_filtered(self):
        self.assertNotIn(self.host_file, self._paths())

    def test_real_dir_mounts_kept(self):
        paths = self._paths()
        self.assertIn(self.config_dir, paths)
        self.assertIn(self.storage_dir, paths)

    def test_overlay_root_flagged_as_container_layer(self):
        root = next(item for item in ls.read_mount_points(self.mounts_file) if item["path"] == "/")
        self.assertTrue(root["is_container_layer"])

    def test_missing_mounts_file_returns_none(self):
        self.assertIsNone(ls.read_mount_points(os.path.join(self.tmp, "no-mounts")))

    def test_escaped_space_unescaped(self):
        spaced = os.path.join(self.tmp, "my disk")
        os.makedirs(spaced)
        escaped = spaced.replace(" ", "\\040")
        extra = os.path.join(self.tmp, "mounts2")
        with open(extra, "w", encoding="utf-8") as fh:
            fh.write(f"/dev/sdc1 {escaped} ext4 rw,relatime 0 0\n")
        self.assertIn(spaced, [item["path"] for item in ls.read_mount_points(extra)])


class TestStorageTier(unittest.TestCase):
    """层级判断：挂载盘 / 容器可写层 / 判断不了。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_tier_")
        self.storage_dir = os.path.join(self.tmp, "storage")
        os.makedirs(self.storage_dir)
        self.mounts = [
            {"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True},
            {"path": self.storage_dir, "fs_type": "ext4", "device": "/dev/sdb1", "is_container_layer": False},
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_inside_mount_is_mount(self):
        target = os.path.join(self.storage_dir, "cross_copy_tmp")
        self.assertEqual(ls.detect_storage_tier(target, self.mounts), "mount")

    def test_under_overlay_root_is_container_layer(self):
        self.assertEqual(ls.detect_storage_tier("/etc/whatever", self.mounts), "container_layer")

    def test_empty_mount_list_is_container_layer(self):
        self.assertEqual(ls.detect_storage_tier("/whatever", []), "container_layer")

    def test_unsupported_env_is_unknown(self):
        with patch.object(ls, "read_mount_points", return_value=None):
            self.assertEqual(ls.detect_storage_tier("/whatever"), "unknown")

    def test_bare_metal_root_counts_as_mount(self):
        """裸机上 / 是真实磁盘，不能当成容器临时层。"""
        mounts = [{"path": "/", "fs_type": "ext4", "device": "/dev/vda1", "is_container_layer": False}]
        self.assertEqual(ls.detect_storage_tier("/home/u/tmp", mounts), "mount")

    def test_warning_only_for_container_layer(self):
        self.assertTrue(ls.storage_tier_warning("container_layer"))
        self.assertEqual(ls.storage_tier_warning("mount"), "")
        self.assertEqual(ls.storage_tier_warning("unknown"), "")


class TestProbeCreateMissing(unittest.TestCase):
    """自动建子目录：只在已挂载的盘里建，绝不在容器临时层里建。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_create_")
        self.storage_dir = os.path.join(self.tmp, "storage")
        os.makedirs(self.storage_dir)
        self.mounts = [
            {"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True},
            {"path": self.storage_dir, "fs_type": "ext4", "device": "/dev/sdb1", "is_container_layer": False},
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_creates_subdir_inside_mounted_disk(self):
        target = os.path.join(self.storage_dir, "cross_copy_tmp")
        with patch.object(ls, "read_mount_points", return_value=self.mounts):
            probe = ls.probe_storage_path(target, create_missing=True)
        self.assertTrue(probe["ok"])
        self.assertTrue(probe["created"])
        self.assertTrue(os.path.isdir(target))
        self.assertEqual(probe["tier"], "mount")

    def test_refuses_to_create_in_container_layer(self):
        target = os.path.join(self.tmp, "stray", "cross_copy_tmp")
        with patch.object(ls, "read_mount_points", return_value=self.mounts):
            probe = ls.probe_storage_path(target, create_missing=True)
        self.assertFalse(probe["ok"])
        self.assertIn("不是挂载进来的磁盘", probe["error"])
        self.assertFalse(os.path.exists(target))

    def test_host_path_not_silently_recreated(self):
        """/mnt/hdd 这种宿主机路径不能在容器里凭空造出来。"""
        with patch.object(ls, "read_mount_points", return_value=self.mounts):
            probe = ls.probe_storage_path("/mnt/hdd-does-not-exist", create_missing=True)
        self.assertFalse(probe["ok"])
        self.assertFalse(os.path.exists("/mnt/hdd-does-not-exist"))

    def test_missing_dir_keeps_old_error_without_create_flag(self):
        target = os.path.join(self.storage_dir, "nope")
        probe = ls.probe_storage_path(target)
        self.assertFalse(probe["ok"])
        self.assertIn("不存在", probe["error"])
        self.assertFalse(probe["created"])
        self.assertFalse(os.path.exists(target))

    def test_unsupported_env_allows_create(self):
        """读不到挂载表（非 Linux 环境）时不拦，否则本地开发没法用。"""
        target = os.path.join(self.tmp, "devbox", "tmp")
        with patch.object(ls, "read_mount_points", return_value=None):
            probe = ls.probe_storage_path(target, create_missing=True)
        self.assertTrue(probe["ok"])
        self.assertTrue(probe["created"])


class TestListStorageCandidates(unittest.TestCase):
    """候选列表：挂载点直接可点，根目录不进列表，老配置也能看见。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb_ls_cand_")
        self.config_dir = os.path.join(self.tmp, "config")
        self.storage_dir = os.path.join(self.tmp, "storage")
        os.makedirs(self.config_dir)
        os.makedirs(self.storage_dir)
        self.mounts = [
            {"path": "/", "fs_type": "overlay", "device": "overlay", "is_container_layer": True},
            {"path": self.config_dir, "fs_type": "ext4", "device": "/dev/sda1", "is_container_layer": False},
            {"path": self.storage_dir, "fs_type": "ext4", "device": "/dev/sdb1", "is_container_layer": False},
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _listing(self, cfg=None):
        with patch.object(ls, "read_mount_points", return_value=self.mounts), \
                patch.object(ls, "default_storage_path", return_value=self.config_dir):
            return ls.list_storage_candidates(cfg or {})

    def test_root_is_not_a_candidate(self):
        self.assertNotIn("/", [item["path"] for item in self._listing()["candidates"]])

    def test_mounts_listed_with_subdir(self):
        paths = [item["path"] for item in self._listing()["candidates"]]
        self.assertIn(os.path.join(self.config_dir, "cross_copy_tmp"), paths)
        self.assertIn(os.path.join(self.storage_dir, "cross_copy_tmp"), paths)

    def test_default_candidate_first(self):
        listing = self._listing()
        self.assertTrue(listing["candidates"][0]["is_default"])
        self.assertEqual(listing["default_dir"], os.path.join(self.config_dir, "cross_copy_tmp"))
        self.assertEqual(listing["tier"], "mount")

    def test_scan_supported_flag(self):
        self.assertTrue(self._listing()["supported"])
        with patch.object(ls, "read_mount_points", return_value=None):
            self.assertFalse(ls.list_storage_candidates({})["supported"])

    def test_stray_current_dir_kept_and_flagged(self):
        """老配置指向非挂载点目录时也要列出来，并标成容器临时层。"""
        stray = os.path.join(self.tmp, "stray")
        os.makedirs(stray)
        listing = self._listing({"cross_copy_temp_dir": stray})
        self.assertIn(stray, [item["path"] for item in listing["candidates"]])
        self.assertEqual(listing["tier"], "container_layer")


if __name__ == "__main__":
    unittest.main()
