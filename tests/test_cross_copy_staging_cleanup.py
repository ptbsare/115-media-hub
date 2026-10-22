"""跨盘补源中转目录（夸克）残留清理的单元测试。

重点不是「删得干净」，而是「不会删错」：
- 没过期的不删、认不出时间的不删、活跃任务引用的不删、活跃文件夹子树整片跳过；
- 只删文件 id，只删确认空的子目录，任务目录与中转根永不删；
- 读目录失败时按「有内容」处理，一个字节都不动。
"""

import time
import unittest
from unittest import mock

from app import core
from app.services import cross_copy_staging_cleanup as ccsc


DAY = 86400


def _ms(ts: float) -> str:
    """夸克给的是毫秒时间戳（字符串）。"""
    return str(int(ts * 1000))


class FakeQuark:
    """只实现清理链路用到的那几个方法。"""

    supports_delete = True
    name = "quark"
    label = "夸克网盘"

    def __init__(self, dirs=None, root_cid="root-cid", broken=None):
        self.dirs = dict(dirs or {})
        self.root_cid = root_cid
        self.broken = set(broken or [])
        self.deleted = []          # [(parent_cid, [ids])]
        self.list_calls = []
        self.cookie = "quark-cookie"

    def get_cookie(self, cfg=None):
        return self.cookie

    def resolve_folder_id_by_path(self, cookie, path):
        return self.root_cid

    def list_entries(self, cookie, cid="0"):
        self.list_calls.append(str(cid))
        if str(cid) in self.broken:
            raise RuntimeError("目录接口 403")
        return list(self.dirs.get(str(cid), []))

    def delete_entries(self, cookie, entry_ids, parent_id=""):
        self.deleted.append((str(parent_id), [str(item) for item in entry_ids]))
        return {"ids": list(entry_ids)}

    # 便捷断言
    def deleted_ids(self):
        return [entry_id for _, ids in self.deleted for entry_id in ids]


def _file(entry_id, name="S01E42.mp4", parent="task-1", age_days=10, size=1024):
    return {
        "id": entry_id,
        "name": name,
        "is_dir": False,
        "size": size,
        "parent_id": parent,
        "modified_at": _ms(time.time() - age_days * DAY),
    }


def _dir(entry_id, name="某剧 全季", parent="task-1"):
    return {"id": entry_id, "name": name, "is_dir": True, "size": 0, "parent_id": parent}


class StagingTimestampTest(unittest.TestCase):
    def test_epoch_seconds(self):
        self.assertAlmostEqual(ccsc.parse_staging_timestamp("1700000000"), 1700000000.0, places=1)

    def test_epoch_milliseconds(self):
        # 夸克实测就是这个量级：1792637476936 → 2026 年
        self.assertAlmostEqual(ccsc.parse_staging_timestamp("1792637476936"), 1792637476.936, places=1)

    def test_iso_string(self):
        self.assertAlmostEqual(
            ccsc.parse_staging_timestamp("2026-10-22T08:00:00"),
            time.mktime(time.strptime("2026-10-22 08:00:00", "%Y-%m-%d %H:%M:%S")),
            places=0,
        )

    def test_empty_and_garbage_return_none(self):
        for value in ("", "   ", None, "不知道", "-1", "0", float("nan")):
            self.assertIsNone(ccsc.parse_staging_timestamp(value), msg=repr(value))

    def test_entry_age_of_future_time_is_zero(self):
        entry = {"modified_at": _ms(time.time() + DAY)}
        self.assertEqual(ccsc.staging_entry_age_seconds(entry), 0.0)

    def test_entry_without_timestamp_is_unknown(self):
        self.assertIsNone(ccsc.staging_entry_age_seconds({"modified_at": ""}))
        self.assertIsNone(ccsc.staging_entry_age_seconds({}))


class CleanupOnceTest(unittest.TestCase):
    def setUp(self):
        self.provider = FakeQuark()
        self.cfg = {"cross_copy_staging_retention_days": 7}
        self.patchers = [
            mock.patch.object(ccsc, "get_config", side_effect=lambda: self.cfg),
            mock.patch.object(ccsc, "_get_provider_or_none", return_value=self.provider),
            mock.patch.object(ccsc, "_collect_active_protection", return_value=(set(), set())),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_disabled_does_not_touch_network(self):
        self.cfg = {"cross_copy_staging_retention_days": 0}
        result = ccsc.run_cross_copy_staging_cleanup_once()
        self.assertEqual(result["skipped"], "disabled")
        self.assertEqual(self.provider.list_calls, [])

    def test_missing_root_is_skipped(self):
        self.provider.root_cid = "0"
        result = ccsc.run_cross_copy_staging_cleanup_once()
        self.assertEqual(result["skipped"], "staging_root_missing")
        self.assertEqual(self.provider.list_calls, [])

    def test_expired_files_deleted_with_their_parent(self):
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_file("f-old"), _file("f-new", age_days=1)],
        }
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["task_dirs"], 1)
        self.assertEqual(result["expired_deleted"], 1)
        self.assertEqual(self.provider.deleted, [("task-1", ["f-old"])])
        self.assertEqual(result["kept_files"], 1)

    def test_unparseable_timestamp_is_never_deleted(self):
        stale = _file("f-unknown")
        stale["modified_at"] = ""
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [stale],
        }
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["expired_deleted"], 0)
        self.assertEqual(self.provider.deleted, [])

    def test_active_file_is_kept_even_when_expired(self):
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_file("f-active"), _file("f-idle")],
        }
        with mock.patch.object(ccsc, "_collect_active_protection", return_value=({"f-active"}, set())):
            ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(self.provider.deleted_ids(), ["f-idle"])

    def test_active_folder_subtree_is_skipped(self):
        # 文件夹任务正在逐文件复制：它引用的目录子树整体不动
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_dir("share-folder", name="某剧 全季"), _file("f-outside")],
            "share-folder": [_file("f-inside", parent="share-folder")],
        }
        with mock.patch.object(ccsc, "_collect_active_protection", return_value=(set(), {"share-folder"})):
            ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(self.provider.deleted_ids(), ["f-outside"])

    def test_empty_dirs_removed_deep_first_but_task_dir_kept(self):
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_dir("share-folder", name="某剧 全季"), _file("f-old")],
            "share-folder": [_file("f-old-2", parent="share-folder")],
        }
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["expired_deleted"], 2)
        self.assertEqual(result["empty_dirs_deleted"], 1)
        deleted_ids = self.provider.deleted_ids()
        self.assertIn("share-folder", deleted_ids)
        # 任务目录（以及中转根）永远不删：它是下轮补源复用的落点
        self.assertNotIn("task-1", deleted_ids)
        self.assertNotIn("root-cid", deleted_ids)

    def test_unreadable_dir_is_treated_as_non_empty(self):
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_dir("share-folder", name="某剧 全季")],
        }
        self.provider.broken = {"share-folder"}
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["empty_dirs_deleted"], 0)
        self.assertEqual(self.provider.deleted, [])

    def test_untouched_scattered_files_at_root(self):
        self.provider.dirs = {
            "root-cid": [_file("f-stray", parent="root-cid")],
        }
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["task_dirs"], 0)
        self.assertEqual(self.provider.deleted, [])

    def test_newly_active_folder_defers_deletion(self):
        # 算完过期集合之后才有新任务注册：本轮放弃删除，下一轮重算
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="光阴之外")],
            "task-1": [_file("f-old")],
        }
        with mock.patch.object(
            ccsc,
            "_collect_active_protection",
            side_effect=[(set(), set()), (set(), {"share-folder"})],
        ):
            result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["deferred"], 1)
        self.assertEqual(self.provider.deleted, [])

    def test_counters_are_additive_across_task_dirs(self):
        self.provider.dirs = {
            "root-cid": [_dir("task-1", name="A"), _dir("task-2", name="B")],
            "task-1": [_file("a-1")],
            "task-2": [_file("b-1", parent="task-2")],
        }
        result = ccsc.run_cross_copy_staging_cleanup_once()

        self.assertEqual(result["task_dirs"], 2)
        self.assertEqual(result["expired_deleted"], 2)
        self.assertEqual(sorted(self.provider.deleted_ids()), ["a-1", "b-1"])


class StagingRetentionSettingTest(unittest.TestCase):
    """设置项归一化：0 是合法值（关），非法值回落 7，超范围夹住。"""

    def test_defaults(self):
        self.assertEqual(core._clamp_cross_copy_staging_retention_days(None), 7)
        self.assertEqual(core._clamp_cross_copy_staging_retention_days(""), 7)
        self.assertEqual(core._clamp_cross_copy_staging_retention_days("7"), 7)

    def test_zero_means_disabled(self):
        self.assertEqual(core._clamp_cross_copy_staging_retention_days(0), 0)
        self.assertEqual(core._clamp_cross_copy_staging_retention_days("0"), 0)

    def test_garbage_falls_back_to_default(self):
        self.assertEqual(core._clamp_cross_copy_staging_retention_days("abc"), 7)

    def test_clamped_to_range(self):
        self.assertEqual(core._clamp_cross_copy_staging_retention_days(-5), 0)
        self.assertEqual(core._clamp_cross_copy_staging_retention_days(999), 365)

    def test_normalize_config_applies_clamp(self):
        merged = {"cross_copy_staging_retention_days": "30"}
        core._normalize_cross_copy_staging_retention_config(merged)
        self.assertEqual(merged["cross_copy_staging_retention_days"], 30)


if __name__ == "__main__":
    unittest.main()
