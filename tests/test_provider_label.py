"""网盘显示名：内部代号不能直接当用户可见文案。

背景：天翼网盘的内部代号是 `tianyi`，有几处文案直接把这个代号拼进了
通知、日志和任务卡片，用户看到的是英文 `tianyi` 而不是「天翼云盘」；
刮削任务卡片更糟——它写的是「不是夸克就是 115」，天翼任务会显示成「115」。
"""

import unittest
from pathlib import Path

from app.core import provider_display_name
from app.providers import registry

ROOT = Path(__file__).resolve().parents[1]


class ProviderDisplayNameTest(unittest.TestCase):
    def test_registry_label_for_known_providers(self):
        self.assertEqual(registry.label_for("tianyi"), "天翼云盘")
        self.assertEqual(registry.label_for("quark"), "夸克网盘")
        self.assertEqual(registry.label_for("aliyun"), "阿里云盘")
        self.assertEqual(registry.label_for("115"), "115网盘")

    def test_registry_label_for_unknown_falls_back_to_code(self):
        self.assertEqual(registry.label_for("not-a-provider"), "not-a-provider")
        self.assertEqual(registry.label_for("not-a-provider", default="某个网盘"), "某个网盘")

    def test_registry_label_for_empty_uses_default(self):
        self.assertEqual(registry.label_for("", default="源网盘"), "源网盘")
        self.assertEqual(registry.label_for(None, default="目标网盘"), "目标网盘")

    def test_core_display_name_covers_all_providers(self):
        cases = {
            "115": "115网盘",
            "quark": "夸克网盘",
            "aliyun": "阿里云盘",
            "tianyi": "天翼云盘",
            "123pan": "123 网盘",
        }
        for key, expected in cases.items():
            with self.subTest(provider=key):
                self.assertEqual(provider_display_name(key), expected)

    def test_core_display_name_is_case_insensitive(self):
        self.assertEqual(provider_display_name("TIANYI"), "天翼云盘")

    def test_core_display_name_empty_and_unknown(self):
        self.assertEqual(provider_display_name(""), "")
        self.assertEqual(provider_display_name("", default="网盘"), "网盘")
        self.assertEqual(provider_display_name("unknown-provider"), "unknown-provider")


class FrontendProviderLabelTest(unittest.TestCase):
    def test_scraper_job_card_does_not_hardcode_two_providers(self):
        """刮削任务卡片的网盘标签不能再是「不是夸克就是 115」。"""
        js = (ROOT / "static/js/modules/resource/job-modal.js").read_text(encoding="utf-8")
        self.assertIn("function resolveProviderLabel", js)
        self.assertNotIn("=== 'quark' ? '夸克' : '115'", js)
        # 静态表要覆盖天翼，否则 providerMeta 未就绪时仍会显示代号
        self.assertIn("tianyi: '天翼云盘'", js)

    def test_cross_copy_card_prefers_provider_meta(self):
        """跨盘复制卡片的 capabilities 只含三家，天翼任务要退到 providerMeta。"""
        js = (ROOT / "static/js/modules/cross_copy/ui.js").read_text(encoding="utf-8")
        self.assertIn("window.providerMeta", js)
        self.assertIn("tianyi: '天翼云盘'", js)


if __name__ == "__main__":
    unittest.main()
