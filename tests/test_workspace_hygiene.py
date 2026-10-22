"""守住「测试不在项目里留渣」这条红线（防回归）。

规则本身写在 `docs/superpowers/conventions.md` 的「测试环境红线」里。
文档只有自觉，断言才会真的拦住——历史教训：2026-10 因为临时目录被指进
项目内、且清理失败被全局吞掉，三天堆出 2.2 万个目录 / 8.4GB，
用户每次跑测试都要为删文件反复批准，工作区也看不见真实结构。

这几条断言只是「别再改回去」的保险丝，不测试业务逻辑。
"""

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_TESTS = ROOT / "scripts" / "run_tests.py"
TESTS_DIR = ROOT / "tests"

# 拆开拼接，避免这个文件自己被下面的「测试目录扫描」命中
PROJECT_TEMP_MARKER = ".workbuddy" + "/tmp"
# 扫描时跳过这个文件本身：它必须引用上面这些反例字符串才能做断言
SELF_NAME = Path(__file__).name


def _candidate_tests():
    return [path for path in sorted(TESTS_DIR.glob("test_*.py")) if path.name != SELF_NAME]


def _code_only(source):
    """去掉行内注释再做字符串断言：注释里本来就会提到这些反例。"""
    return "\n".join(line.split("#", 1)[0] for line in source.splitlines())


def _calls_gettempdir(source):
    """源码里是否真的调用了 gettempdir()。

    用 AST 判断而不是搜字符串：注释和文档字符串里会提到这个名字，
    但它们不构成调用，不该算违规。
    """
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Attribute) and node.attr == "gettempdir":
            return True
        if isinstance(node, ast.Name) and node.id == "gettempdir":
            return True
    return False


class RunTestsScriptTest(unittest.TestCase):
    """`scripts/run_tests.py` 必须保持这套做法。"""

    @classmethod
    def setUpClass(cls):
        cls.source = RUN_TESTS.read_text(encoding="utf-8")
        cls.code = _code_only(cls.source)

    def test_temp_root_is_the_session_dir(self):
        """tempfile 的根只能指到本轮会话目录。"""
        self.assertIn("tempfile.tempdir = SESSION_DIR", self.code)

    def test_never_calls_gettempdir(self):
        """不许调 gettempdir()：它的探针文件删不掉时会掉在仓库根目录。"""
        self.assertFalse(_calls_gettempdir(self.source), "run_tests.py 里不许调 gettempdir()")

    def test_rmtree_wrapper_passes_kwargs_through(self):
        """包装 rmtree 必须透传 kwargs：Python 3.12 的 TemporaryDirectory 用 onexc=。"""
        self.assertRegex(self.code, r"def tolerant_rmtree\([^)]*\*\*kwargs")

    def test_cleanup_tolerance_is_scoped_to_session(self):
        """容忍必须按路径判断，不能无条件吞掉所有清理失败。"""
        self.assertIn("_inside_session(", self.code)
        # 历史写法：不看路径、一律 ignore_errors=True 再吞掉
        self.assertNotIn("real_rmtree(path, ignore_errors=True)", self.code)
        # 额外包一层 TemporaryDirectory.cleanup 会把真实错误也一起吞掉
        self.assertNotIn("TemporaryDirectory.cleanup", self.code)

    def test_stale_session_dirs_are_reclaimed(self):
        """会话目录删不掉时会累积，必须有自动回收。"""
        self.assertIn("_prune_temp_root(", self.code)


class TestsDirTest(unittest.TestCase):
    def test_no_test_writes_into_project(self):
        """测试代码不许把临时目录指到项目内。"""
        offenders = [
            path.name
            for path in _candidate_tests()
            if PROJECT_TEMP_MARKER in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(offenders, [], f"这些测试把临时目录写进了项目内：{offenders}")

    def test_no_test_calls_gettempdir(self):
        """测试代码也不许调 gettempdir()，否则照样留残骸。"""
        offenders = [
            path.name
            for path in _candidate_tests()
            if _calls_gettempdir(path.read_text(encoding="utf-8"))
        ]
        self.assertEqual(offenders, [], f"这些测试调了 gettempdir()：{offenders}")


class GitignoreTest(unittest.TestCase):
    def test_workspace_dir_is_ignored(self):
        """.workbuddy/ 是项目数据目录，不该被提交。"""
        content = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn(".workbuddy/", content)
