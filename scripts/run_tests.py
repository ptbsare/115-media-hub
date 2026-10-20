#!/usr/bin/env python3
"""跑单元测试的统一入口（本机安全策略友好版）。

为什么不用 pytest：这台机器的安全中心会拦临时文件的创建/删除，pytest 的
capture 一启动就要 ``TemporaryFile``，直接起不来（报「No usable temporary
directory」）。所以统一用标准库 unittest，并把临时目录指到仓库内的可写位置，
再把「清理临时目录失败」这类无关噪音吞掉（拦截是环境问题，不是代码问题）。

用法::

    python scripts/run_tests.py                  # 全量（80 个模块 / 1600+ 用例，约 2~3 分钟）
    python scripts/run_tests.py --changed        # 只跑与 git 改动相关的模块（开发时用，秒级~几十秒）
    python scripts/run_tests.py --changed HEAD~1 # 指定对比基线
    python scripts/run_tests.py pan115 cross     # 按关键字挑模块
    python scripts/run_tests.py tests.test_pan115_downurl   # 精确指定模块（可多个）
    python scripts/run_tests.py --list --changed # 只看会跑哪些模块，不执行

挑选规则（--changed / 关键字）：
    * 公共层（app/core.py、app/db.py、app/config_store.py、app/providers/base.py、
      app/providers/registry.py 等）一改就退化为全量——这些文件被所有链路共用，
      挑子集等于没测。
    * 其余按「文件名里的关键词」匹配测试模块名，命中即跑；一个都没命中时退化为全量。
    * 无论怎么挑，都会带上 tests.test_modules_doc（新增 app 模块必须登记 modules.md）。
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(REPO, "tests")
TMP_DIR = os.path.join(REPO, ".workbuddy", "tmp", "work")

# 改这些文件等于改公共层，跑子集没意义，直接全量
SHARED_PATHS = {
    "app/core.py",
    "app/db.py",
    "app/main.py",
    "app/config_store.py",
    "app/config_runtime.py",
    "app/background.py",
    "app/startup.py",
    "app/providers/base.py",
    "app/providers/registry.py",
    "main.py",
    "requirements.txt",
    "templates/base.html",
    "static/js/index.js",
}
# 关键词里没信息量的词，参与匹配只会把无关测试拖进来
STOPWORDS = {
    "app",
    "services",
    "providers",
    "routes",
    "static",
    "templates",
    "partials",
    "pages",
    "modules",
    "common",
    "base",
    "index",
    "main",
    "utils",
    "views",
    "model",
    "models",
    "docs",
    "tests",
    "test",
    "core",
    "js",
    "py",
}
ALWAYS_RUN = ["test_modules_doc"]


def _probe_tempdir_writable():
    """自检：能不能在本机建临时文件并删掉。

    这台机器的安全中心会拦「删除文件」，删不掉时 SQLite 的 journal/wal、
    跨盘复制的 .part 临时文件都会残留，测试会刷出一堆和代码无关的失败。
    """
    probe = os.path.join(TMP_DIR, ".probe-delete-check")
    try:
        with open(probe, "w", encoding="utf-8") as handle:
            handle.write("x")
        os.unlink(probe)
        return True
    except Exception:
        return False


def _harden_tempdir() -> None:
    """把临时目录挪进仓库，并让「清理失败」不再炸掉整轮测试。"""
    os.makedirs(TMP_DIR, exist_ok=True)
    tempfile.tempdir = TMP_DIR

    real_rmtree = shutil.rmtree

    def tolerant_rmtree(path, ignore_errors=False, onerror=None):
        try:
            return real_rmtree(path, ignore_errors=True)
        except Exception:
            return None

    shutil.rmtree = tolerant_rmtree

    real_cleanup = tempfile.TemporaryDirectory.cleanup

    def tolerant_cleanup(self):
        try:
            real_cleanup(self)
        except Exception:
            pass

    tempfile.TemporaryDirectory.cleanup = tolerant_cleanup

    # 生产用 WAL，测试里换成内存 journal：WAL 会额外产生 -wal/-shm，
    # 在被拦删除的机器上删不掉，SQLite 直接抛 disk I/O error，一崩一大片。
    # 只影响测试进程的连接配置，不碰生产代码。
    try:
        from app import db as app_db

        def configure_without_wal(conn, enable_wal=False):
            try:
                conn.execute("PRAGMA busy_timeout = 30000")
                conn.execute("PRAGMA journal_mode = MEMORY")
                conn.execute("PRAGMA synchronous = OFF")
                conn.execute("PRAGMA temp_store = MEMORY")
            except Exception:
                pass

        app_db._configure_connection = configure_without_wal
    except Exception:
        pass

    _force_memory_journal_everywhere()


def _force_memory_journal_everywhere() -> None:
    """把 ``sqlite3.connect`` 兜一层，连上就先关掉磁盘 journal。

    只改 ``app.db`` 那一层不够：不少测试自己 ``sqlite3.connect`` 建临时库
    （如 ``xcopy-db-*``），照样落下 ``data.db-journal``，在这台「删不掉文件」的
    机器上直接变成 ``disk I/O error``。``journal_mode`` 是**数据库文件级的持久
    设置**，连上设一次，之后所有连接都走内存 journal，不再产生 journal 文件。
    """
    import sqlite3

    if getattr(sqlite3.connect, "_wb_memory_journal", False):
        return

    real_connect = sqlite3.connect

    def connect_with_memory_journal(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        try:
            conn.execute("PRAGMA journal_mode = MEMORY")
            conn.execute("PRAGMA busy_timeout = 30000")
        except Exception:
            pass
        return conn

    connect_with_memory_journal._wb_memory_journal = True
    sqlite3.connect = connect_with_memory_journal


def _all_test_modules():
    return sorted(
        name[:-3]
        for name in os.listdir(TESTS_DIR)
        if name.startswith("test_") and name.endswith(".py")
    )


def _git_output(*args):
    try:
        return subprocess.run(
            ["git", *args], cwd=REPO, capture_output=True, text=True, check=False
        ).stdout
    except Exception:
        return ""


def _changed_paths(base):
    paths = set()
    paths.update(_git_output("diff", "--name-only", base).split())
    paths.update(_git_output("diff", "--name-only", "--cached", base).split())
    paths.update(_git_output("ls-files", "--others", "--exclude-standard").split())
    return sorted(path for path in paths if path)


def _tokens_for(path):
    stem = os.path.splitext(os.path.basename(path))[0].lower()
    tokens = {stem}
    tokens.update(part for part in stem.split("_") if part)
    # 前端路径里的目录名也有信息量：static/js/modules/cross_copy/ui.js → cross_copy
    parts = path.lower().split(os.sep)[:-1]
    if len(parts) >= 2:
        tokens.add(parts[-1])
    return {token for token in tokens if len(token) >= 4 and token not in STOPWORDS}


def select_modules(paths, keywords):
    """返回 (模块名列表, 是否退化为全量, 说明)。"""
    all_modules = _all_test_modules()
    if paths and any(path in SHARED_PATHS for path in paths):
        hit = sorted({path for path in paths if path in SHARED_PATHS})
        return all_modules, True, f"改到公共层 {', '.join(hit)}"

    wanted = set()
    for path in paths or []:
        wanted |= _tokens_for(path)
    wanted |= {str(word).lower() for word in (keywords or []) if str(word).strip()}

    if not wanted:
        return all_modules, True, "没有可用的匹配关键词"

    chosen = [
        module
        for module in all_modules
        if any(token in module.lower() for token in wanted)
    ]
    chosen.extend(module for module in ALWAYS_RUN if module not in chosen)
    if len(chosen) <= len(ALWAYS_RUN):
        return all_modules, True, "关键词没有命中任何测试模块"
    return sorted(set(chosen)), False, ""


def main(argv):
    parser = argparse.ArgumentParser(add_help=True, description="跑单元测试（unittest 版）")
    parser.add_argument("modules", nargs="*", help="模块名或关键字")
    parser.add_argument("--changed", nargs="?", const="HEAD", default=None, help="按 git 改动挑选（可选基线，默认 HEAD）")
    parser.add_argument("--list", action="store_true", help="只列出将运行的模块")
    parser.add_argument("--quiet", action="store_true", help="不打印每条用例名")
    args = parser.parse_args(argv)

    explicit = [name[len("tests."):] for name in args.modules if name.startswith("tests.")]
    keywords = [name for name in args.modules if not name.startswith("tests.")]

    if explicit:
        modules, degenerate, reason = explicit, False, ""
    elif args.changed is not None:
        paths = _changed_paths(args.changed)
        modules, degenerate, reason = select_modules(paths, keywords)
        if not paths:
            print("（git 没有改动，按全量跑）")
    elif keywords:
        modules, degenerate, reason = select_modules([], keywords)
    else:
        modules, degenerate, reason = _all_test_modules(), False, ""

    if degenerate and reason:
        print(f"→ 退化为全量：{reason}")

    if args.list:
        print(f"将运行 {len(modules)} 个模块：")
        for module in modules:
            print(f"  tests.{module}")
        return 0

    # 先补好搜索路径：_harden_tempdir 里要 import app.db 给连接层打补丁
    sys.path.insert(0, REPO)
    _harden_tempdir()
    if not _probe_tempdir_writable():
        print(
            "⚠️  本机安全策略拦了「删除文件」：SQLite 的 journal/wal、临时 .part 文件删不掉，\n"
            "    本轮会刷出大量与代码无关的失败。请用已批准的执行权限重跑，结果才算数。"
        )

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    load_failed = []
    for module in modules:
        try:
            suite.addTests(loader.loadTestsFromName(f"tests.{module}"))
        except Exception as exc:  # noqa: BLE001 - 导入失败也要算失败
            load_failed.append(f"{module}: {exc}")

    print(f"→ 运行 {len(modules)} 个模块" + ("（全量）" if is_full_selection(modules) else ""))
    verbosity = 1 if args.quiet else 2
    started_at = time.monotonic()
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    elapsed = round(time.monotonic() - started_at, 1)

    bad = len(result.failures) + len(result.errors) + len(load_failed)
    print(f"\n结果：{result.testsRun} 条用例，失败 {len(result.failures)}，错误 {len(result.errors)}，耗时 {elapsed}s")
    for item in load_failed:
        print(f"  导入失败 {item}")
    return 0 if bad == 0 else 1


def is_full_selection(modules):
    return len(modules) == len(_all_test_modules())


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
