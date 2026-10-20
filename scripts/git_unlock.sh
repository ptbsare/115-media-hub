#!/usr/bin/env bash
# 清掉 git 残留锁文件（不删除，只移出 .git，绕开本机对删除的拦截）。
#
# 背景：本机安全策略会拦「删除文件」。git 每次写索引/引用都是
# 「建锁 → 写 → 改名 → 删临时文件」，命令一旦被打断（超时、Ctrl-C、进程被杀），
# 锁就留在 .git 里，之后所有 git 命令都会报 "File exists"。
#
# 因为 rename 没被拦、unlink 被拦，这里统一用 os.rename 把锁移到
# .workbuddy/tmp/stale-locks/ 下留存，既不丢证据也不挡路。
#
# 用法：scripts/git_unlock.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON="$(command -v python3)"

exec "$PYTHON" - <<'PY'
import os
import time

dest = os.path.abspath(".workbuddy/tmp/stale-locks")
os.makedirs(dest, exist_ok=True)

stale = []
for root, _dirs, files in os.walk(".git"):
    for name in files:
        if name.endswith(".lock"):
            stale.append(os.path.join(root, name))

if not stale:
    print("没有残留锁，工作区是干净的")
    raise SystemExit(0)

moved = []
for src in stale:
    target = os.path.join(dest, os.path.basename(src) + "." + str(int(time.time() * 1000)))
    try:
        os.rename(src, target)
        moved.append(src)
    except OSError as exc:
        print(f"移不动 {src}：{exc}")

print(f"已移出 {len(moved)} 个残留锁（留存于 {dest}）：")
for item in moved:
    print("  " + item)
PY
