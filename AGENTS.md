# Agent Bootstrap

## 项目上下文

- **项目**: 115-media-hub，FastAPI 媒体自动化面板，支持多网盘
- **网盘**: 115 / 夸克 / 天翼云盘 / 123云盘 / 阿里云盘
- **当前状态**: `docs/superpowers/state.md`（每次会话开头先读它）
- **最近交接**: `docs/superpowers/handoff.md`（最新在前，读开头几条就够）
- **历史交接**: `docs/superpowers/handoff-archive.md`（按需 `rg` 检索，不要通读）
- **稳定约定**: `docs/superpowers/conventions.md`（架构不变量、产品口径、已知坑）
- **功能模块索引**: `docs/superpowers/modules.md`（每个模块的用途 / 入口 / 会做 / 不会做；改动后要同步，见「文档同步」）
- **设计文档**: `docs/superpowers/specs/`
- **实施计划**: `docs/superpowers/plans/`

## 阅读纪律（省 token，务必遵守）

- 会话开头只读 `state.md` 全文，加上 `handoff.md` 的头部和开头几条（文件已限制在 32 KB 内）；**不要 `cat` 整个 handoff**。
  历史条目有几百条，用 `rg "关键词" docs/superpowers/handoff-archive.md` 定位后再按行范围读。
- 大文件不要整份读取（`app/core.py` 有 190 KB、`CHANGELOG.md` 有 174 KB）：先 `rg -n "关键词"`，再用 `sed -n 'a,bp'` 只看相关片段；
  `CHANGELOG.md` 每次只看顶部 1～2 个版本段，历史版本用 `rg` 定位。
- 在“一行一条超长记录”的文件里搜索（`handoff-archive.md` / `handoff.md` / `CHANGELOG.md`）**必须截断长行**：
  `rg -n --max-columns 300 "关键词" docs/superpowers/handoff-archive.md`。
  不加参数时，一次 `rg "订阅" docs/superpowers/handoff-archive.md` 会吐出 30 KB 左右（加上 `--max-columns 300` 只剩 1.5 KB）。
- 跑测试只跑与改动相关的文件，全量 `unittest` 留给发布/合并前；只保留失败摘要和统计，不要把完整输出贴进对话。
- 需要跨会话保留的结论写进文件（`state.md` / `conventions.md` / `specs/`），不要在对话里反复复述。
- 想知道“某功能是干什么的、从哪进、明确不做什么”，看 `docs/superpowers/modules.md`，不要靠猜代码。

## 文档同步（改完必做）

- 新增 / 删除 / 改名 路由、服务、provider 文件或页面模板 → 更新 `docs/superpowers/modules.md`；
  `tests/test_modules_doc.py` 会校验每个文件都已登记，漏了直接测试失败。
- 某个模块的“会做 / 不会做”边界变了（新增触发入口、新增自动行为等）→ 先改 `modules.md` 对应条目；
  属于长期口径的再写进 `conventions.md`，不要把同一句抄两遍。
- 完成重要功能 / 重要修复 → 按下面「多人协作」追加一行交接记录，不要只改代码。

## 多人协作

- 完成重要功能、重要修复，或需要其他 agent 接手时，在 `docs/superpowers/handoff.md` 的 `## 交接记录` 下追加**一条一行**记录：
  `- YYYY-MM-DD | 分支或提交 | 版本 | 变更一句话 | 根因或影响 | 验证证据 | 下一步`。
- 追加后运行 `.venv/bin/python scripts/rotate_handoff.py`（`--dry-run` 可预览）完成归档；
  不要手工搬条目，也不要把细节堆进条目（细节写 commit message 或 `specs/` 文档）。
- 有实质进展或发布后，同步更新 `state.md` 的基线与待办。
- 协作前先检查 `git status`、当前分支，并阅读 `state.md` 加最新几条交接；确认不会影响本地改动时，再执行 `git pull --ff-only`

## 通用规则

- 文档与代码冲突时，以代码为准
- 版本号以 `version.json` 为准；更新版本时同步维护 `CHANGELOG.md`、README 与 `state.md`
- 网页需要支持日/夜间模式，设计配色时注意阅读可见性

## 验证环境

- 一键验证：`scripts/check.sh`（编译 + 改动 JS 的 `node --check` + 指定测试 + `handoff.md` 体积预算 + `git diff --check`）：
  `scripts/check.sh tests.test_resource_ed2k`、`scripts/check.sh --all`（全量）、`scripts/check.sh --js static/js/index.js`；用法见脚本头部注释。
- 优先使用项目本地环境：`.venv/bin/python`
- 语法验证：`PYTHONPYCACHEPREFIX=/tmp/115-media-hub-pycache .venv/bin/python -m compileall app main.py`
- 本地调试服务：`.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 18080`
- 不要默认使用系统 `python3` 验证项目依赖
- 容器环境以 `requirements.txt` 和 `Dockerfile` 为准
- 通过 Docker 验证页面、接口或容器行为前，先在项目根目录执行 `export HTTP_PROXY=http://127.0.0.1:7897; export HTTPS_PROXY=http://127.0.0.1:7897; export NO_PROXY=localhost,127.0.0.1,registry-1.docker.io; docker compose up -d --build`，确保测试使用当前代码而不是旧镜像

## 测试与本地产物约定

- **临时文件与清理的硬规则见 `docs/superpowers/conventions.md` 的「测试环境红线」，逐条照做，不要自己另定做法、也不要拿方案选择去问用户。** 一句话版：临时文件只进 `$TMPDIR/115-media-hub-tests/run-XXXX`，源码树里不许有；清理失败只在会话目录内容忍；验收只看 `scripts/check.sh --all --container` 全绿。
- `tests/` 是项目正式测试目录；需要补测试时，优先在这里新增或维护 `test_*.py`，不要把临时验证脚本散落到仓库其他位置。
- `docs/superpowers/handoff.md` 只用于多人/多 agent 交接记录，不用于堆放调试日志、临时方案或测试输出；
  当前状态写 `state.md`，长期约定写 `conventions.md`，超期条目交给 `scripts/rotate_handoff.py` 归档。
- 本地验证产物（如 `pycache`、临时数据库、临时 STRM、截图、导出文件）默认放到 `/tmp`、系统临时目录或仓库里已经忽略的目录；不要为了测试在仓库根目录随手新建临时文件夹。
- 若确实需要新增长期使用但项目本身无关的本地辅助目录，必须同时更新 `.gitignore` 与 `.dockerignore`，再写入用途说明。
- 验证时优先复用已有测试文件、`unittest` 用例和已忽略目录；任务结束前应清理本次新增的临时文件，避免污染工作区。
- 测试临时文件由 `scripts/run_tests.py` 统一管：默认落在系统临时区（`$TMPDIR/115-media-hub-tests/run-XXXX`），
  **不要把 `tempfile.tempdir` 指进项目内**，也**不要在项目代码里调 `tempfile.gettempdir()`**（它自己会建探针文件再删，
  删不掉时会把 8 位 4 字节残骸留在仓库根目录）。项目里出现这类残骸或历史临时目录，用
  `python scripts/run_tests.py --clean-temp` 收掉。
