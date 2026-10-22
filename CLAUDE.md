# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> 本文件只负责“代码地图”：模块职责、目录结构、关键模式。
> **协作规则、阅读纪律、交接与验证约定一律以 `AGENTS.md` 为准**，不要在这里重复维护，避免两处漂移。
> 当前状态看 `docs/superpowers/state.md`；长期约定看 `docs/superpowers/conventions.md`；
> **模块用途 / 入口 / 会做 / 不会做**看 `docs/superpowers/modules.md`（新增或改名文件时必须同步，有测试兜底）。

## Commands

```bash
# Python syntax validation
PYTHONPYCACHEPREFIX=/tmp/115-media-hub-pycache .venv/bin/python -m compileall app main.py

# Run dev server
.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 18080

# Docker build & run
docker compose up -d
```

Always use `.venv/bin/python` for validation, never system `python3`.

## Architecture overview

This is a **FastAPI** media automation panel for 115/Quark cloud drives. It is a monolithic app that runs as a single container with no external service dependencies — it embeds SQLite and serves its own SPA frontend.

### Request life cycle

1. `main.py` (project root) re-exports `app` from `app.main`
2. `app/main.py` imports the FastAPI `app` from `app/core.py` and registers all routers from `app/routes/`
3. `app/core.py` is the **190 KB shared-state hub**: it instantiates `FastAPI`, mounts `StaticFiles`, applies `SessionMiddleware` + `GZipMiddleware`, and re-exports nearly every function/constant from `config_store`, `db`, `resource_*`, providers, and services. Every route does `from ..core import *`.
4. `app/startup.py` (imported at the bottom of `app/main.py`) registers `on_event("startup")` and `on_event("shutdown")` handlers that spawn async scheduler loops (tree cron, monitor cron, subscription cron, sign115 cron) and start the background task runtime.

### Background tasks

`app/background.py` runs a dedicated `asyncio` event loop on a daemon thread. All long-running work (tree sync, monitor scans, subscription jobs, resource imports, scraper jobs) is submitted via `submit_background(func, *args, label=...)` which returns a `concurrent.futures.Future`. This keeps the main event loop responsive for HTTP requests.

### Real-time UI updates

`app/routes/events.py` — SSE endpoint (`GET /events`). An in-memory `asyncio.Queue` per subscriber. State snapshots are pushed to all subscribers on config save, task start/stop, log writes, and periodically via heartbeat.

### Configuration

`app/config_store.py` — `JsonConfigStore`, a thread-safe JSON config with mtime-based cache invalidation. Settings are persisted to `/app/config/settings.json`. Sensitive keys (cookies, passwords, tokens, API keys) are stripped when returning to the frontend via `app/config_runtime.py` (`build_public_settings_payload`).

### Database

`app/db.py` — SQLite at `/app/config/data.db`. All schema DDL is in `ensure_db()` with `CREATE TABLE IF NOT EXISTS`. No migration framework — columns are added via `ALTER TABLE ADD COLUMN` gated by `PRAGMA table_info` checks. Thread-safe via `threading.Lock`. All writes use `open_db()` which returns a new connection with `row_factory = sqlite3.Row`.

### Route modules (all under `app/routes/`)

| Router | Key responsibilities |
| --- | --- |
| `pages.py` | Login page, session auth (`request.session["logged_in"]`), index HTML with template include resolution, userscript serving |
| `settings.py` | Read/write config, cookie health checks, proxy/PanSou/notify tests, sign115 status/manual run |
| `resource.py` | TG channel sync, manual resource import, PanSou search, job creation/monitoring (import jobs), 115/Quark folder browsing and share entry listing |
| `monitor.py` | Webhook receiver (`POST /webhook/{task_name}`) with HMAC-SHA256 signature verification, monitor job queue/start/stop/delete, userscript job listing |
| `tree.py` | Directory tree STRM generation — manual start, log viewing |
| `subscription.py` | Subscription task CRUD, manual start (including quark link injection), episode ledger view, rebuild |
| `scraper.py` | Scraper CRUD (rename/move/delete entries), rename plan building, job creation with rollback |
| `strm.py` | STRM play proxy — resolves 115 download URLs (pick_code → download URL), supports relay redirect and direct streaming proxy, handles 115 RSA/encrypted downurl protocol |
| `tmdb.py` | TMDB search and detail with task binding payload |
| `recommendation.py` | Explore/recommendation state and watchlist (add / remove / update status) |
| `events.py` | SSE state stream |

### Services (all under `app/services/`)

- `tree.py` — Directory tree STRM generation from 115 directory tree TXT files
- `monitor.py` — Folder monitoring: scans 115 dirs, generates `.strm` files for media, supports incremental vs full scan, decides scan scope per trigger
- `monitor_changes.py` — Cloud-change event sync: precise STRM updates, manual-rescan scopes, event state
- `monitor_runs.py` — Structured run/event lifecycle records for monitor work
- `strm_files.py` — Managed `.strm` file IO, empty-dir cleanup, orphan metadata dir detection
- `quick_import.py` — Inbox: identify → organize → distribute into the target monitor dirs
- `ai_match.py` — Optional OpenAI-compatible LLM fallback for scraper identification (cache / retry / usage)
- `subscription.py` — Subscription runner orchestration; imports from `subscription_runner.py` (main loop), `subscription_episode.py` (episode tracking), `subscription_share_selection.py` (share content selection), `subscription_share_runtime.py` (share browsing), `subscription_state.py` (DB state), `subscription_task_runner.py` (single task execution), `subscription_offline_cleanup.py` (unmatched offline leftovers retention)
- `resource.py` — Import job execution (magnet/add_115_task, 115 share receive/copy, quark share save)
- `scraper.py` — Batch rename/identify/move/delete jobs on cloud drive entries
- `notify.py` — WeChat Work (企业微信) notification push (bot webhook + app API)
- `sign115.py` — 115 daily sign-in

### Providers (all under `app/providers/`)

| Provider | File | Status |
|----------|------|--------|
| 115网盘 | `pan115.py` | 已有 |
| 夸克网盘 | `quark.py` | 已有 |
| 天翼云盘 | `tianyi.py` | 已有 |
| 123云盘 | `pan123.py` | 已有 |
| 阿里云盘 | `aliyun.py` | 已有 |

- `base.py` — `CloudProvider` ABC，定义统一接口 + 限流 + get_cookie
- `registry.py` — 线程安全注册表，按名称/link_type 查找，管理启用状态
- `pansou.py` — PanSou 搜索聚合
- `tmdb.py` — TMDB API client
- `common.py` — 共用工具

Provider 通过 `register()` 自注册到 registry，core.py 底部 import 触发。`/api/providers` 端点返回能力清单给前端动态渲染。

### Multi-provider extension

- **设计**: docs/superpowers/specs/2026-05-16-multi-provider-design.md
- **计划**: docs/superpowers/plans/2026-05-16-multi-provider-implementation.md
- **交接**: 历史条目在 `docs/superpowers/handoff-archive.md`（多网盘扩展已在 0.5.x 前合并进 `main`，`feat/multi-provider` 分支不再是当前工作分支）

### Key patterns

- **All state** (config, task status, logs, caches, subscriber sets) lives in module-level variables in `app/core.py` (e.g., `task_status`, `monitor_status`, `subscription_status`, `monitor_queue`, `subscription_queue`). These are mutated directly — there are no repository/DAO classes.
- **Thread safety** for cross-thread state (main event loop ↔ background thread) relies on `threading.Lock` and `asyncio.Queue`. Config uses mtime-based cache invalidation to allow external file edits.
- **`normalize_*` / `build_*`** naming conventions: `normalize_*` validates and coerces input, `build_*` constructs response payloads.
- **115 download URL resolution** (`app/routes/strm.py`) uses RSA public-key cryptography to encrypt/decrypt download URL payloads — this is reverse-engineered from 115's web client and is fragile. There's a primary API (`webapi.115.com/files/download`) with fallback to `proapi.115.com/app/chrome/downurl`.

### Updating the version

When bumping the version:
1. Update `version.json` (version string, buildDate, notes)
2. Update `CHANGELOG.md` with the new entry

Both must always be updated together.

### Persistence directories (container)

- `/app/strm` — generated `.strm` files
- `/app/config/settings.json` — config
- `/app/config/data.db` — SQLite
- `/app/config/trees` — tree cache
- `/app/logs/` — task/monitor/subscription logs
