# 订阅跨盘补源（夸克）修复执行方案

> 目标：把 0.15.0 上线但从未真正生效的「订阅跨盘补源」链路接通。本次只接通「夸克 → 任务网盘」一条组合，阿里/115 作源、其他网盘组合留待下次。
>
> 本文档面向代码能力一般的执行者，每一步都写明了：改哪个文件、在哪个位置、改成什么、为什么。**不要自行发挥，不要改动本文未提到的行为。**

---

## 0. 背景：为什么现在不生效（先理解，再动手）

跨盘补源本来的设计是：订阅任务（比如网盘=115）搜资源时，如果 115 还没发布最新一集，就从夸克（更新更快的网盘）把资源搬回 115。但代码落地时接错了，导致这个功能一次都没真正跑起来。原因是三个断点叠加：

1. **注册入口接错链路（致命）**：负责「把夸克分享转存 + 注册复制任务」的函数 `register_cross_copy_from_candidates`，全代码只有一处调用，而那一处在一个「只服务网盘=夸克的任务」的专用函数里；这个函数开头又判断「任务网盘是夸克 → 直接返回（无需补源）」。等于唯一接线的地方被自己的判断堵死，是死代码。真正需要补源的 115 任务走的是另一条主链路，那里从头到尾没有调用这个函数。
2. **搜索层把夸克链接过滤掉了**：115 任务搜索资源时，只保留 115 的分享，夸克分享在搜索内部就被丢掉了，所以就算接对了线，也拿不到夸克候选。
3. **fallback 语义没实现**：补源模式有「兜底」和「优先跨盘」两档，代码里两档行为完全一样，都只是「开了就补」，没有区分。

本次方案一次性修掉这三个断点。

---

## 1. 核心概念（执行前必须记住）

- **任务网盘 `provider`**：订阅任务里配置的目标网盘，比如 `"115"` 或 `"aliyun"`。
- **补源模式 `cross_copy_mode`**：`"off"`（关闭）、`"fallback"`（兜底：本盘没更新的资源才补）、`"prefer"`（优先跨盘：命中就补）。
- **补源源网盘 `cross_copy_sources`**：一个列表，本次只支持 `["quark"]`（夸克）。
- **候选 candidate**：一次搜索命中的一条资源，是一个 dict，里面 `item` 字段是原始资源条目，`episode`/`range_end` 字段是识别出的集数。
- **本盘候选 `ranked_candidates`**：115 任务搜到的、经过评分的、本盘能直接导入的候选列表。
- **补源候选池**：额外用 `provider_filter="quark"` 搜出来的夸克候选列表，单独存放，不混进本盘候选。

---

## 2. 触发补源的口径（fallback 的「缺」怎么判断）

本次采用**口径 1**：以「本盘本轮搜索是否命中比本地更新的资源」为准。

- **电影**：本盘本轮没搜到任何可导入候选 → 算「缺」→ 触发补源；搜到了 → 不补。
- **电视剧**：本盘候选里，只要有任意一条候选的集数 **大于** 本地已保存的最高集数（`last_episode`）→ 算「不缺」→ 正常导入，不补源；否则（本盘候选为空，或候选集数都 ≤ 本地最高集）→ 算「缺」→ 触发补源。

> 这样「全网才更到 35、本地到 34」的场景：115 搜到了第 35 集就正常导入不补；115 没搜到第 35 集就去夸克补。正对应用户「这个网盘还没发布、就从别的网盘拿」的诉求。

---

## 3. 改动清单（共 3 个文件，按顺序做）

### 改动 A：`app/services/subscription_task_runner.py` —— 把补源注册搬到通用链路

**背景**：现在 `register_cross_copy_from_candidates` 的调用在 `_run_subscription_task_quark` 函数内部（约 2224-2242 行），这个函数只在「任务网盘 = 夸克」时被调用。要把它搬到通用主链路 `run_subscription_task` 里。

**步骤 A1：删除 `_run_subscription_task_quark` 里的补源调用块。**

找到这段代码（约 2224-2242 行）：

```python
    # ---- 跨盘补源：夸克候选注册为复制任务（省去手动跨盘复制） ----
    # fallback 语义由幂等跳过保证：订阅转存成功 → 文件已在目标盘 → 补源任务跳过；
    # 转存失败/未命中 → 补源任务把资源复制进目标网盘。
    cross_copy_registered_job_ids: List[int] = []
    if str(task.get("cross_copy_mode", "off") or "off") != "off" and list(task.get("cross_copy_sources", []) or []):
        try:
            cross_copy_stats = await asyncio.to_thread(
                register_cross_copy_from_candidates,
                task=task,
                cfg=cfg,
                candidates=ranked_candidates,
            )
            cross_copy_registered_job_ids = [
                max(0, int(job_id or 0)) for job_id in (cross_copy_stats.get("registered", []) or [])
            ]
            for log_message, log_level in (cross_copy_stats.get("logs", []) or []):
                await write_subscription_log(log_message, log_level)
        except Exception as exc:
            await write_subscription_log(f"跨盘补源注册失败（不影响本次订阅）：{exc}", "warn")
```

把**整段**删掉（连同上面那三行注释）。

> 注意：删掉后，`_run_subscription_task_quark` 里的 `ranked_candidates` 和 `search_stats` 可能变成「定义了但没用」。不要紧，`_run_subscription_task_quark` 是夸克任务自己的链路，它后面还会用这两个变量（导入流程），不需要动。如果编辑器报「变量未使用」警告，忽略即可，不要删变量定义。

**步骤 A2：在通用主链路 `run_subscription_task` 里插入补源注册。**

找到这段代码（约 3793-3800 行）：

```python
        upsert_subscription_task_state(task_name, status="running", progress=25, detail="候选准备完成，正在匹配评分")
        check_subscription_cancelled()
        ranked_candidates = search_result.get("candidates", []) if isinstance(search_result.get("candidates"), list) else []
        if not ranked_candidates:
            legacy_candidate = search_result.get("candidate", {}) if isinstance(search_result.get("candidate"), dict) else {}
            if legacy_candidate:
                ranked_candidates = [legacy_candidate]
        if not ranked_candidates:
            _subscription_stage_timer_enter(stage_timer, "finalize")
```

在 `if not ranked_candidates:`（第二个，即 3800 行那个）**之前**，插入补源注册代码（见改动 B 和 C 一起配合）。

具体插入位置：紧跟在 `ranked_candidates = [legacy_candidate]` 这个 if 块结束之后、第二个 `if not ranked_candidates:` 之前。

### 改动 B：`app/services/subscription.py` —— 增加「源网盘搜索」函数

**背景**：现有的搜索函数 `find_subscription_task_match_candidate_by_search` 用任务网盘做过滤，只会返回本盘候选。要新增一个「专门搜夸克候选」的入口。

**步骤 B1：在 `find_subscription_task_match_candidate_by_search` 函数内部，确认 provider 过滤参数是怎么传的。**

现有函数签名（约 1050 行）：

```python
async def find_subscription_task_match_candidate_by_search(
    task, last_episode=0, trigger="", total_episodes=0,
) -> Dict[str, Any]:
```

它内部（约 1056 行）有 `provider = normalize_subscription_provider(...)`，然后搜索时把 `provider_filter=provider` 传下去。这就是为什么夸克候选被过滤掉。

**步骤 B2：新增一个薄封装函数，让调用方可以指定 provider 过滤。**

在 `find_subscription_task_match_candidate_by_search` 函数**定义之前**（或之后，只要在模块内、且能被 import），新增：

```python
async def find_subscription_cross_copy_candidates(
    task: Dict[str, Any],
    source_provider: str = "quark",
    last_episode: int = 0,
    trigger: str = "",
    total_episodes: int = 0,
) -> Dict[str, Any]:
    """搜索「源网盘」候选，专供订阅跨盘补源使用。

    复用 find_subscription_task_match_candidate_by_search 的搜索与评分，
    但把 provider 过滤换成 source_provider（默认 quark），返回的候选是
    夸克分享，供 register_cross_copy_from_candidates 注册补源任务。
    """
    # 复制一份 task，临时把 provider 改成源网盘，复用现有搜索逻辑
    cross_task = dict(task)
    cross_task["provider"] = source_provider
    return await find_subscription_task_match_candidate_by_search(
        cross_task,
        last_episode=last_episode,
        trigger=trigger,
        total_episodes=total_episodes,
    )
```

> 说明：现有搜索函数内部用 `task["provider"]` 决定 `provider_filter` 和评分口径。我们不想改动它内部复杂逻辑，所以「复制 task 并把 provider 改成 quark」是改动最小、最安全的做法——夸克搜索和评分本来就有独立分支（`score_subscription_candidate_quark` 等），把 provider 改成 quark 会自然走夸克那套评分。

> 执行者注意：`find_subscription_task_match_candidate_by_search` 是 `async` 函数，返回 dict，里面 `candidates` 是评分后的候选列表、`stats` 是统计。新封装函数保持同样返回结构，调用方按同样方式取 `result.get("candidates", [])`。

### 改动 C：`app/services/subscription_task_runner.py` —— 补源决策 + 调用

**步骤 C1：在通用主链路里，实现「判断是否缺 + 触发补源」。**

在改动 A2 预留的位置，插入以下逻辑（伪代码 + 关键调用，执行者按现有代码风格落地）：

```python
        # ---- 跨盘补源（本次仅夸克源）----
        cross_copy_mode = str(task.get("cross_copy_mode", "off") or "off")
        cross_copy_sources = list(task.get("cross_copy_sources", []) or [])
        cross_copy_candidates: List[Dict[str, Any]] = []
        should_cross_copy = False
        if cross_copy_mode != "off" and "quark" in cross_copy_sources:
            # 只在「本盘缺」时补源：prefer 命中即补，fallback 本盘缺才补
            if cross_copy_mode == "prefer":
                should_cross_copy = True
            else:  # fallback
                should_cross_copy = _subscription_cross_copy_needed(
                    task, ranked_candidates, last_episode
                )
            if should_cross_copy:
                cross_search = await find_subscription_cross_copy_candidates(
                    task,
                    source_provider="quark",
                    last_episode=last_episode,
                    trigger=trigger,
                    total_episodes=known_total,
                )
                cross_copy_candidates = (
                    cross_search.get("candidates", [])
                    if isinstance(cross_search.get("candidates"), list)
                    else []
                )
                if cross_copy_candidates:
                    await write_subscription_log(
                        f"跨盘补源：本盘{'缺最新资源' if cross_copy_mode == 'fallback' else '优先跨盘'}，"
                        f"从夸克找到 {len(cross_copy_candidates)} 条候选，尝试注册补源",
                        "info",
                    )
        if cross_copy_candidates:
            try:
                cross_copy_stats = await asyncio.to_thread(
                    register_cross_copy_from_candidates,
                    task=task,
                    cfg=cfg,
                    candidates=cross_copy_candidates,
                )
                for log_message, log_level in (cross_copy_stats.get("logs", []) or []):
                    await write_subscription_log(log_message, log_level)
            except Exception as exc:
                await write_subscription_log(f"跨盘补源注册失败（不影响本次订阅）：{exc}", "warn")
        elif should_cross_copy:
            await write_subscription_log("跨盘补源：本盘缺最新资源，但夸克也未找到可补候选", "info")
```

**步骤 C2：新增辅助函数 `_subscription_cross_copy_needed`。**

放在文件顶部（和其他 `_xxx` 辅助函数一起，比如 `_format_...` 那一堆附近）。作用是判断「本盘本轮是否缺最新资源」：

```python
def _subscription_cross_copy_needed(
    task: Dict[str, Any],
    ranked_candidates: List[Dict[str, Any]],
    last_episode: int,
) -> bool:
    """判断本盘本轮是否缺资源（fallback 补源的口径 1）。

    电影：本盘无候选 → 缺。
    电视剧：本盘候选里没有比 last_episode 更靠后的集数 → 缺。
    """
    media_type = str(task.get("media_type", "movie") or "movie").strip().lower()
    if not ranked_candidates:
        return True
    if media_type != "tv":
        return False
    # 电视剧：看候选集数有没有超过本地已保存的最高集
    for candidate in ranked_candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_episode = max(
            0,
            int(candidate.get("episode", 0) or 0),
            int(candidate.get("range_end", 0) or 0),
        )
        if candidate_episode > last_episode:
            return False
    return True
```

> 说明：`ranked_candidates` 里的候选是评分后的 dict，`episode` / `range_end` 是识别出的集数字段（见 `subscription.py` 里 `scored_candidates` 的结构，`episode` 是单集、`range_end` 是连续集的末集）。取两者较大值作为「候选最新集数」。
>
> 若某候选没有集数（episode 和 range_end 都是 0），`candidate_episode` 为 0，不会 > last_episode，不影响判断。这是保守做法：拿不准集数的候选不当作「本盘已能补到新集」。

### 改动 D：`app/services/subscription_task_runner.py` —— 无需新增 import

`subscription_task_runner.py` 顶部（第 3-14 行）已经有这段：

```python
from . import subscription as _subscription
...
globals().update(
    {
        key: value
        for key, value in _subscription.__dict__.items()
        ...
    }
)
```

这段代码会把 `subscription.py` 里的**所有公开函数**自动注入到 `subscription_task_runner.py` 的命名空间（`find_subscription_task_match_candidate_by_search` 就是这么被直接调用的，见通用主链路里的 `await find_subscription_task_match_candidate_by_search(...)`，它没有单独 import）。

因此：**改动 B2 新增的 `find_subscription_cross_copy_candidates` 定义在 `subscription.py` 后，会被 `globals().update` 自动带入，在 `subscription_task_runner.py` 里直接 `await find_subscription_cross_copy_candidates(...)` 即可，无需任何 import，也不会循环。**

执行者注意：
- **不要**在文件顶部加 `from .subscription import find_subscription_cross_copy_candidates`（会循环 import）。
- **不要**在函数内做延迟 import（没必要）。
- 直接调用 `find_subscription_cross_copy_candidates` 即可，和现有代码里直接调用 `find_subscription_task_match_candidate_by_search` 的写法保持一致。

---

## 4. 验收标准（改完自己先查这几条）

1. **`_run_subscription_task_quark` 里已无 `register_cross_copy_from_candidates` 调用**（改动 A1 完成）。可用 grep 确认：全文件搜 `register_cross_copy_from_candidates` 只应出现在「通用主链路」那一处 + 顶部 import。
2. **通用主链路里，补源逻辑在 `ranked_candidates` 组装之后、`if not ranked_candidates` 早退之前**。
3. **`cross_copy_mode == "off"` 或 `cross_copy_sources` 不含 `"quark"` 时，完全不触发补源**（不调搜索、不调注册）。
4. **fallback 模式下，本盘有比本地更新的候选时不补源**（`_subscription_cross_copy_needed` 返回 False）。
5. **prefer 模式下，命中就补源**（不看缺不缺）。
6. **不新增任何第三方依赖**；只改 `app/services/` 下的 `.py`，前端 JS / 模板不动（本次范围）。
7. 改完用 `node --check` 无法验 python，需用 `python -m py_compile` 或项目既有静态检查脚本验证语法。

---

## 5. 明确不做（本次范围外，执行时不要碰）

- 不接阿里 / 115 作源网盘（`cross_copy_sources` 仍只认 `["quark"]`）。
- 不改 `register_cross_copy_from_candidates` 内部逻辑（它已经能正确转存+注册，只是没人调用）。
- 不改前端界面、不改 `cross_copy` 相关路由、不动 `cross_copy_jobs` 表结构。
- 不动「删源」相关逻辑、不动非 115 目标相关逻辑（这些是已定稿的红线，见项目记忆）。

---

## 6. 风险与注意

- **循环 import**：`subscription.py` ↔ `subscription_task_runner.py` 互相引用，新函数务必用函数内延迟 import（改动 D 已说明），否则启动就崩。
- **不要改动 `find_subscription_task_match_candidate_by_search` 内部逻辑**，只做「复制 task + 改 provider」的薄封装（改动 B2），避免碰坏现有 115 搜索/评分。
- **`ranked_candidates` 为空时的顺序**：补源逻辑插在「组装 ranked_candidates」之后、「无候选早退」之前，保证「本盘搜到了也搜不到」都能先被补源逻辑看到。
