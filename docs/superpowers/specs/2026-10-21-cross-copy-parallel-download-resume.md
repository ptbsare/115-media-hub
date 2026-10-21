# 跨盘复制：并行分段下载 + 任务断点续传 设计

> 日期：2026-10-21 ｜ 版本目标：0.15.1 ｜ 状态：已定稿，进入实施
>
> 关联：`docs/superpowers/plans/2026-10-21-cross-copy-parallel-resume.md`（实施计划与 Agent 分工）

## 1. 背景与目标

跨盘复制链路已全量上线（0.15.0），当前两个明确的体验短板：

1. **下载是单线程串行**：`_download_to_sink` 按 16MB 段逐段请求，实测 5~10MB/s。
   夸克等网盘只对「整文件 GET」限速，Range 分段请求能跑 4~6MB/s（单段），
   段与段之间没有并发，带宽没用满。
2. **任务重试 / 服务重启后从头再来**：下载到 90% 失败、或上传到一半被取消，
   临时文件在 `finally` 里被删，重试时重新下载 + 重新上传。大文件（GB 级）
   的失败成本极高。

本期目标（均为单文件任务；文件夹任务的语义见 §5.3）：

- **F1 并行分段下载**：同一任务内多段并发拉取，哈希在落盘后顺序计算，
  结果与整文件流完全一致（同一份 bytes、同一个 SHA1/MD5）。
- **F2 断点续传**：任务失败 / 取消 / 服务重启后，本地已下载的数据与
  115 上传会话（分片级）可继承；重试时跳过已完成部分。

硬约束（项目既有约定，不可破坏）：

- 零新增第三方依赖（并发用标准库 `threading` / `queue`）。
- 复制结果必须与现在逐字节一致：同样的源 bytes → 同样的 SHA1/MD5 →
  同样的秒传判定。并行只是「拉数据的方式」变了，不是「算哈希的方式」变了。
- 取消语义不变：取消必须在下一个检查点生效，且不能留下半截任务状态。
- 删源红线不变：断点续传不碰「删源」逻辑，`origin == "subscription"` 的
  删源条件原样保留。

## 2. F1 并行分段下载

### 2.1 现状

`_download_to_sink`（`app/services/cross_cloud_copy.py`）：

- 知道文件大小时按 `DOWNLOAD_SEGMENT_BYTES`（默认 16MB，环境变量
  `CROSS_COPY_DOWNLOAD_SEGMENT_MB`，1~128）逐段 `Range` 请求；
- 段内边下边算 md5/sha1（顺序流，天然一致）；
- 外层 `DOWNLOAD_MAX_RETRIES`（3 次）整文件重试，失败换链（直链过期）；
- ≤64MB 走内存通道（BytesIO），>64MB 走临时文件通道；
- 大小未知时退回整文件流（无法分段）。

### 2.2 设计

**并发模型**（仅临时文件通道，即 `size > MEMORY_CHANNEL_THRESHOLD_BYTES`）：

- 新增 `CROSS_COPY_DOWNLOAD_PARALLEL`（默认 3，取值 1~8，非法值回落 3）。
  1 = 现状（串行），供对照与回退。
- 段序号 0..N-1 组成工作队列；N 个 worker 线程各自「取一段 → Range 请求 →
  流式写盘」，写盘用 `os.pwrite` 按段偏移落位（无共享文件指针，无需锁）。
- 每个 worker 独立 `requests.Session`？——不，段请求本身无状态，共用模块级
  连接池即可；worker 内维护自己的响应流。
- **内存通道（≤64MB）与大小未知场景保持串行**：文件小到并行收益低于线程
  开销，且 BytesIO 并发写需要额外加锁，不值得。

**哈希策略**（一致性关键）：

- 并行段内**不算哈希**；全部段落盘后，按 0..N-1 顺序整文件顺序读一遍
  （`DOWNLOAD_CHUNK_SIZE` = 1MB）累计 md5/sha1。
- 读的是本地盘（机械盘 100~200MB/s 起），相对网络下载是可接受成本；
  语义上与「边下边算」逐字节等价——SHA1/MD5 是对同一段字节的同样计算，
  与切分边界无关（流式哈希与分块后拼接再哈希结果相同，这是哈希的基本性质，
  用测试钉住：并行结果 == 串行结果 == 已知向量）。
- 大文件哈希读盘期间 `progress_cb` 继续按「已下载字节」上报，不伪造进度。

**失败与取消**：

- 任一 worker 失败（网络错误 / 非 2xx / 段内字节不完整）：通知其余 worker
  停止（共享 `threading.Event`），本段记失败；由外层既有重试循环换链重来。
- 取消：`cancel_check` 在每个 worker 的读块循环里检查，命中即停并抛
  `CrossCloudCopyCancelled`（与现状同一异常类型，上层无感）。
- 断流只补当前段的语义保留：段粒度重试由 worker 内部的段级重试兜底
  （沿用 `DOWNLOAD_MAX_RETRIES` 的退避基数，仅重试本段，不换链——
  换链是外层的事）。

**续传接缝**（与 F2 的契约，§3）：

- `_download_to_sink` 新增入参 `start_offset: int = 0`：只请求
  `[start_offset, total)` 范围的段，之前落盘的连续前缀不重下；
  调用方负责保证前缀字节有效（见 §3.2）。
- 新增出参中的 `downloaded_bytes`：本次调用实际落盘的字节数（含前缀），
  供上层写续传状态。

**进度语义**：`progress_cb(sent, total, phase)` 的 `sent` = 已落盘总字节
（跨 worker 汇总，原子计数）。UI 侧「下载 x%」含义不变。

## 3. F2 任务断点续传

### 3.1 续传状态（resume state）

挂在 `cross_copy_jobs` 行上，新增列 `resume_json TEXT NOT NULL DEFAULT '{}'`
（迁移方式沿用既有 `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` 模式）。
运行时结构（dict，字段可空）：

```python
{
  "temp_path": "/app/config/cross_copy_tmp/xcopy-ab12cd.part",  # 本地暂存文件
  "size": 2793458176,            # 预期总大小（来自任务 / 源元信息）
  "sha1": "A1B2...",             # 已算出的 SHA1（大写）；空 = 尚未算
  "md5": "d41d...",              # 已算出的 MD5（小写）；空 = 尚未算
  "downloaded_bytes": 2415919104,# 已落盘的连续前缀字节数（0 = 无前缀）
  "upload": {                    # 目标端上传会话；非 115 或未开始上传时为 {}
      "provider": "115",
      "fields": { ... },         # initupload 原始返回（bucket/object/callback/pickcode…）
      "upload_id": "oss-upload-id",
      "parts": [ {"PartNumber": 1, "ETag": "..."}, ... ],  # 已完成分片
  },
}
```

不变量：

- `sha1` 非空 ⟺ 文件已完整落盘且已算哈希（哈希只在下载完成后计算，
  中断不会留下「算了一半的哈希」）。
- `downloaded_bytes` 始终是**连续前缀**（并行下载中断时，未连续完成的
  后续段即使落盘也不计入，重下时覆盖）。
- `upload.parts` 与 OSS 侧实际状态最终一致（每次 part 完成即写，取消/失败
  时落最后一次已知状态；重启后以 `_oss_list_parts` 实测为准，§3.4）。

### 3.2 生命周期

```
创建任务 ──► 运行 ──► 成功：删临时文件 + 清 resume_json（终态干净）
                │
                ├─ 失败 / 取消：保留临时文件 + 写 resume_json（终态可续）
                │
                └─ 服务重启：requeue 后重新进入运行（同一入口，自动继承）
```

- **运行前校验**（`cross_copy_resume.build_resume_state`）：
  1. `temp_path` 存在、`st_size == size` 或 `>= downloaded_bytes`（前缀完整）；
  2. 任务行的 `source_provider/source_file_id` 与 resume 记录一致
     （resume 挂在任务行上天然一致，防御性检查）；
  3. 重新探测源端哈希（`resolve_source_hash`，元信息级、带备忘录）与
     `sha1` 比对：**不一致说明源文件已变，作废续传**，走全新复制
     （防止把旧内容传到目标端）；探测不到（夸克给不出哈希）时接受续传。
  4. 任一不满足 → 返回 None（调用方按无续传处理），并清掉该行 resume_json。
- **成功**：删临时文件、清 `resume_json`（既有 finally 删临时文件的路径不变，
  由任务层在成功分支显式清状态）。
- **失败 / 取消**：临时文件保留；`resume_json` 落当前最新状态
  （`downloaded_bytes` + `upload` 会话）。`status_detail` 追一句
  「已保留断点，重试可续传」——失败与取消对用户是同一个信号：
  这次没成，但进度在，重试能接着来。
- **孤儿清理联动**：`cleanup_orphan_temp_files` 删掉超过 24h 的
  `xcopy-*.part` 时，对应任务的 `resume_json` 一并作废（扫描时顺带清，
  或定时任务里按「temp_path 不存在」批量清——实现选后者，一次 SQL 搞定）。

### 3.3 引擎侧接缝（`copy_file_across_clouds` 新参数）

```python
def copy_file_across_clouds(
    *, ...既有参数...,
    resume: Optional[Dict[str, Any]] = None,      # 续传状态（可变，就地更新）
    on_resume_update: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> CopyFileResult:
```

- `resume` 由任务层构造并传入；引擎只消费合法状态（校验在任务层）。
- 引擎在三个时点**就地更新** `resume` 并触发 `on_resume_update`：
  ① 下载每完成一段（节流 ≥1s）；② 上传每个分片完成（115）；
  ③ 下载完成算出哈希时（一次性写 `sha1/md5/downloaded_bytes=size`）。
- 分支逻辑：
  - `resume` 有效且 `sha1` 非空且文件大小 == `size` → 直接用本地文件做
    sink（新开只读文件对象），**跳过下载**；`upload` 会话按 §3.4 消费。
  - `resume` 有效但无 `sha1`（上次中断在下载中）→ 以
    `start_offset=downloaded_bytes` 续下剩余段，完成后算全量哈希。
  - `resume` 为 None → 现状路径（不变）。
- 失败/取消时引擎**不删**临时文件（区别于现状的 finally 删除）——由任务层
  按 §3.2 决定保留与落状态；删除只发生在「本次运行全新下载且失败」的场景
  （没有可继承的续传价值时，避免无限堆积）。规则：
  - 本次是「续传运行」→ 失败/取消一律保留；
  - 本次是「全新运行」→ 下载阶段失败保留（下载进度有价），上传阶段失败
    也保留（本地数据 + 上传会话都有价）。简言之：**只要临时文件里已有
    完整或部分有效数据且来源哈希可信，就保留**；全新运行在「还没下到任何
    字节」时失败，照旧删。
- 成功路径：返回前照常删临时文件（现状逻辑），`resume` 的清理由任务层做。

### 3.4 115 上传分片续传（唯一实现的 provider 级续传）

115 上传是 OSS 多段式：`initupload` → fields(bucket/object/callback) →
OSS token → `UploadPart ×N` → `CompleteMultipartUpload`。模块已有
`_oss_list_parts`，续传可行：

- **首run**：`initupload` 后把 `fields + upload_id` 写进 resume；
  每完成一个 part 把 `{PartNumber, ETag}` 追加进 `upload.parts`。
- **续run**：跳过 `initupload` 与 `_oss_multipart_init`，用 resume 里的
  `fields/upload_id`；重新取 OSS token（token 短时效，必须重取）；
  `_oss_list_parts` 拉实测分片列表，与本地 `parts` 取并集后跳过已完成
  part（sink seek 到对应偏移读后续分片）；最后用带回的 `callback`
  `CompleteMultipartUpload`。
- 秒传命中的分支不受影响（探测在 initupload 一步，命中即返回）。
- **夸克 / 阿里**：`upload_file` 接受 `upload_resume` 参数但本期不实现
  会话级续传（夸克 pre/阿里预签名 URL 的跨会话复用未经真机验证，不冒险），
  上传从头再来——但本地数据已由 §3.3 保留，**下载不再重来**，这已经覆盖
  主要成本（115→夸克方向下载是刚需且最慢）。协议调研结论写进代码注释。

### 3.5 provider 接口

`base.CloudProvider.upload_file` 已有 `**kwargs`，新增关键字参数不破坏
现有实现签名：

```python
upload_file(..., upload_resume: Optional[Dict[str, Any]] = None)
```

- `upload_resume` 非空且 `provider == "115"` 时按 §3.4 消费；
- 其他值 / 其他 provider：忽略（参数留着，行为同现状）。
- 分片完成上报：`upload_resume["on_part_done"](part_number, etag)`
  （由引擎在调用前塞入回调；provider 只负责调用，不关心落库）。

## 4. 接口契约（F1 ↔ F2 唯一接缝）

F1（并行下载）与 F2（续传）都改 `cross_cloud_copy.py`，由同一实施方落地，
接缝只有两处：

1. `_download_to_sink(..., start_offset=0)`：只下 `[start_offset, total)`，
   返回 `{"size": 落盘总字节, "md5": ..., "sha1": ..., "downloaded_bytes": 同size}`。
2. `copy_file_across_clouds(..., resume=..., on_resume_update=...)`：§3.3。

任务层（`cross_copy_jobs.py`）只面对 `copy_file_across_clouds` 的公开签名，
不接触下载内部；provider 只面对 `upload_file(..., upload_resume=...)`。

## 5. 边界与不做

1. **不做纯流式边下边传**（既有决议，2026-10-18）：先完整落盘再上传不变。
2. **文件夹任务本期不做 per-file 续传**：文件夹任务内部逐文件复制，
   `check_existing=True` 已保证重试时跳过目标端已存在的文件；进行中的
   那个文件重新下载（其临时文件不保留）。任务记录与文档如实说明。
3. **不做夸克/阿里会话级上传续传**（§3.4）。
4. **不做跨进程断点下载的「部分字节校验」**：前缀有效性由「连续前缀」规则
   与运行前大小校验保证，不做逐字节重校验（本地自有文件，威胁模型内可信）。
5. 不动「删源」红线、不动并发上限语义（`CROSS_COPY_MAX_CONCURRENT` 是
   任务级并发；下载并行是任务内部的线程数，两者独立）。
6. 不新增 CLI 命令（续传状态对用户透明，界面文案已足够）。

## 6. 测试策略

- **并行下载正确性**（tests/test_cross_cloud_copy_segmented_download.py 扩充）：
  - 同一 mock 源，`CROSS_COPY_DOWNLOAD_PARALLEL=1/3/8` 结果三元相等
    （size/md5/sha1/落盘 bytes）；
  - worker 乱序完成（mock 延迟）后落盘内容仍逐字节正确（按偏移写）；
  - 任一 worker 失败 → 整体重试、不产出半截哈希；
  - 取消在各 worker 检查点生效；
  - `start_offset` 续下：前缀不被重下（mock 计数断言），最终哈希一致；
  - 大文件向量：用已知内容的伪随机字节流，并行下载后 SHA1 == 预计算值。
- **续传**（新增 tests/test_cross_copy_resume.py + 扩充 test_cross_copy_jobs.py）：
  - resume state 校验矩阵：文件缺失 / 大小不符 / 源哈希变化作废 /
      哈希探测不到时接受；
  - 中断后续跑：跳过下载（mock `_download_to_sink` 断言调用参数
      `start_offset == downloaded_bytes` 或根本不调用）；
  - `sha1` 已有时直接用本地文件 sink，上传读到正确 bytes；
  - 115 分片续传：mock `_oss_list_parts` 返回部分分片 → 跳过已完成 part，
      最终 complete 的 parts 列表完整且顺序正确；
  - 失败/取消后 `resume_json` 落库、成功后清除、孤儿清理联动清除；
  - 取消文案含「已保留断点，重试可续传」。
- **回归**：test_cross_cloud_copy / test_cross_copy_jobs /
  test_cross_copy_folder / test_subscription_cross_copy /
  test_cli_payloads 全绿；provider 上传既有用例不破（参数新增带默认值）。
- 最终以容器全量收口：`scripts/check.sh --all --container`。
