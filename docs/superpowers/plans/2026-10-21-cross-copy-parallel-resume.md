# 实施计划：并行分段下载 + 任务断点续传（0.15.1）

> 日期：2026-10-21 ｜ 设计文档：`docs/superpowers/specs/2026-10-21-cross-copy-parallel-download-resume.md`
> 执行方式：两个 Agent 并行，文件属地互斥，主编排（主 Agent）负责集成、测试收口与发版。

## 1. 并行安全的关键：文件属地

两个功能都碰 `cross_cloud_copy.py`，为避免并行写同一文件互相覆盖，
按「整文件归属」切分（而非按函数切）：

| 文件 | 归属 | 说明 |
| --- | --- | --- |
| `app/services/cross_cloud_copy.py` | **A** | 并行下载 + 引擎内 resume 管道（§3.3/§4 全部） |
| `tests/test_cross_cloud_copy.py` | **A** | 引擎既有用例，行为变更随之更新 |
| `tests/test_cross_copy_segmented_download.py` | **A** | F1 主战场 |
| `tests/test_cross_copy_zero_traffic.py` | **A** | 惰性 sink 用例，F1/F2 都经过它 |
| `tests/test_cross_copy_folder.py` | **A** | 引擎变更的回归面 |
| `app/db.py` | **B** | `resume_json` 列 + 迁移 |
| `app/services/cross_copy_jobs.py` | **B** | 任务层续传生命周期 |
| `app/services/cross_copy_resume.py`（新） | **B** | 续传状态构造/校验/清理助手 |
| `tests/test_cross_copy_resume.py`（新） | **B** | F2 单测 |
| `tests/test_cross_copy_jobs.py` | **B** | 任务层用例 |
| `tests/test_local_storage.py` | **B** | 引用了 `xcc._precheck_disk_space`，B 改 precheck 时同步 |
| `app/providers/base.py`、`pan115.py`、`quark.py`、`aliyun.py` | **B** | `upload_resume` 参数 + 115 分片续传 |
| 各 provider 既有上传测试（`test_pan115_upload*`、quark/aliyun 上传相关） | **B** | 随签名变更更新 |

**双方都不许碰**：`version.json` / `CHANGELOG.md` / `README.md` / `state.md` /
`handoff.md` / `modules.md` / 其余任何文件。发版与文档由主编派在集成阶段统一做。
（B 新增 `app/services/cross_copy_resume.py` 需要在 `modules.md` 登记——
由主编派补，B 不碰该文件。）

## 2. 接口契约（唯一接缝，先于代码冻结）

```python
# cross_cloud_copy.py（A 实现，B 只调用）
def copy_file_across_clouds(
    *, source_provider, source_cookie, source_file_id,
    target_provider, target_cookie, target_dir_id,
    filename="", size=-1, temp_dir="", cfg=None,
    progress_cb=None, cancel_check=None, check_existing=False,
    source_sha1="", source_size=-1, source_md5="",
    resume=None,            # NEW Optional[dict]：§3.1 结构；None = 无续传
    on_resume_update=None,  # NEW Callable[[dict], None]：状态就地变更后触发
) -> CopyFileResult

# providers（B 实现，A 只调用）
provider.upload_file(cookie, sink, filename, dir_id, sha1=, md5=, size=,
                     progress_cb=, cancel_check=,
                     upload_resume=None)   # NEW Optional[dict]
# upload_resume["on_part_done"] = Callable[[int, str], None] 由 A 在调用前塞入
```

行为细则以设计文档 §2/§3 为准；契约外的内部实现各自自由。

## 3. 任务分解

### Agent A — 并行分段下载（F1）+ 引擎 resume 管道

A1. `_download_to_sink` 并行化：`CROSS_COPY_DOWNLOAD_PARALLEL`（默认 3，1~8），
    仅临时文件通道；段队列 + `os.pwrite` 按偏移写盘；worker 失败联动停止；
    取消检查点；段级重试。内存通道 / 未知大小保持串行。
A2. 下载完成后顺序读盘算 md5/sha1（`DOWNLOAD_CHUNK_SIZE` 块）；
    `progress_cb` 上报已落盘字节。
A3. `_download_to_sink` 新增 `start_offset`（只下剩余段，返回含
    `downloaded_bytes`）。
A4. `copy_file_across_clouds` 新增 `resume` / `on_resume_update`：
    三种分支（完整跳过下载 / 前缀续下 / 无 resume）；就地更新 resume 并
    触发回调（下载每段节流 ≥1s、每 part、哈希完成时）；「保留临时文件」
    规则按设计 §3.3 末段；成功路径删临时文件不变。
A5. 上传调用处把 `resume.get("upload")` 作为 `upload_resume` 传给
    `target_provider.upload_file`，并预先在其上挂 `on_part_done` 回调
    （回调内更新 `upload.parts` + 触发 `on_resume_update`）。
A6. 测试：扩充 `test_cross_copy_segmented_download.py`（并行正确性 /
    乱序 / 失败 / 取消 / start_offset / 大向量），按需更新
    `test_cross_cloud_copy.py` / `test_cross_copy_zero_traffic.py` /
    `test_cross_copy_folder.py`。

### Agent B — 任务断点续传（F2）

B1. `db.py`：`cross_copy_jobs` 增列 `resume_json TEXT NOT NULL DEFAULT '{}'`
    （`PRAGMA table_info` 迁移模式）；更新 `get/update/list` 对字典序列化
    的读写（`update_cross_copy_job` 支持 `resume=dict` 字段，落库 json.dumps）。
B2. 新建 `app/services/cross_copy_resume.py`：
    - `build_resume_state(job: dict, cfg: dict) -> Optional[dict]`：读
      `job["resume"]`（反序列化后 dict）→ 校验 temp_path 存在 / 大小 /
      源哈希复探比对（不一致作废并清状态）→ 返回可用状态或 None；
    - `clear_resume_state(job_id)`；
    - `prune_resume_for_missing_files(temp_dir)`：孤儿清理联动（temp_path
      不在磁盘上的行清空 resume_json）；
    - 常量与 docstring 写清楚「为什么不信任旧 sha1」。
B3. `cross_copy_jobs.py`：
    - `run_cross_copy_job`：单文件分支执行前 `build_resume_state`，传入
      `copy_file_across_clouds(resume=..., on_resume_update=...)`；
      `on_resume_update` 落库带时间节流（复用 `_last_db_progress` 模式）；
      成功分支 `clear_resume_state`；失败 / 取消分支落最终 resume 状态，
      取消文案追加「已保留断点，重试可续传」；
    - 文件夹分支显式不传 resume（代码注释说明）；
    - `cleanup_temp_files_on_startup` / `cross_copy_temp_cleanup_watcher` 联动
      `prune_resume_for_missing_files`；
    - `delete_cross_copy_job` / `clear_cross_copy_jobs` 尽力清理对应临时文件
      （失败仅日志；删记录不碰网盘文件的红线不变）。
B4. providers：
    - `base.py` `upload_file` 签名加 `upload_resume: Optional[dict] = None`
      并补 docstring；
    - `pan115.py`：§3.4 完整实现（跳过 initupload/init、重取 token、
      `_oss_list_parts` 对账、跳过已完成 part、带回 callback complete；
      part 完成调 `upload_resume["on_part_done"]`）；
    - `quark.py` / `aliyun.py`：接收参数、本期不实现会话续传（注释写明
      原因：预签名 / pre 会话跨进程复用未经真机验证），行为同现状。
B5. 测试：新建 `test_cross_copy_resume.py`（校验矩阵 / 跳过下载 /
    本地 sink / 115 分片续传 mock / 生命周期落库与清除 / 联动清理）；
    扩充 `test_cross_copy_jobs.py`；更新 provider 上传既有用例。

## 4. 验证节奏（每个 Agent 内部）

- 只跑**自己属地**的测试模块，例：
  `scripts/run_tests.py tests.test_cross_cloud_copy tests.test_cross_copy_segmented_download tests.test_cross_copy_zero_traffic tests.test_cross_copy_folder`
  （A）；`scripts/run_tests.py tests.test_cross_copy_jobs tests.test_cross_copy_resume tests.test_local_storage` 加 provider 上传相关（B）。
- **不要用 `--changed`**：并行期间 git 工作区包含对方未完成改动，挑选结果不可信。
- 对方属地模块的失败一律不修、只在汇报里提及（集成阶段由主 Agent 定夺）。
- 语法：`PYTHONPYCACHEPREFIX=/tmp/115-media-hub-pycache .venv/bin/python -m compileall app main.py`。
- 完成后汇报：改了哪些文件、新增/修改了哪些测试及结果、已知妥协点。

## 5. 集成与发版（主 Agent）

1. 双方完成后：全量属地测试 + `scripts/run_tests.py --changed`；
2. `modules.md` 登记 `cross_copy_resume.py`；
3. 容器全量 `scripts/check.sh --all --container`（零失败才收口）；
4. 发版 0.15.1：`version.json` / `CHANGELOG.md` / `README.md`（功能描述 +
   近期更新）/ `state.md`；`handoff.md` 追加一条 + `rotate_handoff.py`；
5. 提交（commit message 写清两个功能与验证证据）；推送 / 打 tag 前经用户确认。
