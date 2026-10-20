# 上传分片失败改为「只重发那一片」

日期：2026-10-20

## 要解决的问题

跨盘复制的链路是「源盘下载 → 本地暂存 → 目标盘上传」。原来的上传实现里，三个网盘
都是**一个分片失败就整文件失败**：

- 夸克：`upload_quark_file` 的循环直接 `_quark_upload_part(...)`，抛了就往外抛；
- 阿里：分片 PUT 只发一次，`raise UploadError(retriable=True)` 之后由任务层兜；
- 115：`_oss_upload_part` 同理，只有 token 获取那一段有 405/429 退避。

而任务层重试是**整文件重试**：重新从源盘下载一遍再上传。一部 20GB 的影片传到第
180 个分片抖一下，前面 179 个分片的流量全部作废——在家庭带宽下这是小时级的代价。

## 改法

在 `app/providers/base.py` 加一个分片级重试器，三家 provider 的分片发送统一走它：

- `is_retriable_upload_error(exc)`：只把**瞬时故障**判为可重试。
  `UploadError(retriable=True)` 直接看标志位；内建 `ConnectionError` / `TimeoutError`
  以及 requests / urllib3 家族（按类名与模块名识别，不额外引入依赖）也算；
  `retriable=False` 的业务错误（鉴权失效、参数不合法、目录不存在）立刻抛出。
- `upload_part_with_retry(send_part, part_number=..., label=..., ...)`：
  默认 1 次首发 + 2 次重试，退避 `1s → 3s`；重试前问一次 `cancel_check`，
  用户点了取消就不必干等退避；用尽次数后抛 `UploadError`，文案带分片号与重试次数。

为什么在**分片**这一层：重发的代价只是一片（几 MB~几十 MB），而整文件重试的代价
是「重新下载整个文件」。

## 接入点

| 网盘 | 位置 | 说明 |
|---|---|---|
| 夸克 | `quark.upload_quark_file` 分片循环 | chunk 已读在内存，重试不重新读 sink |
| 阿里 | `aliyun.upload_file` 分片 PUT | 抽出 `_send_part()` 闭包，预签名 URL 复用 |
| 115 | `pan115.upload_115_file` 分片循环 | `_UploadRateLimited`（405/429）就地转成可重试的 `UploadError` |

三家的 chunk 都是「先读进内存再发」，所以重试不会二次消费 sink、也不会打乱分片顺序。

## 明确没做（边界）

- **不做跨进程续传**：进程重启后接着上次的 `uploadId` 继续传，需要把分片会话
  （uploadId / 已成功分片 ETag / 目标 file_id）持久化，三家语义还不一样。这轮只解决
  「同一次上传里某个分片抖一下」这个高频场景。
- 不改 `supports_upload_resume` 能力标志——它表达的是上面那种会话级续传，本次没实现。

## 测试

- `tests/test_upload_part_retry.py`（10 项）：重试到成功、业务错误不重试、
  用尽次数的文案、网络异常识别、取消语义、退避序列取值。
- 三家各一条集成测试，断言「只重发失败的那一片」：
  - `test_quark_upload.py::TestQuarkUploadPartRetry`（2 项）
  - `test_aliyun_upload.py::TestAliyunUploadPartRetry`（2 项）
  - `test_pan115_upload.py::test_flaky_part_is_retried_without_restarting_file`

## 顺带修的一个真 bug（同一轮）

任务卡片的「速度」一直显示不出来：`cross_copy_jobs._report_progress` 用
`prev_ts > 0` 判断「有没有上一帧」，而 `time.monotonic()` 的原点不固定
（沙箱/短命进程里可能从 0 附近起步），时间戳落到 0 以下就被当成「没有上一帧」，
速度静默归零。改为只看 `prev` 是否存在。回归测试见
`test_cross_copy_jobs.py::test_speed_uses_previous_frame_even_with_nonpositive_timestamp`。
