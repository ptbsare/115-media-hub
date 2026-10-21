"""跨盘复制下载侧：Range 分段下载单测（mock HTTP，无真实网络）。

真机背景：夸克对「整文件 GET」限速到 ~100KB/s，但对 Range 分段请求按正常速度返回
（同账号同文件实测 4~11MB/s）。2.6GB 单文件若走整文件流要 7 小时以上。

0.15.1 起临时文件通道支持段并发（CROSS_COPY_DOWNLOAD_PARALLEL，默认 3）：
段队列 + worker 线程 + os.pwrite 按偏移落盘，全部落盘后整文件算哈希。
"""
import hashlib
import io
import os
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

import requests

from app.services import cross_cloud_copy as xcc


class _FakeProvider:
    name = "quark"
    label = "夸克网盘"

    def __init__(self, url="https://dl.example.com/f", cookie="ck-1"):
        self.url = url
        self.cookie = cookie
        self.resolve_calls = 0

    def resolve_download_info(self, cookie, file_id):
        self.resolve_calls += 1
        return {"url": self.url, "cookie": self.cookie}


class _FakeResponse:
    def __init__(self, status_code=206, body=b""):
        self.status_code = int(status_code)
        self._body = body
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def close(self):
        self.closed = True


def _parse_range(header):
    raw = str(header or "").replace("bytes=", "")
    start, _, end = raw.partition("-")
    return int(start), int(end)


class SegmentedDownloadTestBase(unittest.TestCase):
    SEGMENT = 64 * 1024

    def setUp(self):
        self.data = bytes(range(256)) * 1024 * 3  # 768KB，12 段
        self.calls = []
        self.plan = []  # None=正常返回 206；Exception=抛错；bytes=返回该段数据

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            self.calls.append({"url": url, "headers": dict(headers or {})})
            if "Range" not in (headers or {}):
                return _FakeResponse(200, self.data)
            start, end = _parse_range((headers or {}).get("Range"))
            if self.plan:
                action = self.plan.pop(0)
                if isinstance(action, Exception):
                    raise action
                if isinstance(action, bytes):
                    return _FakeResponse(206, action)
            return _FakeResponse(206, self.data[start:end + 1])

        self._patchers = [
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch("app.services.cross_cloud_copy.time.sleep"),
            patch.object(xcc, "DOWNLOAD_SEGMENT_BYTES", self.SEGMENT),
        ]
        for patcher in self._patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _download(self, total=None, sink=None):
        self.sink = sink if sink is not None else io.BytesIO()
        self.progress = []
        result = xcc._download_to_sink(
            source_provider=_FakeProvider(),
            source_cookie="ck-1",
            source_file_id="fid-1",
            sink=self.sink,
            total=len(self.data) if total is None else total,
            progress_cb=lambda sent, tot, phase: self.progress.append((sent, tot, phase)),
        )
        return result


class TestSegmentedDownload(SegmentedDownloadTestBase):
    def test_requests_each_segment_with_increasing_range(self):
        result = self._download()

        self.assertEqual(result["size"], len(self.data))
        self.assertEqual(result["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(result["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(self.sink.getvalue(), self.data)
        self.assertEqual(len(self.calls), len(self.data) // self.SEGMENT)
        starts = [_parse_range(call["headers"]["Range"])[0] for call in self.calls]
        self.assertEqual(starts, [i * self.SEGMENT for i in range(len(self.calls))])
        self.assertEqual(self.progress[-1][0], len(self.data))
        self.assertTrue(all(item[2] == "download" for item in self.progress))

    def test_last_segment_is_clamped_to_total(self):
        self.data = self.data[: len(self.data) - 1000]
        self._download()
        start, end = _parse_range(self.calls[-1]["headers"]["Range"])
        self.assertEqual(end, len(self.data) - 1)
        self.assertEqual(self.sink.getvalue(), self.data)

    def test_server_ignoring_range_falls_back_to_single_stream(self):
        full = _FakeResponse(200, self.data)
        with patch("app.services.cross_cloud_copy.requests.get", return_value=full):
            result = self._download()
        self.assertEqual(result["size"], len(self.data))
        self.assertEqual(self.sink.getvalue(), self.data)

    def test_partial_segment_retries_with_fresh_link(self):
        # 第 2 段第一次只回一半，断流后应重新换链从头再来
        short = self.data[self.SEGMENT: self.SEGMENT + self.SEGMENT // 2]
        self.plan = [None, short]
        provider = _FakeProvider()
        sink = io.BytesIO()
        result = xcc._download_to_sink(
            source_provider=provider,
            source_cookie="ck-1",
            source_file_id="fid-1",
            sink=sink,
            total=len(self.data),
        )
        self.assertEqual(result["size"], len(self.data))
        self.assertEqual(sink.getvalue(), self.data)
        self.assertGreaterEqual(provider.resolve_calls, 2)

    def test_http_error_is_wrapped_as_download_failure(self):
        self.plan = [requests.ConnectionError("boom")] * xcc.DOWNLOAD_MAX_RETRIES
        with self.assertRaises(xcc.CrossCloudCopyError) as ctx:
            self._download()
        self.assertIn("源文件下载失败", str(ctx.exception))
        self.assertTrue(ctx.exception.retriable)

    def test_unknown_size_uses_plain_stream(self):
        self._download(total=-1)
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn("Range", self.calls[0]["headers"])
        self.assertEqual(self.sink.getvalue(), self.data)


class _OrderTrackingResponse(_FakeResponse):
    """响应体被读完后记录一次「该段完成」（用于断言乱序完成顺序）。"""

    def __init__(self, status_code, body, on_finish):
        super().__init__(status_code, body)
        self._on_finish = on_finish

    def iter_content(self, chunk_size=1):
        for chunk in super().iter_content(chunk_size):
            yield chunk
        self._on_finish()


def _pseudo_random_bytes(total: int, seed: str = "cross-copy-parallel-vector") -> bytes:
    """确定性的伪随机字节流（按 1MB 块展开 sha1）：生成快、可复现、内容不可压缩。"""
    block_size = 1024 * 1024
    out = bytearray()
    index = 0
    while len(out) < total:
        digest = hashlib.sha1(f"{seed}:{index}".encode("utf-8")).digest()
        out += digest * (block_size // len(digest))
        index += 1
    return bytes(out[:total])


class ParallelDownloadTestBase(unittest.TestCase):
    """并行分段下载底座：临时文件 sink + 线程安全的 Range 服务。

    与串行底座（SegmentedDownloadTestBase）的两个关键区别：
    1. mock 必须线程安全——worker 并发请求，不能用 pop(0) 式的计划表；
    2. sink 是真正的临时文件——并行按偏移写盘（os.pwrite）只在文件通道生效，
       内存流（BytesIO）没有 fd，仍然走串行。
    """

    SEGMENT = 64 * 1024

    def setUp(self):
        self.data = bytes(range(256)) * 1024 * 12  # 3MB，48 段
        self.lock = threading.Lock()
        self.calls = []      # 每次 Range 请求的段起始偏移（含失败重试）
        self.completed = []  # 段完成顺序（响应体读完后记录）
        self.fail_left = {}  # {段起始偏移: 剩余失败次数}：段级重试注入
        self.delays = {}     # {段起始偏移: 秒}：乱序完成注入
        self.fail_first_link = False  # 首条直链期间全部请求失败（整体重试注入）
        self.provider = _FakeProvider()
        self.progress = []
        self.temp_dir = tempfile.mkdtemp(prefix="xcopy-par-")
        self.addCleanup(shutil.rmtree, self.temp_dir, ignore_errors=True)

        def fake_get(url, stream=False, headers=None, timeout=None, **kwargs):
            start, end = _parse_range((headers or {}).get("Range"))
            with self.lock:
                self.calls.append(start)
                if self.fail_first_link and self.provider.resolve_calls <= 1:
                    raise requests.ConnectionError(f"mock: 首条直链不可用（段 {start}）")
                fails = self.fail_left.get(start, 0)
                if fails > 0:
                    if fails > 1:
                        self.fail_left[start] = fails - 1
                    else:
                        self.fail_left.pop(start, None)
                    raise requests.ConnectionError(f"mock: 段 {start} 断流")
                delay = self.delays.get(start, 0.0)
            if delay > 0:
                # 不能用 time.sleep：测试把模块级 time.sleep 打桩成了 no-op，
                # 这里用 Event.wait（真实定时器）制造乱序完成
                threading.Event().wait(delay)
            return _OrderTrackingResponse(
                206, self.data[start:end + 1], lambda: self.completed.append(start)
            )

        for patcher in (
            patch("app.services.cross_cloud_copy.requests.get", side_effect=fake_get),
            patch("app.services.cross_cloud_copy.time.sleep"),
            patch.object(xcc, "DOWNLOAD_SEGMENT_BYTES", self.SEGMENT),
            patch.object(xcc, "DOWNLOAD_PARALLEL_WORKERS", 3),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _open_file_sink(self, name="sink.part", mode="w+b"):
        return open(os.path.join(self.temp_dir, name), mode)

    def _download(
        self,
        sink=None,
        total=None,
        start_offset=0,
        cancel_check=None,
        on_segment_done=None,
    ):
        self.progress = []
        sink = sink if sink is not None else self._open_file_sink()
        result = xcc._download_to_sink(
            source_provider=self.provider,
            source_cookie="ck-1",
            source_file_id="fid-1",
            sink=sink,
            total=len(self.data) if total is None else total,
            progress_cb=lambda sent, tot, phase: self.progress.append((sent, tot, phase)),
            cancel_check=cancel_check,
            start_offset=start_offset,
            on_segment_done=on_segment_done,
        )
        return sink, result

    def _assert_matches_data(self, sink, result):
        """并行/串行/续传的公共断言：落盘逐字节一致、哈希一致、进度不回退。"""
        sink.seek(0)
        self.assertEqual(sink.read(), self.data)
        self.assertEqual(result["size"], len(self.data))
        self.assertEqual(result["downloaded_bytes"], len(self.data))
        self.assertEqual(result["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(result["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        download_progress = [item[0] for item in self.progress if item[2] == "download"]
        self.assertEqual(download_progress, sorted(download_progress))
        self.assertEqual(download_progress[-1], len(self.data))
        self.assertTrue(all(item[2] == "download" for item in self.progress))


class TestParallelSegmentedDownload(ParallelDownloadTestBase):
    """F1 并行分段下载：正确性 / 乱序 / 失败 / 取消 / 续传接缝 / 大向量。"""

    def test_worker_counts_produce_identical_results(self):
        """同一 mock 源，PARALLEL=1/3/8 三元相等（size/md5/sha1/落盘 bytes）。

        并行只是「拉数据的方式」变了，不是「算哈希的方式」变了。
        """
        results = {}
        for workers in (1, 3, 8):
            with patch.object(xcc, "DOWNLOAD_PARALLEL_WORKERS", workers):
                sink, result = self._download()
                self._assert_matches_data(sink, result)
                results[workers] = dict(result)
                sink.close()
        self.assertEqual(results[1], results[3])
        self.assertEqual(results[1], results[8])

    def test_out_of_order_completion_lands_correct_bytes(self):
        """worker 乱序完成（mock 延迟）：按偏移写的落盘内容仍逐字节正确。"""
        segment_count = len(self.data) // self.SEGMENT
        # 后 8 段里的偶数段故意慢：两个 worker 分别在 40/41 上并行时，
        # 41（快）必然先于 40（慢）完成——完成顺序与段序号相反
        for index in range(40, 48, 2):
            self.delays[index * self.SEGMENT] = 0.05
        sink, result = self._download()
        self._assert_matches_data(sink, result)
        # 慢段必然最后完成（完成顺序由 mock 记录）
        self.assertEqual(len(self.completed), segment_count)
        self.assertEqual(
            set(self.completed[-8:]), {index * self.SEGMENT for index in range(40, 48)}
        )
        # 乱序的硬证据：后序号先完成
        self.assertLess(
            self.completed.index(41 * self.SEGMENT),
            self.completed.index(40 * self.SEGMENT),
        )

    def test_worker_failure_retries_with_fresh_link(self):
        """任一 worker 失败 → 联动停止其余 worker，外层换链整体重试后成功。"""
        self.fail_first_link = True
        sink, result = self._download()
        self._assert_matches_data(sink, result)
        # 换过链（首条直链整个不可用），且重试后全部段下完
        self.assertEqual(self.provider.resolve_calls, 2)
        segment_count = len(self.data) // self.SEGMENT
        self.assertGreaterEqual(len(self.calls), segment_count + 1)

    def test_segment_level_retry_without_link_refresh(self):
        """段内断流只补当前段（worker 内部重试），不换链。"""
        segment_count = len(self.data) // self.SEGMENT
        self.fail_left = {index * self.SEGMENT: 1 for index in range(segment_count)}
        sink, result = self._download()
        self._assert_matches_data(sink, result)
        self.assertEqual(self.provider.resolve_calls, 1)  # 没换链
        self.assertEqual(len(self.calls), segment_count * 2)  # 每段第一次失败、第二次成功

    def test_cancel_takes_effect_in_workers(self):
        """取消在各个 worker 的读块循环检查点生效（同一异常类型，上层无感）。"""

        def cancel_check():
            return len(self.progress) >= 3

        with self.assertRaises(xcc.CrossCloudCopyCancelled):
            self._download(cancel_check=cancel_check)
        # 没下完就停了（48 段不可能在 3 个检查点内完成）
        self.assertLess(len(self.calls), len(self.data) // self.SEGMENT)

    def test_start_offset_skips_prefix_and_hash_stays_correct(self):
        """start_offset：前缀不重下（mock 计数断言），最终哈希与整文件一致。"""
        prefix = 4 * self.SEGMENT
        with self._open_file_sink("resume.part", "wb") as fh:
            fh.write(self.data[:prefix])
        sink = self._open_file_sink("resume.part", "r+b")
        sink.seek(prefix)  # 引擎续传分支同样先把写入口移到断点
        segments = []
        _, result = self._download(
            sink=sink, start_offset=prefix, on_segment_done=segments.append
        )
        self.assertEqual(result["size"], len(self.data))
        self.assertEqual(result["downloaded_bytes"], len(self.data))
        self.assertEqual(result["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(result["md5"], hashlib.md5(self.data).hexdigest())
        # 前缀区间一个请求都没发；剩余段一个不少
        self.assertFalse([call for call in self.calls if call < prefix])
        self.assertEqual(len(self.calls), (len(self.data) - prefix) // self.SEGMENT)
        # on_segment_done 报的是「连续前缀」：首段完成即推进到 prefix+SEG
        self.assertEqual(segments[0], prefix + self.SEGMENT)
        sink.seek(0)
        self.assertEqual(sink.read(), self.data)

    def test_start_offset_misaligned_raises(self):
        """非 0 的 start_offset 必须按段边界对齐，否则直接失败（坏续传状态）。"""
        with self._open_file_sink("bad.part", "wb") as fh:
            fh.write(self.data[:100])
        sink = self._open_file_sink("bad.part", "r+b")
        with self.assertRaises(xcc.CrossCloudCopyError):
            self._download(sink=sink, start_offset=100)
        self.assertEqual(self.calls, [])

    def test_serial_resume_path_hashes_prefix(self):
        """续传且剩余不足 2 段 → 串行分支：前缀先喂进哈希，否则哈希只覆盖后半段。"""
        segment_count = len(self.data) // self.SEGMENT
        prefix = (segment_count - 1) * self.SEGMENT  # 只剩最后 1 段
        with self._open_file_sink("tail.part", "wb") as fh:
            fh.write(self.data[:prefix])
        sink = self._open_file_sink("tail.part", "r+b")
        sink.seek(prefix)
        _, result = self._download(sink=sink, start_offset=prefix)
        self.assertEqual(result["sha1"], hashlib.sha1(self.data).hexdigest().upper())
        self.assertEqual(result["md5"], hashlib.md5(self.data).hexdigest())
        self.assertEqual(len(self.calls), 1)  # 只补最后一段
        sink.seek(0)
        self.assertEqual(sink.read(), self.data)

    def test_server_ignoring_range_in_file_channel(self):
        """临时文件通道 + 服务端不认 Range（200）：自动降级单流，结果不变。"""
        with patch(
            "app.services.cross_cloud_copy.requests.get",
            return_value=_FakeResponse(200, self.data),
        ):
            sink, result = self._download()
        self._assert_matches_data(sink, result)

    def test_big_known_vector_matches_precomputed_hashes(self):
        """大文件已知向量：并行下载后 sha1/md5 与预计算值逐字节一致。"""
        total = 64 * 1024 * 1024
        self.data = _pseudo_random_bytes(total)
        expected_sha1 = hashlib.sha1(self.data).hexdigest().upper()
        expected_md5 = hashlib.md5(self.data).hexdigest()
        with patch.object(xcc, "DOWNLOAD_SEGMENT_BYTES", 1024 * 1024):
            sink, result = self._download()
        self.assertEqual(result["size"], total)
        self.assertEqual(result["sha1"], expected_sha1)
        self.assertEqual(result["md5"], expected_md5)
        # 抽两个偏移验证落盘内容（避免把整文件再读一遍进内存）
        sink.seek(total // 3)
        self.assertEqual(sink.read(4096), self.data[total // 3: total // 3 + 4096])
        sink.seek(0)
        self.assertEqual(sink.read(1024), self.data[:1024])


class TestDownloadParallelConfig(unittest.TestCase):
    """并行 worker 数配置契约：默认 3，1~8，非法回落 3。"""

    def test_clamp(self):
        self.assertEqual(xcc._clamp_parallel_workers("3"), 3)
        self.assertEqual(xcc._clamp_parallel_workers("1"), 1)
        self.assertEqual(xcc._clamp_parallel_workers("8"), 8)
        self.assertEqual(xcc._clamp_parallel_workers("0"), 3)
        self.assertEqual(xcc._clamp_parallel_workers("9"), 3)
        self.assertEqual(xcc._clamp_parallel_workers("-2"), 3)
        self.assertEqual(xcc._clamp_parallel_workers("abc"), 3)
        self.assertEqual(xcc._clamp_parallel_workers(None), 3)
        self.assertEqual(xcc._clamp_parallel_workers(""), 3)
