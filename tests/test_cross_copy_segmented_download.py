"""跨盘复制下载侧：Range 分段下载单测（mock HTTP，无真实网络）。

真机背景：夸克对「整文件 GET」限速到 ~100KB/s，但对 Range 分段请求按正常速度返回
（同账号同文件实测 4~11MB/s）。2.6GB 单文件若走整文件流要 7 小时以上。
"""
import hashlib
import io
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
