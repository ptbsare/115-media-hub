"""分片上传重试的单测（全 mock，无真实网络）。

背景：大文件动辄几百个分片，任何一个分片抖一下就整文件失败；而整文件重试
意味着从源网盘**重新下载一遍**。所以在分片这一层重试，只重发那一片。

覆盖：
1. 瞬时故障重试后成功、只重发失败的那片；
2. 业务错误（retriable=False）立即抛出，不做无意义重试；
3. 用尽次数后抛 UploadError，文案带分片号与重试次数；
4. 网络层异常（requests/urllib3 家族）也算可重试；
5. 取消检查生效，不会在用户取消后继续退避。
"""

import unittest

from app.providers.base import (
    UploadError,
    is_retriable_upload_error,
    upload_part_with_retry,
)


class _RequestsConnectionError(Exception):
    """伪造 requests.ConnectionError：靠类名/模块名被识别为网络异常。"""


_RequestsConnectionError.__module__ = "requests.exceptions"


class TestIsRetriableUploadError(unittest.TestCase):
    def test_upload_error_follows_flag(self):
        self.assertTrue(is_retriable_upload_error(UploadError("网络抖动", retriable=True)))
        self.assertFalse(is_retriable_upload_error(UploadError("鉴权失效", retriable=False)))

    def test_network_exception_is_retriable(self):
        self.assertTrue(is_retriable_upload_error(_RequestsConnectionError("conn reset")))
        self.assertTrue(is_retriable_upload_error(ConnectionResetError("reset")))

    def test_plain_business_exception_is_not_retriable(self):
        self.assertFalse(is_retriable_upload_error(ValueError("参数不对")))


class TestUploadPartWithRetry(unittest.TestCase):
    def setUp(self):
        self.sleeps = []

    def _sleeper(self, seconds):
        self.sleeps.append(seconds)

    def test_retries_until_success(self):
        calls = {"n": 0}

        def send():
            calls["n"] += 1
            if calls["n"] < 3:
                raise UploadError("分片抖动", retriable=True)
            return "etag-ok"

        result = upload_part_with_retry(send, part_number=7, sleep_fn=self._sleeper)
        self.assertEqual(result, "etag-ok")
        self.assertEqual(calls["n"], 3)
        # 退避序列：第一次等 1s，第二次等 3s
        self.assertEqual(self.sleeps, [1.0, 3.0])

    def test_business_error_raises_immediately(self):
        calls = {"n": 0}

        def send():
            calls["n"] += 1
            raise UploadError("目标目录不存在", retriable=False)

        with self.assertRaises(UploadError) as ctx:
            upload_part_with_retry(send, part_number=1, sleep_fn=self._sleeper)
        self.assertEqual(calls["n"], 1)
        self.assertIn("目标目录不存在", str(ctx.exception))
        self.assertEqual(self.sleeps, [])

    def test_exhausted_attempts_mentions_part_and_count(self):
        def send():
            raise UploadError("分片抖动", retriable=True)

        with self.assertRaises(UploadError) as ctx:
            upload_part_with_retry(send, part_number=42, sleep_fn=self._sleeper, attempts=3)
        message = str(ctx.exception)
        self.assertIn("分片 42", message)
        self.assertIn("重试 3 次", message)
        self.assertTrue(ctx.exception.retriable)
        self.assertEqual(len(self.sleeps), 2)

    def test_network_exception_is_retried(self):
        calls = {"n": 0}

        def send():
            calls["n"] += 1
            if calls["n"] == 1:
                raise _RequestsConnectionError("conn reset")
            return "etag-ok"

        self.assertEqual(upload_part_with_retry(send, part_number=2, sleep_fn=self._sleeper), "etag-ok")
        self.assertEqual(calls["n"], 2)

    def test_cancel_before_retry_raises_without_waiting(self):
        """用户取消后不再退避等待，直接抛「已取消」。"""
        calls = {"n": 0}

        def send():
            calls["n"] += 1
            raise UploadError("分片抖动", retriable=True)

        with self.assertRaises(UploadError) as ctx:
            upload_part_with_retry(
                send,
                part_number=3,
                sleep_fn=self._sleeper,
                cancel_check=lambda: True,
            )
        self.assertEqual(calls["n"], 1)
        self.assertEqual(self.sleeps, [])
        self.assertIn("已取消", str(ctx.exception))

    def test_cancel_after_first_failure_stops_further_attempts(self):
        """第一次失败后用户点了取消：不再重试，也不等退避。"""
        state = {"n": 0}

        def send():
            state["n"] += 1
            raise UploadError("分片抖动", retriable=True)

        with self.assertRaises(UploadError):
            upload_part_with_retry(
                send,
                part_number=5,
                attempts=3,
                sleep_fn=self._sleeper,
                cancel_check=lambda: state["n"] >= 1,
            )
        self.assertEqual(state["n"], 1)
        self.assertEqual(self.sleeps, [])

    def test_backoff_repeats_last_value(self):
        def send():
            raise UploadError("分片抖动", retriable=True)

        with self.assertRaises(UploadError):
            upload_part_with_retry(
                send, part_number=1, sleep_fn=self._sleeper, attempts=4, backoff=(0.5, 2.0)
            )
        self.assertEqual(self.sleeps, [0.5, 2.0, 2.0])


if __name__ == "__main__":
    unittest.main()
