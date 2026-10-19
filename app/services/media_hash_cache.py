"""文件哈希备忘录：把「已经确认过的 SHA1」沉淀到 SQLite。

跨盘复制的秒传探测只需要 ``sha1 + size``。115 / 阿里云盘的文件元信息里自带哈希，
夸克没有——只能完整下载一遍算出来。算出来的哈希如果不记下来，同一部电影从夸克
复制到第二个目录时又要重下一遍，白白再吃一次下载流量。

所以这里记一条朴素的事实：**这个网盘的这个文件，我确认过它的 sha1 是 X**。
下次复制同一个文件（哪怕目标是另一个目录）就能直接问目标网盘要秒传。

几条硬约束：
    - 查缓存必须校验 ``size``：大小变了说明文件被换过，旧哈希不可信，当作未命中；
    - ``sha1`` 统一大写十六进制存储与返回；
    - 30 天过期（``updated_ts`` 早于 30 天视为失效，读到就顺手删掉）；
    - 读写全部走 ``db_connection`` + ``retry_sqlite_locked``，不另建连接池；
    - **缓存是纯优化**：任何数据库异常都吞掉并记 warning，绝不影响复制主流程；
    - 复制任务跑在线程池里，可能并发写同一 key，写入一律 ``INSERT OR REPLACE``。
"""

import logging
import re
import time
from typing import Any, Tuple

from ..db import db_connection, retry_sqlite_locked

logger = logging.getLogger(__name__)

# 哈希有效期：30 天。网盘端的文件被替换属于低频事件，真遇到脏缓存由调用方自愈
# （校验失败 → forget + 重新下载算哈希），所以这里不必做得更保守。
HASH_CACHE_TTL_SECONDS = 30 * 24 * 3600

_SHA1_PATTERN = re.compile(r"^[0-9A-F]{40}$")


def _normalize_sha1(value: Any) -> str:
    """统一成大写十六进制 SHA1；格式不对返回空串（宁可不命中，也不要返回错哈希）。"""
    token = str(value or "").strip().upper()
    return token if _SHA1_PATTERN.match(token) else ""


def _normalize_key(provider: Any, file_id: Any) -> Tuple[str, str]:
    """归一化主键：provider 转小写（避免 QUARK / quark 记成两行），file_id 去空白。"""
    return str(provider or "").strip().lower(), str(file_id or "").strip()


def _delete(provider: str, file_id: str) -> None:
    """删除一条记录（内部复用；异常由调用方兜住）。"""

    def operation() -> None:
        with db_connection() as conn:
            conn.execute(
                "DELETE FROM media_hash_cache WHERE provider = ? AND file_id = ?",
                (provider, file_id),
            )
            conn.commit()

    retry_sqlite_locked(operation)


def lookup_file_hash(provider: str, file_id: str, size: int) -> str:
    """查这个文件已确认过的 SHA1；未命中 / 大小不符 / 过期 / 数据库故障都返回空串。

    :param size: 调用方知道的文件大小，必须与缓存里的一致才算命中（文件被换过则作废）
    """
    provider_name, token = _normalize_key(provider, file_id)
    try:
        expected_size = int(size)
    except (TypeError, ValueError):
        return ""
    if not provider_name or not token or expected_size <= 0:
        return ""

    def operation():
        with db_connection() as conn:
            return conn.execute(
                "SELECT size, sha1, updated_ts FROM media_hash_cache WHERE provider = ? AND file_id = ?",
                (provider_name, token),
            ).fetchone()

    try:
        row = retry_sqlite_locked(operation)
    except Exception as exc:  # 缓存故障不能影响复制，静默降级为未命中
        logger.warning(
            "媒体哈希缓存读取失败（按未命中处理）：provider=%s file_id=%s（%s）",
            provider_name,
            token,
            exc,
        )
        return ""
    if row is None:
        return ""
    try:
        cached_size = int(row[0] or 0)
        sha1 = _normalize_sha1(row[1])
        updated_ts = float(row[2] or 0.0)
    except (TypeError, ValueError, IndexError):
        return ""
    if not sha1 or cached_size != expected_size:
        return ""
    if updated_ts < time.time() - HASH_CACHE_TTL_SECONDS:
        # 过期：留着只会让下次再多查一次，直接清掉
        try:
            _delete(provider_name, token)
        except Exception as exc:
            logger.warning(
                "媒体哈希缓存清理过期记录失败：provider=%s file_id=%s（%s）",
                provider_name,
                token,
                exc,
            )
        return ""
    return sha1


def remember_file_hash(provider: str, file_id: str, size: int, sha1: str) -> None:
    """记下「这个文件确认过的 SHA1」。参数不合法或写库失败时安静返回。"""
    provider_name, token = _normalize_key(provider, file_id)
    normalized_sha1 = _normalize_sha1(sha1)
    try:
        file_size = int(size)
    except (TypeError, ValueError):
        return
    if not provider_name or not token or not normalized_sha1 or file_size <= 0:
        return

    def operation() -> None:
        with db_connection() as conn:
            # 并发写同一 key 时交给 SQLite 定序：后来者覆盖，不先查再插
            conn.execute(
                """
                INSERT OR REPLACE INTO media_hash_cache (provider, file_id, size, sha1, updated_ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                (provider_name, token, file_size, normalized_sha1, time.time()),
            )
            conn.commit()

    try:
        retry_sqlite_locked(operation)
    except Exception as exc:
        logger.warning(
            "媒体哈希缓存写入失败（已忽略）：provider=%s file_id=%s size=%s（%s）",
            provider_name,
            token,
            file_size,
            exc,
        )


def forget_file_hash(provider: str, file_id: str) -> None:
    """忘掉一条记录（脏缓存自愈 / 源文件确认变更时调用）。失败时安静返回。"""
    provider_name, token = _normalize_key(provider, file_id)
    if not provider_name or not token:
        return
    try:
        _delete(provider_name, token)
    except Exception as exc:
        logger.warning(
            "媒体哈希缓存删除失败（已忽略）：provider=%s file_id=%s（%s）",
            provider_name,
            token,
            exc,
        )
