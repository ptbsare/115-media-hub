"""跨盘复制任务断点续传：状态构造、校验与清理。

任务层（``cross_copy_jobs.py``）在每次运行单文件复制前调用
``build_resume_state`` 决定能否继承上次的断点；引擎
（``cross_cloud_copy.copy_file_across_clouds``）只消费合法状态并就地更新，
本模块负责「这个状态还敢不敢用」。

**为什么不信任旧 sha1**：断点状态里的 sha1 是上次复制时算出来的，而源网盘
里的文件完全可能被替换（share 转存换源、同名文件重新上传、file_id 不变内容
变）。拿着旧 sha1 续传，等于把「上一次的旧内容」当成新文件传到目标端——
用户看到的是上传成功，实际内容是错的。所以每次续传前都复探一次源端哈希
（``resolve_source_hash``，元信息级、带备忘录，不下载文件）：

- 复探结果与状态里的 sha1 一致 → 源文件没变，放心续传；
- 不一致 → 源文件已变，断点作废（清掉该行 resume_json），按全新复制重来
  （宁可重传，也不能传错内容）；
- 探测不到（夸克给不出哈希，返回 ``{}``）→ 接受续传：本地暂存文件 + 连续
  前缀大小校验已经是最后一道防线，威胁模型内（本机磁盘、自有文件）可信。

生命周期（设计文档 §3.2）：
    运行前 build_resume_state → 运行中引擎就地更新 + on_resume_update 落库
    → 成功 clear_resume_state / 失败取消保底落库 / 孤儿清理 prune 联动。

落库边界（resume_state_for_db）：
    引擎就地更新的状态里混着运行时对象——最典型的是 115 上传会话上的
    ``on_part_done`` 回调闭包（provider 每完成一个分片调一次）。函数对象
    进不了 JSON，不净化就会让 ``json.dumps`` 抛 TypeError，而上层落库都有
    try/except（持久化失败不该打断复制）——异常被吞，断点状态静默停在旧值，
    重启后续传全丢。所有写库路径统一经过 ``resume_state_for_db``。
"""

import logging
import os
from typing import Any, Dict, Optional

from ..db import db_connection, ensure_db, safe_json_dumps, safe_json_loads
from . import cross_cloud_copy as xcc

logger = logging.getLogger(__name__)

# 仅运行时存在的键：provider 回调闭包挂在 resume["upload"] 上方便逐分片记账，
# 但设计上就不落库（函数对象无法 JSON 序列化）。
_RESUME_RUNTIME_KEYS = frozenset({"on_part_done"})


def _clear_resume_in_db(job_id: int) -> None:
    """把某行的断点状态清空（写库失败只记日志：调用方都会按「无断点」继续）。"""
    try:
        ensure_db()
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE cross_copy_jobs SET resume_json = ? WHERE id = ?",
                (safe_json_dumps({}), int(job_id)),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 - 清断点失败不应打断调用方流程
        logger.info("cross-copy resume state clear skipped (job=%s): %s", job_id, exc)


def clear_resume_state(job_id: int) -> None:
    """清空任务断点状态（复制成功 / 断点作废时调用）。"""
    _clear_resume_in_db(int(job_id))


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _probe_source_hash(job: Dict[str, Any], cfg: Dict[str, Any], size: int) -> Dict[str, Any]:
    """复探源端哈希（resolve_source_hash 的薄封装，任何异常都按「探测不到」处理）。"""
    provider_name = str(job.get("source_provider") or "").strip()
    file_id = str(job.get("source_file_id") or "").strip()
    if not provider_name or not file_id:
        return {}
    source_provider = None
    try:
        from ..providers.registry import get_or_none

        source_provider = get_or_none(provider_name)
    except Exception as exc:  # noqa: BLE001 - provider 未注册时按探测不到处理
        logger.info("cross-copy resume source provider lookup failed: %s", exc)
        return {}
    if source_provider is None:
        return {}
    try:
        cookie = str(source_provider.get_cookie(cfg if isinstance(cfg, dict) else {}) or "").strip()
    except Exception:  # noqa: BLE001 - 取不到 cookie 时按探测不到处理
        return {}
    if not cookie:
        return {}
    try:
        probed = xcc.resolve_source_hash(source_provider, cookie, file_id, size=size)
    except Exception as exc:  # noqa: BLE001 - 探测异常按探测不到处理（接受续传）
        logger.info("cross-copy resume source hash probe failed (%s): %s", file_id, exc)
        return {}
    return probed if isinstance(probed, dict) else {}


def build_resume_state(job: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """构造本次运行可用的断点续传状态；不可用返回 None（并按需作废旧状态）。

    校验矩阵（任一不满足 → 清 resume_json + 返回 None）：
    1. 行上有非空 resume 状态（含 temp_path）；
    2. 临时文件还在磁盘上，且大小自洽：与预期总大小一致（完整落盘），
       或 >= downloaded_bytes 且不超过 size（连续前缀完整）；
    3. 复探源端哈希与状态里的 sha1 一致（探测不到时接受，见模块 docstring）。

    :param job: get_cross_copy_job 返回的任务字典（含 resume 键）
    :param cfg: 应用配置（get_config()），仅用于取源网盘 cookie 复探哈希
    """
    if not isinstance(job, dict):
        return None
    raw_state = job.get("resume")
    if not isinstance(raw_state, dict) or not raw_state:
        return None
    try:
        job_id = int(job.get("id") or 0)
    except (TypeError, ValueError):
        job_id = 0

    temp_path = str(raw_state.get("temp_path") or "").strip()
    if not temp_path:
        _clear_resume_in_db(job_id)
        return None
    if not os.path.isfile(temp_path):
        # 临时文件没了（清理/手动删除）：续传失去意义，作废
        logger.info("cross-copy resume invalidated: temp file missing (job=%s, %s)", job_id, temp_path)
        _clear_resume_in_db(job_id)
        return None

    size = _coerce_int(raw_state.get("size"), 0)
    downloaded_bytes = max(0, _coerce_int(raw_state.get("downloaded_bytes"), 0))
    if size <= 0:
        _clear_resume_in_db(job_id)
        return None
    try:
        actual_size = os.path.getsize(temp_path)
    except OSError as exc:
        logger.info("cross-copy resume invalidated: temp file unreadable (job=%s): %s", job_id, exc)
        _clear_resume_in_db(job_id)
        return None
    if actual_size > size:
        # 比预期还大：不是同一份文件，作废
        _clear_resume_in_db(job_id)
        return None
    if actual_size != size and not (0 < downloaded_bytes <= actual_size):
        # 既不是完整文件，连续前缀也不完整：落盘数据不可信，作废
        logger.info(
            "cross-copy resume invalidated: size mismatch (job=%s, file=%s, expect=%s, prefix=%s)",
            job_id, actual_size, size, downloaded_bytes,
        )
        _clear_resume_in_db(job_id)
        return None

    # ---- 复探源端哈希：源文件可能已被替换，旧 sha1 不能照信 ----
    known_sha1 = str(raw_state.get("sha1") or "").strip().upper()
    probed = _probe_source_hash(job, cfg if isinstance(cfg, dict) else {}, size)
    if probed:
        probed_sha1 = str(probed.get("sha1") or "").strip().upper()
        probed_size = _coerce_int(probed.get("size"), 0)
        if probed_size > 0 and probed_size != size:
            logger.info(
                "cross-copy resume invalidated: source size changed (job=%s, %s -> %s)",
                job_id, size, probed_size,
            )
            _clear_resume_in_db(job_id)
            return None
        if probed_sha1 and known_sha1 and probed_sha1 != known_sha1:
            logger.info(
                "cross-copy resume invalidated: source hash changed (job=%s, %s -> %s)",
                job_id, known_sha1[:12], probed_sha1[:12],
            )
            _clear_resume_in_db(job_id)
            return None

    # ---- 校验通过：归一化后交给你引擎就地更新 ----
    upload = raw_state.get("upload")
    state: Dict[str, Any] = {
        "temp_path": temp_path,
        "size": size,
        "sha1": known_sha1,
        "md5": str(raw_state.get("md5") or "").strip().lower(),
        "downloaded_bytes": min(downloaded_bytes, size),
        "upload": upload if isinstance(upload, dict) else {},
    }
    return state


def new_resume_skeleton(job: Dict[str, Any]) -> Dict[str, Any]:
    """构造一个空的断点骨架（全新运行用）。

    为什么全新运行也要传状态：断点续传的链路是「失败时留下什么，重试时
    继承什么」。如果只在「已有断点」时才传状态，首跑失败的数据永远没人
    记录——引擎留了临时文件也没有状态指向它，重试照样从头再来，续传链
    根本 bootstrap 不起来。骨架由引擎在运行中逐步填充（temp_path / 哈希 /
    分片），任务层负责落库。

    temp_path 为空 = 还没有任何落盘数据；此时任何「继承」判断都必须否决
    （见 resume_state_is_inheritable）。
    """
    if not isinstance(job, dict):
        job = {}
    try:
        size = int(job.get("file_size") or 0)
    except (TypeError, ValueError):
        size = 0
    return {
        "temp_path": "",
        "size": size if size > 0 else 0,
        "sha1": "",
        "md5": "",
        "downloaded_bytes": 0,
        "upload": {},
    }


def resume_state_is_inheritable(state: Any) -> bool:
    """这个断点状态里有没有「磁盘上真实存在、值得继承」的数据？

    用于失败/取消时的终态处理：只有 temp_path 指向的文件确实存在，且
    已有下载字节或上传分片时，才落库 + 告诉用户「已保留断点」。空骨架
    （还没下到任何字节就失败）落库只会留下一条永远自愈不掉的悬空记录。
    """
    if not isinstance(state, dict) or not state:
        return False
    temp_path = str(state.get("temp_path") or "").strip()
    if not temp_path or not os.path.isfile(temp_path):
        return False
    downloaded = _coerce_int(state.get("downloaded_bytes"), 0)
    upload = state.get("upload")
    parts = upload.get("parts") if isinstance(upload, dict) else None
    return downloaded > 0 or bool(isinstance(parts, list) and parts)


def resume_state_for_db(state: Any) -> Dict[str, Any]:
    """把续传状态净化成可安全 json.dumps 的字典（深拷贝，不改原对象）。

    任务层每次落库都经过本函数：剔除已知的运行时键（on_part_done 等回调
    闭包），其余任何不可序列化的值也一并丢弃并记日志——宁可少记一个字段，
    也不能让整次写库失败（断点静默丢失比重传的代价大得多）。
    """
    cleaned = _clean_resume_value(state, "resume")
    return cleaned if isinstance(cleaned, dict) else {}


def _clean_resume_value(value: Any, path: str) -> Any:
    """递归净化：只留 JSON 原生类型（字典/列表/字符串/数字/布尔/空）。"""
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            name = str(key)
            if name in _RESUME_RUNTIME_KEYS:
                continue
            out[name] = _clean_resume_value(item, f"{path}.{name}")
        return out
    if isinstance(value, (list, tuple)):
        return [_clean_resume_value(item, path) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    logger.warning(
        "cross-copy resume state dropped non-serializable field %s (%s)",
        path, type(value).__name__,
    )
    return None


def prune_resume_for_missing_files() -> int:
    """孤儿清理联动：temp_path 已不在磁盘上的断点状态一律清空。

    与 ``cross_cloud_copy.cleanup_orphan_temp_files``（删超期临时文件）配套：
    文件没了，断点就是空头支票。返回清理的行数；失败只记日志不抛出。
    """
    try:
        ensure_db()
    except Exception as exc:  # noqa: BLE001 - 库没起来时联动清理直接跳过
        logger.info("cross-copy resume prune skipped: %s", exc)
        return 0
    cleaned = 0
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, resume_json FROM cross_copy_jobs "
                "WHERE resume_json IS NOT NULL AND resume_json != '' AND resume_json != '{}'"
            )
            rows = cursor.fetchall()
            stale_ids = []
            for row in rows:
                try:
                    job_id = int(row[0])
                    state = safe_json_loads(row[1], {})
                except (TypeError, ValueError, IndexError):
                    continue
                if not isinstance(state, dict) or not state:
                    continue
                temp_path = str(state.get("temp_path") or "").strip()
                if temp_path and os.path.exists(temp_path):
                    continue
                stale_ids.append(job_id)
            for job_id in stale_ids:
                cursor.execute(
                    "UPDATE cross_copy_jobs SET resume_json = ? WHERE id = ?",
                    (safe_json_dumps({}), job_id),
                )
                cleaned += 1
            conn.commit()
    except Exception as exc:  # noqa: BLE001 - 清理失败不影响主流程
        logger.warning("cross-copy resume prune skipped: %s", exc)
        return 0
    if cleaned:
        logger.info("cross-copy resume states pruned (temp file missing): %s", cleaned)
    return cleaned
