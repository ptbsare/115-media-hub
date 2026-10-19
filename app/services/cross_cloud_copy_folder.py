"""跨盘复制：整个文件夹（源文件夹 → 目标网盘的对应目录树）。

一个复制任务 = 一个文件夹：任务里先递归列出源文件夹的清单，再在目标网盘
按同样的层级建好目录，最后逐个文件走单文件管道
（``cross_cloud_copy.copy_file_across_clouds``：取直链 → 下载算哈希 →
目标秒传/分片上传 → 清理临时文件）。

关键取舍：
    - 清单先落地再动手：文件数 / 总量在复制前就确定，进度条才有意义；
      列目录失败直接报错，绝不返回空清单当成「复制完了」。
    - 清单有上限（文件数 + 目录层数）：超了就把 truncated 置真并停下，
      由调用方明确告诉用户「没列全」，不允许静默截断后当成功。
    - 单个文件失败不中断整批：继续下一个，最后汇总成「部分完成」，
      用户看到的是「哪几个没成功」，而不是整任务白跑。
    - 目录 id 按相对路径缓存并先建父级：同名目录复用，不重复建。
    - 进度按字节加权：已完成文件的字节 + 当前文件内部进度，映射到
      全局 (sent, total)，现有进度条无需改动即可继续用。
"""

import logging
import os
import time
from collections import deque
from typing import Any, Dict, List, Optional

from . import cross_cloud_copy as xcc

logger = logging.getLogger(__name__)


def _env_positive_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("环境变量 %s=%r 不是整数，改用默认值 %s", name, raw, default)
        return int(default)
    return value if value > 0 else int(default)


# 单个文件夹任务最多处理多少个文件（超出即截断并报错，不静默）
MANIFEST_MAX_FILES = _env_positive_int("CROSS_COPY_FOLDER_MAX_FILES", 2000)
# 目录最多往下钻多少层（防止网盘里出现环或超深目录时卡死）
MANIFEST_MAX_DEPTH = 20
# result_json 里最多留多少条失败明细（前端够看，又不至于把库撑爆）
MAX_RECORDED_FAILURES = 50


class CopyFolderResult:
    """整个文件夹的复制结果。

    status:
        - "completed" 全部文件都成功（含「目标已有同名文件，跳过」）
        - "partial"   一部分成功、一部分失败（任务算完成，但要提示失败明细）
        - "failed"    一个文件都没成功
    """

    __slots__ = (
        "status",
        "source_provider",
        "target_provider",
        "folder_name",
        "target_folder_id",
        "files_total",
        "files_done",
        "files_skipped",
        "files_failed",
        "dirs_created",
        "failures",
        "total_bytes",
        "message",
        "elapsed_ms",
    )

    def __init__(
        self,
        *,
        status: str,
        source_provider: str = "",
        target_provider: str = "",
        folder_name: str = "",
        target_folder_id: str = "",
        files_total: int = 0,
        files_done: int = 0,
        files_skipped: int = 0,
        files_failed: int = 0,
        dirs_created: int = 0,
        failures: Optional[List[Dict[str, str]]] = None,
        total_bytes: int = 0,
        message: str = "",
        elapsed_ms: int = 0,
    ):
        self.status = str(status or "")
        self.source_provider = str(source_provider or "")
        self.target_provider = str(target_provider or "")
        self.folder_name = str(folder_name or "")
        self.target_folder_id = str(target_folder_id or "")
        self.files_total = int(files_total or 0)
        self.files_done = int(files_done or 0)
        self.files_skipped = int(files_skipped or 0)
        self.files_failed = int(files_failed or 0)
        self.dirs_created = int(dirs_created or 0)
        self.failures = list(failures or [])
        self.total_bytes = int(total_bytes or 0)
        self.message = str(message or "")
        self.elapsed_ms = int(elapsed_ms or 0)

    def to_dict(self) -> Dict[str, Any]:
        payload = {key: getattr(self, key) for key in self.__slots__}
        payload["failures"] = list(self.failures)
        return payload

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return (
            f"<CopyFolderResult {self.status} {self.source_provider}->{self.target_provider} "
            f"folder={self.folder_name!r} files={self.files_done}/{self.files_total}>"
        )


def _check_cancelled(cancel_check) -> None:
    """取消检查点（复用单文件引擎的取消异常，任务层只认这一个）。"""
    xcc._check_cancelled(cancel_check)


def _report_status(status_cb, detail: str) -> None:
    """上报「文件 3/12」这类过程文案（回调抛错不影响复制）。"""
    if status_cb is None:
        return
    try:
        status_cb(str(detail or ""))
    except Exception:
        pass


def _join_rel(prefix: str, name: str) -> str:
    return f"{prefix}/{name}" if prefix else name


def _rel_depth(rel_path: str) -> int:
    return len([part for part in str(rel_path or "").split("/") if part])


def build_folder_manifest(
    provider,
    cookie: str,
    folder_id: str,
    *,
    cancel_check=None,
) -> Dict[str, Any]:
    """递归列出源文件夹里的所有文件与子目录（广度优先，逐层下探）。

    返回::

        {
            "files": [{"file_id", "rel_path", "name", "size"}, ...],
            "dirs": ["子目录", "子目录/更深一层", ...],   # 只含子目录，不含根文件夹自身
            "total_bytes": 12345,
            "truncated": False,
            "skipped": [{"path", "reason"}, ...],
        }

    rel_path 用 ``/`` 分隔，且不含根文件夹自己的名字——目标端根目录叫什么
    由任务层决定（与单文件任务的 filename 语义一致）。

    超过 ``MANIFEST_MAX_FILES`` 或 ``MANIFEST_MAX_DEPTH`` 时置
    ``truncated=True`` 并停止继续下探，绝不静默当成功；列目录失败会抛
    ``CrossCloudCopyError``，而不是返回一个空清单。
    """
    root_id = str(folder_id or "").strip() or "0"
    files: List[Dict[str, Any]] = []
    dirs: List[str] = []
    skipped: List[Dict[str, str]] = []
    total_bytes = 0
    truncated = False
    max_files = int(MANIFEST_MAX_FILES)
    max_depth = int(MANIFEST_MAX_DEPTH)

    queue = deque([("", root_id, 0)])
    while queue:
        prefix, cid, depth = queue.popleft()
        _check_cancelled(cancel_check)
        where = prefix or "根目录"
        try:
            payload = provider.list_entries_payload(cookie, cid, folders_only=False)
        except Exception as exc:
            # 读不到就必须报错：返回空清单会让用户以为整个文件夹是空的
            hint = "（可能是网盘登录状态过期，或这个文件夹已经被删掉/改名）"
            raise xcc.CrossCloudCopyError(f"读取源文件夹「{where}」失败：{exc}{hint}") from exc
        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            raise xcc.CrossCloudCopyError(f"读取源文件夹「{where}」失败：网盘没有返回文件列表")

        # 排序只为结果稳定（测试与重复执行时顺序一致），不影响正确性
        ordered = sorted(
            (entry for entry in entries if isinstance(entry, dict)),
            key=lambda item: str(item.get("name", "") or "").lower(),
        )
        for entry in ordered:
            name = str(entry.get("name", "") or "").strip()
            if not name:
                skipped.append({"path": prefix or "", "reason": "有个条目没有名字，已跳过"})
                continue
            rel_path = _join_rel(prefix, name)
            entry_id = str(entry.get("id", "") or entry.get("cid", "") or "").strip()
            if entry.get("is_dir"):
                if not entry_id:
                    skipped.append({"path": rel_path, "reason": "文件夹没有 ID，无法打开"})
                    continue
                dirs.append(rel_path)
                if depth + 1 > max_depth:
                    truncated = True
                    skipped.append(
                        {"path": rel_path, "reason": f"目录层级超过 {max_depth} 层，没有再往下找"}
                    )
                    continue
                queue.append((rel_path, entry_id, depth + 1))
                continue
            if not entry_id:
                skipped.append({"path": rel_path, "reason": "文件没有 ID，无法取下载地址"})
                continue
            if len(files) >= max_files:
                truncated = True
                skipped.append(
                    {
                        "path": rel_path,
                        "reason": f"文件数超过上限 {max_files} 个，后面的没有列出来",
                    }
                )
                break
            size = int(entry.get("size", 0) or 0)
            files.append(
                {
                    "file_id": entry_id,
                    "rel_path": rel_path,
                    "name": name,
                    "size": size,
                }
            )
            total_bytes += size
        if truncated:
            break

    return {
        "files": files,
        "dirs": dirs,
        "total_bytes": total_bytes,
        "truncated": truncated,
        "skipped": skipped,
    }


def _find_child_folder(provider, cookie: str, parent_id: str, name: str) -> str:
    """在目标目录里找同名子文件夹，找到返回 id，找不到返回空串。

    列目录失败不阻断（交给 create_folder 内部复用逻辑兜底）。
    """
    try:
        payload = provider.list_entries_payload(cookie, parent_id, folders_only=True)
    except Exception as exc:
        logger.info("跨盘复制列目标目录失败（改走新建）: %s", exc)
        return ""
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return ""
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("is_dir"):
            continue
        if str(entry.get("name", "") or "").strip() == name:
            return str(entry.get("id", "") or entry.get("cid", "") or "").strip()
    return ""


def _ensure_target_folder(provider, cookie: str, parent_id: str, name: str) -> Dict[str, Any]:
    """目标端确保存在某个名字的文件夹：有就复用，没有就新建。

    返回 {"id": 目录 id, "created": 是否新建}。
    """
    existing = _find_child_folder(provider, cookie, parent_id, name)
    if existing:
        return {"id": existing, "created": False}
    try:
        payload = provider.create_folder(cookie, parent_id, name)
    except Exception as exc:
        raise xcc.CrossCloudCopyError(f"在目标网盘新建文件夹「{name}」失败：{exc}") from exc
    folder_id = ""
    if isinstance(payload, dict):
        folder_id = str(payload.get("id", "") or payload.get("cid", "") or "").strip()
    if not folder_id:
        raise xcc.CrossCloudCopyError(f"在目标网盘新建文件夹「{name}」失败：网盘没有返回目录 ID")
    return {"id": folder_id, "created": True}


def _format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        size = 0.0
    if size <= 0:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024.0
        unit += 1
    return f"{int(size)}B" if unit == 0 else f"{size:.2f}{units[unit]}"


def _build_summary(
    status: str,
    *,
    files_total: int,
    files_done: int,
    files_skipped: int,
    files_failed: int,
    dir_failed: int,
    failures: List[Dict[str, str]],
) -> str:
    """拼一句人话结果（任务卡片的 status_detail 直接用）。"""
    if files_total == 0:
        if dir_failed:
            return f"文件夹里没有文件，但有 {dir_failed} 个目录没建成"
        return "文件夹里没有文件，已在目标网盘建好目录"
    if status == "failed":
        head = f"复制失败：{files_total} 个文件都没成功"
        return f"{head}（{failures[0]['message']}）" if failures else head
    parts = [f"{files_done} 个已复制"]
    if files_skipped:
        parts.append(f"{files_skipped} 个目标已有、跳过")
    if files_failed:
        parts.append(f"{files_failed} 个失败")
    head = ("复制完成" if status == "completed" else "部分完成") + f"：共 {files_total} 个文件，"
    return head + "、".join(parts)


def copy_folder_across_clouds(
    *,
    source_provider,
    source_cookie: str,
    source_folder_id: str,
    target_provider,
    target_cookie: str,
    target_dir_id: str,
    target_folder_name: str = "",
    temp_dir: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    progress_cb=None,
    cancel_check=None,
    check_existing: bool = False,
    status_cb=None,
) -> CopyFolderResult:
    """把一个文件夹整体复制到目标网盘的指定目录下。

    :param source_folder_id: 源网盘里的文件夹 id
    :param target_dir_id: 目标网盘里放这个文件夹的上级目录 id
    :param target_folder_name: 复制过去之后根目录叫什么；为空时用兜底名字
    :param progress_cb: 进度回调 fn(sent_bytes, total_bytes, phase)，按整个文件夹的字节加权
    :param status_cb: 过程文案回调 fn(detail)，用于显示「文件 3/12」
    :param check_existing: 目标已有同名同大小文件就跳过（重试时省流量）
    """
    started = time.monotonic()
    source_name = str(getattr(source_provider, "name", "") or "")
    target_name = str(getattr(target_provider, "name", "") or "")

    if source_provider is None or target_provider is None:
        raise xcc.CrossCloudCopyError("复制源或目标未配置")
    if not str(source_folder_id or "").strip():
        raise xcc.CrossCloudCopyError("源文件夹 ID 不能为空")
    if not getattr(target_provider, "supports_upload", False):
        raise xcc.CrossCloudCopyError(
            f"{getattr(target_provider, 'label', target_name) or '目标网盘'} 暂不支持上传，无法作为复制目标"
        )
    if source_name and source_name == target_name:
        raise xcc.CrossCloudCopyError("复制源与目标不能是同一个网盘")

    folder_name = str(target_folder_name or "").strip() or "跨盘复制文件夹"

    _check_cancelled(cancel_check)
    _report_status(status_cb, "正在统计文件")
    manifest = build_folder_manifest(
        source_provider,
        source_cookie,
        source_folder_id,
        cancel_check=cancel_check,
    )
    if manifest.get("truncated"):
        reason = ""
        skipped = manifest.get("skipped") or []
        if skipped:
            reason = str(skipped[-1].get("reason", "") or "")
        detail = f"（{reason}）" if reason else ""
        raise xcc.CrossCloudCopyError(
            f"文件夹「{folder_name}」里的内容太多{detail}，一次复制不完，这次没有开始复制。"
            f"可以把文件夹拆小再试；单次上限是 {int(MANIFEST_MAX_FILES)} 个文件、"
            f"{int(MANIFEST_MAX_DEPTH)} 层目录（文件数上限可用环境变量 "
            f"CROSS_COPY_FOLDER_MAX_FILES 调整）"
        )

    files: List[Dict[str, Any]] = list(manifest.get("files") or [])
    dirs: List[str] = list(manifest.get("dirs") or [])
    total_bytes = int(manifest.get("total_bytes") or 0)
    failures: List[Dict[str, str]] = []
    dir_failed = 0
    files_done = 0
    files_skipped = 0
    files_failed = 0
    dirs_created = 0

    # 进度：已完成文件的字节 + 当前文件内部进度；峰值只增不减，进度条不回退
    written = 0
    peak = 0

    def _notify(sent: int, total: int, phase: str) -> None:
        xcc._report_progress(progress_cb, sent, total, phase)

    def _emit(file_sent: int, file_size: int) -> int:
        nonlocal peak
        portion = 0
        if file_size > 0:
            try:
                portion = max(0, min(int(file_sent or 0), file_size))
            except (TypeError, ValueError):
                portion = 0
        sent = written + portion
        if total_bytes > 0 and sent > total_bytes:
            sent = total_bytes
        if sent < peak:
            sent = peak
        peak = sent
        return sent

    # ---- 1. 目标端根目录（同名即复用） ----
    _check_cancelled(cancel_check)
    _report_status(status_cb, "正在建目录")
    root = _ensure_target_folder(target_provider, target_cookie, str(target_dir_id or ""), folder_name)
    root_id = str(root.get("id") or "")
    if root.get("created"):
        dirs_created += 1
    dir_ids: Dict[str, str] = {"": root_id}

    # ---- 2. 子目录：先建浅层再建深层，父级 id 直接从缓存取 ----
    ordered_dirs = sorted(dirs, key=lambda rel: (_rel_depth(rel), rel))
    for index, rel in enumerate(ordered_dirs, start=1):
        _check_cancelled(cancel_check)
        _report_status(status_cb, f"正在建目录 {index}/{len(ordered_dirs)}")
        parent_rel = rel.rpartition("/")[0]
        parent_id = dir_ids.get(parent_rel)
        if not parent_id:
            # 父目录没建成，子目录必然建不起来，记一笔就跳过
            dir_failed += 1
            failures.append({"rel_path": rel, "message": "上一级目录没有建成，这个目录和里面的文件都跳过了"})
            continue
        name = rel.rsplit("/", 1)[-1]
        try:
            created = _ensure_target_folder(target_provider, target_cookie, parent_id, name)
        except xcc.CrossCloudCopyCancelled:
            raise
        except Exception as exc:
            dir_failed += 1
            failures.append({"rel_path": rel, "message": str(exc)})
            continue
        dir_ids[rel] = str(created.get("id") or "")
        if created.get("created"):
            dirs_created += 1

    def _file_progress_for(size_value: int):
        """单文件进度 → 整任务进度（已完成字节 + 本文件内部进度）。"""

        def _callback(file_sent, file_total=0, phase="download"):
            _notify(_emit(file_sent, size_value), total_bytes, phase)

        return _callback

    # ---- 3. 逐个文件走单文件管道 ----
    total_files = len(files)
    for index, item in enumerate(files, start=1):
        _check_cancelled(cancel_check)
        rel_path = str(item.get("rel_path", "") or "")
        name = str(item.get("name", "") or "").strip()
        size = int(item.get("size", 0) or 0)
        parent_rel = rel_path.rpartition("/")[0]
        target_sub_dir = dir_ids.get(parent_rel)
        if not target_sub_dir:
            files_failed += 1
            failures.append(
                {"rel_path": rel_path, "message": "目标目录没建成，这个文件跳过了"}
            )
            continue

        _report_status(status_cb, f"文件 {index}/{total_files}")
        _notify(_emit(0, size), total_bytes, "download")

        try:
            result = xcc.copy_file_across_clouds(
                source_provider=source_provider,
                source_cookie=source_cookie,
                source_file_id=str(item.get("file_id", "") or ""),
                target_provider=target_provider,
                target_cookie=target_cookie,
                target_dir_id=target_sub_dir,
                filename=name,
                # 清单里没给大小（0）时交给单文件引擎自己探，免得按 0 字节去校验
                size=size if size > 0 else -1,
                temp_dir=temp_dir,
                cfg=cfg,
                progress_cb=_file_progress_for(size),
                cancel_check=cancel_check,
                check_existing=check_existing,
            )
        except xcc.CrossCloudCopyCancelled:
            raise
        except Exception as exc:
            files_failed += 1
            failures.append({"rel_path": rel_path, "message": str(exc)})
            logger.info("跨盘复制文件夹：单文件失败 %s（%s）", rel_path, exc)
            continue

        result_status = str(getattr(result, "status", "") or "uploaded")
        if result_status == "skipped":
            files_skipped += 1
        else:
            files_done += 1
        written += size if size > 0 else int(getattr(result, "size", 0) or 0)

    # ---- 4. 汇总 ----
    if dir_failed and total_files == 0:
        status = "failed"
    elif total_files == 0:
        status = "completed"
    elif files_done + files_skipped == 0:
        status = "failed"
    elif files_failed:
        status = "partial"
    else:
        status = "completed"

    message = _build_summary(
        status,
        files_total=total_files,
        files_done=files_done,
        files_skipped=files_skipped,
        files_failed=files_failed,
        dir_failed=dir_failed,
        failures=failures,
    )
    if dir_failed and total_files:
        message += f"；另有 {dir_failed} 个目录没建成"
    if total_files:
        message += f"，共 {_format_bytes(total_bytes)}"
    if status == "partial" and failures:
        first = failures[0]
        where = str(first.get("rel_path", "") or "")
        why = str(first.get("message", "") or "")
        detail = f"{where}：{why}" if where and why else (where or why)
        if detail:
            message += f"。第一个失败的是 {detail}"
    if manifest.get("skipped"):
        message += f"；另有 {len(manifest['skipped'])} 个条目没法处理"

    return CopyFolderResult(
        status=status,
        source_provider=source_name,
        target_provider=target_name,
        folder_name=folder_name,
        target_folder_id=root_id,
        files_total=total_files,
        files_done=files_done,
        files_skipped=files_skipped,
        files_failed=files_failed,
        dirs_created=dirs_created,
        failures=failures[:MAX_RECORDED_FAILURES],
        total_bytes=total_bytes,
        message=message,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )
