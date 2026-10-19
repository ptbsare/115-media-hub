import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


class UploadError(RuntimeError):
    """上传失败。

    message 为面向用户的文案（错误码映射见复制引擎的人话映射表）；
    retriable=True 表示瞬时故障（限流/网络），值得按退避策略重试。
    """

    def __init__(self, message: str, *, retriable: bool = False, provider: str = ""):
        super().__init__(str(message))
        self.retriable = bool(retriable)
        self.provider = str(provider or "")


class UploadResult:
    """单文件上传结果。

    status:
        - "rapid"     秒传命中（目标网盘已有同哈希文件，零上传流量）
        - "uploaded"  实际上传完成
    """

    __slots__ = ("status", "file_id", "pick_code", "name", "size", "sha1", "message")

    def __init__(
        self,
        *,
        status: str,
        file_id: str = "",
        pick_code: str = "",
        name: str = "",
        size: int = 0,
        sha1: str = "",
        message: str = "",
    ):
        self.status = str(status or "")
        self.file_id = str(file_id or "")
        self.pick_code = str(pick_code or "")
        self.name = str(name or "")
        self.size = int(size or 0)
        self.sha1 = str(sha1 or "").upper()
        self.message = str(message or "")

    @property
    def is_rapid(self) -> bool:
        return self.status == "rapid"

    def to_dict(self) -> Dict[str, Any]:
        return {key: getattr(self, key) for key in self.__slots__}

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"<UploadResult {self.status} name={self.name!r} size={self.size} sha1={self.sha1[:12]}>"


@runtime_checkable
class UploadSink(Protocol):
    """上传数据源协议（鸭子类型，不要求显式继承）。

    复制引擎以两种形态实现：
        - 内存流（io.BytesIO，小文件）
        - 临时文件（大文件，可断点续传）

    必需：
        read(n) -> bytes      顺序读取
        seek(off, whence)     定位（ whence: 0=开头 1=当前 2=结尾 ）
        tell() -> int         当前位置
        size: int             总大小

    可选：
        sha1: str             上层已同步计算的 SHA1（大写十六进制）；
                              提供则可避免 provider 重复读取一遍
        name: str             文件名（sink 来自临时文件时有用）
    """

    def read(self, n: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...

    def tell(self) -> int: ...

    @property
    def size(self) -> int: ...


class CloudProvider(ABC):
    # === 元数据（子类覆盖） ===
    name: str = ""
    label: str = ""
    link_type: str = ""

    # === 认证 ===
    auth_type: str = "cookie"
    config_keys: List[str] = []

    # === 能力声明 ===
    supports_folder_browse: bool = True
    supports_share_receive: bool = True
    supports_subscription: bool = False
    supports_offline: bool = False
    supports_fixed_share_link: bool = False
    supports_strm: bool = False
    supports_monitor: bool = False
    # NEW — file operations
    supports_rename: bool = False
    supports_move: bool = False
    supports_copy: bool = False
    supports_delete: bool = False
    # NEW — upload / rapid upload (秒传)
    supports_upload: bool = False
    supports_rapid_upload: bool = False
    supports_upload_resume: bool = False

    # === 限流 ===
    rate_limit_seconds: float = 0.0
    _rate_limit_lock: threading.Lock = threading.Lock()
    _last_request_monotonic: float = 0.0

    # === 核心 API ===
    @abstractmethod
    def list_entries_payload(self, cookie: str, cid: str = "0", folders_only: bool = False) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_entries(self, cookie: str, cid: str = "0") -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def create_folder(self, cookie: str, cid: str = "0", folder_name: str = "") -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def resolve_folder_id_by_path(self, cookie: str, relative_path: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def ensure_folder_id_by_path(self, cookie: str, relative_path: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def resolve_share_payload(self, cookie: str, share_url: str, raw_text: str = "", receive_code: str = "") -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_share_entries(self, cookie: str, share_payload: Dict[str, Any], cid: str = "0", offset: int = 0, limit: int = 200) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def prepare_share_receive(self, cookie: str, share_payload: Dict[str, Any], cid: str = "0") -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def submit_share_receive(self, cookie: str, receive_payload: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
        raise NotImplementedError

    def submit_offline_task(self, cookie: str, resource_url: str, folder_id: str = "0") -> Dict[str, Any]:
        raise NotImplementedError(f"{self.label} 不支持离线下载")

    @abstractmethod
    def probe_connectivity(self, cookie: str) -> bool:
        raise NotImplementedError

    def resolve_download_url(self, cookie: str, file_id: str) -> str:
        raise NotImplementedError(f"{self.label} 不支持 STRM 直链")

    # === 文件操作（可选，子类按能力覆写） ===
    def rename_entry(self, cookie: str, entry_id: str, new_name: str, parent_id: str = "") -> Dict[str, Any]:
        raise NotImplementedError(f"{self.label} 不支持重命名")

    def move_entries(self, cookie: str, entry_ids: List[str], target_id: str, source_id: str = "") -> Dict[str, Any]:
        raise NotImplementedError(f"{self.label} 不支持移动")

    def copy_entries(self, cookie: str, entry_ids: List[str], target_id: str, source_id: str = "") -> Dict[str, Any]:
        raise NotImplementedError(f"{self.label} 不支持复制")

    def delete_entries(self, cookie: str, entry_ids: List[str], parent_id: str = "") -> Dict[str, Any]:
        raise NotImplementedError(f"{self.label} 不支持删除")

    # === 上传（可选，子类按能力覆写） ===
    def upload_file(
        self,
        cookie: str,
        sink: "UploadSink",
        filename: str,
        target_dir_id: str = "0",
        *,
        sha1: str = "",
        md5: str = "",
        size: int = -1,
        progress_cb=None,
        cancel_check=None,
        **kwargs: Any,
    ) -> "UploadResult":
        """把 sink 中的数据上传到网盘目录。

        :param sink: 数据源（见 UploadSink 协议，内存流或临时文件均可）
        :param filename: 保存的文件名
        :param target_dir_id: 目标目录 id（"0" 为根目录）
        :param sha1: 上层已计算的 SHA1（大写十六进制）；为空则实现自行读取计算
        :param md5: 上层已计算的 MD5（小写十六进制）；夸克秒传需要，其余网盘可忽略
        :param size: 文件总大小；为 -1 时由 sink.size 提供
        :param progress_cb: 进度回调 fn(sent_bytes, total_bytes)
        :param cancel_check: 取消检查 fn() -> bool，返回 True 时中止上传
        """
        raise NotImplementedError(f"{self.label} 不支持上传")

    def abort_upload(self, cookie: str, upload_handle: Dict[str, Any]) -> bool:
        """尽力中止一次进行中的上传（取消语义见设计文档 §4.5）。默认不支持。"""
        return False

    # === 限流工具 ===
    def throttle(self) -> None:
        if self.rate_limit_seconds <= 0:
            return
        with self._rate_limit_lock:
            elapsed = time.monotonic() - self._last_request_monotonic
            if elapsed < self.rate_limit_seconds:
                time.sleep(self.rate_limit_seconds - elapsed)
            self._last_request_monotonic = time.monotonic()

    def get_cookie(self, cfg: Dict[str, Any]) -> str:
        """从配置字典中获取认证凭据"""
        if not self.name:
            return ""
        candidates = self.config_keys if self.config_keys else [f"cookie_{self.name}"]
        for key in candidates:
            value = str(cfg.get(key, "")).strip()
            if value:
                return value
        return ""

    def is_configured(self, cfg: Dict[str, Any]) -> bool:
        """判断认证配置是否填写完整，不触发远端登录或 token 刷新。"""
        if not self.name:
            return False
        source = cfg if isinstance(cfg, dict) else {}
        candidates = self.config_keys if self.config_keys else [f"cookie_{self.name}"]
        if not candidates:
            return False
        if self.auth_type == "password":
            return all(str(source.get(key, "") or "").strip() for key in candidates)
        return any(str(source.get(key, "") or "").strip() for key in candidates)

    def auth_label(self) -> str:
        if self.auth_type == "cookie":
            return "Cookie"
        if self.auth_type == "password_cookie":
            return "账号密码 / Cookie"
        if self.auth_type == "refresh_token":
            return "refresh_token"
        return "认证信息"
