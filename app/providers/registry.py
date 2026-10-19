import threading
from typing import Any, Dict, List, Optional

from .base import CloudProvider

_providers: Dict[str, CloudProvider] = {}
_lock = threading.Lock()


def register(provider: CloudProvider) -> None:
    with _lock:
        _providers[provider.name] = provider


def get(name: str) -> CloudProvider:
    with _lock:
        p = _providers.get(name)
    if p is None:
        raise RuntimeError(f"未知的网盘提供者: {name}")
    return p


def get_or_none(name: str) -> Optional[CloudProvider]:
    with _lock:
        return _providers.get(name)


def get_by_link_type(link_type: str) -> Optional[CloudProvider]:
    normalized = str(link_type or "").strip().lower()
    if not normalized:
        return None
    with _lock:
        for p in _providers.values():
            if p.link_type == normalized:
                return p
    return None


def list_all() -> List[CloudProvider]:
    with _lock:
        return list(_providers.values())


def label_for(name: str, default: str = "") -> str:
    """把内部代号换成给人看的网盘名。

    `name` 是内部标识（如 "tianyi" / "115" / "quark"），直接拼进用户可见文案
    会显示成英文代号。凡是要展示给用户的地方都应走这里拿中文名。
    """
    key = str(name or "").strip()
    if not key:
        return str(default or "").strip()
    provider = get_or_none(key)
    if provider is None:
        return str(default or "").strip() or key
    label = str(getattr(provider, "label", "") or "").strip()
    return label or str(default or "").strip() or key


def list_enabled(cfg: Optional[Dict[str, Any]] = None) -> List[CloudProvider]:
    enabled_map = {}
    if isinstance(cfg, dict):
        enabled_map = cfg.get("provider_enabled", {})
        if not isinstance(enabled_map, dict):
            enabled_map = {}
    with _lock:
        result = []
        for p in _providers.values():
            is_enabled = enabled_map.get(p.name, p.name in ("115", "quark"))
            if is_enabled:
                result.append(p)
        return result


def get_all_capabilities(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    enabled_map = {}
    if isinstance(cfg, dict):
        enabled_map = cfg.get("provider_enabled", {})
        if not isinstance(enabled_map, dict):
            enabled_map = {}
    with _lock:
        result = []
        for p in _providers.values():
            is_enabled = enabled_map.get(p.name, p.name in ("115", "quark"))
            result.append({
                "name": p.name,
                "label": p.label,
                "link_type": p.link_type,
                "auth_type": p.auth_type,
                "config_keys": list(p.config_keys),
                "enabled": bool(is_enabled),
                "supports_folder_browse": p.supports_folder_browse,
                "supports_share_receive": p.supports_share_receive,
                "supports_subscription": p.supports_subscription,
                "supports_offline": p.supports_offline,
                "supports_fixed_share_link": p.supports_fixed_share_link,
                "supports_strm": p.supports_strm,
                "supports_monitor": p.supports_monitor,
                "supports_rename": p.supports_rename,
                "supports_move": p.supports_move,
                "supports_copy": p.supports_copy,
                "supports_delete": p.supports_delete,
            })
        return result
