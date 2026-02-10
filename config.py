from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from astrbot.core.config.astrbot_config import AstrBotConfig


@dataclass(slots=True)
class PluginConfig:
    enabled: bool
    update_msg_threshold: int
    update_time_threshold_sec: int
    inject_max_chars: int
    inject_max_traits: int
    inject_max_facts: int
    ignore_short_text_len: int
    ignore_regex: str
    debug_mode: bool

    @classmethod
    def from_config(cls, config: AstrBotConfig | dict | None) -> "PluginConfig":
        cfg = config or {}
        basic = cfg.get("Basic", {})
        update = cfg.get("Update", {})
        injection = cfg.get("Injection", {})
        filters = cfg.get("Filters", {})
        debug = cfg.get("Debug", {})

        return cls(
            enabled=bool(basic.get("enabled", True)),
            update_msg_threshold=int(update.get("update_msg_threshold", 50)),
            update_time_threshold_sec=int(update.get("update_time_threshold_sec", 7200)),
            inject_max_chars=int(injection.get("inject_max_chars", 600)),
            inject_max_traits=int(injection.get("inject_max_traits", 8)),
            inject_max_facts=int(injection.get("inject_max_facts", 6)),
            ignore_short_text_len=int(filters.get("ignore_short_text_len", 3)),
            ignore_regex=str(filters.get("ignore_regex", "")),
            debug_mode=bool(debug.get("debug_mode", False)),
        )
