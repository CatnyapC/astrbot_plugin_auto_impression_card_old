from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from astrbot.core.config.astrbot_config import AstrBotConfig


@dataclass(slots=True)
class PluginConfig:
    enabled: bool
    group_whitelist: set[str]
    update_msg_threshold: int
    update_time_threshold_sec: int
    inject_max_chars: int
    inject_max_traits: int
    inject_max_facts: int
    ignore_short_text_len: int
    ignore_regex: str

    @classmethod
    def from_config(cls, config: AstrBotConfig | dict | None) -> "PluginConfig":
        cfg = config or {}
        basic = cfg.get("Basic", {})
        update = cfg.get("Update", {})
        injection = cfg.get("Injection", {})
        filters = cfg.get("Filters", {})

        whitelist = basic.get("group_whitelist", [])
        if not isinstance(whitelist, list):
            whitelist = []

        return cls(
            enabled=bool(basic.get("enabled", True)),
            group_whitelist=set(str(x) for x in whitelist),
            update_msg_threshold=int(update.get("update_msg_threshold", 50)),
            update_time_threshold_sec=int(update.get("update_time_threshold_sec", 7200)),
            inject_max_chars=int(injection.get("inject_max_chars", 600)),
            inject_max_traits=int(injection.get("inject_max_traits", 8)),
            inject_max_facts=int(injection.get("inject_max_facts", 6)),
            ignore_short_text_len=int(filters.get("ignore_short_text_len", 3)),
            ignore_regex=str(filters.get("ignore_regex", "")),
        )

    def is_group_allowed(self, group_id: str) -> bool:
        if not self.group_whitelist:
            return False
        return group_id in self.group_whitelist

    def normalize_whitelist(self, ids: Iterable[str]) -> None:
        self.group_whitelist = set(str(x) for x in ids)
