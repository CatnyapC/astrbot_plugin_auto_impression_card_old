from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from astrbot.core.config.astrbot_config import AstrBotConfig


@dataclass(slots=True)
class PluginConfig:
    enabled: bool
    update_mode: str
    update_msg_threshold: int
    update_time_threshold_sec: int
    group_batch_known_users_max: int
    group_batch_enable_nickname_match: bool
    group_batch_enable_semantic_attribution: bool
    group_batch_attribution_max_messages: int
    group_batch_attribution_max_targets_per_message: int
    group_batch_attribution_include_summary: bool
    alias_analysis_batch_size: int
    update_provider_id: str
    alias_provider_id: str
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
        alias = cfg.get("Alias", {})
        model = cfg.get("Model", {})
        injection = cfg.get("Injection", {})
        filters = cfg.get("Filters", {})
        debug = cfg.get("Debug", {})

        return cls(
            enabled=bool(basic.get("enabled", True)),
            update_mode=str(update.get("update_mode", "group_batch")).strip()
            or "group_batch",
            update_msg_threshold=int(update.get("update_msg_threshold", 50)),
            update_time_threshold_sec=int(update.get("update_time_threshold_sec", 7200)),
            group_batch_known_users_max=int(
                update.get("group_batch_known_users_max", 50)
            ),
            group_batch_enable_nickname_match=bool(
                update.get("group_batch_enable_nickname_match", True)
            ),
            group_batch_enable_semantic_attribution=bool(
                update.get("group_batch_enable_semantic_attribution", True)
            ),
            group_batch_attribution_max_messages=int(
                update.get("group_batch_attribution_max_messages", 120)
            ),
            group_batch_attribution_max_targets_per_message=int(
                update.get("group_batch_attribution_max_targets_per_message", 2)
            ),
            group_batch_attribution_include_summary=bool(
                update.get("group_batch_attribution_include_summary", True)
            ),
            alias_analysis_batch_size=int(alias.get("alias_analysis_batch_size", 15)),
            update_provider_id=str(model.get("update_provider_id", "")).strip(),
            alias_provider_id=str(model.get("alias_provider_id", "")).strip(),
            inject_max_chars=int(injection.get("inject_max_chars", 600)),
            inject_max_traits=int(injection.get("inject_max_traits", 8)),
            inject_max_facts=int(injection.get("inject_max_facts", 6)),
            ignore_short_text_len=int(filters.get("ignore_short_text_len", 3)),
            ignore_regex=str(filters.get("ignore_regex", "")),
            debug_mode=bool(debug.get("debug_mode", False)),
        )
