from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path

from astrbot.api import AstrBotConfig, llm_tool, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.star.filter.command import CommandFilter
from astrbot.core.star.filter.command_group import CommandGroupFilter
from astrbot.core.star.star_handler import EventType, star_handlers_registry
from astrbot.core.utils.path_utils import get_astrbot_plugin_data_path

from .alias_analysis_service import force_alias_analysis, maybe_schedule_alias_analysis
from .alias_service import extract_target_id_from_mentions, resolve_alias
from .config import PluginConfig
from .injection import apply_injection, format_profile_for_injection
from .storage import ImpressionStore, ProfileRecord
from .update_service import force_group_update, maybe_schedule_group_update
from .utils import (
    extract_raw_text,
    extract_plain_text,
    is_self_profile_query,
)

PLUGIN_NAME = "astrbot_plugin_auto_impression_card"

@register(
    PLUGIN_NAME,
    "ninifox",
    "auto impression card for group chat",
    "0.1",
)
class AutoImpressionCard(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.context = context
        self.config = PluginConfig.from_config(config)
        self._ignore_pattern = (
            re.compile(self.config.ignore_regex)
            if self.config.ignore_regex
            else None
        )
        data_dir = Path(get_astrbot_plugin_data_path()) / PLUGIN_NAME
        db_path = data_dir / "impressions.db"
        self.store = ImpressionStore(db_path)
        self.store.initialize()
        self._active_updates: set[str] = set()
        self._update_locks: dict[str, asyncio.Lock] = {}
        self._alias_active_updates: set[str] = set()
        self._alias_update_locks: dict[str, asyncio.Lock] = {}
        self._group_active_updates: set[str] = set()
        self._group_update_locks: dict[str, asyncio.Lock] = {}

    async def initialize(self):
        logger.info("Auto Impression Card plugin initialized")

    async def terminate(self):
        logger.info("Auto Impression Card plugin terminated")

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AiocqhttpMessageEvent):
        if not self.config.enabled:
            return

        group_id = str(event.get_group_id())
        if not group_id:
            return

        if str(event.get_sender_id()) == str(event.get_self_id()):
            return

        components = event.get_messages()
        plain_text = extract_plain_text(components)
        raw_text = extract_raw_text(components)
        if not plain_text and not raw_text:
            return

        if plain_text and len(plain_text) < self.config.ignore_short_text_len:
            return

        if plain_text and self._ignore_pattern and self._ignore_pattern.search(plain_text):
            return

        ts = int(time.time())
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name() or user_id

        await asyncio.to_thread(
            self.store.touch_profile, group_id, user_id, nickname, ts
        )
        if plain_text and self._is_command_message(event, plain_text):
            return
        raw_text = raw_text or plain_text
        await asyncio.to_thread(
            self.store.enqueue_message, group_id, user_id, raw_text, ts
        )
        asyncio.create_task(
            self._run_group_update_with_alias(
                group_id,
                event.unified_msg_origin,
            )
        )

    @filter.on_llm_request()
    async def inject_profile(self, event: AstrMessageEvent, request):
        if not self.config.enabled:
            return
        if event.get_platform_name() != "aiocqhttp":
            return
        await apply_injection(
            event,
            request,
            self.store,
            self.config,
            self.config.debug_mode,
        )

    @llm_tool(name="get_impression_profile")
    async def get_impression_profile(
        self,
        event: AstrMessageEvent,
        target: str,
        detail: str = "summary",
    ) -> str:
        """获取群友印象档案（本地数据库）。

        Args:
            target(string): 群友的 QQ 号或别名/昵称；为空则默认当前发起者。
            detail(string): summary 或 full。summary 返回摘要，full 返回完整卡片。

        """
        if not self.config.enabled:
            return self._tool_result("error", "plugin disabled")
        if event.get_platform_name() != "aiocqhttp":
            return self._tool_result("error", "only supported on aiocqhttp")
        group_id = str(event.get_group_id())
        if not group_id:
            return self._tool_result("error", "group context required")

        speaker_id = str(event.get_sender_id())
        message_text = event.get_message_str() or ""
        target = (target or "").strip()
        if target.startswith("@"):
            target = target[1:].strip()

        target_id = ""
        if not target:
            target_id = speaker_id
        elif target.isdigit():
            target_id = target
        else:
            candidates = await asyncio.to_thread(
                self.store.find_alias_targets, group_id, speaker_id, target
            )
            if len(candidates) == 1:
                target_id = str(candidates[0]["target_id"])
            elif len(candidates) > 1:
                ids = ", ".join(c["target_id"] for c in candidates[:5])
                return self._tool_result("ambiguous", "alias matched multiple users", ids)
            else:
                # fallback: try global alias then nickname match
                candidates = await asyncio.to_thread(
                    self.store.find_alias_targets_global, group_id, target
                )
                if len(candidates) == 1:
                    target_id = str(candidates[0]["target_id"])
                elif len(candidates) > 1:
                    ids = ", ".join(c["target_id"] for c in candidates[:5])
                    return self._tool_result(
                        "ambiguous", "alias matched multiple users", ids
                    )

                if not target_id:
                    profiles = await asyncio.to_thread(
                        self.store.find_profiles_by_nickname, target
                    )
                    if len(profiles) == 1:
                        target_id = profiles[0].user_id
                    elif len(profiles) > 1:
                        ids = ", ".join(p.user_id for p in profiles[:5])
                        return self._tool_result(
                            "ambiguous", "nickname matched multiple users", ids
                        )
                    else:
                        pass

        if not target_id and message_text:
            alias_index = await asyncio.to_thread(self.store.get_alias_index, group_id)
            token_candidates = [
                t for t in re.findall(r"[\w\u4e00-\u9fff]{2,16}", message_text)
            ]

            def _norm_alias(text: str) -> str:
                return re.sub(r"[，。！？,!.?;；:：]+$", "", text.strip()).lower()

            speaker_lookup: dict[str, str] = {}
            for alias in alias_index.get(speaker_id, {}).keys():
                norm = _norm_alias(alias)
                if norm and norm not in speaker_lookup:
                    speaker_lookup[norm] = alias

            global_lookup: dict[str, str] = {}
            for aliases in alias_index.values():
                for alias in aliases.keys():
                    norm = _norm_alias(alias)
                    if norm and norm not in global_lookup:
                        global_lookup[norm] = alias

            for token in token_candidates:
                norm = _norm_alias(token)
                if not norm:
                    continue
                matched_alias = speaker_lookup.get(norm)
                if not matched_alias and norm in global_lookup:
                    matched_alias = global_lookup[norm]
                if not matched_alias:
                    continue
                candidates = await asyncio.to_thread(
                    self.store.find_alias_targets, group_id, speaker_id, matched_alias
                )
                if len(candidates) == 1:
                    target_id = str(candidates[0]["target_id"])
                    break
                if len(candidates) > 1:
                    ids = ", ".join(c["target_id"] for c in candidates[:5])
                    return self._tool_result(
                        "ambiguous", "alias matched multiple users", ids
                    )
                candidates = await asyncio.to_thread(
                    self.store.find_alias_targets_global, group_id, matched_alias
                )
                if len(candidates) == 1:
                    target_id = str(candidates[0]["target_id"])
                    break
                if len(candidates) > 1:
                    ids = ", ".join(c["target_id"] for c in candidates[:5])
                    return self._tool_result(
                        "ambiguous", "alias matched multiple users", ids
                    )

        if not target_id:
            return self._tool_result("not_found", "no profile for alias")

        if target_id == speaker_id and not is_self_profile_query(message_text):
            return self._tool_result(
                "not_allowed",
                "current speaker not allowed unless explicitly requested",
            )

        profile = await asyncio.to_thread(self.store.get_profile, target_id)
        if not profile or not profile.summary:
            return self._tool_result("not_found", "no profile for user")

        alias_rows = await asyncio.to_thread(
            self.store.get_aliases_by_target, group_id, target_id
        )
        aliases_by_speaker: dict[str, list[str]] = {}
        for row in alias_rows:
            speaker_id = str(row.get("speaker_id", "")).strip()
            alias = str(row.get("alias", "")).strip()
            if not speaker_id or not alias:
                continue
            names = aliases_by_speaker.setdefault(speaker_id, [])
            if alias not in names:
                names.append(alias)

        if detail.strip().lower() == "full":
            result = self._format_profile_for_reply(profile)
        else:
            result = format_profile_for_injection(profile, self.config.inject_max_chars)

        payload = {
            "status": "ok",
            "detail": detail.strip().lower(),
            "user_id": profile.user_id,
            "nickname": profile.nickname or "",
            "content": result,
            "aliases_by_speaker": aliases_by_speaker,
        }
        text = json.dumps(payload, ensure_ascii=False)
        if self.config.debug_mode:
            logger.info(
                f"[AIC] Tool get_impression_profile target={target_id}, detail={detail}:\n{text}"
            )
        return text

    @llm_tool(name="resolve_alias")
    async def resolve_alias_tool(
        self,
        event: AstrMessageEvent,
        alias: str,
    ) -> str:
        """解析别称/昵称到 user_id（alias_map 优先，fallback 全局别称）。

        Args:
            alias(string): 别称或昵称（不含 @）。

        """
        if not self.config.enabled:
            return self._tool_result("error", "plugin disabled")
        if event.get_platform_name() != "aiocqhttp":
            return self._tool_result("error", "only supported on aiocqhttp")
        group_id = str(event.get_group_id())
        if not group_id:
            return self._tool_result("error", "group context required")

        speaker_id = str(event.get_sender_id())
        alias = (alias or "").strip()
        if alias.startswith("@"):
            alias = alias[1:].strip()
        if not alias:
            return self._tool_result("not_found", "empty alias")

        candidates = await asyncio.to_thread(
            self.store.find_alias_targets, group_id, speaker_id, alias
        )
        if len(candidates) == 1:
            target_id = str(candidates[0]["target_id"])
            return self._tool_result("ok", target_id, target_id)
        if len(candidates) > 1:
            ids = ", ".join(c["target_id"] for c in candidates[:5])
            return self._tool_result("ambiguous", "alias matched multiple users", ids)

        candidates = await asyncio.to_thread(
            self.store.find_alias_targets_global, group_id, alias
        )
        if len(candidates) == 1:
            target_id = str(candidates[0]["target_id"])
            return self._tool_result("ok", target_id, target_id)
        if len(candidates) > 1:
            ids = ", ".join(c["target_id"] for c in candidates[:5])
            return self._tool_result("ambiguous", "alias matched multiple users", ids)

        profiles = await asyncio.to_thread(
            self.store.find_profiles_by_nickname, alias
        )
        if len(profiles) == 1:
            target_id = profiles[0].user_id
            return self._tool_result("ok", target_id, target_id)
        if len(profiles) > 1:
            ids = ", ".join(p.user_id for p in profiles[:5])
            return self._tool_result("ambiguous", "nickname matched multiple users", ids)

        return self._tool_result("not_found", "no alias match")

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("印象", alias={"/印象"})
    async def impression_command(self, event: AiocqhttpMessageEvent):
        event.should_call_llm(True)
        if not self.config.enabled:
            return
        group_id = str(event.get_group_id())
        if not group_id:
            return

        target_id = extract_target_id_from_mentions(event)
        if not target_id:
            alias = self._extract_alias_from_command(event.message_str)
            if alias:
                target_id = await resolve_alias(
                    self.store, group_id, str(event.get_sender_id()), alias
                )

        if not target_id:
            yield event.plain_result("请 @群友 或提供昵称")
            return

        profile = await asyncio.to_thread(self.store.get_profile, target_id)
        if not profile or not profile.summary:
            yield event.plain_result("暂无该成员档案")
            return

        yield event.plain_result(self._format_profile_for_reply(profile))

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("印象更新", alias={"/印象更新"})
    async def impression_update_command(self, event: AiocqhttpMessageEvent):
        event.should_call_llm(True)
        if not self.config.enabled:
            return
        group_id = str(event.get_group_id())
        if not group_id:
            return

        message_tokens = event.message_str.split()
        clear_old = "清空" in message_tokens
        global_update = self._has_global_update_flag(message_tokens)
        target_id = extract_target_id_from_mentions(event)
        if not target_id:
            alias = self._extract_alias_from_command(
                event.message_str, ignore_tokens={"清空", "全体", "全部", "all", "a", "A"}
            )
            if alias:
                target_id = await resolve_alias(
                    self.store, group_id, str(event.get_sender_id()), alias
                )

        if not target_id:
            target_id = str(event.get_sender_id())

        yield event.plain_result("正在强制更新印象档案...")
        if global_update:
            group_ids = await asyncio.to_thread(self.store.get_group_ids)
            ok = False
            for gid in group_ids:
                await force_alias_analysis(
                    self.context,
                    self.store,
                    self.config,
                    self._debug_log,
                    self._alias_update_locks,
                    gid,
                    event.unified_msg_origin,
                )
                updated = await force_group_update(
                    self.context,
                    self.store,
                    self.config,
                    self._debug_log,
                    self._group_update_locks,
                    event.unified_msg_origin,
                    gid,
                )
                ok = ok or updated
        else:
            await force_alias_analysis(
                self.context,
                self.store,
                self.config,
                self._debug_log,
                self._alias_update_locks,
                group_id,
                event.unified_msg_origin,
            )
            ok = await force_group_update(
                self.context,
                self.store,
                self.config,
                self._debug_log,
                self._group_update_locks,
                event.unified_msg_origin,
                group_id,
            )
        if ok:
            yield event.plain_result("印象档案已更新")
        else:
            yield event.plain_result("印象档案更新失败，请稍后重试")

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.regex(r".*(更新|刷新|修改|补充|调整).*(印象).*")
    async def impression_update_nl(self, event: AiocqhttpMessageEvent):
        event.should_call_llm(True)
        if not self.config.enabled:
            return
        message_text = event.message_str.strip()
        if message_text.startswith(("印象更新", "/印象更新")):
            return
        group_id = str(event.get_group_id())
        if not group_id:
            return

        target_id = extract_target_id_from_mentions(event)
        if not target_id:
            target_text = self._extract_target_from_update_phrase(event.message_str)
            if target_text:
                if target_text.isdigit():
                    target_id = target_text
                else:
                    target_id = await resolve_alias(
                        self.store, group_id, str(event.get_sender_id()), target_text
                    )

        if not target_id:
            yield event.plain_result("请 @群友 或提供昵称")
            return

        nickname = event.get_sender_name() or target_id
        yield event.plain_result("正在强制更新印象档案...")
        ok = await force_group_update(
            self.context,
            self.store,
            self.config,
            self._debug_log,
            self._group_update_locks,
            event.unified_msg_origin,
            group_id,
        )
        if ok:
            yield event.plain_result("印象档案已更新")
        else:
            yield event.plain_result("印象档案更新失败，请稍后重试")

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("称呼分析", alias={"/称呼分析", "别称分析", "/别称分析"})
    async def alias_analysis_command(self, event: AiocqhttpMessageEvent):
        event.should_call_llm(True)
        if not self.config.enabled:
            return
        group_id = str(event.get_group_id())
        if not group_id:
            return

        yield event.plain_result("正在分析称呼...")
        ok = await force_alias_analysis(
            self.context,
            self.store,
            self.config,
            self._debug_log,
            self._alias_update_locks,
            group_id,
            event.unified_msg_origin,
        )
        if ok:
            yield event.plain_result("称呼分析已完成")
        else:
            yield event.plain_result("称呼分析失败或暂无待分析消息")

    @staticmethod
    def _extract_target_from_update_phrase(message_str: str) -> str:
        text = message_str.strip()
        match = re.search(r"对(?P<name>[^\s@]{1,12})的?印象", text)
        if match:
            return match.group("name").strip()
        return ""

    @staticmethod
    def _extract_alias_from_command(
        message_str: str, ignore_tokens: set[str] | None = None
    ) -> str:
        parts = message_str.strip().split()
        if len(parts) <= 1:
            return ""
        rest = parts[1:]
        if ignore_tokens:
            rest = [p for p in rest if p not in ignore_tokens]
        return " ".join(rest).strip()

    def _is_command_message(
        self, event: AstrMessageEvent, plain_text: str
    ) -> bool:
        message = re.sub(r"\s+", " ", plain_text.strip())
        if not message:
            return False

        cfg = self.context.get_config(event.unified_msg_origin)
        wake_prefixes = cfg.get("wake_prefix", [])
        if isinstance(wake_prefixes, str):
            wake_prefixes = [wake_prefixes]

        for prefix in wake_prefixes:
            if prefix and message.startswith(prefix):
                message = message[len(prefix) :].strip()
                break

        if not message:
            return False

        for handler in star_handlers_registry:
            if handler.event_type != EventType.AdapterMessageEvent:
                continue
            for event_filter in handler.event_filters:
                if isinstance(event_filter, (CommandFilter, CommandGroupFilter)):
                    for cmd in event_filter.get_complete_command_names():
                        if message == cmd or message.startswith(f"{cmd} "):
                            return True
        return False

    def _format_profile_for_reply(self, profile: ProfileRecord) -> str:
        impressions = profile.impressions[: self.config.inject_max_impressions]
        lines = [
            f"昵称: {profile.nickname or ''}",
        ]
        if impressions:
            lines.append("Impressions: " + "; ".join(impressions))
        return "\n".join(lines)

    def _debug_log(self, message: str) -> None:
        if self.config.debug_mode:
            logger.info(message)

    @staticmethod
    def _has_global_update_flag(tokens: list[str]) -> bool:
        for token in tokens:
            if token in {"全体", "全部", "all", "a", "A"}:
                return True
            if "全体" in token or "全部" in token:
                return True
        return False

    async def _run_group_update_with_alias(self, group_id: str, umo: str) -> None:
        await maybe_schedule_alias_analysis(
            self.context,
            self.store,
            self.config,
            self._debug_log,
            self._alias_active_updates,
            self._alias_update_locks,
            group_id,
            umo,
        )
        await maybe_schedule_group_update(
            self.context,
            self.store,
            self.config,
            self._debug_log,
            self._group_active_updates,
            self._group_update_locks,
            group_id,
            umo,
        )

    @staticmethod
    def _tool_result(status: str, message: str, extra: str | None = None) -> str:
        payload = {"status": status, "message": message}
        if extra:
            payload["candidates"] = extra
        return json.dumps(payload, ensure_ascii=False)
