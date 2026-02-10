from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.utils.path_utils import get_astrbot_plugin_data_path

from .config import PluginConfig
from .prompts import PROFILE_UPDATE_SYSTEM_PROMPT
from .storage import ImpressionStore, ProfileRecord

PLUGIN_NAME = "astrbot_plugin_auto_impression_card"
MAX_PENDING_MESSAGES = 200


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
        if not group_id or not self.config.is_group_allowed(group_id):
            return

        if str(event.get_sender_id()) == str(event.get_self_id()):
            return

        plain_text = self._extract_plain_text(event.get_messages())
        if not plain_text:
            return

        if len(plain_text) < self.config.ignore_short_text_len:
            return

        if self._ignore_pattern and self._ignore_pattern.search(plain_text):
            return

        ts = int(time.time())
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name() or user_id

        await asyncio.to_thread(
            self.store.touch_profile, group_id, user_id, nickname, ts
        )
        await asyncio.to_thread(
            self.store.enqueue_message, group_id, user_id, plain_text, ts
        )

        await self._learn_aliases(event, group_id, user_id)
        await self._maybe_schedule_update(event, group_id, user_id, nickname)

    @filter.on_llm_request()
    async def inject_profile(self, event: AstrMessageEvent, request):
        if not self.config.enabled:
            return
        if event.get_platform_name() != "aiocqhttp":
            return
        group_id = str(event.get_group_id())
        if not group_id or not self.config.is_group_allowed(group_id):
            return

        user_id = str(event.get_sender_id())
        profile = await asyncio.to_thread(self.store.get_profile, group_id, user_id)
        if not profile or not profile.summary:
            return

        injection = self._format_profile_for_injection(profile)
        if not injection:
            return

        request.system_prompt = (request.system_prompt or "") + "\n\n" + injection

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("印象")
    async def impression_command(self, event: AiocqhttpMessageEvent):
        if not self.config.enabled:
            return
        group_id = str(event.get_group_id())
        if not group_id or not self.config.is_group_allowed(group_id):
            return

        target_id = self._extract_target_id_from_mentions(event)
        if not target_id:
            alias = self._extract_alias_from_command(event.message_str)
            if alias:
                target_id = await self._resolve_alias(
                    group_id, str(event.get_sender_id()), alias
                )

        if not target_id:
            yield event.plain_result("请 @群友 或提供昵称")
            return

        profile = await asyncio.to_thread(self.store.get_profile, group_id, target_id)
        if not profile or not profile.summary:
            yield event.plain_result("暂无该成员档案")
            return

        yield event.plain_result(self._format_profile_for_reply(profile))

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.command("印象更新")
    async def impression_update_command(self, event: AiocqhttpMessageEvent):
        if not self.config.enabled:
            return
        group_id = str(event.get_group_id())
        if not group_id or not self.config.is_group_allowed(group_id):
            return

        clear_old = "清空" in event.message_str.split()
        target_id = self._extract_target_id_from_mentions(event)
        if not target_id:
            alias = self._extract_alias_from_command(
                event.message_str, ignore_tokens={"清空"}
            )
            if alias:
                target_id = await self._resolve_alias(
                    group_id, str(event.get_sender_id()), alias
                )

        if not target_id:
            target_id = str(event.get_sender_id())

        nickname = event.get_sender_name() or target_id

        yield event.plain_result("正在强制更新印象档案...")
        ok = await self._force_update(
            event.unified_msg_origin,
            group_id,
            target_id,
            nickname,
            clear_old,
        )
        if ok:
            yield event.plain_result("印象档案已更新")
        else:
            yield event.plain_result("印象档案更新失败，请稍后重试")

    async def _maybe_schedule_update(
        self,
        event: AiocqhttpMessageEvent,
        group_id: str,
        user_id: str,
        nickname: str,
    ) -> None:
        pending_count = await asyncio.to_thread(
            self.store.get_pending_count, group_id, user_id
        )
        if pending_count <= 0:
            return

        profile = await asyncio.to_thread(self.store.get_profile, group_id, user_id)
        last_updated = profile.updated_at if profile and profile.updated_at else 0
        now = int(time.time())

        should_update = pending_count >= self.config.update_msg_threshold
        should_update = should_update or (
            now - last_updated >= self.config.update_time_threshold_sec
        )

        if not should_update:
            return

        key = f"{group_id}:{user_id}"
        if key in self._active_updates:
            return
        self._active_updates.add(key)
        asyncio.create_task(
            self._run_update(
                key,
                event.unified_msg_origin,
                group_id,
                user_id,
                nickname,
            )
        )

    async def _run_update(
        self,
        key: str,
        umo: str,
        group_id: str,
        user_id: str,
        nickname: str,
    ) -> None:
        lock = self._update_locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                pending = await asyncio.to_thread(
                    self.store.get_pending_messages,
                    group_id,
                    user_id,
                    MAX_PENDING_MESSAGES,
                )
                if not pending:
                    return

                profile = await asyncio.to_thread(self.store.get_profile, group_id, user_id)
                existing = {
                    "summary": profile.summary if profile else "",
                    "traits": profile.traits if profile else [],
                    "facts": profile.facts if profile else [],
                    "examples": profile.examples if profile else [],
                }

                try:
                    provider = self.context.get_using_provider(umo=umo)
                except ValueError as exc:
                    logger.warning(f"No LLM provider configured: {exc}")
                    return
                if not provider:
                    logger.warning("No LLM provider configured for impression update")
                    return

                prompt = self._build_update_prompt(existing, pending)
                try:
                    resp = await provider.text_chat(
                        system_prompt=PROFILE_UPDATE_SYSTEM_PROMPT,
                        prompt=prompt,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"LLM update call failed: {exc}")
                    return

                data, ok = self._parse_profile_json(
                    resp.completion_text or "", existing
                )
                if not ok:
                    logger.warning(
                        "LLM update returned invalid JSON, keeping pending messages"
                    )
                    return

                updated_at = int(time.time())
                last_seen = max(p.ts for p in pending)
                record = ProfileRecord(
                    group_id=group_id,
                    user_id=user_id,
                    nickname=profile.nickname if profile and profile.nickname else nickname,
                    last_seen=last_seen,
                    summary=data["summary"],
                    traits=data["traits"],
                    facts=data["facts"],
                    examples=data["examples"],
                    updated_at=updated_at,
                    version=(profile.version if profile else 1),
                )
                await asyncio.to_thread(self.store.upsert_profile, record)
                await asyncio.to_thread(
                    self.store.delete_pending_messages, [p.id for p in pending]
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Impression update failed: {exc}")
            finally:
                self._active_updates.discard(key)

    async def _force_update(
        self,
        umo: str,
        group_id: str,
        user_id: str,
        nickname: str,
        clear_old: bool,
    ) -> bool:
        key = f"{group_id}:{user_id}"
        lock = self._update_locks.setdefault(key, asyncio.Lock())
        async with lock:
            pending = await asyncio.to_thread(
                self.store.get_pending_messages,
                group_id,
                user_id,
                MAX_PENDING_MESSAGES,
            )
            profile = await asyncio.to_thread(self.store.get_profile, group_id, user_id)
            existing = {
                "summary": "",
                "traits": [],
                "facts": [],
                "examples": [],
            }
            if profile and not clear_old:
                existing = {
                    "summary": profile.summary or "",
                    "traits": profile.traits,
                    "facts": profile.facts,
                    "examples": profile.examples,
                }

            try:
                provider = self.context.get_using_provider(umo=umo)
            except ValueError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                return False
            if not provider:
                logger.warning("No LLM provider configured for impression update")
                return False

            prompt = self._build_update_prompt(existing, pending)
            try:
                resp = await provider.text_chat(
                    system_prompt=PROFILE_UPDATE_SYSTEM_PROMPT,
                    prompt=prompt,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(f"LLM update call failed: {exc}")
                return False

            data, ok = self._parse_profile_json(resp.completion_text or "", existing)
            if not ok:
                logger.warning("LLM update returned invalid JSON")
                return False

            updated_at = int(time.time())
            last_seen = max([p.ts for p in pending], default=updated_at)
            record = ProfileRecord(
                group_id=group_id,
                user_id=user_id,
                nickname=profile.nickname if profile and profile.nickname else nickname,
                last_seen=last_seen,
                summary=data["summary"],
                traits=data["traits"],
                facts=data["facts"],
                examples=data["examples"],
                updated_at=updated_at,
                version=(profile.version if profile else 1),
            )
            await asyncio.to_thread(self.store.upsert_profile, record)

            if pending:
                await asyncio.to_thread(
                    self.store.delete_pending_messages, [p.id for p in pending]
                )
            return True

    async def _learn_aliases(
        self, event: AiocqhttpMessageEvent, group_id: str, speaker_id: str
    ) -> None:
        candidates = self._extract_alias_candidates(event)
        if not candidates:
            return
        now = int(time.time())
        for alias, target_id, confidence in candidates:
            await asyncio.to_thread(
                self.store.upsert_alias,
                group_id,
                speaker_id,
                alias,
                target_id,
                confidence,
                now,
            )

    async def _resolve_alias(
        self, group_id: str, speaker_id: str, alias: str
    ) -> str | None:
        candidates = await asyncio.to_thread(
            self.store.find_alias_targets, group_id, speaker_id, alias
        )
        if not candidates:
            return None
        if len(candidates) == 1:
            return str(candidates[0]["target_id"])
        return None

    @staticmethod
    def _extract_plain_text(components) -> str:
        parts: list[str] = []
        for comp in components:
            if isinstance(comp, Plain):
                text = comp.text.strip()
                if text:
                    parts.append(text)
        return " ".join(parts).strip()

    def _extract_alias_candidates(self, event: AiocqhttpMessageEvent):
        components = event.get_messages()
        candidates: list[tuple[str, str, float]] = []
        buffer = ""

        for comp in components:
            if isinstance(comp, Plain):
                buffer += comp.text
            elif isinstance(comp, At):
                alias = self._last_token(buffer)
                if alias:
                    candidates.append((alias, str(comp.qq), 0.9))
                buffer = ""
            elif isinstance(comp, Reply):
                buffer += " "

        if not candidates:
            reply_target = self._extract_reply_target_id(event)
            if reply_target:
                alias = self._last_token(buffer)
                if alias and self._token_count(buffer) <= 2:
                    candidates.append((alias, reply_target, 0.7))

        return candidates

    @staticmethod
    def _extract_target_id_from_mentions(event: AiocqhttpMessageEvent) -> str | None:
        for comp in event.get_messages():
            if isinstance(comp, At) and str(comp.qq) != str(event.get_self_id()):
                return str(comp.qq)
        return None

    @staticmethod
    def _extract_reply_target_id(event: AiocqhttpMessageEvent) -> str | None:
        for comp in event.get_messages():
            if isinstance(comp, Reply) and comp.sender_id is not None:
                return str(comp.sender_id)
        return None

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

    @staticmethod
    def _last_token(text: str) -> str:
        tokens = re.findall(r"[\w\u4e00-\u9fff]+", text)
        if not tokens:
            return ""
        token = tokens[-1].strip()
        return token if len(token) >= 2 else ""

    @staticmethod
    def _token_count(text: str) -> int:
        return len(re.findall(r"[\w\u4e00-\u9fff]+", text))

    def _build_update_prompt(self, existing: dict, pending) -> str:
        lines = [
            "Existing profile (JSON):",
            json.dumps(existing, ensure_ascii=False),
            "",
            "New messages:",
        ]
        for idx, msg in enumerate(pending, 1):
            lines.append(f"{idx}. {msg.message}")
        return "\n".join(lines)

    def _parse_profile_json(self, text: str, existing: dict) -> tuple[dict, bool]:
        raw = self._extract_json(text)
        if not raw:
            return {}, False
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}, False

        summary = str(data.get("summary", "")).strip() or existing.get("summary", "")
        traits = data.get("traits")
        facts = data.get("facts")
        examples = data.get("examples")

        return (
            {
                "summary": summary,
                "traits": self._safe_list(traits, existing.get("traits", [])),
                "facts": self._safe_list(facts, existing.get("facts", [])),
                "examples": self._safe_list(examples, existing.get("examples", [])),
            },
            True,
        )

    @staticmethod
    def _safe_list(value, fallback: list[str]) -> list[str]:
        if isinstance(value, list) and value:
            return [str(x) for x in value]
        return fallback

    @staticmethod
    def _extract_json(text: str) -> str:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return ""
        return text[start : end + 1]

    def _format_profile_for_injection(self, profile: ProfileRecord) -> str:
        summary = (profile.summary or "").strip()
        if not summary:
            return ""
        if len(summary) > self.config.inject_max_chars:
            summary = summary[: self.config.inject_max_chars].rstrip() + "..."

        traits = profile.traits[: self.config.inject_max_traits]
        facts = profile.facts[: self.config.inject_max_facts]

        parts = [
            "[Group Member Impression]",
            f"User ID: {profile.user_id}",
            f"Nickname: {profile.nickname or ''}",
            f"Summary: {summary}",
        ]
        if traits:
            parts.append("Traits: " + ", ".join(traits))
        if facts:
            parts.append("Facts: " + "; ".join(facts))
        return "\n".join(parts)

    def _format_profile_for_reply(self, profile: ProfileRecord) -> str:
        summary = (profile.summary or "").strip()
        traits = profile.traits[: self.config.inject_max_traits]
        facts = profile.facts[: self.config.inject_max_facts]
        lines = [
            f"昵称: {profile.nickname or ''}",
            f"Summary: {summary}" if summary else "Summary: (empty)",
        ]
        if traits:
            lines.append("Traits: " + ", ".join(traits))
        if facts:
            lines.append("Facts: " + "; ".join(facts))
        return "\n".join(lines)
