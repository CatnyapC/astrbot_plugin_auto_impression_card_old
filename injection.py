from __future__ import annotations

import asyncio

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .alias_service import extract_target_id_from_mentions
from .storage import ImpressionStore
from .utils import is_impression_query


def build_tool_guidance() -> str:
    return (
        "[AIC Tool Guidance]\n"
        "当用户询问某个群友的印象、回忆、评价、了解、关系、兴趣、偏好、"
        "身份/角色等，或需要你基于档案回答时，必须先调用工具。\n"
        "若用户提及具体对象但未 @，请先调用 `resolve_alias` 把别称解析成 user_id。\n"
        "解析成功后再用 `get_impression_profile` 获取档案，再作答。\n"
        "若用户提及具体对象并且明确 @ 了对方，可直接用 `get_impression_profile`。\n"
        "本轮请先调用工具，不要先给出用户可见的回答。\n"
        "除非用户明确要求（例如“对我/我自己”），否则不要对当前发起者调用该工具。\n"
        "如果工具返回 not_found/ambiguous，请向用户澄清后再答复，不要编造。"
    )


def build_speaker_guard(speaker_name: str, speaker_id: str) -> str:
    return (
        "[AIC Notice]\n"
        f"当前发起者是：{speaker_name}（{speaker_id}）。\n"
        "必须优先回应当前发起者，群友印象仅供参考，不得覆盖当前发起者。\n"
        "如提及他人，仅在不影响对当前发起者的回应前提下参考其印象。"
    )


def format_profile_for_injection(
    profile, inject_max_chars: int
) -> str:
    summary = (profile.summary or "").strip()
    if not summary:
        return ""
    if len(summary) > inject_max_chars:
        summary = summary[:inject_max_chars].rstrip() + "..."
    parts = [
        "[Group Member Impression]",
        f"User ID: {profile.user_id}",
        f"Nickname: {profile.nickname or ''}",
        f"Summary: {summary}",
    ]
    return "\n".join(parts)


async def apply_injection(
    event: AstrMessageEvent,
    request,
    store: ImpressionStore,
    config,
    debug_mode: bool,
) -> None:
    group_id = str(event.get_group_id())
    if not group_id:
        return

    user_id = str(event.get_sender_id())
    message_text = event.get_message_str() or ""
    needs_tool = True if config.force_tool_guidance else is_impression_query(message_text)
    injections: list[str] = []

    if not needs_tool:
        profile = await asyncio.to_thread(store.get_profile, group_id, user_id)
        if profile and profile.summary:
            injection = format_profile_for_injection(profile, config.inject_max_chars)
            if injection:
                injections.append(injection)
                if debug_mode:
                    logger.info(
                        f"[AIC] Injecting profile for user={user_id}, group={group_id}:\n{injection}"
                    )

        if isinstance(event, AiocqhttpMessageEvent):
            target_id = extract_target_id_from_mentions(event)
            if target_id and str(target_id) != user_id:
                target_profile = await asyncio.to_thread(
                    store.get_profile, group_id, str(target_id)
                )
                if target_profile and target_profile.summary:
                    target_injection = format_profile_for_injection(
                        target_profile, config.inject_max_chars
                    )
                    if target_injection:
                        injections.append(target_injection)
                        if debug_mode:
                            logger.info(
                                "[AIC] Injecting mentioned profile "
                                f"target={target_id}, group={group_id}:\n{target_injection}"
                            )

    if injections:
        speaker_name = event.get_sender_name() or user_id
        guard = build_speaker_guard(speaker_name, user_id)
        request.system_prompt = (
            (request.system_prompt or "")
            + "\n\n"
            + guard
            + "\n\n"
            + "\n\n".join(injections)
        )

    if needs_tool:
        request.system_prompt = (request.system_prompt or "") + "\n\n" + build_tool_guidance()
