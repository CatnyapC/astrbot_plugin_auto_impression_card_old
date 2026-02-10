from __future__ import annotations

import asyncio
import time

from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .storage import ImpressionStore
from .utils import last_token, token_count


async def learn_aliases(
    event: AiocqhttpMessageEvent,
    store: ImpressionStore,
    group_id: str,
    speaker_id: str,
) -> None:
    candidates = extract_alias_candidates(event)
    if not candidates:
        return
    now = int(time.time())
    for alias, target_id, confidence in candidates:
        await asyncio.to_thread(
            store.upsert_alias,
            group_id,
            speaker_id,
            alias,
            target_id,
            confidence,
            now,
        )


async def resolve_alias(
    store: ImpressionStore, group_id: str, speaker_id: str, alias: str
) -> str | None:
    candidates = await asyncio.to_thread(
        store.find_alias_targets, group_id, speaker_id, alias
    )
    if not candidates:
        return None
    if len(candidates) == 1:
        return str(candidates[0]["target_id"])
    return None


def extract_target_id_from_mentions(event: AiocqhttpMessageEvent) -> str | None:
    for comp in event.get_messages():
        if isinstance(comp, At) and str(comp.qq) != str(event.get_self_id()):
            return str(comp.qq)
    return None


def extract_reply_target_id(event: AiocqhttpMessageEvent) -> str | None:
    for comp in event.get_messages():
        if isinstance(comp, Reply) and comp.sender_id is not None:
            return str(comp.sender_id)
    return None


def extract_alias_candidates(event: AiocqhttpMessageEvent):
    components = event.get_messages()
    candidates: list[tuple[str, str, float]] = []
    buffer = ""

    for comp in components:
        if isinstance(comp, Plain):
            buffer += comp.text
        elif isinstance(comp, At):
            alias = last_token(buffer)
            if alias:
                candidates.append((alias, str(comp.qq), 0.9))
            buffer = ""
        elif isinstance(comp, Reply):
            buffer += " "

    if not candidates:
        reply_target = extract_reply_target_id(event)
        if reply_target:
            alias = last_token(buffer)
            if alias and token_count(buffer) <= 2:
                candidates.append((alias, reply_target, 0.7))

    return candidates
