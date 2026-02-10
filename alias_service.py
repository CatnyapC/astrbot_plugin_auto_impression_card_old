from __future__ import annotations

import asyncio
import re
import time

from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .storage import ImpressionStore
from .utils import last_token, token_count

_ALIAS_MIN_LEN = 2
_ALIAS_MAX_LEN = 8
_ALIAS_MAX_BUFFER_LEN = 12
_ALIAS_MAX_TOKENS = 2
_ALIAS_CORE_RE = re.compile(r"^[\w\u4e00-\u9fff]+$")
_ALIAS_PUNCT_RE = re.compile(r"[，。！？,!.?;；:：]+$")


def _strip_trailing_punct(text: str) -> str:
    return _ALIAS_PUNCT_RE.sub("", text.strip())


def _is_plausible_alias(alias: str, buffer_text: str, *, strict: bool) -> bool:
    alias = alias.strip()
    if not alias:
        return False
    if len(alias) < _ALIAS_MIN_LEN or len(alias) > _ALIAS_MAX_LEN:
        return False
    if not _ALIAS_CORE_RE.fullmatch(alias):
        return False
    buffer_text = buffer_text.strip()
    if not buffer_text:
        return False
    if len(buffer_text) > _ALIAS_MAX_BUFFER_LEN:
        return False
    if token_count(buffer_text) > _ALIAS_MAX_TOKENS:
        return False

    buffer_core = _strip_trailing_punct(buffer_text)
    if strict:
        return buffer_core == alias
    return buffer_core.endswith(alias)


async def learn_aliases(
    event: AiocqhttpMessageEvent,
    store: ImpressionStore,
    group_id: str,
    speaker_id: str,
) -> None:
    candidates = extract_alias_candidates(event)
    if not candidates:
        return
    evidence_text = event.get_message_str() or ""
    now = int(time.time())
    nickname_map = await asyncio.to_thread(
        store.get_nickname_map,
        group_id,
        {speaker_id, *[target_id for _, target_id, _ in candidates]},
    )
    for alias, target_id, confidence in candidates:
        await asyncio.to_thread(
            store.upsert_alias,
            group_id,
            speaker_id,
            alias,
            target_id,
            confidence,
            nickname_map.get(speaker_id),
            nickname_map.get(target_id),
            evidence_text,
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
            if alias and _is_plausible_alias(alias, buffer, strict=False):
                candidates.append((alias, str(comp.qq), 0.9))
            buffer = ""
        elif isinstance(comp, Reply):
            buffer += " "

    if not candidates:
        reply_target = extract_reply_target_id(event)
        if reply_target:
            alias = last_token(buffer)
            if alias and _is_plausible_alias(alias, buffer, strict=True):
                candidates.append((alias, reply_target, 0.7))

    return candidates
