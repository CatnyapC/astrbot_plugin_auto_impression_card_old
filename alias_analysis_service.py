from __future__ import annotations

import asyncio
import json
import re
import time

from astrbot.api import logger
from astrbot.core.exceptions import ProviderNotFoundError
from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .prompts import ALIAS_ANALYSIS_SYSTEM_PROMPT
from .storage import ImpressionStore
from .utils import extract_json

MAX_ALIAS_PENDING_MESSAGES = 200
MAX_ALIAS_RESULTS = 100
ALIAS_RE = re.compile(r"^[\w\u4e00-\u9fff]{2,8}$")


def build_alias_message_for_queue(event: AiocqhttpMessageEvent) -> str:
    self_id = str(event.get_self_id())
    parts: list[str] = []
    has_non_self_target = False
    reply_to_self = False

    for comp in event.get_messages():
        if isinstance(comp, Plain):
            text = comp.text.strip()
            if text:
                parts.append(text)
        elif isinstance(comp, At):
            parts.append(f"@{comp.qq}")
            if str(comp.qq) != self_id:
                has_non_self_target = True
        elif isinstance(comp, Reply):
            if comp.sender_id is None:
                continue
            parts.append(f"[reply_to:{comp.sender_id}]")
            if str(comp.sender_id) == self_id:
                reply_to_self = True
            else:
                has_non_self_target = True

    message = " ".join(parts).strip()
    if not message:
        return ""
    if reply_to_self:
        return ""
    if not has_non_self_target:
        return ""
    if message.startswith("/") or message.startswith("／"):
        return ""
    return message


async def maybe_schedule_alias_analysis(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    active_updates: set[str],
    update_locks: dict[str, asyncio.Lock],
    group_id: str,
    umo: str,
) -> None:
    key = f"alias:{group_id}"
    if key in active_updates:
        return
    active_updates.add(key)
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        try:
            pending = await asyncio.to_thread(
                store.get_alias_messages,
                group_id,
                MAX_ALIAS_PENDING_MESSAGES,
            )
            if not pending:
                return
            if len(pending) < config.alias_analysis_batch_size:
                return

            try:
                provider_id = (
                    config.alias_provider_id
                    or await context.get_current_chat_provider_id(umo=umo)
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                return

            prompt = build_alias_prompt(pending)
            debug_log("[AIC] Alias analysis prompt:\n" + prompt)
            try:
                resp = await context.llm_generate(
                    chat_provider_id=provider_id,
                    system_prompt=ALIAS_ANALYSIS_SYSTEM_PROMPT,
                    prompt=prompt,
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"Provider not found for alias analysis: {exc}")
                return
            except Exception as exc:  # noqa: BLE001
                logger.error(f"LLM alias analysis call failed: {exc}")
                return

            raw_text = resp.completion_text or ""
            debug_log("[AIC] Alias analysis raw response:\n" + raw_text)
            aliases, ok = parse_alias_json(raw_text)
            if not ok:
                logger.warning(
                    "LLM alias analysis returned invalid JSON, keeping pending messages"
                )
                return

            now = int(time.time())
            for item in aliases[:MAX_ALIAS_RESULTS]:
                await asyncio.to_thread(
                    store.upsert_alias,
                    group_id,
                    item["speaker_id"],
                    item["alias"],
                    item["target_id"],
                    item["confidence"],
                    now,
                )

            await asyncio.to_thread(
                store.delete_alias_messages, [p.id for p in pending]
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Alias analysis failed: {exc}")
        finally:
            active_updates.discard(key)


def build_alias_prompt(pending) -> str:
    lines = [
        "Messages:",
    ]
    for idx, msg in enumerate(pending, 1):
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
        lines.append(
            f"{idx}. [{ts_text}] speaker={msg.speaker_id} text={msg.message}"
        )
    return "\n".join(lines)


def parse_alias_json(text: str) -> tuple[list[dict], bool]:
    raw = extract_json(text or "")
    if not raw:
        return [], False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return [], False
    items = data.get("aliases", [])
    if not isinstance(items, list):
        return [], False
    results: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        speaker_id = str(item.get("speaker_id", "")).strip()
        target_id = str(item.get("target_id", "")).strip()
        alias = str(item.get("alias", "")).strip()
        if not speaker_id or not target_id or not alias:
            continue
        if not _is_clean_alias(alias):
            continue
        confidence = _normalize_confidence(item.get("confidence"))
        results.append(
            {
                "speaker_id": speaker_id,
                "target_id": target_id,
                "alias": alias,
                "confidence": confidence,
            }
        )
    return results, True


def _is_clean_alias(alias: str) -> bool:
    if not ALIAS_RE.fullmatch(alias):
        return False
    return True


def _normalize_confidence(value) -> float:
    try:
        conf = float(value)
    except (TypeError, ValueError):
        return 0.8
    if conf < 0.5:
        return 0.5
    if conf > 0.95:
        return 0.95
    return conf
