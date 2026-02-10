from __future__ import annotations

import asyncio
import json
import re
import time

from astrbot.api import logger
from astrbot.core.exceptions import ProviderNotFoundError
from .prompts import ALIAS_ANALYSIS_SYSTEM_PROMPT
from .storage import ImpressionStore
from .update_service import force_update
from .utils import extract_json

MAX_ALIAS_PENDING_MESSAGES = 200
MAX_ALIAS_RESULTS = 100
MAX_ALIASES_PER_PAIR = 4
ALIAS_RE = re.compile(r"^[\w\u4e00-\u9fff]{2,8}$")


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
                store.get_pending_messages_by_group,
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
            pairs: set[tuple[str, str]] = set()
            for item in aliases[:MAX_ALIAS_RESULTS]:
                speaker_id = item["speaker_id"]
                target_id = item["target_id"]
                await asyncio.to_thread(
                    store.upsert_alias,
                    group_id,
                    speaker_id,
                    item["alias"],
                    target_id,
                    item["confidence"],
                    now,
                )
                pairs.add((speaker_id, target_id))
            for speaker_id, target_id in pairs:
                await asyncio.to_thread(
                    store.prune_aliases,
                    group_id,
                    speaker_id,
                    target_id,
                    MAX_ALIASES_PER_PAIR,
                )

            await _force_updates_for_alias_targets(
                context,
                store,
                config,
                debug_log,
                update_locks,
                group_id,
                umo,
                {item["target_id"] for item in aliases},
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Alias analysis failed: {exc}")
        finally:
            active_updates.discard(key)


async def force_alias_analysis(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    update_locks: dict[str, asyncio.Lock],
    group_id: str,
    umo: str,
) -> bool:
    key = f"alias:{group_id}"
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        try:
            pending = await asyncio.to_thread(
                store.get_pending_messages_by_group,
                group_id,
                MAX_ALIAS_PENDING_MESSAGES,
            )
            if not pending:
                return False

            try:
                provider_id = (
                    config.alias_provider_id
                    or await context.get_current_chat_provider_id(umo=umo)
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                return False

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
                return False
            except Exception as exc:  # noqa: BLE001
                logger.error(f"LLM alias analysis call failed: {exc}")
                return False

            raw_text = resp.completion_text or ""
            debug_log("[AIC] Alias analysis raw response:\n" + raw_text)
            aliases, ok = parse_alias_json(raw_text)
            if not ok:
                logger.warning(
                    "LLM alias analysis returned invalid JSON, keeping pending messages"
                )
                return False

            now = int(time.time())
            pairs: set[tuple[str, str]] = set()
            for item in aliases[:MAX_ALIAS_RESULTS]:
                speaker_id = item["speaker_id"]
                target_id = item["target_id"]
                await asyncio.to_thread(
                    store.upsert_alias,
                    group_id,
                    speaker_id,
                    item["alias"],
                    target_id,
                    item["confidence"],
                    now,
                )
                pairs.add((speaker_id, target_id))
            for speaker_id, target_id in pairs:
                await asyncio.to_thread(
                    store.prune_aliases,
                    group_id,
                    speaker_id,
                    target_id,
                    MAX_ALIASES_PER_PAIR,
                )

            await _force_updates_for_alias_targets(
                context,
                store,
                config,
                debug_log,
                update_locks,
                group_id,
                umo,
                {item["target_id"] for item in aliases},
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Alias analysis failed: {exc}")
            return False


async def _force_updates_for_alias_targets(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    update_locks: dict[str, asyncio.Lock],
    group_id: str,
    umo: str,
    target_ids: set[str],
) -> None:
    for target_id in target_ids:
        pending_count = await asyncio.to_thread(
            store.get_pending_count, group_id, target_id
        )
        if pending_count <= 0:
            continue
        profile = await asyncio.to_thread(store.get_profile, group_id, target_id)
        nickname = profile.nickname if profile and profile.nickname else target_id
        await force_update(
            context,
            store,
            config,
            debug_log,
            update_locks,
            umo,
            group_id,
            target_id,
            nickname,
            clear_old=False,
        )


def build_alias_prompt(pending) -> str:
    lines = [
        "Messages:",
    ]
    for idx, msg in enumerate(pending, 1):
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
        lines.append(f"{idx}. [{ts_text}] speaker={msg.user_id} text={msg.message}")
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
