from __future__ import annotations

import asyncio
import json
import math
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
            start_ts = time.time()
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
            debug_log(f"[AIC] Alias analysis duration: {time.time() - start_ts:.2f}s")
            debug_log("[AIC] Alias analysis raw response:\n" + raw_text)
            aliases, ok = parse_alias_json(raw_text)
            if not ok:
                logger.warning(
                    "LLM alias analysis returned invalid JSON, keeping pending messages"
                )
                return

            now = int(time.time())
            nickname_map = await asyncio.to_thread(
                store.get_nickname_map,
                group_id,
                {item["speaker_id"] for item in aliases}
                | {item["target_id"] for item in aliases},
            )
            pending_by_id = {msg.id: msg for msg in pending}
            pairs: set[tuple[str, str]] = set()
            for item in aliases[:MAX_ALIAS_RESULTS]:
                speaker_id = item["speaker_id"]
                target_id = item["target_id"]
                evidence_records = _build_alias_evidence_records(
                    group_id,
                    target_id,
                    speaker_id,
                    item,
                    pending_by_id,
                    now,
                )
                if evidence_records:
                    await asyncio.to_thread(store.insert_evidence, evidence_records)
                    await asyncio.to_thread(
                        store.prune_evidence_by_speaker,
                        group_id,
                        target_id,
                        "alias",
                        f"alias:{item['alias']}",
                        speaker_id,
                        MAX_ALIASES_PER_PAIR,
                    )
                alias_conf = _recompute_alias_confidence(
                    store,
                    group_id,
                    target_id,
                    speaker_id,
                    item["alias"],
                    config.evidence_half_life_days,
                )
                await asyncio.to_thread(
                    store.upsert_alias,
                    group_id,
                    speaker_id,
                    item["alias"],
                    target_id,
                    alias_conf,
                    nickname_map.get(speaker_id),
                    nickname_map.get(target_id),
                    "",
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
            start_ts = time.time()
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
            debug_log(f"[AIC] Alias analysis duration: {time.time() - start_ts:.2f}s")
            debug_log("[AIC] Alias analysis raw response:\n" + raw_text)
            aliases, ok = parse_alias_json(raw_text)
            if not ok:
                logger.warning(
                    "LLM alias analysis returned invalid JSON, keeping pending messages"
                )
                return False

            now = int(time.time())
            nickname_map = await asyncio.to_thread(
                store.get_nickname_map,
                group_id,
                {item["speaker_id"] for item in aliases}
                | {item["target_id"] for item in aliases},
            )
            pending_by_id = {msg.id: msg for msg in pending}
            pairs: set[tuple[str, str]] = set()
            for item in aliases[:MAX_ALIAS_RESULTS]:
                speaker_id = item["speaker_id"]
                target_id = item["target_id"]
                evidence_records = _build_alias_evidence_records(
                    group_id,
                    target_id,
                    speaker_id,
                    item,
                    pending_by_id,
                    now,
                )
                if evidence_records:
                    await asyncio.to_thread(store.insert_evidence, evidence_records)
                    await asyncio.to_thread(
                        store.prune_evidence_by_speaker,
                        group_id,
                        target_id,
                        "alias",
                        f"alias:{item['alias']}",
                        speaker_id,
                        MAX_ALIASES_PER_PAIR,
                    )
                alias_conf = _recompute_alias_confidence(
                    store,
                    group_id,
                    target_id,
                    speaker_id,
                    item["alias"],
                    config.evidence_half_life_days,
                )
                await asyncio.to_thread(
                    store.upsert_alias,
                    group_id,
                    speaker_id,
                    item["alias"],
                    target_id,
                    alias_conf,
                    nickname_map.get(speaker_id),
                    nickname_map.get(target_id),
                    "",
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
        lines.append(
            f"{msg.id}. [{ts_text}] speaker={msg.user_id} text={msg.message}"
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
        evidence_ids = item.get("evidence_ids") if isinstance(item.get("evidence_ids"), list) else []
        evidence_confidences = (
            item.get("evidence_confidences")
            if isinstance(item.get("evidence_confidences"), list)
            else []
        )
        joke_likelihoods = (
            item.get("joke_likelihoods")
            if isinstance(item.get("joke_likelihoods"), list)
            else []
        )
        source_types = (
            item.get("source_types")
            if isinstance(item.get("source_types"), list)
            else []
        )
        results.append(
            {
                "speaker_id": speaker_id,
                "target_id": target_id,
                "alias": alias,
                "confidence": confidence,
                "evidence_ids": evidence_ids,
                "evidence_confidences": evidence_confidences,
                "joke_likelihoods": joke_likelihoods,
                "source_types": source_types,
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


def _build_alias_evidence_records(
    group_id: str,
    target_id: str,
    speaker_id: str,
    item: dict,
    pending_by_id: dict[int, object],
    created_at: int,
) -> list[tuple]:
    evidence_ids = item.get("evidence_ids", [])
    evidence_confidences = item.get("evidence_confidences", [])
    joke_likelihoods = item.get("joke_likelihoods", [])
    source_types = item.get("source_types", [])
    records: list[tuple] = []
    for idx, ev in enumerate(evidence_ids):
        try:
            ev_id = int(ev)
        except (TypeError, ValueError):
            continue
        msg = pending_by_id.get(ev_id)
        if not msg:
            continue
        evidence_conf = (
            float(evidence_confidences[idx])
            if idx < len(evidence_confidences)
            else 0.6
        )
        joke_lik = float(joke_likelihoods[idx]) if idx < len(joke_likelihoods) else 0.2
        source_type = (
            str(source_types[idx]).strip().lower()
            if idx < len(source_types)
            else "other"
        )
        records.append(
            (
                group_id,
                target_id,
                "alias",
                f"alias:{item['alias']}",
                msg.id,
                str(speaker_id),
                msg.message,
                msg.ts,
                evidence_conf,
                joke_lik,
                source_type if source_type in {"self", "other"} else "other",
                None,
                created_at,
            )
        )
    return records


def _recompute_alias_confidence(
    store: ImpressionStore,
    group_id: str,
    target_id: str,
    speaker_id: str,
    alias: str,
    half_life_days: float,
) -> float:
    rows = store.get_evidence_for_item_and_speaker(
        group_id, target_id, "alias", f"alias:{alias}", speaker_id
    )
    if not rows:
        return 0.0
    half_life_sec = max(0.0, half_life_days) * 86400.0
    now = time.time()
    prod = 1.0
    for row in rows:
        evidence_conf = float(row.get("evidence_confidence") or 0.6)
        joke_lik = float(row.get("joke_likelihood") or 0.2)
        source_type = row.get("source_type") or "other"
        source_weight = 1.0 if source_type == "self" else 0.7
        trust = store.get_user_trust(group_id, speaker_id)
        signal = evidence_conf * (1 - joke_lik) * source_weight * trust
        if half_life_sec > 0:
            delta = max(0.0, now - float(row.get("message_ts") or now))
            decay = math.exp(-delta / half_life_sec)
        else:
            decay = 1.0
        signal = max(0.0, min(1.0, signal * decay))
        prod *= 1 - signal
    return max(0.0, min(1.0, 1 - prod))
