from __future__ import annotations

import asyncio
import math
import time

from astrbot.api import logger
from astrbot.core.exceptions import ProviderNotFoundError

from .prompts import (
    GROUP_ATTRIBUTION_SYSTEM_PROMPT,
    PHASE1_CANDIDATE_SYSTEM_PROMPT,
    PHASE2_MERGE_SYSTEM_PROMPT,
    PHASE3_SUMMARY_SYSTEM_PROMPT,
)
from .storage import ImpressionStore, ProfileRecord
from .utils import (
    extract_target_ids_from_raw_text,
    parse_attribution_json,
    parse_phase1_candidates,
    parse_phase2_merge,
    parse_phase3_summaries,
)
import re

MAX_EVIDENCE_PER_ITEM = 3


async def maybe_schedule_group_update(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    active_updates: set[str],
    update_locks: dict[str, asyncio.Lock],
    group_id: str,
    umo: str,
) -> None:
    key = f"group:{group_id}"
    if key in active_updates:
        return
    active_updates.add(key)
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        try:
            max_batch_messages = max(1, config.update_msg_threshold)
            pending = await asyncio.to_thread(
                store.get_pending_messages_by_group,
                group_id,
                max_batch_messages,
            )
            if not pending:
                return
            group_last_update = await asyncio.to_thread(
                store.get_group_last_update, group_id
            )
            now = int(time.time())
            if len(pending) < config.update_msg_threshold:
                if group_last_update == 0:
                    return
                if now - group_last_update < config.update_time_threshold_sec:
                    return
            debug_log(
                "[AIC] Group update trigger: "
                f"group={group_id} pending={len(pending)} "
                f"threshold={config.update_msg_threshold} "
                f"group_last_update={group_last_update} "
                f"time_threshold={config.update_time_threshold_sec}"
            )

            recent_profiles = await asyncio.to_thread(
                store.get_recent_profiles_by_group,
                group_id,
                config.group_batch_known_users_max,
            )
            nickname_by_user = {
                p.user_id: (p.nickname or "").strip()
                for p in recent_profiles
                if p.nickname
            }
            nickname_to_user: dict[str, str] = {}
            if config.group_batch_enable_nickname_match:
                for user_id, nickname in nickname_by_user.items():
                    if nickname and nickname not in nickname_to_user:
                        nickname_to_user[nickname] = user_id

            alias_index = await asyncio.to_thread(store.get_alias_index, group_id)

            attribution_map: dict[int, list[str]] = {}
            if config.group_batch_enable_semantic_attribution:
                attribution_prompt = build_group_attribution_prompt(
                    pending[
                        : min(
                            config.group_batch_attribution_max_messages,
                            max_batch_messages,
                        )
                    ],
                    recent_profiles,
                    config.group_batch_attribution_include_summary,
                )
                start_ts = time.time()
                debug_log("[AIC] Group attribution prompt:\n" + attribution_prompt)
                provider_id = await _get_provider_id(
                    context, config, umo, config.attribution_provider_id
                )
                if provider_id:
                    try:
                        resp = await context.llm_generate(
                            chat_provider_id=provider_id,
                            system_prompt=GROUP_ATTRIBUTION_SYSTEM_PROMPT,
                            prompt=attribution_prompt,
                        )
                        raw_text = resp.completion_text or ""
                        debug_log(
                            f"[AIC] Group attribution duration: {time.time() - start_ts:.2f}s"
                        )
                        debug_log("[AIC] Group attribution raw response:\n" + raw_text)
                        attribution_map, _ = parse_attribution_json(
                            raw_text, {p.user_id for p in recent_profiles}
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(f"LLM group attribution failed: {exc}")

            pending_by_user = _build_pending_by_user(
                pending,
                attribution_map,
                nickname_to_user,
                alias_index,
                config.bot_user_id,
                set(config.bot_aliases),
            )
            if not pending_by_user:
                return

            eligible_users = await _select_eligible_users(
                store,
                group_id,
                pending_by_user,
                config,
                debug_log,
            )
            if not eligible_users:
                return

            pending_by_user = {
                user_id: msgs
                for user_id, msgs in pending_by_user.items()
                if user_id in eligible_users
            }
            if not pending_by_user:
                return

            profiles_by_user = {
                user_id: await asyncio.to_thread(store.get_profile, user_id)
                for user_id in pending_by_user.keys()
            }

            ok = await _run_phase_updates(
                context,
                store,
                config,
                debug_log,
                group_id,
                pending_by_user,
                profiles_by_user,
                umo,
                clear_old_user_ids=set(),
            )
            if ok:
                await asyncio.to_thread(
                    store.set_group_last_update, group_id, int(time.time())
                )
                delete_ids = [msg.id for msgs in pending_by_user.values() for msg in msgs]
                if delete_ids:
                    await asyncio.to_thread(store.delete_pending_messages, delete_ids)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Group impression update failed: {exc}")
        finally:
            active_updates.discard(key)


async def force_group_update(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    update_locks: dict[str, asyncio.Lock],
    umo: str,
    group_id: str,
) -> bool:
    key = f"group:{group_id}"
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        max_batch_messages = max(1, config.update_msg_threshold)
        pending = await asyncio.to_thread(
            store.get_pending_messages_by_group,
            group_id,
            max_batch_messages,
        )
        if not pending:
            return False

        recent_profiles = await asyncio.to_thread(
            store.get_recent_profiles_by_group,
            group_id,
            config.group_batch_known_users_max,
        )
        nickname_by_user = {
            p.user_id: (p.nickname or "").strip()
            for p in recent_profiles
            if p.nickname
        }
        nickname_to_user: dict[str, str] = {}
        if config.group_batch_enable_nickname_match:
            for user_id, nickname in nickname_by_user.items():
                if nickname and nickname not in nickname_to_user:
                    nickname_to_user[nickname] = user_id
        alias_index = await asyncio.to_thread(store.get_alias_index, group_id)

        attribution_map: dict[int, list[str]] = {}
        if config.group_batch_enable_semantic_attribution:
            attribution_prompt = build_group_attribution_prompt(
                pending[
                    : min(
                        config.group_batch_attribution_max_messages,
                        max_batch_messages,
                    )
                ],
                recent_profiles,
                config.group_batch_attribution_include_summary,
            )
            start_ts = time.time()
            debug_log("[AIC] Group attribution prompt:\n" + attribution_prompt)
            provider_id = await _get_provider_id(
                context, config, umo, config.attribution_provider_id
            )
            if provider_id:
                try:
                    resp = await context.llm_generate(
                        chat_provider_id=provider_id,
                        system_prompt=GROUP_ATTRIBUTION_SYSTEM_PROMPT,
                        prompt=attribution_prompt,
                    )
                    raw_text = resp.completion_text or ""
                    debug_log(
                        f"[AIC] Group attribution duration: {time.time() - start_ts:.2f}s"
                    )
                    debug_log("[AIC] Group attribution raw response:\n" + raw_text)
                    attribution_map, _ = parse_attribution_json(
                        raw_text, {p.user_id for p in recent_profiles}
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"LLM group attribution failed: {exc}")

        pending_by_user = _build_pending_by_user(
            pending,
            attribution_map,
            nickname_to_user,
            alias_index,
            config.bot_user_id,
            set(config.bot_aliases),
        )
        if not pending_by_user:
            return False

        profiles_by_user = {
            user_id: await asyncio.to_thread(store.get_profile, user_id)
            for user_id in pending_by_user.keys()
        }

        ok = await _run_phase_updates(
            context,
            store,
            config,
            debug_log,
            group_id,
            pending_by_user,
            profiles_by_user,
            umo,
            clear_old_user_ids=set(),
        )
        if ok:
            await asyncio.to_thread(
                store.set_group_last_update, group_id, int(time.time())
            )
            delete_ids = [msg.id for msgs in pending_by_user.values() for msg in msgs]
            if delete_ids:
                await asyncio.to_thread(store.delete_pending_messages, delete_ids)
        return ok


def _build_pending_by_user(
    pending,
    attribution_map,
    nickname_to_user,
    alias_index: dict[str, dict[str, list[str]]],
    bot_user_id: str,
    bot_aliases: set[str],
) -> dict[str, list]:
    pending_by_user: dict[str, list] = {}
    for msg in pending:
        targets = list(extract_target_ids_from_raw_text(msg.message))

        if not targets:
            bot_targets = _resolve_bot_alias_targets(msg.message, bot_user_id, bot_aliases)
            targets.extend(bot_targets)

        if not targets:
            alias_targets = _resolve_alias_targets(
                msg.user_id, msg.message, alias_index
            )
            targets.extend(alias_targets)

        if not targets and nickname_to_user:
            nickname_targets = _resolve_nickname_targets(msg.message, nickname_to_user)
            targets.extend(nickname_targets)

        if not targets:
            targets = attribution_map.get(msg.id, [])

        if targets:
            for target_id in targets:
                pending_by_user.setdefault(target_id, []).append(msg)
        else:
            pending_by_user.setdefault(msg.user_id, []).append(msg)
    return pending_by_user


def _resolve_bot_alias_targets(
    message: str, bot_user_id: str, bot_aliases: set[str]
) -> list[str]:
    if not bot_user_id or not bot_aliases:
        return []
    tokens = _extract_tokens(message)
    for token in tokens:
        if token in bot_aliases:
            return [bot_user_id]
    return []


def _resolve_alias_targets(
    speaker_id: str,
    message: str,
    alias_index: dict[str, dict[str, list[str]]],
) -> list[str]:
    speaker_map = alias_index.get(str(speaker_id), {})
    if not speaker_map:
        return []
    tokens = _extract_tokens(message)
    targets: list[str] = []
    for token in tokens:
        if token in speaker_map:
            for target_id in speaker_map[token]:
                if target_id not in targets:
                    targets.append(target_id)
    return targets


def _resolve_nickname_targets(
    message: str, nickname_to_user: dict[str, str]
) -> list[str]:
    tokens = _extract_tokens(message)
    targets: list[str] = []
    for token in tokens:
        target_id = nickname_to_user.get(token)
        if target_id and target_id not in targets:
            targets.append(target_id)
    return targets


def _extract_tokens(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r"[\\w\\u4e00-\\u9fff]{2,16}", text)


async def _select_eligible_users(
    store: ImpressionStore,
    group_id: str,
    pending_by_user: dict[str, list],
    config,
    debug_log,
) -> list[str]:
    eligible: list[str] = []
    now = int(time.time())
    group_last_update = await asyncio.to_thread(store.get_group_last_update, group_id)
    for user_id, msgs in pending_by_user.items():
        pending_count = len(msgs)
        if pending_count >= config.update_msg_threshold:
            debug_log(
                f"[AIC] Auto update threshold reached for {group_id}:{user_id} "
                f"(pending={pending_count}, threshold={config.update_msg_threshold})"
            )
            eligible.append(user_id)
            continue
        if group_last_update == 0:
            continue
        if now - group_last_update >= config.update_time_threshold_sec:
            eligible.append(user_id)
    return eligible


async def _run_phase_updates(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    group_id: str,
    pending_by_user: dict[str, list],
    profiles_by_user: dict[str, ProfileRecord | None],
    umo: str,
    clear_old_user_ids: set[str],
) -> bool:
    if not pending_by_user:
        return False

    provider_id = await _get_provider_id(
        context, config, umo, config.phase1_provider_id
    )
    if not provider_id:
        return False

    known_user_ids = set(pending_by_user.keys())
    nickname_by_user = {
        user_id: (profiles_by_user.get(user_id).nickname or "")
        if profiles_by_user.get(user_id)
        else ""
        for user_id in known_user_ids
    }

    phase1_prompt = build_phase1_prompt(pending_by_user, known_user_ids, nickname_by_user)
    phase1_start = time.time()
    debug_log("[AIC] Phase1 prompt:\n" + phase1_prompt)
    try:
            resp = await context.llm_generate(
                chat_provider_id=provider_id,
                system_prompt=PHASE1_CANDIDATE_SYSTEM_PROMPT,
                prompt=phase1_prompt,
            )
    except Exception as exc:  # noqa: BLE001
        logger.error(f"LLM phase1 call failed: {exc}")
        return False
    raw_text = resp.completion_text or ""
    debug_log(f"[AIC] Phase1 duration: {time.time() - phase1_start:.2f}s")
    debug_log("[AIC] Phase1 raw response:\n" + raw_text)
    phase1_data, ok = parse_phase1_candidates(raw_text, known_user_ids)
    if not ok:
        logger.warning("LLM phase1 returned invalid JSON")
        return False

    candidate_by_user = _normalize_phase1_candidates(phase1_data)
    if not candidate_by_user:
        return True

    existing_by_user = {}
    users_for_merge = set()
    for user_id in known_user_ids:
        profile = profiles_by_user.get(user_id)
        existing_impressions = (
            profile.impressions if profile and user_id not in clear_old_user_ids else []
        )
        existing_by_user[user_id] = {
            "impressions": existing_impressions,
        }
        if existing_impressions:
            users_for_merge.add(user_id)

    final_by_user: dict[str, dict] = {}
    mapping_by_user: dict[str, dict] = {}
    consistency_by_user: dict[str, dict] = {}

    if users_for_merge:
        phase2_prompt = build_phase2_prompt(
            {uid: existing_by_user[uid] for uid in users_for_merge},
            {
                uid: candidate_by_user.get(uid, {"impressions": {}})
                for uid in users_for_merge
            },
        )
        phase2_start = time.time()
        debug_log("[AIC] Phase2 prompt:\n" + phase2_prompt)
        try:
            phase2_provider_id = await _get_provider_id(
                context, config, umo, config.phase2_provider_id
            )
            resp = await context.llm_generate(
                chat_provider_id=phase2_provider_id,
                system_prompt=PHASE2_MERGE_SYSTEM_PROMPT,
                prompt=phase2_prompt,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"LLM phase2 call failed: {exc}")
            return False
        raw_text = resp.completion_text or ""
        debug_log(f"[AIC] Phase2 duration: {time.time() - phase2_start:.2f}s")
        debug_log("[AIC] Phase2 raw response:\n" + raw_text)
        phase2_data, ok = parse_phase2_merge(raw_text, users_for_merge)
        if not ok:
            logger.warning("LLM phase2 returned invalid JSON")
            return False
        for user_id, payload in phase2_data.items():
            final_by_user[user_id] = {
                "impressions": payload.get("impressions", []),
            }
            mapping_by_user[user_id] = payload.get("mapping", {})
            consistency_by_user[user_id] = payload.get("consistency", {})

    for user_id in known_user_ids:
        if user_id in final_by_user:
            continue
        candidates = candidate_by_user.get(user_id, {"impressions": {}})
        final_impressions = list(candidates.get("impressions", {}).keys())
        final_by_user[user_id] = {"impressions": final_impressions}
        mapping_by_user[user_id] = {
            "impressions": {t: [t] for t in final_impressions},
        }
        consistency_by_user[user_id] = {
            "impressions": {t: "neutral" for t in final_impressions},
        }

    summaries: dict[str, str] = {}
    if _should_run_phase3(final_by_user, profiles_by_user, users_for_merge):
        summary_prompt = build_phase3_prompt(final_by_user, profiles_by_user)
        phase3_start = time.time()
        debug_log("[AIC] Phase3 prompt:\n" + summary_prompt)
        try:
            phase3_provider_id = await _get_provider_id(
                context, config, umo, config.phase3_provider_id
            )
            resp = await context.llm_generate(
                chat_provider_id=phase3_provider_id,
                system_prompt=PHASE3_SUMMARY_SYSTEM_PROMPT,
                prompt=summary_prompt,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"LLM phase3 call failed: {exc}")
            return False
        raw_text = resp.completion_text or ""
        debug_log(f"[AIC] Phase3 duration: {time.time() - phase3_start:.2f}s")
        debug_log("[AIC] Phase3 raw response:\n" + raw_text)
        summaries, ok = parse_phase3_summaries(raw_text, known_user_ids)
        if not ok:
            logger.warning("LLM phase3 returned invalid JSON")
            summaries = {}

    now = int(time.time())
    pending_by_id = {msg.id: msg for msgs in pending_by_user.values() for msg in msgs}
    trust_scores = {
        uid: await asyncio.to_thread(store.get_user_trust, group_id, uid)
        for uid in {msg.user_id for msg in pending_by_id.values()}
    }

    for user_id in known_user_ids:
        profile = profiles_by_user.get(user_id)
        nickname = profile.nickname if profile and profile.nickname else user_id
        last_seen = max(m.ts for m in pending_by_user[user_id])
        summary = summaries.get(user_id) or (profile.summary if profile else "") or ""
        final_impressions = final_by_user[user_id]["impressions"]
        evidence_records = build_evidence_records(
            group_id,
            user_id,
            final_impressions,
            mapping_by_user.get(user_id, {}),
            consistency_by_user.get(user_id, {}),
            candidate_by_user.get(user_id, {}),
            pending_by_id,
            now,
        )
        if evidence_records:
            await asyncio.to_thread(store.insert_evidence, evidence_records)
            _prune_evidence(store, group_id, user_id, final_impressions)
        impression_conf_map = _recompute_confidence_map(
            store, group_id, user_id, "impression", final_impressions, trust_scores, config
        )
        filtered_impressions = []
        for item in final_impressions:
            conf = impression_conf_map.get(item, 0.0)
            if conf >= config.impression_confidence_min:
                filtered_impressions.append(item)
            else:
                await asyncio.to_thread(
                    store.delete_evidence_for_item,
                    group_id,
                    user_id,
                    "impression",
                    item,
                )
                impression_conf_map.pop(item, None)
        final_impressions = filtered_impressions

        if impression_conf_map:
            order = {item: idx for idx, item in enumerate(final_impressions)}
            final_impressions.sort(
                key=lambda x: (-float(impression_conf_map.get(x, 0.0)), order.get(x, 0))
            )

        record = ProfileRecord(
            user_id=user_id,
            nickname=nickname,
            last_seen=last_seen,
            summary=summary,
            impressions=final_impressions,
            impressions_confidence=impression_conf_map,
            updated_at=now,
            version=(profile.version if profile else 1),
        )
        await asyncio.to_thread(
            store.upsert_profile_with_confidence,
            record,
            impression_conf_map,
        )

    return True


async def _get_provider_id(
    context, config, umo: str, provider_override: str | None = None
) -> str:
    try:
        return (
            (provider_override or "").strip()
            or config.update_provider_id
            or await context.get_current_chat_provider_id(umo=umo)
        )
    except ProviderNotFoundError as exc:
        logger.warning(f"No LLM provider configured: {exc}")
        return ""


def _normalize_phase1_candidates(raw: dict[str, dict[str, list[dict]]]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for user_id, payload in raw.items():
        impressions = _normalize_candidate_items(payload.get("impressions", []))
        results[user_id] = {"impressions": impressions}
    return results


def _normalize_candidate_items(items: list[dict]) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        evidence_ids = item.get("evidence_ids")
        evidence_confidences = item.get("evidence_confidences")
        joke_likelihoods = item.get("joke_likelihoods")
        source_types = item.get("source_types")
        if not isinstance(evidence_ids, list):
            evidence_ids = []
        if not isinstance(evidence_confidences, list):
            evidence_confidences = []
        if not isinstance(joke_likelihoods, list):
            joke_likelihoods = []
        if not isinstance(source_types, list):
            source_types = []
        signals: list[dict] = []
        for idx, ev in enumerate(evidence_ids):
            try:
                ev_id = int(ev)
            except (TypeError, ValueError):
                continue
            evidence_conf = (
                float(evidence_confidences[idx])
                if idx < len(evidence_confidences)
                else 0.6
            )
            joke_lik = (
                float(joke_likelihoods[idx]) if idx < len(joke_likelihoods) else 0.2
            )
            source_type = (
                str(source_types[idx]).strip().lower()
                if idx < len(source_types)
                else "other"
            )
            signals.append(
                {
                    "evidence_id": ev_id,
                    "evidence_confidence": _clamp(evidence_conf),
                    "joke_likelihood": _clamp(joke_lik),
                    "source_type": source_type if source_type in {"self", "other"} else "other",
                }
            )
        existing = results.get(text, [])
        for signal in signals:
            if signal["evidence_id"] not in {s["evidence_id"] for s in existing}:
                existing.append(signal)
        results[text] = existing
    return results


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _prune_evidence(
    store: ImpressionStore,
    group_id: str,
    user_id: str,
    impressions: list[str],
) -> None:
    for item_text in impressions:
        store.prune_evidence(
            group_id, user_id, "impression", item_text, MAX_EVIDENCE_PER_ITEM
        )


def build_evidence_records(
    group_id: str,
    user_id: str,
    final_impressions: list[str],
    mapping: dict,
    consistency: dict,
    candidates: dict,
    pending_by_id: dict[int, object],
    created_at: int,
) -> list[tuple]:
    records: list[tuple] = []

    mapping_block = mapping.get("impressions", {}) if isinstance(mapping, dict) else {}
    consistency_block = (
        consistency.get("impressions", {}) if isinstance(consistency, dict) else {}
    )
    candidate_items = candidates.get("impressions", {})
    for item_text in final_impressions:
        candidate_texts = []
        if isinstance(mapping_block, dict) and item_text in mapping_block:
            mapped = mapping_block.get(item_text)
            if isinstance(mapped, list):
                candidate_texts = [str(x) for x in mapped if str(x).strip()]
            elif isinstance(mapped, str):
                candidate_texts = [mapped]
        if not candidate_texts:
            candidate_texts = [item_text]

        evidence_msgs = []
        for candidate_text in candidate_texts:
            items = candidate_items.get(candidate_text, [])
            for item in items:
                ev_id = item.get("evidence_id")
                if ev_id is None:
                    continue
                msg = pending_by_id.get(ev_id)
                if not msg:
                    continue
                evidence_conf = float(item.get("evidence_confidence", 0.6))
                joke_lik = float(item.get("joke_likelihood", 0.2))
                source_type = item.get("source_type", "other")
                consistency_tag = (
                    consistency_block.get(item_text)
                    if isinstance(consistency_block, dict)
                    else None
                )
                evidence_msgs.append((msg, item, evidence_conf, joke_lik, source_type, consistency_tag))

        evidence_msgs.sort(key=lambda x: (x[0].ts, x[0].id), reverse=True)
        for msg, item, evidence_conf, joke_lik, source_type, consistency_tag in evidence_msgs[
            :MAX_EVIDENCE_PER_ITEM
        ]:
            records.append(
                (
                    group_id,
                    user_id,
                    "impression",
                    item_text,
                    msg.id,
                    str(msg.user_id),
                    msg.message,
                    msg.ts,
                    evidence_conf,
                    joke_lik,
                    source_type,
                    consistency_tag,
                    created_at,
                )
            )
    return records


def _recompute_confidence_map(
    store: ImpressionStore,
    group_id: str,
    user_id: str,
    item_type: str,
    items: list[str],
    trust_scores: dict[str, float],
    config,
) -> dict[str, float]:
    result: dict[str, float] = {}
    half_life_days = max(0.0, float(config.evidence_half_life_days))
    half_life_sec = half_life_days * 86400.0
    now = time.time()
    for item_text in items:
        evidence_rows = store.get_evidence_for_item(group_id, user_id, item_type, item_text)
        if not evidence_rows:
            result[item_text] = 0.0
            continue
        prod = 1.0
        for row in evidence_rows:
            evidence_conf = float(row.get("evidence_confidence") or 0.6)
            joke_lik = float(row.get("joke_likelihood") or 0.2)
            source_type = row.get("source_type") or "other"
            source_weight = 1.0 if source_type == "self" else 0.7
            speaker_id = str(row.get("speaker_id") or "")
            trust = trust_scores.get(speaker_id)
            if trust is None:
                trust = store.get_user_trust(group_id, speaker_id) if speaker_id else 0.7
            consistency_tag = row.get("consistency_tag")
            consistency_weight = 1.0
            if consistency_tag == "conflicting":
                consistency_weight = 0.4
            elif consistency_tag == "neutral":
                consistency_weight = 0.7
            signal = evidence_conf * (1 - joke_lik) * source_weight * trust * consistency_weight
            if half_life_sec > 0:
                delta = max(0.0, now - float(row.get("message_ts") or now))
                decay = math.exp(-delta / half_life_sec)
            else:
                decay = 1.0
            signal = _clamp(signal * decay)
            prod *= 1 - signal
        result[item_text] = _clamp(1 - prod)
    return result


def _should_run_phase3(
    final_by_user: dict[str, dict],
    profiles_by_user: dict[str, ProfileRecord | None],
    users_for_merge: set[str],
) -> bool:
    if not users_for_merge:
        return False
    for user_id in users_for_merge:
        items = final_by_user.get(user_id, {})
        profile = profiles_by_user.get(user_id)
        old_impressions = profile.impressions if profile else []
        if sorted(old_impressions) != sorted(items.get("impressions", [])):
            return True
    return False


def build_group_attribution_prompt(
    pending,
    recent_profiles: list[ProfileRecord],
    include_summary: bool,
) -> str:
    lines = [
        "Known user ids:",
    ]
    lines.append(", ".join([p.user_id for p in recent_profiles]))
    lines.extend(["", "Known users (id -> nickname):"])
    for profile in recent_profiles:
        if not profile.nickname:
            continue
        lines.append(f"{profile.user_id}: {profile.nickname}")
        if include_summary and profile.summary:
            summary = profile.summary.replace("\n", " ").strip()
            if len(summary) > 80:
                summary = summary[:80] + "..."
            lines.append(f"summary: {summary}")
    lines.extend(["", "Messages:"])
    for msg in pending:
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
        lines.append(
            f"{msg.id}. [{ts_text}] speaker_id={msg.user_id} text={msg.message}"
        )
    return "\n".join(lines).strip()


def build_phase1_prompt(
    pending_by_user: dict[str, list],
    known_user_ids: set[str],
    nickname_by_user: dict[str, str],
) -> str:
    lines = [
        "Known user ids:",
        ", ".join(sorted(known_user_ids)),
    ]
    if nickname_by_user:
        lines.extend(["", "Known users (id -> nickname):"])
        for user_id in sorted(known_user_ids):
            nickname = nickname_by_user.get(user_id, "")
            if nickname:
                lines.append(f"{user_id}: {nickname}")
    lines.extend(["", "Messages (grouped by target_id):"])
    for user_id, messages in pending_by_user.items():
        lines.append(f"target_id={user_id}")
        for msg in messages:
            ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
            lines.append(
                f"{msg.id}. [{ts_text}] speaker={msg.user_id} text={msg.message}"
            )
        lines.append("")
    return "\n".join(lines).strip()


def build_phase2_prompt(
    existing_by_user: dict[str, dict],
    candidates_by_user: dict[str, dict],
) -> str:
    lines = [
        "Existing impressions (JSON by user_id):",
        json_dumps(existing_by_user),
        "",
        "Candidate impressions (JSON by user_id):",
        json_dumps(
            {
                user_id: {
                    "impressions": list(payload.get("impressions", {}).keys()),
                }
                for user_id, payload in candidates_by_user.items()
            }
        ),
    ]
    return "\n".join(lines).strip()


def build_phase3_prompt(
    final_by_user: dict[str, dict],
    profiles_by_user: dict[str, ProfileRecord | None],
) -> str:
    payload = {}
    for user_id, items in final_by_user.items():
        profile = profiles_by_user.get(user_id)
        payload[user_id] = {
            "summary": profile.summary if profile and profile.summary else "",
            "impressions": items.get("impressions", []),
        }
    lines = [
        "Final impressions with existing summaries (JSON by user_id):",
        json_dumps(payload),
    ]
    return "\n".join(lines).strip()


def json_dumps(data: dict) -> str:
    import json

    return json.dumps(data, ensure_ascii=False)
