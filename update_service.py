from __future__ import annotations

import asyncio
import time

from astrbot.api import logger
from astrbot.core.exceptions import ProviderNotFoundError

from .prompts import (
    GROUP_ATTRIBUTION_SYSTEM_PROMPT,
    GROUP_PROFILE_UPDATE_SYSTEM_PROMPT,
    PROFILE_UPDATE_SYSTEM_PROMPT,
)
from .storage import ImpressionStore, ProfileRecord
from .utils import (
    extract_target_ids_from_raw_text,
    parse_attribution_json,
    parse_group_profile_json,
    parse_profile_json,
    plain_from_raw_text,
)

MAX_PENDING_MESSAGES = 200


async def maybe_schedule_update(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    active_updates: set[str],
    update_locks: dict[str, asyncio.Lock],
    group_id: str,
    user_id: str,
    nickname: str,
    umo: str,
    ) -> None:
    key = f"{group_id}:{user_id}"
    if key in active_updates:
        return
    active_updates.add(key)
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        try:
            pending = await asyncio.to_thread(
                store.get_pending_messages,
                group_id,
                user_id,
                MAX_PENDING_MESSAGES,
            )
            if not pending:
                return

            profile = await asyncio.to_thread(store.get_profile, group_id, user_id)
            last_update = profile.updated_at if profile else 0
            last_seen = max(p.ts for p in pending)
            pending_count = len(pending)

            if pending_count < config.update_msg_threshold:
                if last_seen - last_update < config.update_time_threshold_sec:
                    return

            existing = {
                "summary": profile.summary if profile else "",
                "traits": profile.traits if profile else [],
                "facts": profile.facts if profile else [],
                "examples": profile.examples if profile else [],
            }

            try:
                provider_id = (
                    config.update_provider_id
                    or await context.get_current_chat_provider_id(umo=umo)
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                return

            prompt = build_update_prompt(existing, pending)
            debug_log("[AIC] Update prompt:\n" + prompt)
            try:
                resp = await context.llm_generate(
                    chat_provider_id=provider_id,
                    system_prompt=PROFILE_UPDATE_SYSTEM_PROMPT,
                    prompt=prompt,
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"Provider not found for impression update: {exc}")
                return
            except Exception as exc:  # noqa: BLE001
                logger.error(f"LLM update call failed: {exc}")
                return

            debug_log("[AIC] Update raw response:\n" + (resp.completion_text or ""))
            data, ok = parse_profile_json(resp.completion_text or "", existing)
            if not ok:
                logger.warning(
                    "LLM update returned invalid JSON, keeping pending messages"
                )
                return

            updated_at = int(time.time())
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
            await asyncio.to_thread(store.upsert_profile, record)
            await asyncio.to_thread(
                store.delete_pending_messages, [p.id for p in pending]
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Impression update failed: {exc}")
        finally:
            active_updates.discard(key)


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

            stats: dict[str, dict[str, int]] = {}
            pending_by_id = {msg.id: msg for msg in pending}
            mentioned_targets: set[str] = set()
            for msg in pending:
                stat = stats.setdefault(msg.user_id, {"count": 0, "last_seen": 0})
                stat["count"] += 1
                if msg.ts > stat["last_seen"]:
                    stat["last_seen"] = msg.ts
                mentioned_targets.update(extract_target_ids_from_raw_text(msg.message))
                if nickname_to_user:
                    for nickname, target_id in nickname_to_user.items():
                        if target_id == msg.user_id:
                            continue
                        if nickname and nickname in msg.message:
                            mentioned_targets.add(target_id)

            eligible_users: list[str] = []
            profile_cache: dict[str, ProfileRecord | None] = {}

            attribution_map: dict[int, list[str]] = {}
            attributed_targets: set[str] = set()
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
                debug_log("[AIC] Group attribution prompt:\n" + attribution_prompt)
                try:
                    provider_id = (
                        config.update_provider_id
                        or await context.get_current_chat_provider_id(umo=umo)
                    )
                except ProviderNotFoundError as exc:
                    logger.warning(f"No LLM provider configured: {exc}")
                    provider_id = ""

                if provider_id:
                    try:
                        resp = await context.llm_generate(
                            chat_provider_id=provider_id,
                            system_prompt=GROUP_ATTRIBUTION_SYSTEM_PROMPT,
                            prompt=attribution_prompt,
                        )
                        raw_text = resp.completion_text or ""
                        debug_log("[AIC] Group attribution raw response:\n" + raw_text)
                        attribution_map, _ = parse_attribution_json(
                            raw_text, {p.user_id for p in recent_profiles}
                        )
                        for msg_id, targets in attribution_map.items():
                            msg = pending_by_id.get(msg_id)
                            if not msg:
                                continue
                            for target_id in targets:
                                attributed_targets.add(target_id)
                                stat = stats.setdefault(
                                    target_id, {"count": 0, "last_seen": 0}
                                )
                                stat["count"] += 1
                                if msg.ts > stat["last_seen"]:
                                    stat["last_seen"] = msg.ts
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(f"LLM group attribution failed: {exc}")

            for user_id, stat in stats.items():
                if stat["count"] >= config.update_msg_threshold:
                    eligible_users.append(user_id)
                    continue
                profile = await asyncio.to_thread(
                    store.get_profile, group_id, user_id
                )
                profile_cache[user_id] = profile
                last_update = profile.updated_at if profile else 0
                if stat["last_seen"] - last_update >= config.update_time_threshold_sec:
                    eligible_users.append(user_id)

            if attributed_targets:
                for user_id in attributed_targets:
                    if user_id not in eligible_users:
                        eligible_users.append(user_id)

            if not eligible_users:
                return

            pending_by_user: dict[str, list] = {uid: [] for uid in eligible_users}
            for msg in pending:
                targets = attribution_map.get(msg.id)
                if targets:
                    if config.group_batch_attribution_max_targets_per_message > 0:
                        targets = targets[
                            : config.group_batch_attribution_max_targets_per_message
                        ]
                    for target_id in targets:
                        if target_id not in pending_by_user:
                            continue
                        bucket = pending_by_user[target_id]
                        bucket.append(msg)
                    continue

                if msg.user_id not in pending_by_user:
                    continue
                bucket = pending_by_user[msg.user_id]
                bucket.append(msg)

            pending_by_user = {
                uid: msgs for uid, msgs in pending_by_user.items() if msgs
            }
            if not pending_by_user:
                return

            candidate_user_ids = (
                set(pending_by_user.keys())
                | mentioned_targets
                | set(nickname_by_user.keys())
            )
            existing_by_user: dict[str, dict] = {}
            profile_by_user: dict[str, ProfileRecord | None] = {}
            for user_id in candidate_user_ids:
                profile = profile_cache.get(user_id)
                if profile is None:
                    profile = await asyncio.to_thread(
                        store.get_profile, group_id, user_id
                    )
                profile_by_user[user_id] = profile
                existing_by_user[user_id] = {
                    "nickname": profile.nickname if profile else "",
                    "summary": profile.summary if profile else "",
                    "traits": profile.traits if profile else [],
                    "facts": profile.facts if profile else [],
                    "examples": profile.examples if profile else [],
                }

            try:
                provider_id = (
                    config.update_provider_id
                    or await context.get_current_chat_provider_id(umo=umo)
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                return

            prompt = build_group_update_prompt(
                existing_by_user,
                pending_by_user,
                sorted(candidate_user_ids),
                nickname_by_user,
            )
            debug_log("[AIC] Group update prompt:\n" + prompt)
            try:
                resp = await context.llm_generate(
                    chat_provider_id=provider_id,
                    system_prompt=GROUP_PROFILE_UPDATE_SYSTEM_PROMPT,
                    prompt=prompt,
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"Provider not found for group impression update: {exc}")
                return
            except Exception as exc:  # noqa: BLE001
                logger.error(f"LLM group update call failed: {exc}")
                return

            raw_text = resp.completion_text or ""
            debug_log("[AIC] Group update raw response:\n" + raw_text)
            updates, ok = parse_group_profile_json(raw_text, existing_by_user)
            if not ok:
                logger.warning(
                    "LLM group update returned invalid JSON, keeping pending messages"
                )
                return

            now = int(time.time())
            for user_id, data in updates.items():
                msgs = pending_by_user.get(user_id)
                if not msgs:
                    continue
                profile = profile_by_user.get(user_id)
                nickname = (
                    profile.nickname if profile and profile.nickname else user_id
                )
                last_seen = max(m.ts for m in msgs)
                record = ProfileRecord(
                    group_id=group_id,
                    user_id=user_id,
                    nickname=nickname,
                    last_seen=last_seen,
                    summary=data["summary"],
                    traits=data["traits"],
                    facts=data["facts"],
                    examples=data["examples"],
                    updated_at=now,
                    version=(profile.version if profile else 1),
                )
                await asyncio.to_thread(store.upsert_profile, record)

            delete_ids = [
                msg.id for msgs in pending_by_user.values() for msg in msgs
            ]
            if delete_ids:
                await asyncio.to_thread(store.delete_pending_messages, delete_ids)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Group impression update failed: {exc}")
        finally:
            active_updates.discard(key)


async def force_update(
    context,
    store: ImpressionStore,
    config,
    debug_log,
    update_locks: dict[str, asyncio.Lock],
    umo: str,
    group_id: str,
    user_id: str,
    nickname: str,
    clear_old: bool,
) -> bool:
    key = f"{group_id}:{user_id}"
    lock = update_locks.setdefault(key, asyncio.Lock())
    async with lock:
        pending = await asyncio.to_thread(
            store.get_pending_messages,
            group_id,
            user_id,
            MAX_PENDING_MESSAGES,
        )
        profile = await asyncio.to_thread(store.get_profile, group_id, user_id)
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
            provider_id = (
                config.update_provider_id
                or await context.get_current_chat_provider_id(umo=umo)
            )
        except ProviderNotFoundError as exc:
            logger.warning(f"No LLM provider configured: {exc}")
            return False

        prompt = build_update_prompt(existing, pending)
        debug_log("[AIC] Force update prompt:\n" + prompt)
        try:
            resp = await context.llm_generate(
                chat_provider_id=provider_id,
                system_prompt=PROFILE_UPDATE_SYSTEM_PROMPT,
                prompt=prompt,
            )
        except ProviderNotFoundError as exc:
            logger.warning(f"Provider not found for impression update: {exc}")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error(f"LLM update call failed: {exc}")
            return False

        debug_log("[AIC] Force update raw response:\n" + (resp.completion_text or ""))
        data, ok = parse_profile_json(resp.completion_text or "", existing)
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
        await asyncio.to_thread(store.upsert_profile, record)

        if pending:
            await asyncio.to_thread(
                store.delete_pending_messages, [p.id for p in pending]
            )
        return True


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
        pending = await asyncio.to_thread(
            store.get_pending_messages_by_group,
            group_id,
            max(1, config.update_msg_threshold),
        )
        if not pending:
            return False

        recent_profiles = await asyncio.to_thread(
            store.get_recent_profiles_by_group,
            group_id,
            config.group_batch_known_users_max,
        )
        nickname_by_user = {
            p.user_id: (p.nickname or "").strip() for p in recent_profiles if p.nickname
        }

        attribution_map: dict[int, list[str]] = {}
        if config.group_batch_enable_semantic_attribution:
            attribution_prompt = build_group_attribution_prompt(
                pending[
                    : min(
                        config.group_batch_attribution_max_messages,
                        max(1, config.update_msg_threshold),
                    )
                ],
                recent_profiles,
                config.group_batch_attribution_include_summary,
            )
            debug_log("[AIC] Group attribution prompt:\n" + attribution_prompt)
            try:
                provider_id = (
                    config.update_provider_id
                    or await context.get_current_chat_provider_id(umo=umo)
                )
            except ProviderNotFoundError as exc:
                logger.warning(f"No LLM provider configured: {exc}")
                provider_id = ""

            if provider_id:
                try:
                    resp = await context.llm_generate(
                        chat_provider_id=provider_id,
                        system_prompt=GROUP_ATTRIBUTION_SYSTEM_PROMPT,
                        prompt=attribution_prompt,
                    )
                    raw_text = resp.completion_text or ""
                    debug_log("[AIC] Group attribution raw response:\n" + raw_text)
                    attribution_map, _ = parse_attribution_json(
                        raw_text, {p.user_id for p in recent_profiles}
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"LLM group attribution failed: {exc}")

        pending_by_user: dict[str, list] = {}
        for msg in pending:
            targets = attribution_map.get(msg.id)
            if targets:
                if config.group_batch_attribution_max_targets_per_message > 0:
                    targets = targets[
                        : config.group_batch_attribution_max_targets_per_message
                    ]
                for target_id in targets:
                    pending_by_user.setdefault(target_id, []).append(msg)
                continue
            pending_by_user.setdefault(msg.user_id, []).append(msg)

        pending_by_user = {
            uid: msgs for uid, msgs in pending_by_user.items() if msgs
        }
        if not pending_by_user:
            return False

        candidate_user_ids = (
            set(pending_by_user.keys()) | set(nickname_by_user.keys())
        )
        existing_by_user: dict[str, dict] = {}
        profile_by_user: dict[str, ProfileRecord | None] = {}
        for user_id in candidate_user_ids:
            profile = await asyncio.to_thread(store.get_profile, group_id, user_id)
            profile_by_user[user_id] = profile
            existing_by_user[user_id] = {
                "nickname": profile.nickname if profile else "",
                "summary": profile.summary if profile else "",
                "traits": profile.traits if profile else [],
                "facts": profile.facts if profile else [],
                "examples": profile.examples if profile else [],
            }

        try:
            provider_id = (
                config.update_provider_id
                or await context.get_current_chat_provider_id(umo=umo)
            )
        except ProviderNotFoundError as exc:
            logger.warning(f"No LLM provider configured: {exc}")
            return False

        prompt = build_group_update_prompt(
            existing_by_user,
            pending_by_user,
            sorted(candidate_user_ids),
            nickname_by_user,
        )
        debug_log("[AIC] Group update prompt:\n" + prompt)
        try:
            resp = await context.llm_generate(
                chat_provider_id=provider_id,
                system_prompt=GROUP_PROFILE_UPDATE_SYSTEM_PROMPT,
                prompt=prompt,
            )
        except ProviderNotFoundError as exc:
            logger.warning(f"Provider not found for group impression update: {exc}")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error(f"LLM group update call failed: {exc}")
            return False

        raw_text = resp.completion_text or ""
        debug_log("[AIC] Group update raw response:\n" + raw_text)
        updates, ok = parse_group_profile_json(raw_text, existing_by_user)
        if not ok:
            logger.warning(
                "LLM group update returned invalid JSON, keeping pending messages"
            )
            return False

        now = int(time.time())
        for user_id, data in updates.items():
            msgs = pending_by_user.get(user_id)
            if not msgs:
                continue
            profile = profile_by_user.get(user_id)
            nickname = profile.nickname if profile and profile.nickname else user_id
            last_seen = max(m.ts for m in msgs)
            record = ProfileRecord(
                group_id=group_id,
                user_id=user_id,
                nickname=nickname,
                last_seen=last_seen,
                summary=data["summary"],
                traits=data["traits"],
                facts=data["facts"],
                examples=data["examples"],
                updated_at=now,
                version=(profile.version if profile else 1),
            )
            await asyncio.to_thread(store.upsert_profile, record)

        delete_ids = [msg.id for msgs in pending_by_user.values() for msg in msgs]
        if delete_ids:
            await asyncio.to_thread(store.delete_pending_messages, delete_ids)
        return True


def build_update_prompt(existing: dict, pending) -> str:
    lines = [
        "Existing profile (JSON):",
        json_dumps(existing),
        "",
        "New messages:",
    ]
    for idx, msg in enumerate(pending, 1):
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
        text = plain_from_raw_text(msg.message)
        if not text:
            continue
        lines.append(f"{idx}. [{ts_text}] {text}")
    return "\n".join(lines)


def build_group_update_prompt(
    existing_by_user: dict[str, dict],
    pending_by_user,
    known_user_ids: list[str],
    nickname_by_user: dict[str, str],
) -> str:
    lines = [
        "Existing profiles (JSON by user_id):",
        json_dumps(existing_by_user),
        "",
        "Known users (id list):",
    ]
    lines.append(", ".join(known_user_ids))
    if nickname_by_user:
        lines.extend(["", "Known users (id -> nickname):"])
        for user_id, nickname in nickname_by_user.items():
            lines.append(f"{user_id}: {nickname}")
    lines.extend(["", "New messages:"])
    for user_id, messages in pending_by_user.items():
        lines.append(f"target_id={user_id}")
        for idx, msg in enumerate(messages, 1):
            ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
            lines.append(f"{idx}. [{ts_text}] speaker={msg.user_id} text={msg.message}")
        lines.append("")
    return "\n".join(lines).strip()


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


def json_dumps(data: dict) -> str:
    # Keep a tiny wrapper so we can centralize formatting later if needed.
    import json

    return json.dumps(data, ensure_ascii=False)
