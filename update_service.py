from __future__ import annotations

import asyncio
import time

from astrbot.api import logger
from astrbot.core.exceptions import ProviderNotFoundError

from .prompts import PROFILE_UPDATE_SYSTEM_PROMPT
from .storage import ImpressionStore, ProfileRecord
from .utils import parse_profile_json

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


def build_update_prompt(existing: dict, pending) -> str:
    lines = [
        "Existing profile (JSON):",
        json_dumps(existing),
        "",
        "New messages:",
    ]
    for idx, msg in enumerate(pending, 1):
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.ts))
        lines.append(f"{idx}. [{ts_text}] {msg.message}")
    return "\n".join(lines)


def json_dumps(data: dict) -> str:
    # Keep a tiny wrapper so we can centralize formatting later if needed.
    import json

    return json.dumps(data, ensure_ascii=False)
