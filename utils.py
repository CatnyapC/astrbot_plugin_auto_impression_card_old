from __future__ import annotations

import json
import re

from astrbot.core.message.components import At, Plain, Reply


def extract_plain_text(components) -> str:
    parts: list[str] = []
    for comp in components:
        if isinstance(comp, Plain):
            text = comp.text.strip()
            if text:
                parts.append(text)
    return " ".join(parts).strip()


def extract_raw_text(components) -> str:
    parts: list[str] = []
    for comp in components:
        if isinstance(comp, Plain):
            text = comp.text.strip()
            if text:
                parts.append(text)
        elif isinstance(comp, At):
            parts.append(f"@{comp.qq}")
        elif isinstance(comp, Reply) and comp.sender_id is not None:
            parts.append(f"[reply_to:{comp.sender_id}]")
    return " ".join(parts).strip()


def safe_list(value, fallback: list[str]) -> list[str]:
    if isinstance(value, list) and value:
        return [str(x) for x in value]
    return fallback


def extract_json(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return ""
    return text[start : end + 1]


def plain_from_raw_text(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"\[reply_to:\d+\]", " ", text)
    cleaned = re.sub(r"@\d+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()




def extract_target_ids_from_raw_text(text: str) -> set[str]:
    if not text:
        return set()
    targets = set(re.findall(r"@(\d+)", text))
    targets.update(re.findall(r"\[reply_to:(\d+)\]", text))
    return {t for t in targets if t}


def last_token(text: str) -> str:
    tokens = re.findall(r"[\w\u4e00-\u9fff]+", text)
    if not tokens:
        return ""
    token = tokens[-1].strip()
    return token if len(token) >= 2 else ""


def token_count(text: str) -> int:
    return len(re.findall(r"[\w\u4e00-\u9fff]+", text))


def is_impression_query(text: str) -> bool:
    if not text:
        return False
    patterns = [
        "印象",
        "记得",
        "回忆",
        "回想",
        "了解",
        "认识",
        "评价",
        "看法",
        "印记",
        "档案",
        "资料",
        "卡片",
        "人物",
        "身份",
        "角色",
        "关系",
        "背景",
        "喜欢",
        "爱吃",
        "偏好",
        "兴趣",
        "性格",
        "特征",
        "是谁",
        "谁是",
        "有谁",
        "什么样",
        "种族"
    ]
    return any(p in text for p in patterns)


def is_self_profile_query(text: str) -> bool:
    if not text:
        return False
    patterns = [
        r"(我|本人|自己).{0,6}(印象|记得|回忆|了解|评价|认识|是谁|什么样|资料|档案)",
        r"(印象|记得|回忆|了解|评价|认识).{0,6}(我|本人|自己)",
    ]
    return any(re.search(p, text) for p in patterns)


def parse_profile_json(text: str, existing: dict) -> tuple[dict, bool]:
    raw = extract_json(text)
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
            "traits": safe_list(traits, existing.get("traits", [])),
            "facts": safe_list(facts, existing.get("facts", [])),
            "examples": safe_list(examples, existing.get("examples", [])),
        },
        True,
    )


def parse_group_profile_json(
    text: str, existing_by_user: dict[str, dict]
) -> tuple[dict[str, dict], bool]:
    raw = extract_json(text)
    if not raw:
        return {}, False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}, False

    users = data.get("users") if isinstance(data, dict) else None
    if users is None:
        users = data

    if isinstance(users, list):
        items = []
        for item in users:
            if not isinstance(item, dict):
                continue
            user_id = str(item.get("user_id") or item.get("id") or "").strip()
            if not user_id:
                continue
            items.append((user_id, item))
    elif isinstance(users, dict):
        items = [(str(k).strip(), v) for k, v in users.items()]
    else:
        return {}, False

    results: dict[str, dict] = {}
    for user_id, payload in items:
        if not user_id or user_id not in existing_by_user:
            continue
        if not isinstance(payload, dict):
            continue
        existing = existing_by_user[user_id]
        summary = str(payload.get("summary", "")).strip() or existing.get("summary", "")
        traits = payload.get("traits")
        facts = payload.get("facts")
        examples = payload.get("examples")
        results[user_id] = {
            "summary": summary,
            "traits": safe_list(traits, existing.get("traits", [])),
            "facts": safe_list(facts, existing.get("facts", [])),
            "examples": safe_list(examples, existing.get("examples", [])),
        }

    if not results:
        return {}, False
    return results, True


def parse_attribution_json(text: str, known_user_ids: set[str]) -> tuple[dict[int, list[str]], bool]:
    raw = extract_json(text)
    if not raw:
        return {}, False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}, False

    assignments = data.get("assignments") if isinstance(data, dict) else None
    if not isinstance(assignments, list):
        return {}, False

    results: dict[int, list[str]] = {}
    for item in assignments:
        if not isinstance(item, dict):
            continue
        message_id = item.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            continue
        target_ids = item.get("target_ids")
        if not isinstance(target_ids, list):
            continue
        filtered = [str(t).strip() for t in target_ids if str(t).strip() in known_user_ids]
        if filtered:
            results[message_id_int] = filtered

    return results, True
