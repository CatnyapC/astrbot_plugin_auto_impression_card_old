from __future__ import annotations

import json
import re

from astrbot.core.message.components import Plain


def extract_plain_text(components) -> str:
    parts: list[str] = []
    for comp in components:
        if isinstance(comp, Plain):
            text = comp.text.strip()
            if text:
                parts.append(text)
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
