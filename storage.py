from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(slots=True)
class ProfileRecord:
    group_id: str
    user_id: str
    nickname: str | None
    last_seen: int | None
    summary: str | None
    traits: list[str]
    facts: list[str]
    examples: list[str]
    updated_at: int | None
    version: int


@dataclass(slots=True)
class PendingMessage:
    id: int
    message: str
    ts: int


@dataclass(slots=True)
class GroupMessage:
    id: int
    group_id: str
    user_id: str
    message: str
    ts: int


class ImpressionStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    nickname TEXT,
                    last_seen INTEGER,
                    summary TEXT,
                    traits TEXT,
                    facts TEXT,
                    examples TEXT,
                    updated_at INTEGER,
                    version INTEGER DEFAULT 1,
                    PRIMARY KEY (group_id, user_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    message TEXT NOT NULL,
                    ts INTEGER NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_message_queue_user
                ON message_queue (group_id, user_id)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alias_map (
                    group_id TEXT NOT NULL,
                    speaker_id TEXT NOT NULL,
                    speaker_nickname TEXT,
                    alias TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    target_nickname TEXT,
                    evidence_text TEXT,
                    confidence REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (group_id, speaker_id, alias, target_id)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alias_lookup
                ON alias_map (group_id, speaker_id, alias)
                """
            )
            self._ensure_alias_map_columns(cur)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _ensure_alias_map_columns(cur: sqlite3.Cursor) -> None:
        cols = {row[1] for row in cur.execute("PRAGMA table_info(alias_map)")}
        if "speaker_nickname" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN speaker_nickname TEXT")
        if "target_nickname" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN target_nickname TEXT")
        if "evidence_text" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN evidence_text TEXT")

    def touch_profile(self, group_id: str, user_id: str, nickname: str, ts: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO profiles (group_id, user_id, nickname, last_seen, updated_at, version)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=excluded.last_seen
                """,
                (group_id, user_id, nickname, ts, ts),
            )
            conn.commit()

    def enqueue_message(self, group_id: str, user_id: str, message: str, ts: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO message_queue (group_id, user_id, message, ts)
                VALUES (?, ?, ?, ?)
                """,
                (group_id, user_id, message, ts),
            )
            conn.commit()

    def get_pending_count(self, group_id: str, user_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM message_queue
                WHERE group_id=? AND user_id=?
                """,
                (group_id, user_id),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    def get_pending_messages(
        self, group_id: str, user_id: str, limit: int | None = None
    ) -> list[PendingMessage]:
        sql = """
            SELECT id, message, ts
            FROM message_queue
            WHERE group_id=? AND user_id=?
            ORDER BY ts ASC, id ASC
        """
        params: tuple = (group_id, user_id)
        if limit is not None:
            sql += " LIMIT ?"
            params = (group_id, user_id, limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [PendingMessage(id=row["id"], message=row["message"], ts=row["ts"]) for row in rows]

    def delete_pending_messages(self, ids: Iterable[int]) -> None:
        ids = list(ids)
        if not ids:
            return
        placeholders = ",".join(["?"] * len(ids))
        sql = f"DELETE FROM message_queue WHERE id IN ({placeholders})"
        with self._connect() as conn:
            conn.execute(sql, ids)
            conn.commit()

    def get_pending_messages_by_group(
        self, group_id: str, limit: int | None = None
    ) -> list[GroupMessage]:
        sql = """
            SELECT id, group_id, user_id, message, ts
            FROM message_queue
            WHERE group_id=?
            ORDER BY ts ASC, id ASC
        """
        params: tuple = (group_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (group_id, limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [
                GroupMessage(
                    id=row["id"],
                    group_id=row["group_id"],
                    user_id=row["user_id"],
                    message=row["message"],
                    ts=row["ts"],
                )
                for row in rows
            ]

    def get_profile(self, group_id: str, user_id: str) -> ProfileRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM profiles WHERE group_id=? AND user_id=?
                """,
                (group_id, user_id),
            ).fetchone()
            if not row:
                return None
            return ProfileRecord(
                group_id=row["group_id"],
                user_id=row["user_id"],
                nickname=row["nickname"],
                last_seen=row["last_seen"],
                summary=row["summary"],
                traits=self._load_list(row["traits"]),
                facts=self._load_list(row["facts"]),
                examples=self._load_list(row["examples"]),
                updated_at=row["updated_at"],
                version=row["version"] or 1,
            )

    def get_recent_profiles_by_group(
        self, group_id: str, limit: int | None = None
    ) -> list[ProfileRecord]:
        sql = """
            SELECT * FROM profiles
            WHERE group_id=?
            ORDER BY last_seen DESC, updated_at DESC
        """
        params: tuple = (group_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (group_id, limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            results = []
            for row in rows:
                results.append(
                    ProfileRecord(
                        group_id=row["group_id"],
                        user_id=row["user_id"],
                        nickname=row["nickname"],
                        last_seen=row["last_seen"],
                        summary=row["summary"],
                        traits=self._load_list(row["traits"]),
                        facts=self._load_list(row["facts"]),
                        examples=self._load_list(row["examples"]),
                        updated_at=row["updated_at"],
                        version=row["version"] or 1,
                    )
                )
            return results

    def find_profiles_by_nickname(
        self, group_id: str, nickname: str
    ) -> list[ProfileRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM profiles WHERE group_id=? AND nickname=?
                """,
                (group_id, nickname),
            ).fetchall()
            results = []
            for row in rows:
                results.append(
                    ProfileRecord(
                        group_id=row["group_id"],
                        user_id=row["user_id"],
                        nickname=row["nickname"],
                        last_seen=row["last_seen"],
                        summary=row["summary"],
                        traits=self._load_list(row["traits"]),
                        facts=self._load_list(row["facts"]),
                        examples=self._load_list(row["examples"]),
                        updated_at=row["updated_at"],
                        version=row["version"] or 1,
                    )
                )
            return results

    def upsert_profile(
        self,
        record: ProfileRecord,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO profiles (
                    group_id, user_id, nickname, last_seen, summary, traits, facts, examples, updated_at, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=excluded.last_seen,
                    summary=excluded.summary,
                    traits=excluded.traits,
                    facts=excluded.facts,
                    examples=excluded.examples,
                    updated_at=excluded.updated_at,
                    version=excluded.version
                """,
                (
                    record.group_id,
                    record.user_id,
                    record.nickname,
                    record.last_seen,
                    record.summary,
                    json.dumps(record.traits, ensure_ascii=False),
                    json.dumps(record.facts, ensure_ascii=False),
                    json.dumps(record.examples, ensure_ascii=False),
                    record.updated_at,
                    record.version,
                ),
            )
            conn.commit()

    def upsert_alias(
        self,
        group_id: str,
        speaker_id: str,
        alias: str,
        target_id: str,
        confidence: float,
        speaker_nickname: str | None = None,
        target_nickname: str | None = None,
        evidence_text: str | None = None,
        ts: int | None = None,
    ) -> None:
        if ts is None:
            ts = int(time.time())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO alias_map (
                    group_id,
                    speaker_id,
                    speaker_nickname,
                    alias,
                    target_id,
                    target_nickname,
                    evidence_text,
                    confidence,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id, speaker_id, alias, target_id) DO UPDATE SET
                    speaker_nickname=excluded.speaker_nickname,
                    target_nickname=excluded.target_nickname,
                    evidence_text=excluded.evidence_text,
                    confidence=excluded.confidence,
                    updated_at=excluded.updated_at
                """,
                (
                    group_id,
                    speaker_id,
                    speaker_nickname,
                    alias,
                    target_id,
                    target_nickname,
                    evidence_text,
                    confidence,
                    ts,
                ),
            )
            conn.commit()

    def get_nickname_map(
        self, group_id: str, user_ids: Iterable[str]
    ) -> dict[str, str]:
        ids = [str(uid) for uid in user_ids if str(uid)]
        if not ids:
            return {}
        placeholders = ",".join(["?"] * len(ids))
        sql = f"""
            SELECT user_id, nickname
            FROM profiles
            WHERE group_id=? AND user_id IN ({placeholders})
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (group_id, *ids)).fetchall()
            return {
                row["user_id"]: row["nickname"]
                for row in rows
                if row["nickname"]
            }

    def find_alias_targets(
        self, group_id: str, speaker_id: str, alias: str
    ) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT target_id, confidence, updated_at
                FROM alias_map
                WHERE group_id=? AND speaker_id=? AND alias=?
                ORDER BY confidence DESC, updated_at DESC
                """,
                (group_id, speaker_id, alias),
            ).fetchall()
            return [dict(row) for row in rows]

    def find_alias_targets_global(self, group_id: str, alias: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT target_id, confidence, updated_at
                FROM alias_map
                WHERE group_id=? AND alias=?
                ORDER BY confidence DESC, updated_at DESC
                """,
                (group_id, alias),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_aliases_by_target(self, group_id: str, target_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT speaker_id, alias
                FROM alias_map
                WHERE group_id=? AND target_id=?
                ORDER BY updated_at DESC
                """,
                (group_id, target_id),
            ).fetchall()
            return [dict(row) for row in rows]

    def prune_aliases(
        self, group_id: str, speaker_id: str, target_id: str, limit: int
    ) -> None:
        if limit <= 0:
            return
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM alias_map
                WHERE group_id=? AND speaker_id=? AND target_id=? AND alias IN (
                    SELECT alias FROM (
                        SELECT alias,
                               ROW_NUMBER() OVER (
                                   ORDER BY confidence DESC, updated_at DESC
                               ) AS rn
                        FROM alias_map
                        WHERE group_id=? AND speaker_id=? AND target_id=?
                    ) ranked
                    WHERE rn > ?
                )
                """,
                (
                    group_id,
                    speaker_id,
                    target_id,
                    group_id,
                    speaker_id,
                    target_id,
                    limit,
                ),
            )
            conn.commit()

    @staticmethod
    def _load_list(value: str | None) -> list[str]:
        if not value:
            return []
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(data, list):
            return [str(x) for x in data]
        return []
