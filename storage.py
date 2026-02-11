from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(slots=True)
class ProfileRecord:
    user_id: str
    nickname: str | None
    last_seen: int | None
    summary: str | None
    impressions: list[str]
    impressions_confidence: dict[str, float]
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
        attempts = 5
        for idx in range(attempts):
            try:
                with self._connect() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS profiles (
                            user_id TEXT NOT NULL,
                            nickname TEXT,
                            last_seen INTEGER,
                            summary TEXT,
                            impressions TEXT,
                            impressions_confidence TEXT,
                            updated_at INTEGER,
                            version INTEGER DEFAULT 1,
                            PRIMARY KEY (user_id)
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
                            speaker_id TEXT NOT NULL,
                            speaker_nickname TEXT,
                            alias TEXT NOT NULL,
                            target_id TEXT NOT NULL,
                            target_nickname TEXT,
                            evidence_text TEXT,
                            confidence REAL NOT NULL,
                            updated_at INTEGER NOT NULL,
                            source_group_id TEXT,
                            PRIMARY KEY (speaker_id, alias, target_id)
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_alias_lookup
                        ON alias_map (speaker_id, alias)
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS impression_evidence (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            group_id TEXT NOT NULL,
                            user_id TEXT NOT NULL,
                            item_type TEXT NOT NULL,
                            item_text TEXT NOT NULL,
                            message_id INTEGER NOT NULL,
                            speaker_id TEXT NOT NULL,
                            message_text TEXT NOT NULL,
                            message_ts INTEGER NOT NULL,
                            evidence_confidence REAL,
                            joke_likelihood REAL,
                            source_type TEXT,
                            consistency_tag TEXT,
                            created_at INTEGER NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS user_trust (
                            group_id TEXT NOT NULL,
                            user_id TEXT NOT NULL,
                            trust REAL NOT NULL,
                            updated_at INTEGER NOT NULL,
                            PRIMARY KEY (group_id, user_id)
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS group_state (
                            group_id TEXT NOT NULL,
                            last_update INTEGER NOT NULL,
                            PRIMARY KEY (group_id)
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_impression_evidence_item
                        ON impression_evidence (group_id, user_id, item_type, item_text, message_ts)
                        """
                    )
                    self._ensure_alias_map_columns(cur)
                    self._ensure_profile_columns(cur)
                    self._ensure_evidence_columns(cur)
                    conn.commit()
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or idx == attempts - 1:
                    raise
                time.sleep(0.2 * (idx + 1))

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
        if "group_id" in cols or "source_group_id" not in cols:
            ImpressionStore._migrate_alias_map_table(cur)
            cols = {row[1] for row in cur.execute("PRAGMA table_info(alias_map)")}
        if "speaker_nickname" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN speaker_nickname TEXT")
        if "target_nickname" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN target_nickname TEXT")
        if "evidence_text" not in cols:
            cur.execute("ALTER TABLE alias_map ADD COLUMN evidence_text TEXT")

    @staticmethod
    def _migrate_alias_map_table(cur: sqlite3.Cursor) -> None:
        cols = {row[1] for row in cur.execute("PRAGMA table_info(alias_map)")}
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alias_map_new (
                speaker_id TEXT NOT NULL,
                speaker_nickname TEXT,
                alias TEXT NOT NULL,
                target_id TEXT NOT NULL,
                target_nickname TEXT,
                evidence_text TEXT,
                confidence REAL NOT NULL,
                updated_at INTEGER NOT NULL,
                source_group_id TEXT,
                PRIMARY KEY (speaker_id, alias, target_id)
            )
            """
        )
        rows = cur.execute("SELECT * FROM alias_map").fetchall()
        grouped: dict[tuple[str, str, str], dict] = {}
        for row in rows:
            speaker_id = str(row["speaker_id"] or "").strip()
            alias = str(row["alias"] or "").strip()
            target_id = str(row["target_id"] or "").strip()
            if not speaker_id or not alias or not target_id:
                continue
            key = (speaker_id, alias, target_id)
            existing = grouped.get(key)
            confidence = float(row["confidence"]) if row["confidence"] is not None else 0.0
            updated_at = int(row["updated_at"]) if row["updated_at"] is not None else 0
            if not existing or confidence > existing["confidence"] or updated_at > existing["updated_at"]:
                grouped[key] = {
                    "speaker_id": speaker_id,
                    "speaker_nickname": row["speaker_nickname"] if "speaker_nickname" in cols else None,
                    "alias": alias,
                    "target_id": target_id,
                    "target_nickname": row["target_nickname"] if "target_nickname" in cols else None,
                    "evidence_text": row["evidence_text"] if "evidence_text" in cols else None,
                    "confidence": confidence,
                    "updated_at": updated_at,
                    "source_group_id": (
                        row["group_id"]
                        if "group_id" in cols
                        else (row["source_group_id"] if "source_group_id" in cols else None)
                    ),
                }
        for item in grouped.values():
            cur.execute(
                """
                INSERT OR REPLACE INTO alias_map_new (
                    speaker_id, speaker_nickname, alias, target_id, target_nickname,
                    evidence_text, confidence, updated_at, source_group_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["speaker_id"],
                    item["speaker_nickname"],
                    item["alias"],
                    item["target_id"],
                    item["target_nickname"],
                    item["evidence_text"],
                    item["confidence"],
                    item["updated_at"],
                    item["source_group_id"],
                ),
            )
        cur.execute("DROP TABLE alias_map")
        cur.execute("ALTER TABLE alias_map_new RENAME TO alias_map")

    @staticmethod
    def _ensure_profile_columns(cur: sqlite3.Cursor) -> None:
        cols = {row[1] for row in cur.execute("PRAGMA table_info(profiles)")}
        has_old = {
            "group_id",
            "traits",
            "facts",
            "examples",
            "traits_confidence",
            "facts_confidence",
        } & cols
        has_new = {"impressions", "impressions_confidence"} <= cols and "group_id" not in cols
        if has_old or not has_new:
            ImpressionStore._migrate_profiles_table(cur)

    @staticmethod
    def _migrate_profiles_table(cur: sqlite3.Cursor) -> None:
        cols = {row[1] for row in cur.execute("PRAGMA table_info(profiles)")}
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS profiles_new (
                user_id TEXT NOT NULL,
                nickname TEXT,
                last_seen INTEGER,
                summary TEXT,
                impressions TEXT,
                impressions_confidence TEXT,
                updated_at INTEGER,
                version INTEGER DEFAULT 1,
                PRIMARY KEY (user_id)
            )
            """
        )
        rows = cur.execute("SELECT * FROM profiles").fetchall()
        grouped: dict[str, dict] = {}
        for row in rows:
            user_id = str(row["user_id"]).strip()
            if not user_id:
                continue
            item = grouped.setdefault(
                user_id,
                {
                    "user_id": user_id,
                    "nickname": "",
                    "last_seen": 0,
                    "summary": "",
                    "impressions": [],
                    "impressions_conf": {},
                    "updated_at": 0,
                    "version": 1,
                },
            )
            impressions: list[str] = []
            impressions_conf: dict[str, float] = {}

            traits = ImpressionStore._load_list(row["traits"]) if "traits" in cols else []
            facts = ImpressionStore._load_list(row["facts"]) if "facts" in cols else []

            traits_conf = (
                ImpressionStore._load_dict(row["traits_confidence"])
                if "traits_confidence" in cols
                else {}
            )
            facts_conf = (
                ImpressionStore._load_dict(row["facts_confidence"])
                if "facts_confidence" in cols
                else {}
            )
            for key, val in traits_conf.items():
                impressions_conf[key] = float(val)
            for key, val in facts_conf.items():
                existing = impressions_conf.get(key)
                value = float(val)
                if existing is None or value > existing:
                    impressions_conf[key] = value

            merged = traits + facts
            for val in merged:
                text = str(val).strip()
                if text:
                    impressions.append(text)

            for key, val in impressions_conf.items():
                existing = item["impressions_conf"].get(key)
                if existing is None or val > existing:
                    item["impressions_conf"][key] = val
            item["impressions"].extend(impressions)

            updated_at = int(row["updated_at"]) if row["updated_at"] is not None else 0
            last_seen = int(row["last_seen"]) if row["last_seen"] is not None else 0
            if updated_at > item["updated_at"]:
                item["updated_at"] = updated_at
                if row["nickname"]:
                    item["nickname"] = row["nickname"]
                if row["summary"]:
                    item["summary"] = row["summary"]
            item["last_seen"] = max(item["last_seen"], last_seen)
            version = int(row["version"]) if row["version"] is not None else 1
            if version > item["version"]:
                item["version"] = version

        for item in grouped.values():
            seen: set[str] = set()
            merged: list[str] = []
            for val in item["impressions"]:
                if val not in seen:
                    seen.add(val)
                    merged.append(val)
            if item["impressions_conf"]:
                order = {val: idx for idx, val in enumerate(merged)}
                merged.sort(
                    key=lambda x: (
                        -float(item["impressions_conf"].get(x, 0.0)),
                        order.get(x, 0),
                    )
                )
            cur.execute(
                """
                INSERT OR REPLACE INTO profiles_new (
                    user_id, nickname, last_seen, summary,
                    impressions, impressions_confidence, updated_at, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["user_id"],
                    item["nickname"],
                    item["last_seen"],
                    item["summary"],
                    json.dumps(merged, ensure_ascii=False),
                    json.dumps(item["impressions_conf"], ensure_ascii=False),
                    item["updated_at"],
                    item["version"],
                ),
            )
        cur.execute("DROP TABLE profiles")
        cur.execute("ALTER TABLE profiles_new RENAME TO profiles")

    @staticmethod
    def _ensure_evidence_columns(cur: sqlite3.Cursor) -> None:
        cols = {row[1] for row in cur.execute("PRAGMA table_info(impression_evidence)")}
        if "speaker_id" not in cols:
            cur.execute("ALTER TABLE impression_evidence ADD COLUMN speaker_id TEXT")
        if "evidence_confidence" not in cols:
            cur.execute("ALTER TABLE impression_evidence ADD COLUMN evidence_confidence REAL")
        if "joke_likelihood" not in cols:
            cur.execute("ALTER TABLE impression_evidence ADD COLUMN joke_likelihood REAL")
        if "source_type" not in cols:
            cur.execute("ALTER TABLE impression_evidence ADD COLUMN source_type TEXT")
        if "consistency_tag" not in cols:
            cur.execute("ALTER TABLE impression_evidence ADD COLUMN consistency_tag TEXT")
        cur.execute(
            """
            UPDATE impression_evidence
            SET item_type='impression'
            WHERE item_type IN ('trait', 'fact')
            """
        )

    def touch_profile(self, group_id: str, user_id: str, nickname: str, ts: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO profiles (user_id, nickname, last_seen, updated_at, version)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=excluded.last_seen
                """,
                (user_id, nickname, ts, ts),
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

    def get_profile(self, user_id: str) -> ProfileRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM profiles WHERE user_id=?
                """,
                (user_id,),
            ).fetchone()
            if not row:
                return None
            return ProfileRecord(
                user_id=row["user_id"],
                nickname=row["nickname"],
                last_seen=row["last_seen"],
                summary=row["summary"],
                impressions=self._load_list(row["impressions"]),
                impressions_confidence=self._load_dict(row["impressions_confidence"]),
                updated_at=row["updated_at"],
                version=row["version"] or 1,
            )

    def get_group_ids(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT group_id FROM message_queue
                UNION
                SELECT DISTINCT group_id FROM group_state
                """
            ).fetchall()
            return [str(row["group_id"]) for row in rows if row["group_id"]]

    def get_recent_profiles_by_group(
        self, group_id: str, limit: int | None = None
    ) -> list[ProfileRecord]:
        sql = """
            SELECT user_id, MAX(ts) AS last_seen
            FROM message_queue
            WHERE group_id=?
            GROUP BY user_id
            ORDER BY last_seen DESC
        """
        params: tuple = (group_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (group_id, limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            user_ids = [str(row["user_id"]) for row in rows if row["user_id"]]
            results: list[ProfileRecord] = []
            for row in rows:
                user_id = str(row["user_id"])
                if not user_id:
                    continue
                profile = self.get_profile(user_id)
                if profile:
                    results.append(profile)
                else:
                    results.append(
                        ProfileRecord(
                            user_id=user_id,
                            nickname=None,
                            last_seen=row["last_seen"],
                            summary=None,
                            impressions=[],
                            impressions_confidence={},
                            updated_at=None,
                            version=1,
                        )
                    )
            return results

    def find_profiles_by_nickname(self, nickname: str) -> list[ProfileRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM profiles WHERE nickname=?
                """,
                (nickname,),
            ).fetchall()
            results = []
            for row in rows:
                results.append(
                    ProfileRecord(
                        user_id=row["user_id"],
                        nickname=row["nickname"],
                        last_seen=row["last_seen"],
                        summary=row["summary"],
                        impressions=self._load_list(row["impressions"]),
                        impressions_confidence=self._load_dict(row["impressions_confidence"]),
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
                    user_id, nickname, last_seen, summary,
                    impressions, impressions_confidence, updated_at, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=excluded.last_seen,
                    summary=excluded.summary,
                    impressions=excluded.impressions,
                    impressions_confidence=excluded.impressions_confidence,
                    updated_at=excluded.updated_at,
                    version=excluded.version
                """,
                (
                    record.user_id,
                    record.nickname,
                    record.last_seen,
                    record.summary,
                    json.dumps(record.impressions, ensure_ascii=False),
                    json.dumps(record.impressions_confidence, ensure_ascii=False),
                    record.updated_at,
                    record.version,
                ),
            )
            conn.commit()

    def upsert_profile_with_confidence(
        self,
        record: ProfileRecord,
        impressions_confidence: dict[str, float],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO profiles (
                    user_id, nickname, last_seen, summary,
                    impressions, impressions_confidence, updated_at, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=excluded.last_seen,
                    summary=excluded.summary,
                    impressions=excluded.impressions,
                    impressions_confidence=excluded.impressions_confidence,
                    updated_at=excluded.updated_at,
                    version=excluded.version
                """,
                (
                    record.user_id,
                    record.nickname,
                    record.last_seen,
                    record.summary,
                    json.dumps(record.impressions, ensure_ascii=False),
                    json.dumps(impressions_confidence, ensure_ascii=False),
                    record.updated_at,
                    record.version,
                ),
            )
            conn.commit()

    def insert_evidence(self, records: list[tuple]) -> None:
        if not records:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO impression_evidence (
                    group_id,
                    user_id,
                    item_type,
                    item_text,
                    message_id,
                    speaker_id,
                    message_text,
                    message_ts,
                    evidence_confidence,
                    joke_likelihood,
                    source_type,
                    consistency_tag,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )
            conn.commit()

    def get_evidence_for_item(
        self, group_id: str, user_id: str, item_type: str, item_text: str
    ) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT message_id, speaker_id, message_ts, evidence_confidence,
                       joke_likelihood, source_type, consistency_tag
                FROM impression_evidence
                WHERE user_id=? AND item_type=? AND item_text=?
                ORDER BY message_ts DESC, id DESC
                """,
                (user_id, item_type, item_text),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_evidence_for_item_and_speaker(
        self,
        group_id: str,
        user_id: str,
        item_type: str,
        item_text: str,
        speaker_id: str,
    ) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT message_id, speaker_id, message_ts, evidence_confidence,
                       joke_likelihood, source_type, consistency_tag
                FROM impression_evidence
                WHERE user_id=? AND item_type=? AND item_text=? AND speaker_id=?
                ORDER BY message_ts DESC, id DESC
                """,
                (user_id, item_type, item_text, speaker_id),
            ).fetchall()
            return [dict(row) for row in rows]

    def prune_evidence_by_speaker(
        self,
        group_id: str,
        user_id: str,
        item_type: str,
        item_text: str,
        speaker_id: str,
        max_items: int,
    ) -> None:
        if max_items <= 0:
            return
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM impression_evidence
                WHERE user_id=? AND item_type=? AND item_text=? AND speaker_id=?
                ORDER BY message_ts DESC, id DESC
                LIMIT -1 OFFSET ?
                """,
                (user_id, item_type, item_text, speaker_id, max_items),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if not ids:
                return
            placeholders = ",".join(["?"] * len(ids))
            conn.execute(
                f"DELETE FROM impression_evidence WHERE id IN ({placeholders})",
                ids,
            )
            conn.commit()

    def get_user_trust(self, group_id: str, user_id: str) -> float:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT trust FROM user_trust
                WHERE group_id=? AND user_id=?
                """,
                (group_id, user_id),
            ).fetchone()
            if row is None:
                default_trust = 0.7
                conn.execute(
                    """
                    INSERT INTO user_trust (group_id, user_id, trust, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (group_id, user_id, default_trust, int(time.time())),
                )
                conn.commit()
                return default_trust
            return float(row["trust"])

    def upsert_user_trust(self, group_id: str, user_id: str, trust: float) -> None:
        trust = max(0.0, min(1.0, trust))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_trust (group_id, user_id, trust, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    trust=excluded.trust,
                    updated_at=excluded.updated_at
                """,
                (group_id, user_id, trust, int(time.time())),
            )
            conn.commit()

    def get_group_last_update(self, group_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_update FROM group_state WHERE group_id=?",
                (group_id,),
            ).fetchone()
            return int(row["last_update"]) if row else 0

    def set_group_last_update(self, group_id: str, ts: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_state (group_id, last_update)
                VALUES (?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    last_update=excluded.last_update
                """,
                (group_id, ts),
            )
            conn.commit()

    def prune_evidence(
        self,
        group_id: str,
        user_id: str,
        item_type: str,
        item_text: str,
        max_items: int,
    ) -> None:
        if max_items <= 0:
            return
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM impression_evidence
                WHERE user_id=? AND item_type=? AND item_text=?
                ORDER BY message_ts DESC, id DESC
                LIMIT -1 OFFSET ?
                """,
                (user_id, item_type, item_text, max_items),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if not ids:
                return
            placeholders = ",".join(["?"] * len(ids))
            conn.execute(
                f"DELETE FROM impression_evidence WHERE id IN ({placeholders})",
                ids,
            )
            conn.commit()

    def delete_evidence_for_item(
        self, group_id: str, user_id: str, item_type: str, item_text: str
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM impression_evidence
                WHERE user_id=? AND item_type=? AND item_text=?
                """,
                (user_id, item_type, item_text),
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
                    speaker_id,
                    speaker_nickname,
                    alias,
                    target_id,
                    target_nickname,
                    evidence_text,
                    confidence,
                    updated_at,
                    source_group_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(speaker_id, alias, target_id) DO UPDATE SET
                    speaker_nickname=excluded.speaker_nickname,
                    target_nickname=excluded.target_nickname,
                    evidence_text=excluded.evidence_text,
                    confidence=excluded.confidence,
                    updated_at=excluded.updated_at,
                    source_group_id=excluded.source_group_id
                """,
                (
                    speaker_id,
                    speaker_nickname,
                    alias,
                    target_id,
                    target_nickname,
                    evidence_text,
                    confidence,
                    ts,
                    group_id,
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
            WHERE user_id IN ({placeholders})
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (*ids,)).fetchall()
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
                WHERE speaker_id=? AND alias=?
                ORDER BY confidence DESC, updated_at DESC
                """,
                (speaker_id, alias),
            ).fetchall()
            return [dict(row) for row in rows]

    def find_alias_targets_global(self, group_id: str, alias: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT target_id, confidence, updated_at
                FROM alias_map
                WHERE alias=?
                ORDER BY confidence DESC, updated_at DESC
                """,
                (alias,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_aliases_by_target(self, group_id: str, target_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT speaker_id, alias
                FROM alias_map
                WHERE target_id=?
                ORDER BY updated_at DESC
                """,
                (target_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_alias_index(self, group_id: str) -> dict[str, dict[str, list[str]]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT speaker_id, alias, target_id, confidence, updated_at
                FROM alias_map
                ORDER BY confidence DESC, updated_at DESC
                """,
            ).fetchall()
        index: dict[str, dict[str, list[str]]] = {}
        for row in rows:
            speaker_id = str(row["speaker_id"])
            alias = str(row["alias"]).strip()
            target_id = str(row["target_id"])
            if not speaker_id or not alias or not target_id:
                continue
            speaker_map = index.setdefault(speaker_id, {})
            targets = speaker_map.setdefault(alias, [])
            if target_id not in targets:
                targets.append(target_id)
        return index

    def prune_aliases(
        self, group_id: str, speaker_id: str, target_id: str, limit: int
    ) -> None:
        if limit <= 0:
            return
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM alias_map
                WHERE speaker_id=? AND target_id=? AND alias IN (
                    SELECT alias FROM (
                        SELECT alias,
                               ROW_NUMBER() OVER (
                                   ORDER BY confidence DESC, updated_at DESC
                               ) AS rn
                        FROM alias_map
                        WHERE speaker_id=? AND target_id=?
                    ) ranked
                    WHERE rn > ?
                )
                """,
                (
                    speaker_id,
                    target_id,
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

    @staticmethod
    def _load_dict(value: str | None) -> dict[str, float]:
        if not value:
            return {}
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(data, dict):
            result: dict[str, float] = {}
            for key, val in data.items():
                try:
                    result[str(key)] = float(val)
                except (TypeError, ValueError):
                    continue
            return result
        return {}
