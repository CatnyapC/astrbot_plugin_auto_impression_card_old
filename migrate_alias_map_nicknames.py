from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


PLUGIN_NAME = "astrbot_plugin_auto_impression_card"


DESIRED_SCHEMA = """
CREATE TABLE alias_map (
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


DESIRED_INDEX = """
CREATE INDEX idx_alias_lookup
ON alias_map (group_id, speaker_id, alias)
"""


def resolve_db_path(db_path: str | None) -> Path:
    if not db_path:
        raise RuntimeError("--db is required for this migration script")
    return Path(db_path).expanduser().resolve()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


def get_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def rename_alias_map(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE alias_map RENAME TO alias_map_old")


def create_alias_map(conn: sqlite3.Connection) -> None:
    conn.execute(DESIRED_SCHEMA)
    conn.execute("DROP INDEX IF EXISTS idx_alias_lookup")
    conn.execute(DESIRED_INDEX)


def copy_alias_map(
    conn: sqlite3.Connection,
    *,
    has_nickname: bool,
    has_speaker: bool,
    has_target: bool,
) -> None:
    speaker_select = "speaker_nickname" if has_speaker else "NULL"
    target_select = "target_nickname" if has_target else "NULL"
    evidence_select = "evidence_text" if "evidence_text" in get_columns(conn, "alias_map_old") else "NULL"
    if has_nickname:
        target_select = "nickname"
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
            SELECT
                group_id,
                speaker_id,
                {speaker_select},
                alias,
                target_id,
                {target_select},
                {evidence_select},
                confidence,
                updated_at
            FROM alias_map_old
            """
            .format(
                speaker_select=speaker_select,
                target_select=target_select,
                evidence_select=evidence_select,
            )
        )
        return

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
        SELECT
            group_id,
            speaker_id,
            {speaker_select},
            alias,
            target_id,
            {target_select},
            {evidence_select},
            confidence,
            updated_at
        FROM alias_map_old
        """
        .format(
            speaker_select=speaker_select,
            target_select=target_select,
            evidence_select=evidence_select,
        )
    )


def drop_old_alias_map(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE alias_map_old")


def add_column(conn: sqlite3.Connection, column: str) -> None:
    conn.execute(f"ALTER TABLE alias_map ADD COLUMN {column} TEXT")


def backfill_nicknames(conn: sqlite3.Connection) -> tuple[int, int]:
    cur_target = conn.execute(
        """
        UPDATE alias_map
        SET target_nickname = (
            SELECT p.nickname
            FROM profiles p
            WHERE p.group_id = alias_map.group_id
              AND p.user_id = alias_map.target_id
        )
        WHERE (target_nickname IS NULL OR target_nickname = "")
          AND EXISTS (
            SELECT 1
            FROM profiles p
            WHERE p.group_id = alias_map.group_id
              AND p.user_id = alias_map.target_id
              AND p.nickname IS NOT NULL
              AND p.nickname != ""
        )
        """
    )
    cur_speaker = conn.execute(
        """
        UPDATE alias_map
        SET speaker_nickname = (
            SELECT p.nickname
            FROM profiles p
            WHERE p.group_id = alias_map.group_id
              AND p.user_id = alias_map.speaker_id
        )
        WHERE (speaker_nickname IS NULL OR speaker_nickname = "")
          AND EXISTS (
            SELECT 1
            FROM profiles p
            WHERE p.group_id = alias_map.group_id
              AND p.user_id = alias_map.speaker_id
              AND p.nickname IS NOT NULL
              AND p.nickname != ""
        )
        """
    )
    return cur_target.rowcount, cur_speaker.rowcount


def migrate(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "alias_map"):
        tables = ", ".join(list_tables(conn)) or "(none)"
        raise RuntimeError(
            "alias_map table not found. Tables in DB: " + tables
        )

    columns = get_columns(conn, "alias_map")
    has_nickname = "nickname" in columns
    has_target = "target_nickname" in columns
    has_speaker = "speaker_nickname" in columns
    has_evidence = "evidence_text" in columns

    if has_nickname or (not has_target) or (not has_speaker):
        rename_alias_map(conn)
        create_alias_map(conn)
        copy_alias_map(
            conn,
            has_nickname=has_nickname,
            has_speaker=has_speaker,
            has_target=has_target,
        )
        drop_old_alias_map(conn)
    else:
        if not has_speaker:
            add_column(conn, "speaker_nickname")
        if not has_target:
            add_column(conn, "target_nickname")
        if not has_evidence:
            add_column(conn, "evidence_text")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate alias_map nickname columns and backfill nicknames",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default=None,
        help="Path to impressions.db",
    )
    args = parser.parse_args()

    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        migrate(conn)
        target_count, speaker_count = backfill_nicknames(conn)
        conn.commit()
    finally:
        conn.close()

    print(
        "Backfilled nicknames: "
        f"target_nickname={target_count}, "
        f"speaker_nickname={speaker_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
