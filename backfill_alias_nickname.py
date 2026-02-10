from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

PLUGIN_NAME = "astrbot_plugin_auto_impression_card"


def resolve_db_path(db_path: str | None) -> Path:
    if db_path:
        return Path(db_path).expanduser().resolve()
    try:
        from astrbot.core.utils.path_utils import get_astrbot_plugin_data_path
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime env only
        raise RuntimeError(
            "astrbot is not available. Please pass --db with the impressions.db path."
        ) from exc
    data_dir = Path(get_astrbot_plugin_data_path()) / PLUGIN_NAME
    return data_dir / "impressions.db"


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def add_nickname_column(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE alias_map ADD COLUMN nickname TEXT")


def backfill_nickname(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        UPDATE alias_map
        SET nickname = (
            SELECT p.nickname
            FROM profiles p
            WHERE p.group_id = alias_map.group_id
              AND p.user_id = alias_map.target_id
        )
        WHERE (nickname IS NULL OR nickname = "")
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
    return cur.rowcount


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
    return [row["name"] for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill alias_map.nickname from profiles.nickname",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default=None,
        help="Path to impressions.db (defaults to plugin data directory)",
    )
    args = parser.parse_args()

    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, "alias_map"):
            tables = ", ".join(list_tables(conn)) or "(none)"
            raise RuntimeError(
                "alias_map table not found. This likely isn't the impressions.db for "
                f"astrbot_plugin_auto_impression_card. Tables in DB: {tables}"
            )
        if not has_column(conn, "alias_map", "nickname"):
            add_nickname_column(conn)
        updated = backfill_nickname(conn)
        conn.commit()
    finally:
        conn.close()

    print(f"Updated alias_map.nickname rows: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
