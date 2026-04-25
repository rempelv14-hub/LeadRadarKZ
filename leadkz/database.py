from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from .filters import NICHE_LABELS, detect_city, normalize_text
from .sales import DEFAULT_CASES, contains_segment, numeric_budget


@dataclass(frozen=True)
class Lead:
    chat_id: int
    message_id: int
    chat_title: str
    chat_username: str | None
    sender_id: int | None
    sender_username: str | None
    text: str
    score: int
    category: str
    geo_score: int
    reasons: str
    message_date: str
    link: str | None
    status: str
    lead_hash: str | None = None
    duplicate_count: int = 0


@dataclass(frozen=True)
class RejectedMessage:
    chat_id: int
    message_id: int
    chat_title: str
    chat_username: str | None
    sender_id: int | None
    sender_username: str | None
    text: str
    score: int
    category: str
    reasons: str
    message_date: str
    link: str | None
    created_at: str


@dataclass(frozen=True)
class GroupCandidate:
    chat_id: int
    title: str
    username: str | None
    query: str
    kind: str
    discovered_at: str
    status: str
    geo_score: int = 0
    group_score: int = 0
    validation_status: str = "unchecked"
    evaluation_reasons: str = ""
    lead_hits: int = 0
    spam_hits: int = 0
    developer_hits: int = 0
    recent_messages: int = 0
    checked_at: str | None = None
    last_error: str | None = None


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.init()

    def init(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                sender_id INTEGER,
                sender_username TEXT,
                text TEXT NOT NULL,
                score INTEGER NOT NULL,
                reasons TEXT NOT NULL,
                message_date TEXT NOT NULL,
                link TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS group_candidates (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                query TEXT NOT NULL,
                kind TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'candidate'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS runtime_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lead_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lead_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                remind_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL
            )
        """)
        self.conn.commit()
        self._migrate()
        self.ensure_default_cases()

    def _migrate(self) -> None:
        self._ensure_column("leads", "category", "TEXT NOT NULL DEFAULT 'unknown'")
        self._ensure_column("leads", "geo_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("leads", "lead_hash", "TEXT")
        self._ensure_column("leads", "duplicate_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("leads", "updated_at", "TEXT")
        self._ensure_column("group_candidates", "geo_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "updated_at", "TEXT")
        self._ensure_column("group_candidates", "group_score", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "validation_status", "TEXT NOT NULL DEFAULT 'unchecked'")
        self._ensure_column("group_candidates", "evaluation_reasons", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("group_candidates", "lead_hits", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "spam_hits", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "developer_hits", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "recent_messages", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("group_candidates", "checked_at", "TEXT")
        self._ensure_column("group_candidates", "last_error", "TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_message_date ON leads(message_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_hash ON leads(lead_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_groups_status ON group_candidates(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_groups_score ON group_candidates(group_score)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_groups_validation ON group_candidates(validation_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_lead ON lead_notes(chat_id, message_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_reminders_due ON lead_reminders(status, remind_at)")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS filter_feedback (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                vote TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phrase TEXT NOT NULL,
                kind TEXT NOT NULL,
                weight INTEGER NOT NULL DEFAULT 10,
                created_at TEXT NOT NULL,
                UNIQUE(phrase, kind)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS deal_values (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rejected_messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                sender_id INTEGER,
                sender_username TEXT,
                text TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                category TEXT NOT NULL DEFAULT 'not_lead',
                reasons TEXT NOT NULL DEFAULT '',
                message_date TEXT NOT NULL,
                link TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rejected_created ON rejected_messages(created_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rejected_category ON rejected_messages(category)")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS case_library (
                key TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                area TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                score INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS setup_answers (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_learning_kind ON learning_keywords(kind)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_vote ON filter_feedback(vote)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_error_logs_created ON error_logs(created_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_notification_log_created ON notification_log(created_at)")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        existing = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            self.conn.commit()

    def add_lead(self, *, chat_id: int, message_id: int, chat_title: str, chat_username: str | None,
                 sender_id: int | None, sender_username: str | None, text: str, score: int,
                 category: str, geo_score: int, reasons: Iterable[str], message_date: datetime,
                 link: str | None, lead_hash: str | None) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        if lead_hash:
            existing = self.conn.execute(
                "SELECT chat_id, message_id FROM leads WHERE lead_hash = ? AND status != 'hidden' LIMIT 1",
                (lead_hash,),
            ).fetchone()
            if existing and (int(existing["chat_id"]) != chat_id or int(existing["message_id"]) != message_id):
                self.conn.execute(
                    "UPDATE leads SET duplicate_count = duplicate_count + 1, updated_at = ? WHERE chat_id = ? AND message_id = ?",
                    (now, existing["chat_id"], existing["message_id"]),
                )
                self.conn.commit()
                return False
        try:
            self.conn.execute("""
                INSERT INTO leads (
                    chat_id, message_id, chat_title, chat_username, sender_id, sender_username,
                    text, score, category, geo_score, reasons, message_date, link, status,
                    lead_hash, duplicate_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, 0, ?, ?)
            """, (
                chat_id, message_id, chat_title, chat_username, sender_id, sender_username,
                text, score, category, geo_score, " | ".join(reasons),
                message_date.astimezone(timezone.utc).isoformat(), link, lead_hash, now, now,
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_leads_since(self, since: datetime, limit: int = 10, include_archived: bool = False) -> list[Lead]:
        status_clause = "status != 'hidden'" if include_archived else "status NOT IN ('hidden', 'archived')"
        rows = self.conn.execute(f"""
            SELECT * FROM leads WHERE message_date >= ? AND {status_clause}
            ORDER BY message_date DESC, score DESC LIMIT ?
        """, (since.astimezone(timezone.utc).isoformat(), limit)).fetchall()
        return [self._row_to_lead(row) for row in rows]

    def get_favorite_leads(self, limit: int = 15) -> list[Lead]:
        rows = self.conn.execute("""
            SELECT * FROM leads WHERE status = 'favorite'
            ORDER BY message_date DESC, score DESC LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_lead(row) for row in rows]

    def get_archived_leads(self, limit: int = 15) -> list[Lead]:
        rows = self.conn.execute("""
            SELECT * FROM leads WHERE status = 'archived'
            ORDER BY updated_at DESC, message_date DESC LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_lead(row) for row in rows]

    def archive_old_leads(self, older_than_days: int = 7) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).isoformat()
        cur = self.conn.execute("""
            UPDATE leads SET status = 'archived', updated_at = ?
            WHERE message_date < ? AND status IN ('new', 'contacted', 'talking', 'lost')
        """, (datetime.now(timezone.utc).isoformat(), cutoff))
        self.conn.commit()
        return int(cur.rowcount or 0)

    def get_lead(self, chat_id: int, message_id: int) -> Optional[Lead]:
        row = self.conn.execute("SELECT * FROM leads WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)).fetchone()
        return self._row_to_lead(row) if row else None

    def get_export_rows_since(self, since: datetime, limit: int = 5000) -> list[sqlite3.Row]:
        return self.conn.execute("""
            SELECT * FROM leads WHERE message_date >= ? AND status != 'hidden'
            ORDER BY message_date DESC, score DESC LIMIT ?
        """, (since.astimezone(timezone.utc).isoformat(), limit)).fetchall()

    def set_lead_status(self, chat_id: int, message_id: int, status: str) -> None:
        self.conn.execute(
            "UPDATE leads SET status = ?, updated_at = ? WHERE chat_id = ? AND message_id = ?",
            (status, datetime.now(timezone.utc).isoformat(), chat_id, message_id),
        )
        self.conn.commit()

    def add_note(self, chat_id: int, message_id: int, note: str) -> None:
        self.conn.execute("""
            INSERT INTO lead_notes (chat_id, message_id, note, created_at) VALUES (?, ?, ?, ?)
        """, (chat_id, message_id, note, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def get_notes(self, chat_id: int, message_id: int, limit: int = 5) -> list[sqlite3.Row]:
        return self.conn.execute("""
            SELECT * FROM lead_notes WHERE chat_id = ? AND message_id = ? ORDER BY created_at DESC LIMIT ?
        """, (chat_id, message_id, limit)).fetchall()

    def add_reminder(self, chat_id: int, message_id: int, remind_at: datetime) -> None:
        self.conn.execute("""
            INSERT INTO lead_reminders (chat_id, message_id, remind_at, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
        """, (chat_id, message_id, remind_at.astimezone(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def get_pending_reminders(self, now: datetime) -> list[sqlite3.Row]:
        return self.conn.execute("""
            SELECT * FROM lead_reminders WHERE status = 'pending' AND remind_at <= ? ORDER BY remind_at ASC LIMIT 20
        """, (now.astimezone(timezone.utc).isoformat(),)).fetchall()

    def get_upcoming_reminders(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.conn.execute("""
            SELECT * FROM lead_reminders WHERE status = 'pending' ORDER BY remind_at ASC LIMIT ?
        """, (limit,)).fetchall()

    def mark_reminder_sent(self, reminder_id: int) -> None:
        self.conn.execute("UPDATE lead_reminders SET status = 'sent' WHERE id = ?", (reminder_id,))
        self.conn.commit()

    def add_group_candidate(self, *, chat_id: int, title: str, username: str | None, query: str, kind: str, geo_score: int = 0) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        try:
            self.conn.execute("""
                INSERT INTO group_candidates (chat_id, title, username, query, kind, discovered_at, status, geo_score, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'candidate', ?, ?)
            """, (chat_id, title, username, query, kind, now, geo_score, now))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            self.conn.execute("""
                UPDATE group_candidates
                SET title = ?, username = COALESCE(?, username), query = ?, kind = ?,
                    discovered_at = ?, geo_score = MAX(geo_score, ?), updated_at = ?
                WHERE chat_id = ?
            """, (title, username, query, kind, now, geo_score, now, chat_id))
            self.conn.commit()
            return False

    def get_group_candidates(self, limit: int = 10) -> list[GroupCandidate]:
        rows = self.conn.execute("""
            SELECT * FROM group_candidates WHERE status != 'hidden'
            ORDER BY CASE status WHEN 'priority' THEN 0 WHEN 'valid' THEN 1 WHEN 'active' THEN 2 WHEN 'candidate' THEN 3 WHEN 'blocked' THEN 4 ELSE 5 END,
                     group_score DESC, geo_score DESC, discovered_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_group(row) for row in rows]

    def set_group_status(self, chat_id: int, status: str) -> None:
        self.conn.execute("UPDATE group_candidates SET status = ?, updated_at = ? WHERE chat_id = ?", (status, datetime.now(timezone.utc).isoformat(), chat_id))
        self.conn.commit()


    def update_group_evaluation(self, *, chat_id: int, score: int, validation_status: str, reasons: Iterable[str],
                                lead_hits: int = 0, spam_hits: int = 0, developer_hits: int = 0,
                                recent_messages: int = 0, status: str | None = None, last_error: str | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        if status is None:
            self.conn.execute("""
                UPDATE group_candidates
                SET group_score = ?, validation_status = ?, evaluation_reasons = ?, lead_hits = ?,
                    spam_hits = ?, developer_hits = ?, recent_messages = ?, checked_at = ?,
                    last_error = ?, updated_at = ?
                WHERE chat_id = ?
            """, (int(score), validation_status, " | ".join(reasons), int(lead_hits), int(spam_hits),
                  int(developer_hits), int(recent_messages), now, last_error, now, chat_id))
        else:
            self.conn.execute("""
                UPDATE group_candidates
                SET group_score = ?, validation_status = ?, evaluation_reasons = ?, lead_hits = ?,
                    spam_hits = ?, developer_hits = ?, recent_messages = ?, checked_at = ?,
                    last_error = ?, status = ?, updated_at = ?
                WHERE chat_id = ?
            """, (int(score), validation_status, " | ".join(reasons), int(lead_hits), int(spam_hits),
                  int(developer_hits), int(recent_messages), now, last_error, status, now, chat_id))
        self.conn.commit()

    def mark_group_error(self, chat_id: int, error: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            UPDATE group_candidates SET last_error = ?, checked_at = ?, updated_at = ? WHERE chat_id = ?
        """, (error[:500], now, now, chat_id))
        self.conn.commit()

    def get_valid_group_candidates(self, limit: int = 50) -> list[GroupCandidate]:
        rows = self.conn.execute("""
            SELECT * FROM group_candidates
            WHERE status IN ('valid', 'priority', 'active') AND username IS NOT NULL
            ORDER BY CASE status WHEN 'priority' THEN 0 WHEN 'valid' THEN 1 WHEN 'active' THEN 2 ELSE 3 END,
                     group_score DESC, checked_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_group(row) for row in rows]

    def get_best_auto_groups(self, limit: int = 15) -> list[GroupCandidate]:
        rows = self.conn.execute("""
            SELECT * FROM group_candidates
            WHERE status NOT IN ('hidden', 'blocked') AND validation_status IN ('valid', 'strong')
            ORDER BY group_score DESC, lead_hits DESC, recent_messages DESC, checked_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_group(row) for row in rows]

    def get_trash_auto_groups(self, limit: int = 15) -> list[GroupCandidate]:
        rows = self.conn.execute("""
            SELECT * FROM group_candidates
            WHERE status NOT IN ('priority') AND validation_status IN ('trash', 'weak', 'error')
            ORDER BY group_score ASC, spam_hits DESC, developer_hits DESC, checked_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_group(row) for row in rows]

    def get_group_status(self, chat_id: int) -> str:
        row = self.conn.execute("SELECT status FROM group_candidates WHERE chat_id = ?", (chat_id,)).fetchone()
        return str(row["status"]) if row else "candidate"

    def is_group_blocked(self, chat_id: int) -> bool:
        return self.get_group_status(chat_id) in {"blocked", "hidden"}

    def get_group_ratings(self, since: datetime, limit: int = 10) -> list[dict[str, object]]:
        since_iso = since.astimezone(timezone.utc).isoformat()
        rows = self.conn.execute("""
            SELECT chat_id, chat_title, chat_username,
                   COUNT(*) AS leads_count,
                   SUM(CASE WHEN score >= 80 THEN 1 ELSE 0 END) AS hot_count,
                   AVG(score) AS avg_score,
                   COALESCE(SUM(duplicate_count), 0) AS duplicates
            FROM leads
            WHERE message_date >= ? AND status != 'hidden'
            GROUP BY chat_id, chat_title, chat_username
            ORDER BY hot_count DESC, leads_count DESC, avg_score DESC
            LIMIT ?
        """, (since_iso, limit)).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            leads_count = int(row["leads_count"] or 0)
            hot_count = int(row["hot_count"] or 0)
            avg_score = float(row["avg_score"] or 0)
            duplicates = int(row["duplicates"] or 0)
            quality = min(100, int(leads_count * 12 + hot_count * 18 + avg_score * 0.55 - duplicates * 3))
            result.append({
                "chat_id": int(row["chat_id"]),
                "title": row["chat_title"],
                "username": row["chat_username"],
                "leads_count": leads_count,
                "hot_count": hot_count,
                "avg_score": round(avg_score, 1),
                "duplicates": duplicates,
                "quality": max(0, quality),
            })
        return result

    def get_bad_group_recommendations(self, days: int = 7, limit: int = 10) -> list[dict[str, object]]:
        since_iso = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self.conn.execute("""
            SELECT g.chat_id, g.title, g.username, g.status, g.geo_score,
                   COUNT(l.message_id) AS leads_count,
                   AVG(l.score) AS avg_score
            FROM group_candidates g
            LEFT JOIN leads l ON l.chat_id = g.chat_id AND l.message_date >= ? AND l.status != 'hidden'
            WHERE g.status != 'blocked'
            GROUP BY g.chat_id
            ORDER BY leads_count ASC, avg_score ASC, g.geo_score ASC
            LIMIT ?
        """, (since_iso, limit)).fetchall()
        result = []
        for row in rows:
            leads = int(row["leads_count"] or 0)
            avg = float(row["avg_score"] or 0)
            reason = "0 лидов за 7 дней" if leads == 0 else f"низкий средний балл {avg:.1f}"
            result.append({"chat_id": row["chat_id"], "title": row["title"], "username": row["username"], "leads_count": leads, "avg_score": round(avg, 1), "reason": reason})
        return result

    def get_top_group_titles(self, since: datetime, limit: int = 5) -> list[str]:
        return [str(row["chat_title"]) for row in self.conn.execute("""
            SELECT chat_title, COUNT(*) AS c FROM leads WHERE message_date >= ? AND status != 'hidden'
            GROUP BY chat_title ORDER BY c DESC LIMIT ?
        """, (since.astimezone(timezone.utc).isoformat(), limit)).fetchall()]

    def get_stats_since(self, since: datetime) -> dict[str, object]:
        since_iso = since.astimezone(timezone.utc).isoformat()
        one = lambda sql: self.conn.execute(sql, (since_iso,)).fetchone()["c"]
        total = one("SELECT COUNT(*) AS c FROM leads WHERE message_date >= ? AND status != 'hidden'")
        hot = one("SELECT COUNT(*) AS c FROM leads WHERE message_date >= ? AND score >= 80 AND status != 'hidden'")
        favorite = one("SELECT COUNT(*) AS c FROM leads WHERE message_date >= ? AND status = 'favorite'")
        archived = one("SELECT COUNT(*) AS c FROM leads WHERE message_date >= ? AND status = 'archived'")
        duplicates = one("SELECT COALESCE(SUM(duplicate_count), 0) AS c FROM leads WHERE message_date >= ?")
        top_groups = self.conn.execute("""
            SELECT chat_title, COUNT(*) AS c FROM leads WHERE message_date >= ? AND status != 'hidden'
            GROUP BY chat_title ORDER BY c DESC LIMIT 5
        """, (since_iso,)).fetchall()
        statuses = self.conn.execute("SELECT status, COUNT(*) AS c FROM leads WHERE message_date >= ? GROUP BY status ORDER BY c DESC", (since_iso,)).fetchall()
        categories = self.conn.execute("""
            SELECT category, COUNT(*) AS c FROM leads WHERE message_date >= ? AND status != 'hidden'
            GROUP BY category ORDER BY c DESC LIMIT 7
        """, (since_iso,)).fetchall()
        city_rows = self.conn.execute("SELECT text, chat_title FROM leads WHERE message_date >= ? AND status != 'hidden'", (since_iso,)).fetchall()
        city_counter: dict[str, int] = {}
        for row in city_rows:
            city = detect_city(f"{row['text']} {row['chat_title']}")
            city_counter[city] = city_counter.get(city, 0) + 1
        return {
            "total": int(total), "hot": int(hot), "favorite": int(favorite), "archived": int(archived), "duplicates": int(duplicates),
            "top_groups": [(row["chat_title"], int(row["c"])) for row in top_groups],
            "statuses": [(row["status"], int(row["c"])) for row in statuses],
            "categories": [(row["category"], int(row["c"])) for row in categories],
            "city_counts": sorted(city_counter.items(), key=lambda item: item[1], reverse=True)[:7],
        }

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM runtime_settings WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str | int | bool) -> None:
        stored = "1" if value is True else "0" if value is False else str(value)
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            INSERT INTO runtime_settings (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """, (key, stored, now))
        self.conn.commit()

    def get_setting_bool(self, key: str, default: bool = False) -> bool:
        value = self.get_setting(key)
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "да", "on"}

    def get_setting_int(self, key: str, default: int) -> int:
        value = self.get_setting(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def get_all_settings(self) -> dict[str, str]:
        rows = self.conn.execute("SELECT key, value FROM runtime_settings ORDER BY key").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def import_settings(self, values: dict[str, object]) -> int:
        count = 0
        for key, value in values.items():
            if isinstance(key, str) and key:
                self.set_setting(key, value if isinstance(value, (str, int, bool)) else str(value))
                count += 1
        return count

    def get_enabled_niches(self) -> set[str]:
        enabled = set()
        for key in NICHE_LABELS:
            if self.get_setting_bool(f"niche_{key}", True):
                enabled.add(key)
        return enabled

    @staticmethod
    def _row_to_lead(row: sqlite3.Row) -> Lead:
        return Lead(
            chat_id=row["chat_id"], message_id=row["message_id"], chat_title=row["chat_title"],
            chat_username=row["chat_username"], sender_id=row["sender_id"], sender_username=row["sender_username"],
            text=row["text"], score=row["score"], category=row["category"], geo_score=row["geo_score"],
            reasons=row["reasons"], message_date=row["message_date"], link=row["link"], status=row["status"],
            lead_hash=row["lead_hash"], duplicate_count=row["duplicate_count"],
        )

    @staticmethod
    def _row_to_group(row: sqlite3.Row) -> GroupCandidate:
        return GroupCandidate(
            chat_id=row["chat_id"], title=row["title"], username=row["username"], query=row["query"],
            kind=row["kind"], discovered_at=row["discovered_at"], status=row["status"], geo_score=row["geo_score"],
            group_score=row["group_score"], validation_status=row["validation_status"],
            evaluation_reasons=row["evaluation_reasons"], lead_hits=row["lead_hits"], spam_hits=row["spam_hits"],
            developer_hits=row["developer_hits"], recent_messages=row["recent_messages"],
            checked_at=row["checked_at"], last_error=row["last_error"],
        )

    def add_feedback(self, chat_id: int, message_id: int, vote: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            INSERT INTO filter_feedback (chat_id, message_id, vote, created_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET vote = excluded.vote, created_at = excluded.created_at
        """, (chat_id, message_id, vote, now))
        self.conn.commit()


    def add_rejected_message(self, *, chat_id: int, message_id: int, chat_title: str, chat_username: str | None,
                             sender_id: int | None, sender_username: str | None, text: str, score: int,
                             category: str, reasons: Iterable[str], message_date: datetime, link: str | None) -> None:
        """Сохраняет скрытые/отклонённые сообщения, чтобы можно было проверить фильтр."""
        if not text or len(text.strip()) < 8:
            return
        now = datetime.now(timezone.utc).isoformat()
        try:
            self.conn.execute("""
                INSERT INTO rejected_messages (
                    chat_id, message_id, chat_title, chat_username, sender_id, sender_username,
                    text, score, category, reasons, message_date, link, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                chat_id, message_id, chat_title, chat_username, sender_id, sender_username,
                text[:2000], int(score), category, " | ".join(reasons),
                message_date.astimezone(timezone.utc).isoformat(), link, now,
            ))
            self.conn.commit()
        except sqlite3.IntegrityError:
            return

    def get_rejected_messages(self, limit: int = 12) -> list[RejectedMessage]:
        rows = self.conn.execute("""
            SELECT * FROM rejected_messages
            ORDER BY message_date DESC, created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_rejected(row) for row in rows]

    def delete_rejected_message(self, chat_id: int, message_id: int) -> None:
        self.conn.execute("DELETE FROM rejected_messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        self.conn.commit()

    def restore_rejected_as_lead(self, chat_id: int, message_id: int, min_score: int = 55) -> bool:
        row = self.conn.execute("SELECT * FROM rejected_messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)).fetchone()
        if not row:
            return False
        try:
            msg_date = datetime.fromisoformat(str(row["message_date"]))
        except Exception:
            msg_date = datetime.now(timezone.utc)
        inserted = self.add_lead(
            chat_id=int(row["chat_id"]),
            message_id=int(row["message_id"]),
            chat_title=str(row["chat_title"]),
            chat_username=row["chat_username"],
            sender_id=row["sender_id"],
            sender_username=row["sender_username"],
            text=str(row["text"]),
            score=max(int(row["score"] or 0), min_score),
            category=str(row["category"] or "restored"),
            geo_score=0,
            reasons=[str(row["reasons"] or "вернул вручную как лид")],
            message_date=msg_date,
            link=row["link"],
            lead_hash=None,
        )
        if inserted:
            self.delete_rejected_message(chat_id, message_id)
        return inserted

    @staticmethod
    def _row_to_rejected(row: sqlite3.Row) -> RejectedMessage:
        return RejectedMessage(
            chat_id=row["chat_id"], message_id=row["message_id"], chat_title=row["chat_title"],
            chat_username=row["chat_username"], sender_id=row["sender_id"], sender_username=row["sender_username"],
            text=row["text"], score=row["score"], category=row["category"], reasons=row["reasons"],
            message_date=row["message_date"], link=row["link"], created_at=row["created_at"],
        )

    def get_feedback_summary(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT vote, COUNT(*) AS c FROM filter_feedback GROUP BY vote").fetchall()
        return {str(row["vote"]): int(row["c"]) for row in rows}

    def add_learning_keyword(self, kind: str, phrase: str, weight: int | None = None) -> None:
        kind = kind.strip().lower()
        phrase = normalize_text(phrase)
        if not phrase or kind not in {"positive", "negative", "spam", "geo", "niche"}:
            return
        if weight is None:
            weight = {"positive": 12, "negative": 18, "spam": 80, "geo": 15, "niche": 8}.get(kind, 10)
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            INSERT INTO learning_keywords (phrase, kind, weight, created_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(phrase, kind) DO UPDATE SET weight = excluded.weight, created_at = excluded.created_at
        """, (phrase, kind, int(weight), now))
        self.conn.commit()

    def get_learning_keywords(self, kind: str | None = None) -> list[sqlite3.Row]:
        if kind:
            return self.conn.execute("SELECT * FROM learning_keywords WHERE kind = ? ORDER BY created_at DESC", (kind,)).fetchall()
        return self.conn.execute("SELECT * FROM learning_keywords ORDER BY kind, created_at DESC").fetchall()

    def apply_learning_to_text(self, text: str) -> tuple[int, list[str], bool, list[str]]:
        cleaned = normalize_text(text)
        delta = 0
        reasons: list[str] = []
        spam = False
        geo_hits: list[str] = []
        for row in self.get_learning_keywords():
            phrase = str(row["phrase"])
            if phrase and phrase in cleaned:
                kind = str(row["kind"])
                weight = int(row["weight"] or 0)
                if kind == "positive":
                    delta += weight
                    reasons.append(f"твое плюс-слово: {phrase}")
                elif kind == "negative":
                    delta -= weight
                    reasons.append(f"твое минус-слово: {phrase}")
                elif kind == "spam":
                    spam = True
                    reasons.append(f"твое спам-слово: {phrase}")
                elif kind == "geo":
                    delta += min(weight, 15)
                    geo_hits.append(phrase)
        return delta, reasons, spam, geo_hits[:5]

    def get_priority_queue(self, limit: int = 15) -> list[Lead]:
        rows = self.conn.execute("""
            SELECT *,
                CASE
                    WHEN message_date >= datetime('now', '-1 hour') THEN 20
                    WHEN message_date >= datetime('now', '-6 hours') THEN 10
                    ELSE 0
                END AS freshness_bonus
            FROM leads
            WHERE status NOT IN ('hidden', 'archived', 'closed', 'lost')
            ORDER BY
                CASE status WHEN 'new' THEN 0 WHEN 'favorite' THEN 1 WHEN 'talking' THEN 2 WHEN 'contacted' THEN 3 ELSE 4 END,
                (score + freshness_bonus) DESC,
                score DESC,
                geo_score DESC,
                message_date DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [self._row_to_lead(row) for row in rows]

    def get_segment_leads(self, segment: str, since: datetime, limit: int = 15) -> list[Lead]:
        rows = self.conn.execute("""
            SELECT * FROM leads WHERE message_date >= ? AND status NOT IN ('hidden', 'archived')
            ORDER BY message_date DESC, score DESC LIMIT 300
        """, (since.astimezone(timezone.utc).isoformat(),)).fetchall()
        result: list[Lead] = []
        for row in rows:
            text = f"{row['text']} {row['category']} {row['chat_title']}"
            if contains_segment(text, segment):
                result.append(self._row_to_lead(row))
                if len(result) >= limit:
                    break
        return result

    def get_funnel(self, since: datetime) -> dict[str, int]:
        rows = self.conn.execute("""
            SELECT status, COUNT(*) AS c FROM leads WHERE message_date >= ? GROUP BY status
        """, (since.astimezone(timezone.utc).isoformat(),)).fetchall()
        result = {str(row["status"]): int(row["c"]) for row in rows}
        for key in ["new", "favorite", "contacted", "talking", "waiting_payment", "closed", "lost"]:
            result.setdefault(key, 0)
        return result

    def set_deal_amount(self, chat_id: int, message_id: int, amount: int, note: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            INSERT INTO deal_values (chat_id, message_id, amount, note, created_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET amount = excluded.amount, note = excluded.note, created_at = excluded.created_at
        """, (chat_id, message_id, int(amount), note, now))
        self.conn.commit()

    def get_revenue_stats(self, since: datetime) -> dict[str, object]:
        rows = self.conn.execute("""
            SELECT l.status, COALESCE(d.amount, 0) AS amount, l.text, l.category
            FROM leads l LEFT JOIN deal_values d ON d.chat_id = l.chat_id AND d.message_id = l.message_id
            WHERE l.message_date >= ? AND l.status != 'hidden'
        """, (since.astimezone(timezone.utc).isoformat(),)).fetchall()
        potential = 0
        closed = 0
        deals = 0
        for row in rows:
            amount = int(row["amount"] or 0) or numeric_budget(str(row["text"]), str(row["category"]))
            if row["status"] in {"new", "favorite", "contacted", "talking", "waiting_payment"}:
                potential += amount
            if row["status"] == "closed":
                closed += amount
                deals += 1
        avg = int(closed / deals) if deals else 0
        return {"potential": potential, "closed": closed, "deals": deals, "avg_check": avg}

    def ensure_default_cases(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        for key, title, category, text in DEFAULT_CASES:
            self.conn.execute("""
                INSERT OR IGNORE INTO case_library (key, title, category, text, created_at) VALUES (?, ?, ?, ?, ?)
            """, (key, title, category, text, now))
        self.conn.commit()

    def get_cases(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM case_library ORDER BY title").fetchall()

    def get_case_by_key(self, key: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM case_library WHERE key = ?", (key,)).fetchone()


    def log_error(self, area: str, message: str) -> None:
        self.conn.execute(
            "INSERT INTO error_logs (area, message, created_at) VALUES (?, ?, ?)",
            (str(area)[:120], str(message)[:1500], datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def get_recent_errors(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM error_logs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

    def add_notification_log(self, chat_id: int | None, message_id: int | None, score: int) -> None:
        self.conn.execute(
            "INSERT INTO notification_log (chat_id, message_id, score, created_at) VALUES (?, ?, ?, ?)",
            (chat_id, message_id, int(score), datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def get_notifications_count_since(self, since: datetime) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM notification_log WHERE created_at >= ?",
            (since.astimezone(timezone.utc).isoformat(),),
        ).fetchone()
        return int(row["c"] or 0) if row else 0

    def cleanup_old_data(self, *, spam_days: int = 7, archive_days: int = 30, errors_days: int = 14, notifications_days: int = 7) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        spam_cutoff = (now - timedelta(days=spam_days)).isoformat()
        archive_cutoff = (now - timedelta(days=archive_days)).isoformat()
        errors_cutoff = (now - timedelta(days=errors_days)).isoformat()
        notifications_cutoff = (now - timedelta(days=notifications_days)).isoformat()
        deleted_hidden = self.conn.execute(
            "DELETE FROM leads WHERE status = 'hidden' AND message_date < ?",
            (spam_cutoff,),
        ).rowcount or 0
        archived = self.conn.execute(
            "UPDATE leads SET status = 'archived', updated_at = ? WHERE message_date < ? AND status IN ('new','contacted','talking','lost')",
            (now.isoformat(), archive_cutoff),
        ).rowcount or 0
        deleted_errors = self.conn.execute("DELETE FROM error_logs WHERE created_at < ?", (errors_cutoff,)).rowcount or 0
        deleted_notifications = self.conn.execute("DELETE FROM notification_log WHERE created_at < ?", (notifications_cutoff,)).rowcount or 0
        self.conn.commit()
        return {
            "deleted_hidden": int(deleted_hidden),
            "archived": int(archived),
            "deleted_errors": int(deleted_errors),
            "deleted_notifications": int(deleted_notifications),
        }


    def auto_disable_bad_groups(self, days: int = 7, min_group_score: int = 45) -> int:
        """Скрывает слабые группы, которые не дают лидов и имеют низкий group_score."""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self.conn.execute("""
            SELECT gc.chat_id, gc.group_score, COUNT(l.message_id) AS leads_count
            FROM group_candidates gc
            LEFT JOIN leads l ON l.chat_id = gc.chat_id AND l.message_date >= ?
            WHERE gc.status IN ('candidate', 'valid', 'active')
            GROUP BY gc.chat_id, gc.group_score
            HAVING leads_count = 0 AND COALESCE(gc.group_score, 0) < ?
            LIMIT 50
        """, (since, int(min_group_score))).fetchall()
        count = 0
        for row in rows:
            self.set_group_status(int(row["chat_id"]), "hidden")
            count += 1
        return count

    def get_top_words_from_feedback(self, vote: str, limit: int = 12) -> list[tuple[str, int]]:
        rows = self.conn.execute("""
            SELECT l.text FROM leads l
            JOIN filter_feedback f ON f.chat_id = l.chat_id AND f.message_id = l.message_id
            WHERE f.vote = ?
            ORDER BY f.created_at DESC LIMIT 300
        """, (vote,)).fetchall()
        stop = {
            "что","это","для","как","кто","мне","нам","вам","нужно","нужен","нужна","есть","или",
            "the","and","with","надо","можно","где","уже","все","меня","чтобы","через","сделать"
        }
        counts: dict[str, int] = {}
        for row in rows:
            for raw in normalize_text(str(row["text"])).split():
                word = raw.strip(".,!?;:()[]{}<>«»\"'")
                if len(word) < 4 or word in stop or word.isdigit():
                    continue
                counts[word] = counts.get(word, 0) + 1
        return sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]

    def get_keyword_suggestions(self, limit: int = 8) -> list[tuple[str, str, int]]:
        good = dict(self.get_top_words_from_feedback("good", limit=50))
        bad = dict(self.get_top_words_from_feedback("bad", limit=50))
        suggestions: list[tuple[str, str, int]] = []
        existing = {(str(r["phrase"]), str(r["kind"])) for r in self.get_learning_keywords()}
        for word, count in good.items():
            if count >= 2 and bad.get(word, 0) == 0 and (word, "positive") not in existing:
                suggestions.append((word, "positive", min(100, count * 20)))
        for word, count in bad.items():
            if count >= 2 and good.get(word, 0) == 0 and (word, "negative") not in existing:
                suggestions.append((word, "negative", min(100, count * 20)))
        return suggestions[:limit]

    def close(self) -> None:
        self.conn.close()
