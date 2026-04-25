from __future__ import annotations

import html
import os
import platform
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .filters import detect_city


def _ok(value: bool) -> str:
    return "✅" if value else "❌"


def _size(path: Path) -> str:
    try:
        n = path.stat().st_size
    except OSError:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    try:
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return 0
        if isinstance(row, sqlite3.Row):
            return int(row[0] or 0)
        return int(row[0] or 0)
    except Exception:
        return 0


def format_check_report(db, settings, telethon_authorized: bool | None = None) -> str:
    """Команда /check: короткая проверка, можно ли запускать на Railway/локально."""
    db_ok = False
    try:
        db.conn.execute("SELECT 1").fetchone()
        db_ok = True
    except Exception:
        pass

    session_file = Path(str(settings.tg_session) + ".session")
    if not session_file.exists():
        session_file = settings.database_path.parent.parent / (str(settings.tg_session) + ".session")
    has_session = bool(settings.tg_session_string) or session_file.exists()
    volume_ok = str(settings.database_path.parent).replace("\\", "/").endswith("/data") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("DATA_DIR") == "/data"

    rows = [
        "✅ <b>Проверка LeadKZ Free v8</b>\n",
        f"{_ok(bool(settings.bot_token))} BOT_TOKEN заполнен",
        f"{_ok(bool(settings.admin_id))} ADMIN_ID заполнен: <code>{settings.admin_id}</code>",
        f"{_ok(bool(settings.api_id and settings.api_hash))} API_ID/API_HASH заполнены",
        f"{_ok(has_session)} Telegram-сессия найдена",
        f"{_ok(db_ok)} SQLite база доступна",
        f"{_ok(settings.auto_discovery_enabled)} Автопоиск групп включён",
        f"{_ok(settings.geo_only_kazakhstan)} Гео Казахстан включён",
        f"{_ok(settings.only_valid_groups)} Режим только валидных групп",
        f"{_ok(volume_ok)} Railway Volume: {'/data' if volume_ok else 'локальный режим / не подключён'}",
    ]
    if telethon_authorized is not None:
        rows.append(f"{_ok(telethon_authorized)} Telethon авторизован")
    rows += [
        "",
        f"База: <code>{html.escape(str(settings.database_path))}</code>",
        f"Размер базы: <b>{_size(settings.database_path)}</b>",
        f"Python: <code>{platform.python_version()}</code>",
    ]
    return "\n".join(rows)


def format_diagnostics(db, settings, app_started_at: datetime) -> str:
    now = datetime.now(timezone.utc)
    since24 = (now - timedelta(hours=24)).isoformat()
    groups_total = _one(db.conn, "SELECT COUNT(*) FROM group_candidates")
    groups_valid = _one(db.conn, "SELECT COUNT(*) FROM group_candidates WHERE status IN ('valid','priority','active')")
    groups_trash = _one(db.conn, "SELECT COUNT(*) FROM group_candidates WHERE validation_status IN ('trash','weak','error')")
    leads24 = _one(db.conn, "SELECT COUNT(*) FROM leads WHERE message_date >= ? AND status != 'hidden'", (since24,))
    hot24 = _one(db.conn, "SELECT COUNT(*) FROM leads WHERE message_date >= ? AND score >= 80 AND status != 'hidden'", (since24,))
    errors24 = _one(db.conn, "SELECT COUNT(*) FROM error_logs WHERE created_at >= ?", (since24,))
    notifications24 = _one(db.conn, "SELECT COUNT(*) FROM notification_log WHERE created_at >= ?", (since24,))
    uptime = datetime.now(timezone.utc) - app_started_at

    return (
        "🛠 <b>Диагностика LeadKZ</b>\n\n"
        f"Статус: <b>работает</b>\n"
        f"Uptime: <b>{str(uptime).split('.')[0]}</b>\n"
        f"Лидов за 24ч: <b>{leads24}</b> | горячих: <b>{hot24}</b>\n"
        f"Групп найдено: <b>{groups_total}</b>\n"
        f"Валидных групп: <b>{groups_valid}</b>\n"
        f"Слабых/мусорных групп: <b>{groups_trash}</b>\n"
        f"Уведомлений за 24ч: <b>{notifications24}</b>\n"
        f"Ошибок за 24ч: <b>{errors24}</b>\n"
        f"Размер базы: <b>{_size(settings.database_path)}</b>\n"
        f"Автопоиск групп: {'✅' if settings.auto_discovery_enabled else '❌'}\n"
        f"Автобэкап: {'✅' if settings.autobackup_enabled else '❌'}\n"
        f"Web-панель: <code>http://127.0.0.1:{settings.health_port}</code> локально или домен Railway"
    )


def format_setup_wizard(db) -> str:
    geo = db.get_setting_bool("geo_only", True)
    hot = db.get_setting_bool("hot_only", False)
    valid = db.get_setting_bool("only_valid_groups", True)
    min_score = db.get_setting_int("min_score", 70)
    work = db.get_setting_bool("working_hours", True)
    quiet = db.get_setting_bool("quiet_mode", False)
    return (
        "🧩 <b>Мастер настройки</b>\n\n"
        "Настраивается кнопками. Файл .env трогать не нужно.\n\n"
        f"🇰🇿 Гео Казахстан: {'✅' if geo else '❌'}\n"
        f"✅ Только валидные группы: {'✅' if valid else '❌'}\n"
        f"🔥 Только горячие лиды 80+: {'✅' if hot else '❌'}\n"
        f"🎯 Минимальный балл: <b>{min_score}</b>\n"
        f"🕘 Рабочее время: {'✅' if work else '❌'}\n"
        f"🔕 Тихий режим: {'✅' if quiet else '❌'}\n\n"
        "Рекомендуемый старт: гео ✅, валидные группы ✅, минимальный балл 70, только горячие ❌."
    )


def format_error_logs(rows) -> str:
    if not rows:
        return "✅ Ошибок пока нет."
    parts = ["⚠️ <b>Последние ошибки</b>\n"]
    for i, row in enumerate(rows, start=1):
        parts.append(
            f"{i}. <b>{html.escape(str(row['area']))}</b> — {html.escape(str(row['created_at']))}\n"
            f"<code>{html.escape(str(row['message'])[:700])}</code>"
        )
    return "\n\n".join(parts)


def format_words_report(db) -> str:
    pos = db.get_top_words_from_feedback("good", limit=12)
    neg = db.get_top_words_from_feedback("bad", limit=12)
    suggestions = db.get_keyword_suggestions(limit=8)
    parts = ["🧠 <b>Лучшие и плохие слова</b>\n"]
    parts.append("<b>Чаще в хороших лидах:</b>")
    parts.extend([f"➕ {html.escape(word)} — {count}" for word, count in pos] or ["пока нет данных"])
    parts.append("\n<b>Чаще в плохих лидах:</b>")
    parts.extend([f"➖ {html.escape(word)} — {count}" for word, count in neg] or ["пока нет данных"])
    if suggestions:
        parts.append("\n<b>Рекомендации для автоулучшения:</b>")
        for word, kind, score in suggestions:
            label = "добавить в плюс" if kind == "positive" else "добавить в минус"
            parts.append(f"• {html.escape(word)} → {label} / уверенность {score}")
    parts.append("\nКоманды: <code>/learn_plus слово</code>, <code>/learn_minus слово</code>, <code>/learn_spam слово</code>")
    return "\n".join(parts)


def format_weekly_groups_report(rows) -> str:
    if not rows:
        return "✅ За неделю нет групп, которые явно нужно удалить."
    parts = ["🚫 <b>Недельная проверка групп</b>\n"]
    for i, row in enumerate(rows, start=1):
        username = row.get("username")
        link = f"https://t.me/{username}" if username else "нет публичной ссылки"
        parts.append(
            f"{i}. <b>{html.escape(str(row['title']))}</b>\n"
            f"Причина: {html.escape(str(row['reason']))}\n"
            f"Лидов: {row.get('leads_count', 0)} | Средний балл: {row.get('avg_score', 0)}\n"
            f"Ссылка: {html.escape(link)}"
        )
    return "\n\n".join(parts)


def build_dashboard_html(db, settings, app_started_at: datetime) -> str:
    stats = db.get_stats_since(datetime.now(timezone.utc) - timedelta(hours=24))
    groups_total = _one(db.conn, "SELECT COUNT(*) FROM group_candidates")
    groups_valid = _one(db.conn, "SELECT COUNT(*) FROM group_candidates WHERE status IN ('valid','priority','active')")
    leads = db.get_priority_queue(limit=20)
    rows = []
    for lead in leads:
        city = detect_city(f"{lead.text} {lead.chat_title}")
        text = html.escape(" ".join(lead.text.split())[:220])
        rows.append(f"<tr><td>{lead.score}</td><td>{html.escape(lead.category)}</td><td>{html.escape(city)}</td><td>{html.escape(lead.chat_title)}</td><td>{text}</td></tr>")
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LeadKZ Dashboard</title>
<style>
body{{font-family:Arial,sans-serif;background:#0f172a;color:#e5e7eb;margin:0;padding:24px}}
.card{{background:#111827;border:1px solid #334155;border-radius:16px;padding:18px;margin:12px 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.num{{font-size:28px;font-weight:700}} table{{width:100%;border-collapse:collapse}} td,th{{border-bottom:1px solid #334155;padding:10px;text-align:left}}
.badge{{color:#93c5fd}}
</style></head><body>
<h1>LeadKZ Free v8 Dashboard</h1>
<p class="badge">Бот работает. Uptime: {html.escape(str(datetime.now(timezone.utc)-app_started_at).split('.')[0])}</p>
<div class="grid">
<div class="card"><div class="num">{stats.get('total',0)}</div><div>лидов за 24ч</div></div>
<div class="card"><div class="num">{stats.get('hot',0)}</div><div>горячих</div></div>
<div class="card"><div class="num">{groups_total}</div><div>найденных групп</div></div>
<div class="card"><div class="num">{groups_valid}</div><div>валидных групп</div></div>
</div>
<div class="card"><h2>Кому писать первым</h2><table><tr><th>Балл</th><th>Категория</th><th>Город</th><th>Группа</th><th>Текст</th></tr>{''.join(rows)}</table></div>
</body></html>"""


def railway_variables_template(settings) -> str:
    return (
        "BOT_TOKEN=...\n"
        f"ADMIN_ID={settings.admin_id}\n"
        "API_ID=...\n"
        "API_HASH=...\n"
        "TG_SESSION_STRING=...\n"
        "DATA_DIR=/data\n"
        "GEO_ONLY_KAZAKHSTAN=true\n"
        "ONLY_VALID_GROUPS=true\n"
        "AUTO_DISCOVERY_ENABLED=true\n"
        "HEALTH_SERVER_ENABLED=true\n"
        "PORT=8080\n"
    )
