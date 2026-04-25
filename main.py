from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from html import escape
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, FSInputFile, Message
from telethon import TelegramClient
from telethon.sessions import StringSession

from leadkz.config import settings
from leadkz.database import Database
from leadkz.discovery import discover_public_groups, discover_similar_groups, scan_existing_dialog_groups
from leadkz.export import backup_database, export_leads_csv, export_leads_xlsx, export_settings_json
from leadkz.formatting import (
    anti_ban_text,
    format_bad_groups,
    format_group_ratings,
    format_groups,
    format_lead,
    format_leads_list,
    format_niches,
    format_price_calculator,
    format_settings,
    format_stats,
    format_upcoming_reminders,
    format_exact_scan_result,
    format_limits_status,
    format_exact_mode_settings,
    format_priority_queue,
    format_segment_leads,
    format_learning,
    format_cases,
    format_funnel,
    format_revenue,
    format_lead_advice,
    format_daily_plan,
    since_hours,
)
from leadkz.keyboards import groups_actions, hidden_actions, lead_actions, main_menu, niches_actions, settings_actions, wizard_actions
from leadkz.monitor import register_new_message_handler, scan_recent_messages, scan_valid_public_groups
from leadkz.pdf_tools import create_offer_pdf
from leadkz.replies import render_template, templates_menu_text
from leadkz.filters import score_message
from leadkz.pro_features import (
    build_dashboard_html,
    format_check_report,
    format_diagnostics,
    format_error_logs,
    format_setup_wizard,
    format_weekly_groups_report,
    format_words_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("leadkz")
LOCAL_TZ = ZoneInfo("Asia/Almaty")
APP_STARTED_AT = datetime.now(timezone.utc)

bot = Bot(settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db = Database(settings.database_path)
session_obj = StringSession(settings.tg_session_string) if settings.tg_session_string else settings.tg_session
tg_client = TelegramClient(session_obj, settings.api_id, settings.api_hash)


def init_runtime_defaults() -> None:
    defaults = {
        "min_score": settings.min_lead_score,
        "geo_only": settings.geo_only_kazakhstan,
        "hot_only": False,
        "quiet_mode": False,
        "working_hours": True,
        "work_start": 9,
        "work_end": 22,
        "niche_bots": True,
        "niche_automation": True,
        "niche_sites": True,
        "niche_smm": True,
        "niche_design": True,
        "autobackup": True,
        "warm_digest": True,
        "only_valid_groups": settings.only_valid_groups,
        "smart_notification_limit": True,
        "max_notifications_per_hour": settings.max_notifications_per_hour,
        "web_dashboard": settings.web_dashboard_enabled,
        "cleanup_enabled": settings.cleanup_enabled,
        "save_hidden_messages": True,
        "exact_leads_mode": True,
        "chatbot_only_mode": False,
    }
    for key, value in defaults.items():
        if db.get_setting(key) is None:
            db.set_setting(key, value)


def current_geo_required() -> bool:
    return db.get_setting_bool("geo_only", settings.geo_only_kazakhstan)


def discovery_blocked_until_dt() -> datetime | None:
    raw = db.get_setting("discovery_blocked_until")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def discovery_is_blocked() -> tuple[bool, str | None]:
    dt = discovery_blocked_until_dt()
    if not dt:
        return False, None
    if dt <= datetime.now(timezone.utc):
        return False, dt.isoformat()
    return True, dt.isoformat()


async def ensure_admin(message_or_query) -> bool:
    user = getattr(message_or_query, "from_user", None)
    if not user or user.id not in set(settings.admin_ids):
        user_id = getattr(user, "id", "unknown")
        text = (
            "⛔ Доступ запрещён.\n\n"
            f"Твой Telegram ID: <code>{user_id}</code>\n"
            "Если это ты владелец, вставь этот ID в Railway Variables → ADMIN_ID, "
            "нажми Apply changes и Deploy."
        )
        if isinstance(message_or_query, Message):
            await message_or_query.answer(text)
        elif isinstance(message_or_query, CallbackQuery):
            await message_or_query.answer("Доступ запрещён. Проверь ADMIN_ID.", show_alert=True)
        return False
    return True


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if not await ensure_admin(message):
        return
    init_runtime_defaults()
    await message.answer(
        "🤖 <b>LeadKZ Free v8.1.1 Easy Start + Railway</b>\n\n"
        "Бесплатно ищу свежие заявки по Казахстану: чат-боты, CRM, автоматизация, сайты, SMM и дизайн.\n"
        "Внутри: KZ-гео, скоринг, бюджет, риск ответа, CRM, обучение фильтра, очередь приоритетов, воронка, учет денег, кейсы, PDF-КП, заметки, напоминания, бэкап и экспорт.\n\n"
        "Авторассылки и авто-вступления нет — это сделано специально для безопасности аккаунта.",
        reply_markup=main_menu(),
    )


@dp.callback_query(F.data == "menu")
async def cb_menu(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text("🤖 <b>LeadKZ Free v8.1.1 Easy Start + Railway</b>\n\nВыбери действие:", reply_markup=main_menu())
    await query.answer()


@dp.callback_query(F.data == "help")
async def cb_help(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(
        "ℹ️ <b>Как работает бот</b>\n\n"
        "1. Telethon читает группы, где уже состоит твой Telegram-аккаунт.\n"
        "2. Бот ищет заявки по смысловым признакам: бот, CRM, заявки, запись, Kaspi, сайт, SMM, дизайн.\n"
        "3. Гео-фильтр Казахстана ищет признаки: Казахстан, KZ, города, Kaspi, +7, тенге, ИП/ТОО.\n"
        "4. Рекламные сообщения разработчиков, спам и дубликаты отсекаются.\n"
        "5. Автопоиск групп сам оценивает публичные KZ-группы по последним сообщениям.\n"
        "6. Валидные публичные группы автоматически попадают в мониторинг, мусорные скрываются.\n"
        "7. Все данные хранятся локально или на Railway Volume в SQLite, без платных API.\n\n"
        "Команды: /note chat_id message_id текст — добавить заметку; /import_settings JSON — импорт настроек.",
        reply_markup=main_menu(),
        disable_web_page_preview=True,
    )
    await query.answer()



@dp.message(Command("check"))
async def cmd_check(message: Message) -> None:
    if not await ensure_admin(message):
        return
    authorized = None
    try:
        authorized = await tg_client.is_user_authorized()
    except Exception:
        authorized = False
    await message.answer(format_check_report(db, settings, authorized), disable_web_page_preview=True)


@dp.callback_query(F.data == "check:run")
async def cb_check(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    authorized = None
    try:
        authorized = await tg_client.is_user_authorized()
    except Exception:
        authorized = False
    await query.message.edit_text(format_check_report(db, settings, authorized), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.message(Command("setup"))
async def cmd_setup(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await message.answer(format_setup_wizard(db), reply_markup=wizard_actions())


@dp.callback_query(F.data == "setup:wizard")
async def cb_setup_wizard(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_setup_wizard(db), reply_markup=wizard_actions(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data.startswith("setup:toggle:"))
async def cb_setup_toggle(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    key = query.data.split(":", 2)[2]
    current = db.get_setting_bool(key, False)
    db.set_setting(key, not current)
    await query.message.edit_text(format_setup_wizard(db), reply_markup=wizard_actions(), disable_web_page_preview=True)
    await query.answer("Настройка изменена")


@dp.callback_query(F.data == "diagnostics:show")
async def cb_diagnostics(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_diagnostics(db, settings, APP_STARTED_AT), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.message(Command("diagnostics"))
async def cmd_diagnostics(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await message.answer(format_diagnostics(db, settings, APP_STARTED_AT), disable_web_page_preview=True)


@dp.callback_query(F.data == "errors:list")
async def cb_errors(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_error_logs(db.get_recent_errors()), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "words:report")
async def cb_words_report(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_words_report(db), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "cleanup:run")
async def cb_cleanup(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    result = db.cleanup_old_data()
    await query.message.edit_text(
        "🧹 <b>Очистка базы завершена</b>\n\n"
        f"Скрытый мусор удалён: <b>{result['deleted_hidden']}</b>\n"
        f"Старые лиды архивированы: <b>{result['archived']}</b>\n"
        f"Старые ошибки удалены: <b>{result['deleted_errors']}</b>\n"
        f"Старые уведомления удалены: <b>{result['deleted_notifications']}</b>",
        reply_markup=main_menu(),
    )
    await query.answer()


@dp.callback_query(F.data == "daily:report")
async def cb_daily_report_now(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_stats(db.get_stats_since(since_hours(24)), "📊 Отчёт за день"), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "groups:weekly")
async def cb_weekly_groups(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_weekly_groups_report(db.get_bad_group_recommendations(days=7, limit=12)), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "web:info")
async def cb_web_info(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(
        "🌐 <b>Web-панель</b>\n\n"
        f"Локально: <code>http://127.0.0.1:{settings.health_port}</code>\n"
        "На Railway открой домен проекта. Там будет простая таблица лидов, групп и статистики.\n\n"
        "Если Railway показывает только JSON, проверь переменную <code>HEALTH_SERVER_ENABLED=true</code>.",
        reply_markup=main_menu(),
        disable_web_page_preview=True,
    )
    await query.answer()


@dp.message(Command("money"))
async def cmd_money(message: Message) -> None:
    if not await ensure_admin(message):
        return
    parts = (message.text or "").split(maxsplit=4)
    if len(parts) < 4:
        await message.answer("Формат: <code>/money chat_id message_id 150000 комментарий</code>")
        return
    try:
        chat_id, message_id, amount = int(parts[1]), int(parts[2]), int(parts[3])
    except ValueError:
        await message.answer("chat_id, message_id и сумма должны быть числами.")
        return
    note = parts[4] if len(parts) > 4 else ""
    db.set_deal_amount(chat_id, message_id, amount, note)
    await message.answer("✅ Сумма сделки сохранена.")



@dp.callback_query(F.data == "antiban")
async def cb_antiban(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(anti_ban_text(), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data.startswith("leads:"))
async def cb_leads(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    value = query.data.split(":", 1)[1]
    if value == "favorites":
        leads = db.get_favorite_leads(limit=15)
        await query.message.edit_text(format_leads_list(leads, "⭐ <b>Избранные лиды</b>"), reply_markup=main_menu(), disable_web_page_preview=True)
        await query.answer(); return
    if value == "archived":
        leads = db.get_archived_leads(limit=15)
        await query.message.edit_text(format_leads_list(leads, "🗂 <b>Архив лидов</b>"), reply_markup=main_menu(), disable_web_page_preview=True)
        await query.answer(); return
    if value.startswith("segment:"):
        segment = value.split(":", 1)[1]
        leads = db.get_segment_leads(segment, since_hours(24), limit=15)
        await query.message.edit_text(format_segment_leads(leads, segment), reply_markup=main_menu(), disable_web_page_preview=True)
        await query.answer(); return
    hours = int(value)
    title = "🔥 <b>Лиды за последний час</b>" if hours == 1 else "📅 <b>Лиды за 24 часа</b>"
    leads = db.get_leads_since(since_hours(hours), limit=12)
    await query.message.edit_text(format_leads_list(leads, title), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data.startswith("scan:exact:"))
async def cb_exact_scan(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    hours = int(query.data.rsplit(":", 1)[1])
    try:
        await query.answer("Проверяю текущие группы…")
    except Exception:
        pass
    per_group_limit = 90 if hours == 1 else 250
    saved = await scan_recent_messages(
        client=tg_client, db=db, bot=bot, admin_id=settings.admin_id,
        min_score=db.get_setting_int("min_score", settings.min_lead_score),
        max_age_hours=hours,
        monitored_chat_ids=settings.monitored_chat_ids,
        geo_keywords=settings.geo_keywords,
        geo_required=current_geo_required(),
        per_group_limit=per_group_limit,
    )
    await query.message.answer(format_exact_scan_result(hours, saved), reply_markup=main_menu(), disable_web_page_preview=True)


@dp.callback_query(F.data == "limits:show")
async def cb_limits_show(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_limits_status(db.get_setting("discovery_blocked_until")), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "groups:auto_disable")
async def cb_auto_disable_groups(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    count = db.auto_disable_bad_groups(days=7, min_group_score=db.get_setting_int("auto_disable_group_score", 45))
    await query.message.edit_text(
        f"🧹 <b>Слабые группы отключены</b>\n\nСкрыто групп: <b>{count}</b>.\n\n"
        "Логика: группа не дала лидов за 7 дней и имеет низкий рейтинг.",
        reply_markup=main_menu(),
        disable_web_page_preview=True,
    )
    await query.answer("Готово")


@dp.callback_query(F.data == "discover:run")
async def cb_discover_run(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    blocked, blocked_until = discovery_is_blocked()
    if blocked:
        await query.message.edit_text(format_limits_status(blocked_until), reply_markup=main_menu(), disable_web_page_preview=True)
        await query.answer("Telegram временно ограничил поиск групп", show_alert=True)
        return
    await query.answer("Ищу публичные группы по Казахстану…")
    added = await discover_public_groups(
        tg_client, db, settings.discovery_keywords,
        limit_per_keyword=settings.discovery_limit_per_keyword,
        geo_keywords=settings.geo_keywords,
        geo_required=current_geo_required(),
        history_limit=settings.auto_discovery_history_limit,
        min_valid_score=settings.auto_discovery_min_group_score,
        auto_hide_bad=settings.auto_hide_bad_groups,
    )
    groups = db.get_group_candidates(limit=10)
    await query.message.edit_text(f"🔎 Поиск завершён. Новых групп: <b>{added}</b>\n\n" + format_groups(groups), reply_markup=groups_actions(groups), disable_web_page_preview=True)


@dp.callback_query(F.data == "discover:similar")
async def cb_discover_similar(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    blocked, blocked_until = discovery_is_blocked()
    if blocked:
        await query.message.edit_text(format_limits_status(blocked_until), reply_markup=main_menu(), disable_web_page_preview=True)
        await query.answer("Telegram временно ограничил поиск групп", show_alert=True)
        return
    await query.answer("Ищу похожие группы…")
    added = await discover_similar_groups(
        tg_client, db,
        limit_per_keyword=settings.discovery_limit_per_keyword,
        geo_keywords=settings.geo_keywords,
        geo_required=current_geo_required(),
        history_limit=settings.auto_discovery_history_limit,
        min_valid_score=settings.auto_discovery_min_group_score,
        auto_hide_bad=settings.auto_hide_bad_groups,
    )
    groups = db.get_group_candidates(limit=10)
    await query.message.edit_text(f"🔍 Поиск похожих групп завершён. Новых групп: <b>{added}</b>\n\n" + format_groups(groups), reply_markup=groups_actions(groups), disable_web_page_preview=True)


@dp.callback_query(F.data == "discover:list")
async def cb_discover_list(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    groups = db.get_group_candidates(limit=15)
    await query.message.edit_text(format_groups(groups), reply_markup=groups_actions(groups), disable_web_page_preview=True)
    await query.answer()




@dp.callback_query(F.data == "discover:best")
async def cb_discover_best(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    groups = db.get_best_auto_groups(limit=15)
    await query.message.edit_text(format_groups(groups, "🏆 <b>Лучшие валидные группы для авто-мониторинга</b>"), reply_markup=groups_actions(groups), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "discover:trash")
async def cb_discover_trash(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    groups = db.get_trash_auto_groups(limit=15)
    await query.message.edit_text(format_groups(groups, "🗑 <b>Мусорные/слабые группы</b>"), reply_markup=groups_actions(groups), disable_web_page_preview=True)
    await query.answer()

@dp.callback_query(F.data == "groups:rating")
async def cb_group_rating(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_group_ratings(db.get_group_ratings(since_hours(24 * 7), limit=10)), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "groups:bad")
async def cb_group_bad(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_bad_groups(db.get_bad_group_recommendations(days=7, limit=10)), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data.startswith("group:"))
async def cb_group_status(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, status, chat_id = query.data.split(":", 2)
    db.set_group_status(int(chat_id), status)
    groups = db.get_group_candidates(limit=15)
    labels = {"valid": "добавлена в авто-мониторинг", "priority": "добавлена в важные", "blocked": "добавлена в черный список", "candidate": "вернул в кандидаты"}
    await query.answer(labels.get(status, "статус обновлен"))
    await query.message.edit_text(format_groups(groups), reply_markup=groups_actions(groups), disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("lead:status:"))
async def cb_lead_status(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, status, chat_id, message_id = query.data.split(":", 4)
    db.set_lead_status(int(chat_id), int(message_id), status)
    lead = db.get_lead(int(chat_id), int(message_id))
    labels = {"hidden":"Скрыто", "favorite":"Добавлено в избранное", "contacted":"Статус: написал", "talking":"Статус: обсуждение", "waiting_payment":"Статус: ждет оплату", "closed":"Статус: закрыт", "lost":"Статус: не подошел", "archived":"В архиве"}
    await query.answer(labels.get(status, "Статус обновлен"))
    if lead and status != "hidden":
        await query.message.edit_text(format_lead(lead), reply_markup=lead_actions(lead.chat_id, lead.message_id, lead.link, lead.category), disable_web_page_preview=True)
    elif status == "hidden":
        await query.message.edit_reply_markup(reply_markup=None)


@dp.callback_query(F.data.startswith("lead:template:"))
async def cb_lead_template(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, key, chat_id, message_id = query.data.split(":", 4)
    lead = db.get_lead(int(chat_id), int(message_id))
    await query.message.answer(render_template(lead, key), disable_web_page_preview=True)
    await query.answer("Готово")


@dp.callback_query(F.data.startswith("lead:smart_reply:"))
async def cb_lead_smart_reply(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, chat_id, message_id = query.data.split(":", 3)
    lead = db.get_lead(int(chat_id), int(message_id))
    if not lead:
        await query.answer("Лид не найден", show_alert=True)
        return
    await query.message.answer(format_smart_reply(lead), disable_web_page_preview=True)
    await query.answer("Ответ готов")


@dp.callback_query(F.data.startswith("lead:pdf:"))
async def cb_lead_pdf(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, chat_id, message_id = query.data.split(":", 3)
    lead = db.get_lead(int(chat_id), int(message_id))
    if not lead:
        await query.answer("Лид не найден", show_alert=True); return
    path = create_offer_pdf(lead, settings.export_dir)
    await query.message.answer_document(FSInputFile(path), caption="📄 PDF-КП по лиду" if path.suffix == ".pdf" else "📄 КП текстом: PDF-библиотека/шрифт не найден")
    await query.answer("КП готово")


@dp.callback_query(F.data.startswith("lead:remind:"))
async def cb_lead_remind(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, hours, chat_id, message_id = query.data.split(":", 4)
    remind_at = datetime.now(timezone.utc) + timedelta(hours=int(hours))
    db.add_reminder(int(chat_id), int(message_id), remind_at)
    await query.answer("Напоминание добавлено")


@dp.callback_query(F.data.startswith("lead:note:"))
async def cb_lead_note(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, chat_id, message_id = query.data.split(":", 3)
    await query.message.answer(f"📝 Чтобы добавить заметку, отправь:\n<code>/note {chat_id} {message_id} твой текст заметки</code>")
    await query.answer()


@dp.message(Command("note"))
async def cmd_note(message: Message) -> None:
    if not await ensure_admin(message):
        return
    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 4:
        await message.answer("Формат: <code>/note chat_id message_id текст заметки</code>")
        return
    try:
        chat_id, message_id = int(parts[1]), int(parts[2])
    except ValueError:
        await message.answer("chat_id и message_id должны быть числами.")
        return
    db.add_note(chat_id, message_id, parts[3])
    await message.answer("✅ Заметка сохранена.")



@dp.callback_query(F.data.startswith("lead:block_like:"))
async def cb_lead_block_like(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, chat_id, message_id = query.data.split(":", 3)
    lead = db.get_lead(int(chat_id), int(message_id))
    if not lead:
        await query.answer("Лид не найден", show_alert=True)
        return
    words = []
    for raw in score_message(lead.text, min_score=1, geo_required=False).reasons[:3]:
        # причины бывают длинными, поэтому сохраняем только короткие понятные фразы
        clean = raw.replace("слово:", "").replace("фраза:", "").strip(" .:—-").lower()
        if 3 <= len(clean) <= 60:
            words.append(clean)
    if not words:
        words = [" ".join(lead.text.lower().split()[:3])]
    for phrase in words[:3]:
        db.add_learning_keyword("negative", phrase, 12)
    db.set_lead_status(int(chat_id), int(message_id), "hidden")
    await query.message.edit_reply_markup(reply_markup=None)
    await query.answer("Похожее будет показываться реже")


@dp.callback_query(F.data.startswith("lead:feedback:"))
async def cb_lead_feedback(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, vote, chat_id, message_id = query.data.split(":", 4)
    db.add_feedback(int(chat_id), int(message_id), vote)
    lead = db.get_lead(int(chat_id), int(message_id))
    if lead and vote == "good":
        # Автообучение: хорошие лиды чуть усиливают свою категорию и город.
        db.add_learning_keyword("positive", lead.category, 6)
    elif lead and vote == "bad":
        db.add_learning_keyword("negative", lead.category, 8)
    await query.answer("Спасибо, фильтр запомнил оценку")


@dp.callback_query(F.data.startswith("lead:advice:"))
async def cb_lead_advice(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, _, chat_id, message_id = query.data.split(":", 3)
    lead = db.get_lead(int(chat_id), int(message_id))
    if not lead:
        await query.answer("Лид не найден", show_alert=True); return
    await query.message.answer(format_lead_advice(lead), disable_web_page_preview=True)
    await query.answer("Совет готов")


@dp.callback_query(F.data == "priority:queue")
async def cb_priority_queue(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_priority_queue(db.get_priority_queue(limit=15)), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "learning:list")
async def cb_learning(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_learning(db.get_learning_keywords(), db.get_feedback_summary()), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "cases:list")
async def cb_cases(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_cases(db.get_cases()), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "sales:funnel")
async def cb_funnel(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_funnel(db.get_funnel(since_hours(24))), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "sales:revenue")
async def cb_revenue(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_revenue(db.get_revenue_stats(since_hours(24))), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "plan:today")
async def cb_plan(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_daily_plan(db.get_stats_since(since_hours(24))), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "security:info")
async def cb_security(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(
        "🔐 <b>Защита базы</b>\n\n"
        "— доступ к меню только по ADMIN_ID;\n"
        "— файл .session никому не отправлять;\n"
        "— есть ручной бэкап через кнопку 💾;\n"
        "— автобэкап отправляется ежедневно, если включен AUTOBACKUP_ENABLED;\n"
        "— база хранится локально в leadkz.sqlite3.",
        reply_markup=main_menu(), disable_web_page_preview=True
    )
    await query.answer()



@dp.message(Command("learn_plus"))
async def cmd_learn_plus(message: Message) -> None:
    if not await ensure_admin(message):
        return
    phrase = (message.text or "").partition(" ")[2].strip()
    if not phrase:
        await message.answer("Формат: <code>/learn_plus фраза</code>"); return
    db.add_learning_keyword("positive", phrase)
    await message.answer("✅ Плюс-слово добавлено.")


@dp.message(Command("learn_minus"))
async def cmd_learn_minus(message: Message) -> None:
    if not await ensure_admin(message):
        return
    phrase = (message.text or "").partition(" ")[2].strip()
    if not phrase:
        await message.answer("Формат: <code>/learn_minus фраза</code>"); return
    db.add_learning_keyword("negative", phrase)
    await message.answer("✅ Минус-слово добавлено.")


@dp.message(Command("learn_spam"))
async def cmd_learn_spam(message: Message) -> None:
    if not await ensure_admin(message):
        return
    phrase = (message.text or "").partition(" ")[2].strip()
    if not phrase:
        await message.answer("Формат: <code>/learn_spam фраза</code>"); return
    db.add_learning_keyword("spam", phrase)
    await message.answer("✅ Спам-слово добавлено.")


@dp.message(Command("learn_geo"))
async def cmd_learn_geo(message: Message) -> None:
    if not await ensure_admin(message):
        return
    phrase = (message.text or "").partition(" ")[2].strip()
    if not phrase:
        await message.answer("Формат: <code>/learn_geo фраза</code>"); return
    db.add_learning_keyword("geo", phrase)
    await message.answer("✅ Гео-слово добавлено.")


@dp.message(Command("testlead"))
async def cmd_testlead(message: Message) -> None:
    if not await ensure_admin(message):
        return
    text = (message.text or "").partition(" ")[2].strip()
    if not text:
        await message.answer("Формат: <code>/testlead текст сообщения</code>"); return
    result = score_message(text, min_score=db.get_setting_int("min_score", settings.min_lead_score), geo_keywords=settings.geo_keywords, geo_required=current_geo_required())
    delta, learned_reasons, learned_spam, learned_geo = db.apply_learning_to_text(text)
    score = max(0, min(100, result.score + delta))
    status = "✅ лид" if result.is_lead and not learned_spam else "🚫 не лид"
    await message.answer(
        f"🧪 <b>Тест лида</b>\n\nСтатус: <b>{status}</b>\nБалл: <b>{score}/100</b>\nКатегория: {escape(result.category)}\nПричины: {escape(' | '.join(result.reasons + learned_reasons))}"
    )


@dp.message(Command("money"))
async def cmd_money(message: Message) -> None:
    if not await ensure_admin(message):
        return
    parts = (message.text or "").split(maxsplit=4)
    if len(parts) < 4:
        await message.answer("Формат: <code>/money chat_id message_id 150000 комментарий</code>"); return
    try:
        chat_id, message_id, amount = int(parts[1]), int(parts[2]), int(parts[3])
    except ValueError:
        await message.answer("chat_id, message_id и сумма должны быть числами."); return
    note = parts[4] if len(parts) > 4 else ""
    db.set_deal_amount(chat_id, message_id, amount, note)
    await message.answer("✅ Сумма сделки сохранена.")

@dp.callback_query(F.data == "templates:list")
async def cb_templates_list(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(templates_menu_text(), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data == "stats:24")
async def cb_stats(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_stats(db.get_stats_since(since_hours(24))), reply_markup=main_menu(), disable_web_page_preview=True)
    await query.answer()


@dp.callback_query(F.data.startswith("export:"))
async def cb_export(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    _, kind, hours_text = query.data.split(":", 2)
    hours = int(hours_text)
    await query.answer("Готовлю файл…")
    if kind == "csv":
        path = export_leads_csv(db, since_hours(hours), settings.export_dir); caption = f"📄 Лиды за последние {hours} ч. в CSV"
    else:
        path = export_leads_xlsx(db, since_hours(hours), settings.export_dir); caption = f"📥 Лиды за последние {hours} ч. в Excel"
    await query.message.answer_document(FSInputFile(path), caption=caption)


@dp.callback_query(F.data == "backup:db")
async def cb_backup(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    path = backup_database(db, settings.export_dir)
    await query.message.answer_document(FSInputFile(path), caption="💾 Резервная копия базы SQLite")
    await query.answer("Бэкап готов")


@dp.callback_query(F.data.startswith("settings:"))
async def cb_settings(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    action = query.data.split(":", 1)[1]
    init_runtime_defaults()
    if action == "toggle_geo":
        db.set_setting("geo_only", not db.get_setting_bool("geo_only", settings.geo_only_kazakhstan)); await query.answer("Гео переключено")
    elif action == "toggle_hot":
        db.set_setting("hot_only", not db.get_setting_bool("hot_only", False)); await query.answer("Горячие переключены")
    elif action == "toggle_quiet":
        db.set_setting("quiet_mode", not db.get_setting_bool("quiet_mode", False)); await query.answer("Тихий режим переключён")
    elif action == "toggle_working":
        db.set_setting("working_hours", not db.get_setting_bool("working_hours", True)); await query.answer("Рабочее время переключено")
    elif action == "toggle_valid_groups":
        db.set_setting("only_valid_groups", not db.get_setting_bool("only_valid_groups", True)); await query.answer("Фильтр валидных групп переключён")
    elif action == "toggle_exact":
        db.set_setting("exact_leads_mode", not db.get_setting_bool("exact_leads_mode", True)); await query.answer("Точный режим переключён")
        await query.message.edit_text(format_exact_mode_settings(db.get_all_settings()), reply_markup=main_menu(), disable_web_page_preview=True); return
    elif action == "toggle_chatbot_only":
        db.set_setting("chatbot_only_mode", not db.get_setting_bool("chatbot_only_mode", False)); await query.answer("Режим чат-ботов переключён")
        await query.message.edit_text(format_exact_mode_settings(db.get_all_settings()), reply_markup=main_menu(), disable_web_page_preview=True); return
    elif action == "score_up":
        db.set_setting("min_score", min(95, db.get_setting_int("min_score", settings.min_lead_score) + 5)); await query.answer("Минимальный балл повышен")
    elif action == "score_down":
        db.set_setting("min_score", max(20, db.get_setting_int("min_score", settings.min_lead_score) - 5)); await query.answer("Минимальный балл снижен")
    elif action == "export":
        path = export_settings_json(db, settings.export_dir)
        await query.message.answer_document(FSInputFile(path), caption="📤 Экспорт настроек")
        await query.answer("Настройки экспортированы"); return
    else:
        await query.answer()
    await query.message.edit_text(format_settings(db.get_all_settings(), settings.min_lead_score, settings.geo_only_kazakhstan), reply_markup=settings_actions(), disable_web_page_preview=True)


@dp.message(Command("import_settings"))
async def cmd_import_settings(message: Message) -> None:
    if not await ensure_admin(message):
        return
    raw = (message.text or "").partition(" ")[2].strip()
    if not raw:
        await message.answer("Формат: <code>/import_settings {\"min_score\":70}</code>")
        return
    try:
        payload = json.loads(raw)
        values = payload.get("settings", payload) if isinstance(payload, dict) else {}
        count = db.import_settings(values)
    except Exception as exc:
        await message.answer(f"Не смог импортировать настройки: {escape(str(exc))}")
        return
    await message.answer(f"✅ Импортировано настроек: {count}")


@dp.callback_query(F.data == "niches:list")
async def cb_niches_list(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_niches(db.get_all_settings()), reply_markup=niches_actions())
    await query.answer()


@dp.callback_query(F.data.startswith("niche:toggle:"))
async def cb_niche_toggle(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    key = query.data.split(":", 2)[2]
    setting_key = f"niche_{key}"
    db.set_setting(setting_key, not db.get_setting_bool(setting_key, True))
    await query.message.edit_text(format_niches(db.get_all_settings()), reply_markup=niches_actions())
    await query.answer("Ниша переключена")


@dp.callback_query(F.data == "price:calc")
async def cb_price(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_price_calculator(), reply_markup=main_menu())
    await query.answer()


@dp.callback_query(F.data == "reminders:list")
async def cb_reminders(query: CallbackQuery) -> None:
    if not await ensure_admin(query):
        return
    await query.message.edit_text(format_upcoming_reminders(db.get_upcoming_reminders()), reply_markup=main_menu())
    await query.answer()



async def run_existing_dialog_group_scan(reason: str = "manual") -> int:
    """Ищет валидные группы среди чатов, где аккаунт уже состоит.
    Работает без Telegram Search и без AUTO_DISCOVERY_ENABLED.
    """
    try:
        count = await scan_existing_dialog_groups(
            tg_client, db,
            geo_keywords=settings.geo_keywords,
            geo_required=current_geo_required(),
            history_limit=settings.scan_my_groups_history_limit,
            min_valid_score=settings.auto_discovery_min_group_score,
            max_groups=settings.scan_my_groups_limit,
            auto_hide_bad=False,
        )
        log.info("Existing dialog group scan completed reason=%s valid=%s", reason, count)
        return count
    except Exception as exc:
        db.log_error("scan_existing_dialog_groups", f"{type(exc).__name__}: {exc}")
        log.exception("Existing dialog group scan failed")
        return 0


async def periodic_existing_dialog_group_scan() -> None:
    while True:
        await asyncio.sleep(max(1, int(settings.scan_my_groups_interval_hours * 3600)))
        await run_existing_dialog_group_scan(reason="periodic")

async def periodic_scan() -> None:
    while True:
        try:
            count = await scan_recent_messages(client=tg_client, db=db, bot=bot, admin_id=settings.admin_id,
                                               min_score=settings.min_lead_score, max_age_hours=settings.lead_max_age_hours,
                                               monitored_chat_ids=settings.monitored_chat_ids, geo_keywords=settings.geo_keywords,
                                               geo_required=current_geo_required())
            public_count = 0
            if settings.auto_monitor_valid_groups:
                public_count = await scan_valid_public_groups(
                    client=tg_client, db=db, bot=bot, admin_id=settings.admin_id,
                    min_score=settings.min_lead_score, max_age_hours=settings.lead_max_age_hours,
                    geo_keywords=settings.geo_keywords, geo_required=current_geo_required(),
                )
            total_count = count + public_count
            if total_count:
                log.info("Saved %s leads during periodic scan (dialogs=%s, public_groups=%s)", total_count, count, public_count)
        except Exception as exc:
            db.log_error("periodic_scan", f"{type(exc).__name__}: {exc}")
            log.exception("Periodic scan failed")
        await asyncio.sleep(settings.scan_interval_seconds)


async def periodic_discovery() -> None:
    if settings.auto_discovery_enabled and settings.discovery_start_delay_hours > 0:
        delay_seconds = int(settings.discovery_start_delay_hours * 3600)
        log.warning(
            "Auto group discovery is enabled but delayed for %.2f hours (%s seconds)",
            settings.discovery_start_delay_hours, delay_seconds,
        )
        await asyncio.sleep(delay_seconds)

    while True:
        if not settings.auto_discovery_enabled:
            await asyncio.sleep(settings.discovery_interval_hours * 3600)
            continue
        try:
            added = await discover_public_groups(
                tg_client, db, settings.discovery_keywords,
                limit_per_keyword=settings.discovery_limit_per_keyword,
                geo_keywords=settings.geo_keywords,
                geo_required=current_geo_required(),
                history_limit=settings.auto_discovery_history_limit,
                min_valid_score=settings.auto_discovery_min_group_score,
                auto_hide_bad=settings.auto_hide_bad_groups,
            )
            log.info("Group discovery finished, added=%s", added)
        except Exception as exc:
            db.log_error("group_discovery", f"{type(exc).__name__}: {exc}")
            log.exception("Group discovery failed")
        await asyncio.sleep(settings.discovery_interval_hours * 3600)


async def periodic_daily_report() -> None:
    while True:
        now = datetime.now(LOCAL_TZ)
        next_run = now.replace(hour=settings.daily_report_hour, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            db.archive_old_leads(older_than_days=14)
            await bot.send_message(settings.admin_id, format_stats(db.get_stats_since(since_hours(24)), "📊 Ежедневный отчёт за 24 часа"), disable_web_page_preview=True)
        except Exception as exc:
            db.log_error("daily_report", f"{type(exc).__name__}: {exc}")
            log.exception("Daily report failed")


async def periodic_reminders() -> None:
    while True:
        try:
            rows = db.get_pending_reminders(datetime.now(timezone.utc))
            for row in rows:
                lead = db.get_lead(int(row["chat_id"]), int(row["message_id"]))
                if lead:
                    await bot.send_message(settings.admin_id, "⏰ <b>Напоминание по лиду</b>\n\n" + format_lead(lead), reply_markup=lead_actions(lead.chat_id, lead.message_id, lead.link, lead.category), disable_web_page_preview=True)
                db.mark_reminder_sent(int(row["id"]))
        except Exception as exc:
            db.log_error("reminders", f"{type(exc).__name__}: {exc}")
            log.exception("Reminder check failed")
        await asyncio.sleep(60)


async def periodic_autobackup() -> None:
    while True:
        now = datetime.now(LOCAL_TZ)
        next_run = now.replace(hour=settings.autobackup_hour, minute=15, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            path = backup_database(db, settings.export_dir)
            await bot.send_document(settings.admin_id, FSInputFile(path), caption="💾 Автобэкап базы LeadKZ")
        except Exception as exc:
            db.log_error("autobackup", f"{type(exc).__name__}: {exc}")
            log.exception("Autobackup failed")



async def periodic_cleanup() -> None:
    while True:
        now = datetime.now(LOCAL_TZ)
        next_run = now.replace(hour=settings.cleanup_hour, minute=30, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            result = db.cleanup_old_data()
            log.info("Cleanup finished: %s", result)
        except Exception as exc:
            db.log_error("cleanup", f"{type(exc).__name__}: {exc}")
            log.exception("Cleanup failed")


async def periodic_weekly_group_report() -> None:
    while True:
        now = datetime.now(LOCAL_TZ)
        days_ahead = (settings.weekly_report_day - now.weekday()) % 7
        next_run = (now + timedelta(days=days_ahead)).replace(hour=18, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=7)
        await asyncio.sleep((next_run - now).total_seconds())
        try:
            await bot.send_message(settings.admin_id, format_weekly_groups_report(db.get_bad_group_recommendations(days=7, limit=12)), disable_web_page_preview=True)
        except Exception as exc:
            db.log_error("weekly_groups", f"{type(exc).__name__}: {exc}")
            log.exception("Weekly groups report failed")


async def health_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request = await reader.read(2048)
        first = request.split(b"\r\n", 1)[0].decode("latin1", "ignore")
        path = first.split(" ")[1] if " " in first else "/"
        if path.startswith("/health"):
            body = b'{"status":"ok","app":"leadkz-v8"}'
            content_type = b"application/json; charset=utf-8"
        else:
            html = build_dashboard_html(db, settings, APP_STARTED_AT)
            body = html.encode("utf-8")
            content_type = b"text/html; charset=utf-8"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: " + content_type + b"\r\n"
            b"Connection: close\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
            + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def start_health_server() -> None:
    if not settings.health_server_enabled:
        return
    try:
        server = await asyncio.start_server(health_handler, host="0.0.0.0", port=settings.health_port)
        log.info("Health server started on port %s", settings.health_port)
        async with server:
            await server.serve_forever()
    except Exception:
        log.exception("Health server failed")


async def start_telethon() -> None:
    await tg_client.connect()
    if not await tg_client.is_user_authorized():
        raise RuntimeError("Telethon session is not authorized. Run: python create_session.py first.")
    register_new_message_handler(client=tg_client, db=db, bot=bot, admin_id=settings.admin_id, min_score=settings.min_lead_score,
                                 max_age_hours=settings.lead_max_age_hours, monitored_chat_ids=settings.monitored_chat_ids,
                                 geo_keywords=settings.geo_keywords, geo_required=current_geo_required())
    me = await tg_client.get_me()
    log.info("Telethon connected as %s", getattr(me, "username", None) or getattr(me, "id", "unknown"))


async def main() -> None:
    init_runtime_defaults()
    await start_telethon()
    try:
        await bot.send_message(settings.admin_id, "✅ LeadKZ Free v8.4 запущен. Нажми /start для меню.")
    except Exception as exc:
        # Важно: Railway не должен падать, если ADMIN_ID неверный или пользователь ещё не нажал /start.
        db.log_error("startup_notify", f"{type(exc).__name__}: {exc}")
        log.warning("Startup notification was not delivered. Check ADMIN_ID and press /start in the bot. Error: %s", exc)
    if settings.scan_my_groups_on_start:
        asyncio.create_task(run_existing_dialog_group_scan(reason="startup"))
        asyncio.create_task(periodic_existing_dialog_group_scan())
    if settings.health_server_enabled:
        asyncio.create_task(start_health_server())
    asyncio.create_task(periodic_scan())
    asyncio.create_task(periodic_discovery())
    asyncio.create_task(periodic_reminders())
    if settings.daily_report_enabled:
        asyncio.create_task(periodic_daily_report())
    if settings.autobackup_enabled:
        asyncio.create_task(periodic_autobackup())
    if settings.cleanup_enabled:
        asyncio.create_task(periodic_cleanup())
    if settings.weekly_report_enabled:
        asyncio.create_task(periodic_weekly_group_report())
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        db.close()
