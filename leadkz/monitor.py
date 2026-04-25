from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from aiogram import Bot
from telethon import TelegramClient, events, types
from telethon.errors import FloodWaitError

from .database import Database, Lead
from .filters import detect_niche, geo_score_for_text, normalize_text, score_message
from .formatting import format_lead
from .keyboards import lead_actions

log = logging.getLogger(__name__)
LOCAL_TZ = ZoneInfo("Asia/Almaty")


def is_group_entity(entity: object) -> bool:
    if isinstance(entity, types.Chat):
        return True
    if isinstance(entity, types.Channel):
        return bool(getattr(entity, "megagroup", False))
    return False


def make_message_link(chat: object, message_id: int) -> str | None:
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{message_id}"
    return None


def should_notify_now(db: Database, score: int = 0) -> bool:
    if db.get_setting_bool("quiet_mode", False):
        return False
    if db.get_setting_bool("smart_notification_limit", True):
        max_per_hour = db.get_setting_int("max_notifications_per_hour", 10)
        if max_per_hour > 0 and score < 90:
            recent = db.get_notifications_count_since(datetime.now(timezone.utc) - timedelta(hours=1))
            if recent >= max_per_hour:
                return False
    if not db.get_setting_bool("working_hours", True):
        return True
    now = datetime.now(LOCAL_TZ)
    start = db.get_setting_int("work_start", 9)
    end = db.get_setting_int("work_end", 22)
    if start <= end:
        return start <= now.hour < end
    return now.hour >= start or now.hour < end


async def save_and_notify_if_lead(*, db: Database, bot: Bot, admin_id: int, chat: object, message: object,
                                  min_score: int, max_age_hours: int, monitored_chat_ids: list[int],
                                  geo_keywords: Iterable[str], geo_required: bool) -> Optional[Lead]:
    chat_id = int(getattr(chat, "id", 0) or 0)
    if monitored_chat_ids and chat_id not in monitored_chat_ids:
        return None
    if db.is_group_blocked(chat_id) or not is_group_entity(chat):
        return None
    if db.get_setting_bool("only_valid_groups", True) and not monitored_chat_ids:
        status = db.get_group_status(chat_id)
        if status not in {"valid", "priority", "active"}:
            return None

    effective_min_score = db.get_setting_int("min_score", min_score)
    effective_geo_required = db.get_setting_bool("geo_only", geo_required)
    hot_only = db.get_setting_bool("hot_only", False)
    enabled_niches = db.get_enabled_niches()

    text = getattr(message, "message", None) or getattr(message, "text", None) or ""
    chat_title = getattr(chat, "title", "Без названия") or "Без названия"
    chat_username = getattr(chat, "username", None)

    os.environ["EXACT_LEADS_MODE"] = "true" if db.get_setting_bool("exact_leads_mode", True) else "false"
    os.environ["BUYER_ONLY_MODE"] = "true"
    os.environ["REJECT_SELLERS"] = "true"
    os.environ["REQUIRE_BUYER_INTENT"] = "true"
    result = score_message(text, min_score=effective_min_score, geo_keywords=geo_keywords, geo_required=False)
    delta, learned_reasons, learned_spam, learned_geo_hits = db.apply_learning_to_text(f"{text}\n{chat_title}\n@{chat_username or ''}")

    msg_date_for_reject = getattr(message, "date", None)
    if not isinstance(msg_date_for_reject, datetime):
        msg_date_for_reject = datetime.now(timezone.utc)
    if msg_date_for_reject.tzinfo is None:
        msg_date_for_reject = msg_date_for_reject.replace(tzinfo=timezone.utc)

    sender_for_reject = getattr(message, "sender", None)
    sender_id_for_reject = getattr(message, "sender_id", None)
    sender_username_for_reject = getattr(sender_for_reject, "username", None) if sender_for_reject else None
    message_id_for_reject = int(getattr(message, "id", 0) or 0)
    link_for_reject = make_message_link(chat, message_id_for_reject)

    if learned_spam:
        db.add_rejected_message(
            chat_id=chat_id, message_id=message_id_for_reject, chat_title=chat_title, chat_username=chat_username,
            sender_id=sender_id_for_reject, sender_username=sender_username_for_reject, text=text, score=0,
            category="learned_spam", reasons=["скрыто обученным спам-словом"],
            message_date=msg_date_for_reject, link=link_for_reject,
        )
        return None
    if not result.is_lead and delta < 20:
        if db.get_setting_bool("save_hidden_messages", True):
            db.add_rejected_message(
                chat_id=chat_id, message_id=message_id_for_reject, chat_title=chat_title, chat_username=chat_username,
                sender_id=sender_id_for_reject, sender_username=sender_username_for_reject, text=text, score=result.score,
                category=result.category, reasons=result.reasons,
                message_date=msg_date_for_reject, link=link_for_reject,
            )
        return None

    niche = detect_niche(result.category, text)
    if db.get_setting_bool("chatbot_only_mode", False):
        cleaned_for_bot_mode = normalize_text(text + " " + result.category)
        bot_related = niche == "bots" or (
            niche == "automation" and any(x in cleaned_for_bot_mode for x in [
                "заявк", "запис", "kaspi", "каспи", "рассыл", "уведомл", "telegram", "телеграм", "бот"
            ])
        )
        if not bot_related:
            if db.get_setting_bool("save_hidden_messages", True):
                db.add_rejected_message(
                    chat_id=chat_id, message_id=message_id_for_reject, chat_title=chat_title, chat_username=chat_username,
                    sender_id=sender_id_for_reject, sender_username=sender_username_for_reject, text=text, score=result.score,
                    category="not_chatbot_lead", reasons=["скрыто: включён режим «только чат-боты/автоматизация»"],
                    message_date=msg_date_for_reject, link=link_for_reject,
                )
            return None
    if enabled_niches and niche not in enabled_niches:
        return None

    geo_score, geo_hits = geo_score_for_text(f"{text}\n{chat_title}\n@{chat_username or ''}", list(geo_keywords) + learned_geo_hits)
    if learned_geo_hits:
        geo_score = min(100, geo_score + 15)
        geo_hits.extend(learned_geo_hits)
    if effective_geo_required and geo_score <= 0:
        return None

    final_score = min(max(result.score + delta, 0) + min(geo_score, 30), 100)
    if final_score < effective_min_score or (hot_only and final_score < 80):
        return None

    msg_date = getattr(message, "date", None)
    if not isinstance(msg_date, datetime):
        msg_date = datetime.now(timezone.utc)
    if msg_date.tzinfo is None:
        msg_date = msg_date.replace(tzinfo=timezone.utc)
    if msg_date < datetime.now(timezone.utc) - timedelta(hours=max_age_hours):
        return None

    sender = getattr(message, "sender", None)
    sender_id = getattr(message, "sender_id", None)
    sender_username = getattr(sender, "username", None) if sender else None
    message_id = int(getattr(message, "id", 0) or 0)
    link = make_message_link(chat, message_id)
    reasons = list(result.reasons) + learned_reasons
    if geo_hits:
        reasons.append(f"гео Казахстан: {', '.join(geo_hits[:4])}")

    inserted = db.add_lead(
        chat_id=chat_id, message_id=message_id, chat_title=chat_title, chat_username=chat_username,
        sender_id=sender_id, sender_username=sender_username, text=text, score=final_score,
        category=result.category, geo_score=geo_score, reasons=reasons, message_date=msg_date,
        link=link, lead_hash=result.lead_hash,
    )
    if not inserted:
        return None

    lead = Lead(chat_id=chat_id, message_id=message_id, chat_title=chat_title, chat_username=chat_username,
                sender_id=sender_id, sender_username=sender_username, text=text, score=final_score,
                category=result.category, geo_score=geo_score, reasons=" | ".join(reasons),
                message_date=msg_date.astimezone(timezone.utc).isoformat(), link=link, status="new",
                lead_hash=result.lead_hash, duplicate_count=0)

    if should_notify_now(db, final_score):
        await bot.send_message(admin_id, format_lead(lead), reply_markup=lead_actions(chat_id, message_id, link, result.category), disable_web_page_preview=True)
        db.add_notification_log(chat_id, message_id, final_score)
    return lead


async def scan_recent_messages(*, client: TelegramClient, db: Database, bot: Bot, admin_id: int, min_score: int,
                               max_age_hours: int, monitored_chat_ids: list[int], geo_keywords: Iterable[str],
                               geo_required: bool, per_group_limit: int = 80) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    saved_count = 0
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if not is_group_entity(entity):
            continue
        entity_id = int(getattr(entity, "id", 0) or 0)
        if monitored_chat_ids and entity_id not in monitored_chat_ids:
            continue
        if db.is_group_blocked(entity_id):
            continue
        try:
            status = db.get_group_status(entity_id)
            limit = per_group_limit * 2 if status == "priority" else per_group_limit
            async for message in client.iter_messages(entity, limit=limit):
                msg_date = message.date
                if msg_date and msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)
                if msg_date and msg_date < cutoff:
                    break
                lead = await save_and_notify_if_lead(db=db, bot=bot, admin_id=admin_id, chat=entity, message=message,
                                                     min_score=min_score, max_age_hours=max_age_hours,
                                                     monitored_chat_ids=monitored_chat_ids, geo_keywords=geo_keywords,
                                                     geo_required=geo_required)
                if lead:
                    saved_count += 1
        except FloodWaitError as exc:
            log.warning("Flood wait while scanning %s: %s seconds", getattr(entity, "title", entity_id), exc.seconds)
            await asyncio.sleep(min(exc.seconds, 60))
        except Exception as exc:
            db.log_error("scan_dialog", f"{getattr(entity, 'title', entity_id)}: {type(exc).__name__}: {exc}")
            log.exception("Failed to scan dialog: %s", getattr(entity, "title", entity_id))
        await asyncio.sleep(0.5)
    return saved_count


async def scan_valid_public_groups(*, client: TelegramClient, db: Database, bot: Bot, admin_id: int, min_score: int,
                                   max_age_hours: int, geo_keywords: Iterable[str], geo_required: bool,
                                   per_group_limit: int = 80, max_groups: int = 50) -> int:
    """Сканирует публичные валидные группы из автопоиска даже если их нет в списке dialogs."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    saved_count = 0
    for group in db.get_valid_group_candidates(limit=max_groups):
        if not group.username or db.is_group_blocked(group.chat_id):
            continue
        try:
            entity = await client.get_entity(group.username)
            limit = per_group_limit * 2 if group.status == "priority" else per_group_limit
            async for message in client.iter_messages(entity, limit=limit):
                msg_date = message.date
                if msg_date and msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)
                if msg_date and msg_date < cutoff:
                    break
                lead = await save_and_notify_if_lead(
                    db=db, bot=bot, admin_id=admin_id, chat=entity, message=message,
                    min_score=min_score, max_age_hours=max_age_hours, monitored_chat_ids=[],
                    geo_keywords=geo_keywords, geo_required=geo_required,
                )
                if lead:
                    saved_count += 1
        except FloodWaitError as exc:
            log.warning("Flood wait while scanning public group %s: %s seconds", group.title, exc.seconds)
            await asyncio.sleep(min(exc.seconds, 60))
        except Exception as exc:
            db.mark_group_error(group.chat_id, f"scan error: {type(exc).__name__}")
            db.log_error("scan_public_group", f"{group.title}: {type(exc).__name__}: {exc}")
            log.info("Failed to scan valid public group %s: %s", group.title, exc)
        await asyncio.sleep(0.6)
    return saved_count


def register_new_message_handler(*, client: TelegramClient, db: Database, bot: Bot, admin_id: int, min_score: int,
                                 max_age_hours: int, monitored_chat_ids: list[int], geo_keywords: Iterable[str], geo_required: bool) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def handler(event: events.NewMessage.Event) -> None:
        try:
            chat = await event.get_chat()
            message = event.message
            # Telethon Message.sender is a read-only property.
            # Не присваиваем message.sender, иначе Railway падает с:
            # AttributeError: property 'sender' of 'Message' object has no setter
            await save_and_notify_if_lead(db=db, bot=bot, admin_id=admin_id, chat=chat, message=message,
                                          min_score=min_score, max_age_hours=max_age_hours,
                                          monitored_chat_ids=monitored_chat_ids, geo_keywords=geo_keywords,
                                          geo_required=geo_required)
        except Exception as exc:
            try:
                db.log_error("new_message", f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            log.exception("Failed to process new Telegram message")
