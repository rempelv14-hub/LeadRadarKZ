from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, RPCError

from .database import Database
from .filters import (
    BUYER_PHRASES,
    BUYER_WORDS,
    DEVELOPER_PHRASES,
    SPAM_PHRASES,
    SOLUTION_CONTEXT_WORDS,
    geo_score_for_text,
    normalize_text,
    score_message,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GroupEvaluation:
    score: int
    validation_status: str
    reasons: list[str]
    lead_hits: int
    spam_hits: int
    developer_hits: int
    recent_messages: int
    total_messages: int


def _group_text(chat: object, query: str) -> str:
    title = getattr(chat, "title", "") or ""
    username = getattr(chat, "username", "") or ""
    about = getattr(chat, "about", "") or ""
    return f"{title} @{username} {about} {query}"


def _contains_any(cleaned: str, phrases: Iterable[str]) -> list[str]:
    return [phrase for phrase in phrases if phrase and phrase.lower().replace("ё", "е") in cleaned]


def similar_queries_from_titles(titles: Iterable[str]) -> list[str]:
    queries: list[str] = []
    stop = {"чат", "группа", "казахстан", "қазақстан", "kz", "для", "және", "и", "в", "по", "the", "of"}
    for title in titles:
        cleaned = re.sub(r"[^a-zA-Zа-яА-Яәғқңөұүіһ0-9\s-]", " ", title.lower())
        words = [w for w in cleaned.split() if len(w) >= 4 and w not in stop]
        base = " ".join(words[:3]).strip()
        if base:
            queries.extend([base, f"{base} казахстан", f"{base} алматы", f"{base} астана", f"{base} бизнес"])
    defaults = [
        "предприниматели казахстан", "бизнес алматы", "стартапы казахстан", "маркетинг казахстан",
        "онлайн школы казахстан", "kaspi бизнес", "crm казахстан", "автоматизация бизнеса казахстан",
    ]
    result: list[str] = []
    for q in queries + defaults:
        if q not in result:
            result.append(q)
    return result[:25]


def _status_from_score(score: int, min_valid_score: int, hide_bad: bool) -> tuple[str, str]:
    if score >= 88:
        return "strong", "valid"
    if score >= min_valid_score:
        return "valid", "valid"
    if score >= 50:
        return "weak", "candidate"
    return "trash", "hidden" if hide_bad else "candidate"


async def evaluate_public_group(
    client: TelegramClient,
    chat: object,
    query: str,
    geo_keywords: Iterable[str],
    history_limit: int = 120,
    min_valid_score: int = 75,
    geo_required: bool = True,
) -> GroupEvaluation:
    """Оценивает группу по названию и доступным последним сообщениям. Не вступает в группу."""
    title = getattr(chat, "title", "") or ""
    username = getattr(chat, "username", "") or ""
    base_text = _group_text(chat, query)
    geo_score, geo_hits = geo_score_for_text(base_text, geo_keywords)

    score = 0
    reasons: list[str] = []
    if geo_score > 0:
        add = min(30, geo_score)
        score += add
        reasons.append(f"KZ-гео в названии/описании: +{add} ({', '.join(geo_hits[:4])})")
    elif geo_required:
        reasons.append("нет признаков Казахстана в названии/описании")

    lead_hits = 0
    spam_hits = 0
    developer_hits = 0
    buyer_hits = 0
    solution_hits = 0
    question_hits = 0
    recent_messages = 0
    total_messages = 0
    now = datetime.now(timezone.utc)
    collected_text = []

    try:
        async for message in client.iter_messages(chat, limit=max(20, history_limit)):
            text = getattr(message, "message", None) or getattr(message, "text", None) or ""
            if not text:
                continue
            msg_date = getattr(message, "date", None)
            if isinstance(msg_date, datetime):
                if msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)
                if msg_date >= now - timedelta(hours=24):
                    recent_messages += 1
            total_messages += 1
            cleaned = normalize_text(text)
            collected_text.append(cleaned[:400])

            if _contains_any(cleaned, SPAM_PHRASES):
                spam_hits += 1
            if _contains_any(cleaned, DEVELOPER_PHRASES):
                developer_hits += 1
            if _contains_any(cleaned, BUYER_PHRASES) or _contains_any(cleaned, BUYER_WORDS):
                buyer_hits += 1
            if _contains_any(cleaned, SOLUTION_CONTEXT_WORDS):
                solution_hits += 1
            if "?" in text or "кто" in cleaned or "кім" in cleaned or "посоветуйте" in cleaned or "подскажите" in cleaned:
                question_hits += 1

            lead_result = score_message(text, min_score=55, geo_keywords=geo_keywords, geo_required=False)
            if lead_result.is_lead or lead_result.score >= 60:
                lead_hits += 1
    except RPCError as exc:
        raise RuntimeError(f"нет доступа к истории или Telegram ограничил запрос: {type(exc).__name__}") from exc
    except Exception as exc:
        raise RuntimeError(f"не удалось прочитать последние сообщения: {type(exc).__name__}") from exc

    if total_messages == 0:
        score -= 30
        reasons.append("последние сообщения недоступны или группа пустая: -30")
    else:
        if recent_messages >= 20:
            score += 25
            reasons.append(f"активная за 24 часа: +25 ({recent_messages} сообщений)")
        elif recent_messages >= 5:
            score += 16
            reasons.append(f"есть активность за 24 часа: +16 ({recent_messages} сообщений)")
        elif recent_messages >= 1:
            score += 8
            reasons.append(f"низкая активность за 24 часа: +8 ({recent_messages} сообщений)")
        else:
            score -= 25
            reasons.append("нет свежих сообщений за 24 часа: -25")

        if lead_hits:
            add = min(32, lead_hits * 8)
            score += add
            reasons.append(f"в истории есть похожие заявки: +{add} ({lead_hits})")
        if buyer_hits:
            add = min(20, buyer_hits * 4)
            score += add
            reasons.append(f"есть слова заказчиков: +{add} ({buyer_hits})")
        if solution_hits:
            add = min(16, solution_hits * 3)
            score += add
            reasons.append(f"есть темы боты/CRM/Kaspi/сайты: +{add} ({solution_hits})")
        if question_hits:
            add = min(14, question_hits * 2)
            score += add
            reasons.append(f"есть вопросы людей: +{add} ({question_hits})")

        spam_penalty = min(45, spam_hits * 8)
        dev_penalty = min(50, developer_hits * 9)
        if spam_penalty:
            score -= spam_penalty
            reasons.append(f"спам/мусор: -{spam_penalty} ({spam_hits})")
        if dev_penalty:
            score -= dev_penalty
            reasons.append(f"много исполнителей/рекламы услуг: -{dev_penalty} ({developer_hits})")

        spam_ratio = spam_hits / max(total_messages, 1)
        dev_ratio = developer_hits / max(total_messages, 1)
        if spam_ratio < 0.08 and dev_ratio < 0.10 and total_messages >= 20:
            score += 10
            reasons.append("мало спама и рекламы: +10")

    if geo_required and geo_score <= 0:
        score = min(score, 49)

    score = max(0, min(100, int(score)))
    validation_status, _ = _status_from_score(score, min_valid_score, hide_bad=False)
    if score >= 88:
        reasons.insert(0, "сильная валидная группа")
    elif score >= min_valid_score:
        reasons.insert(0, "валидная группа")
    elif score >= 50:
        reasons.insert(0, "средняя группа, лучше проверить")
    else:
        reasons.insert(0, "мусорная/слабая группа")

    return GroupEvaluation(
        score=score,
        validation_status=validation_status,
        reasons=reasons[:10],
        lead_hits=lead_hits,
        spam_hits=spam_hits,
        developer_hits=developer_hits,
        recent_messages=recent_messages,
        total_messages=total_messages,
    )


async def discover_public_groups(
    client: TelegramClient,
    db: Database,
    keywords: Iterable[str],
    limit_per_keyword: int = 15,
    geo_keywords: Iterable[str] | None = None,
    geo_required: bool = True,
    history_limit: int = 120,
    min_valid_score: int = 75,
    auto_hide_bad: bool = True,
) -> int:
    """Ищет публичные группы, оценивает валидность и автодобавляет сильные в мониторинг."""
    added_or_updated = 0
    geo_keywords = list(geo_keywords or [])
    for keyword in keywords:
        query = keyword.strip()
        if not query:
            continue
        try:
            found = await client(functions.contacts.SearchRequest(q=query, limit=limit_per_keyword))
        except FloodWaitError as exc:
            log.warning("Flood wait while discovering groups: %s seconds. Stopping this discovery cycle.", exc.seconds)
            # Do not continue with other keywords. Continuing would only hit the same Telegram limit again.
            # The next attempt will happen after DISCOVERY_INTERVAL_HOURS / AUTO_DISCOVERY_INTERVAL_MINUTES.
            return added_or_updated
        except Exception:
            log.exception("Failed to search public groups for query=%r", query)
            continue

        for chat in getattr(found, "chats", []):
            kind = None
            username = None
            title = getattr(chat, "title", "Без названия") or "Без названия"
            if isinstance(chat, types.Channel):
                if not bool(getattr(chat, "megagroup", False)):
                    continue
                kind = "megagroup"
                username = getattr(chat, "username", None)
            elif isinstance(chat, types.Chat):
                kind = "group"
            else:
                continue

            base_geo_score, _ = geo_score_for_text(_group_text(chat, query), geo_keywords)
            if geo_required and base_geo_score <= 0:
                continue

            chat_id = int(getattr(chat, "id", 0) or 0)
            db.add_group_candidate(chat_id=chat_id, title=title, username=username, query=query, kind=kind, geo_score=base_geo_score)
            try:
                evaluation = await evaluate_public_group(
                    client, chat, query, geo_keywords=geo_keywords, history_limit=history_limit,
                    min_valid_score=min_valid_score, geo_required=geo_required,
                )
                validation, suggested_status = _status_from_score(evaluation.score, min_valid_score, auto_hide_bad)
                db.update_group_evaluation(
                    chat_id=chat_id,
                    score=evaluation.score,
                    validation_status=validation,
                    reasons=evaluation.reasons,
                    lead_hits=evaluation.lead_hits,
                    spam_hits=evaluation.spam_hits,
                    developer_hits=evaluation.developer_hits,
                    recent_messages=evaluation.recent_messages,
                    status=suggested_status,
                )
                added_or_updated += 1
            except Exception as exc:
                db.mark_group_error(chat_id, str(exc))
                log.info("Group evaluation failed for %s: %s", title, exc)
        await asyncio.sleep(1.2)
    return added_or_updated


async def discover_similar_groups(
    client: TelegramClient,
    db: Database,
    limit_per_keyword: int,
    geo_keywords: Iterable[str],
    geo_required: bool,
    history_limit: int = 120,
    min_valid_score: int = 75,
    auto_hide_bad: bool = True,
) -> int:
    titles = db.get_top_group_titles(datetime.now(timezone.utc) - timedelta(days=7), limit=6)
    titles.extend([g.title for g in db.get_best_auto_groups(limit=6)])
    queries = similar_queries_from_titles(titles)
    return await discover_public_groups(
        client, db, queries, limit_per_keyword=limit_per_keyword, geo_keywords=geo_keywords,
        geo_required=geo_required, history_limit=history_limit, min_valid_score=min_valid_score,
        auto_hide_bad=auto_hide_bad,
    )
