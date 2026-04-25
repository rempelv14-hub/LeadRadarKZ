from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
from zoneinfo import ZoneInfo

from .database import GroupCandidate, Lead
from .filters import NICHE_LABELS, detect_city, estimate_budget, reply_risk

LOCAL_TZ = ZoneInfo("Asia/Almaty")

STATUS_LABELS = {
    "new": "🆕 Новый", "favorite": "⭐ Избранное", "contacted": "💬 Написал",
    "talking": "🤝 Обсуждение", "waiting_payment": "💰 Ждет оплату", "closed": "✅ Закрыт",
    "lost": "❌ Не подошел", "hidden": "🚫 Скрыт", "archived": "🗂 Архив",
}
GROUP_STATUS_LABELS = {
    "candidate": "🔎 Кандидат",
    "valid": "✅ Валидная / авто-мониторинг",
    "active": "✅ Активная",
    "priority": "⭐ Важная",
    "blocked": "🚫 Черный список",
    "hidden": "🗑 Скрыта как мусор",
}


def dt_to_local_text(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ).strftime("%d.%m.%Y %H:%M")


def trim_text(text: str, limit: int = 700) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


def lead_heat(score: int) -> str:
    if score >= 80:
        return "🔥 Горячий"
    if score >= 60:
        return "🟡 Тёплый"
    return "⚪ Слабый"


def format_lead(lead: Lead) -> str:
    link_line = f"\n<a href=\"{escape(lead.link)}\">Открыть сообщение</a>" if lead.link else "\nСсылка: нет публичной ссылки"
    sender = f"@{lead.sender_username}" if lead.sender_username else "не указан"
    duplicate_line = f"\n<b>Дубликаты:</b> {lead.duplicate_count}" if lead.duplicate_count else ""
    context = f"{lead.text}\n{lead.chat_title}\n{lead.category}"
    city = detect_city(context)
    budget = estimate_budget(lead.text, lead.category)
    risk = reply_risk(lead.text, lead.score, lead.category)
    return (
        "🔥 <b>Новый лид / digital-заказ</b>\n\n"
        f"<b>Оценка:</b> {lead.score}/100 — {lead_heat(lead.score)}\n"
        f"<b>Риск ответа:</b> {escape(risk)}\n"
        f"<b>Бюджет:</b> {escape(budget)}\n"
        f"<b>Категория:</b> {escape(lead.category)}\n"
        f"<b>Город:</b> {escape(city)}\n"
        f"<b>Гео KZ:</b> {lead.geo_score}/100\n"
        f"<b>Статус:</b> {STATUS_LABELS.get(lead.status, escape(lead.status))}\n"
        f"<b>Группа:</b> {escape(lead.chat_title)}\n"
        f"<b>Автор:</b> {escape(sender)}\n"
        f"<b>Время:</b> {dt_to_local_text(lead.message_date)}\n"
        f"<b>Причина:</b> {escape(lead.reasons)}{duplicate_line}{link_line}\n\n"
        f"<b>Текст:</b>\n{escape(trim_text(lead.text))}"
    )


def format_leads_list(leads: list[Lead], title: str) -> str:
    if not leads:
        return f"{title}\n\nПока нет свежих лидов."
    parts = [title]
    for i, lead in enumerate(leads, start=1):
        link = f" — <a href=\"{escape(lead.link)}\">открыть</a>" if lead.link else ""
        city = detect_city(f"{lead.text} {lead.chat_title}")
        budget = estimate_budget(lead.text, lead.category)
        parts.append(
            f"\n{i}. <b>{lead.score}/100</b> {lead_heat(lead.score)} — {escape(lead.category)}\n"
            f"📍 {escape(city)} | {escape(budget)}\n"
            f"{escape(lead.chat_title)}{link}\n"
            f"Статус: {STATUS_LABELS.get(lead.status, escape(lead.status))}\n"
            f"{escape(trim_text(lead.text, 180))}"
        )
    return "\n".join(parts)


def format_groups(groups: list[GroupCandidate], title: str = "🔎 <b>Найденные публичные группы по Казахстану</b>") -> str:
    if not groups:
        return f"{title}\n\nПока нет групп. Нажми «Найти группы KZ»."
    parts = [title + "\n"]
    for i, group in enumerate(groups, start=1):
        link = f"https://t.me/{group.username}" if group.username else None
        link_text = f" — <a href=\"{escape(link)}\">открыть</a>" if link else ""
        status = GROUP_STATUS_LABELS.get(group.status, group.status)
        city = detect_city(f"{group.title} {group.query}")
        verdict = {
            "strong": "🔥 сильная",
            "valid": "✅ валидная",
            "weak": "🟡 средняя",
            "trash": "🗑 мусор",
            "error": "⚠️ ошибка",
            "unchecked": "⚪ не проверена",
        }.get(group.validation_status, group.validation_status)
        checked = f" | Проверка: {dt_to_local_text(group.checked_at)}" if group.checked_at else ""
        error = f"\n⚠️ {escape(group.last_error)}" if group.last_error else ""
        reasons = f"\nПричины: {escape(trim_text(group.evaluation_reasons, 220))}" if group.evaluation_reasons else ""
        parts.append(
            f"{i}. <b>{escape(group.title)}</b>{link_text}\n"
            f"Рейтинг: <b>{group.group_score}/100</b> — {verdict} | Статус: {status}\n"
            f"Лиды в истории: {group.lead_hits} | Спам: {group.spam_hits} | Исполнители: {group.developer_hits} | Сообщений 24ч: {group.recent_messages}\n"
            f"Гео: {group.geo_score}/100 | 📍 {escape(city)}\n"
            f"Тип: {escape(group.kind)} | Поиск: {escape(group.query)} | Найдена: {dt_to_local_text(group.discovered_at)}{checked}"
            f"{reasons}{error}"
        )
    return "\n\n".join(parts)


def format_group_ratings(ratings: list[dict[str, object]]) -> str:
    if not ratings:
        return "🏆 Пока нет рейтинга групп. Нужно, чтобы бот нашёл лиды за последние дни."
    parts = ["🏆 <b>Рейтинг групп за 7 дней</b>\n"]
    for i, row in enumerate(ratings, start=1):
        username = row.get("username")
        link = f" — <a href=\"https://t.me/{escape(str(username))}\">открыть</a>" if username else ""
        parts.append(
            f"{i}. <b>{escape(str(row['title']))}</b>{link}\n"
            f"Оценка группы: <b>{row['quality']}/100</b> | Лидов: {row['leads_count']} | Горячих: {row['hot_count']} | Средний балл: {row['avg_score']}"
        )
    return "\n\n".join(parts)


def format_bad_groups(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "🚫 Плохих групп для удаления пока не вижу."
    parts = ["🚫 <b>Рекомендации по плохим группам</b>\n"]
    for i, row in enumerate(rows, start=1):
        username = row.get("username")
        link = f" — <a href=\"https://t.me/{escape(str(username))}\">открыть</a>" if username else ""
        parts.append(f"{i}. <b>{escape(str(row['title']))}</b>{link}\nПричина: {escape(str(row['reason']))}")
    parts.append("\nКнопкой 🚫 в списке групп можно добавить группу в черный список.")
    return "\n\n".join(parts)


def format_stats(stats: dict[str, object], title: str = "📊 Статистика за 24 часа") -> str:
    parts = [
        f"{title}\n", f"Всего лидов: <b>{stats.get('total', 0)}</b>",
        f"Горячих 80+: <b>{stats.get('hot', 0)}</b>", f"В избранном: <b>{stats.get('favorite', 0)}</b>",
        f"В архиве: <b>{stats.get('archived', 0)}</b>", f"Дубликатов отсеяно: <b>{stats.get('duplicates', 0)}</b>",
    ]
    if stats.get("city_counts"):
        parts.append("\n<b>📍 Карта лидов по городам:</b>")
        for city, count in stats["city_counts"]:
            parts.append(f"— {escape(str(city))}: {count}")
    if stats.get("categories"):
        parts.append("\n<b>Категории:</b>")
        for category, count in stats["categories"]:
            parts.append(f"— {escape(str(category))}: {count}")
    if stats.get("top_groups"):
        parts.append("\n<b>Лучшие группы:</b>")
        for group, count in stats["top_groups"]:
            parts.append(f"— {escape(str(group))}: {count}")
    if stats.get("statuses"):
        parts.append("\n<b>CRM-статусы:</b>")
        for status, count in stats["statuses"]:
            parts.append(f"— {STATUS_LABELS.get(str(status), escape(str(status)))}: {count}")
    return "\n".join(parts)


def format_settings(settings_map: dict[str, str], default_min_score: int, default_geo: bool) -> str:
    min_score = int(settings_map.get("min_score", str(default_min_score)))
    hot_only = settings_map.get("hot_only", "0") == "1"
    geo_only = settings_map.get("geo_only", "1" if default_geo else "0") == "1"
    quiet = settings_map.get("quiet_mode", "0") == "1"
    only_valid = settings_map.get("only_valid_groups", "1") == "1"
    work = settings_map.get("working_hours", "1") == "1"
    start = settings_map.get("work_start", "9")
    end = settings_map.get("work_end", "22")
    return (
        "⚙️ <b>Настройки LeadKZ Free v8</b>\n\n"
        f"Гео Казахстан: {'✅ включено' if geo_only else '❌ выключено'}\n"
        f"Только горячие 80+: {'✅ включено' if hot_only else '❌ выключено'}\n"
        f"Минимальный балл лида: <b>{min_score}</b>\n"
        f"Рабочее время уведомлений: {'✅' if work else '❌'} {start}:00–{end}:00\n"
        f"Тихий режим: {'✅ включен' if quiet else '❌ выключен'}\n"
        f"Только валидные группы: {'✅ включено' if only_valid else '❌ выключено'}\n\n"
        "Ночью/в тихом режиме лиды сохраняются, но уведомления не отправляются сразу."
    )


def format_niches(settings_map: dict[str, str]) -> str:
    parts = ["🎯 <b>Ниши поиска</b>\n"]
    for key, label in NICHE_LABELS.items():
        default = True
        enabled = settings_map.get(f"niche_{key}", "1" if default else "0") == "1"
        parts.append(f"{'✅' if enabled else '⬜'} {label}")
    parts.append("\nМожно включить сайты/SMM/дизайн, если хочешь искать клиентов шире, чем чат-боты.")
    return "\n".join(parts)


def format_price_calculator() -> str:
    return (
        "💰 <b>Калькулятор цены</b>\n\n"
        "Бот с меню: <b>от 30 000 ₸</b>\n"
        "Заявки + уведомления: <b>от 50 000 ₸</b>\n"
        "Запись клиентов: <b>от 70 000 ₸</b>\n"
        "Оплата/Kaspi-сценарии: <b>от 80 000 ₸</b>\n"
        "CRM/Google Sheets/статусы: <b>от 120 000 ₸</b>\n"
        "Рассылки/сегменты/админка: <b>от 150 000 ₸</b>\n"
        "Mini App/API/много филиалов: <b>от 200 000 ₸</b>\n\n"
        "Формула: базовая цена + сложность + интеграции + срочность. Точную цену лучше называть после 5–7 вопросов клиенту."
    )


def format_upcoming_reminders(rows) -> str:
    if not rows:
        return "⏰ Активных напоминаний нет."
    parts = ["⏰ <b>Ближайшие напоминания</b>\n"]
    for i, row in enumerate(rows, start=1):
        parts.append(f"{i}. {dt_to_local_text(row['remind_at'])} — lead {row['chat_id']}:{row['message_id']}")
    return "\n".join(parts)


def anti_ban_text() -> str:
    return (
        "🛡 <b>Антибан-режим</b>\n\n"
        "Бот работает безопасно:\n"
        "— не пишет клиентам автоматически;\n"
        "— не вступает в группы автоматически;\n"
        "— не делает массовую рассылку;\n"
        "— показывает риск ответа по каждому лиду;\n"
        "— шаблоны нужно отправлять вручную;\n"
        "— дубликаты скрываются;\n"
        "— есть рабочее время и тихий режим для уведомлений."
    )


def since_hours(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)

# ===== LeadKZ Free v5 extra formatters =====
from .sales import (
    client_questions_text,
    daily_plan_text,
    format_case_line,
    freshness_label,
    lead_confidence,
    sales_script_text,
    segment_title,
    suggested_offer,
)


def format_priority_queue(leads: list[Lead]) -> str:
    if not leads:
        return "🎯 Очередь пустая. Пока нет лидов, которым нужно написать."
    parts = ["🎯 <b>Кому писать первым</b>\n"]
    for i, lead in enumerate(leads, start=1):
        link = f" — <a href=\"{escape(lead.link)}\">открыть</a>" if lead.link else ""
        parts.append(
            f"{i}. <b>{lead.score}/100</b> {lead_heat(lead.score)} | {freshness_label(lead.message_date)}\n"
            f"{escape(lead.category)} | {escape(detect_city(lead.text + ' ' + lead.chat_title))}\n"
            f"{escape(trim_text(lead.text, 180))}{link}"
        )
    return "\n\n".join(parts)


def format_segment_leads(leads: list[Lead], segment: str) -> str:
    return format_leads_list(leads, f"{segment_title(segment)} за 24 часа")


def format_learning(rows, feedback: dict[str, int]) -> str:
    parts = [
        "🧠 <b>Обучение фильтра</b>\n",
        f"👍 Хороших отметок: <b>{feedback.get('good', 0)}</b>",
        f"👎 Плохих отметок: <b>{feedback.get('bad', 0)}</b>",
        "\nКоманды:",
        "<code>/learn_plus фраза</code> — добавить плюс-слово",
        "<code>/learn_minus фраза</code> — добавить минус-слово",
        "<code>/learn_spam фраза</code> — добавить спам-слово",
        "<code>/learn_geo фраза</code> — добавить гео-слово Казахстана",
        "<code>/testlead текст</code> — проверить, как бот оценит сообщение",
    ]
    if rows:
        parts.append("\n<b>Последние обучающие слова:</b>")
        for row in rows[:12]:
            kind = str(row["kind"])
            labels = {"positive": "➕", "negative": "➖", "spam": "🚫", "geo": "📍", "niche": "🎯"}
            parts.append(f"{labels.get(kind, '•')} {escape(str(row['phrase']))} / вес {row['weight']}")
    return "\n".join(parts)


def format_cases(rows) -> str:
    if not rows:
        return "📁 Кейсов пока нет."
    parts = ["📁 <b>Библиотека кейсов</b>\n"]
    for i, row in enumerate(rows, start=1):
        parts.append(format_case_line(row, i))
    return "\n\n".join(parts)


def format_funnel(funnel: dict[str, int]) -> str:
    return (
        "📈 <b>Воронка продаж за 24 часа</b>\n\n"
        f"🆕 Новые: <b>{funnel.get('new', 0)}</b>\n"
        f"⭐ Избранные: <b>{funnel.get('favorite', 0)}</b>\n"
        f"💬 Написал: <b>{funnel.get('contacted', 0)}</b>\n"
        f"🤝 Обсуждение: <b>{funnel.get('talking', 0)}</b>\n"
        f"💰 Ждет оплату: <b>{funnel.get('waiting_payment', 0)}</b>\n"
        f"✅ Закрыто: <b>{funnel.get('closed', 0)}</b>\n"
        f"❌ Не подошло: <b>{funnel.get('lost', 0)}</b>"
    )


def format_revenue(stats: dict[str, object]) -> str:
    return (
        "💰 <b>Учет денег за 24 часа</b>\n\n"
        f"Потенциально в работе: <b>{int(stats.get('potential', 0)):,} ₸</b>\n".replace(',', ' ')
        + f"Закрыто сделок: <b>{int(stats.get('closed', 0)):,} ₸</b>\n".replace(',', ' ')
        + f"Количество закрытых сделок: <b>{stats.get('deals', 0)}</b>\n"
        + f"Средний чек: <b>{int(stats.get('avg_check', 0)):,} ₸</b>\n\n".replace(',', ' ')
        + "Чтобы записать сумму по лиду: <code>/money chat_id message_id 150000 комментарий</code>"
    )


def format_lead_advice(lead: Lead) -> str:
    return (
        "🧭 <b>Что предложить клиенту</b>\n\n"
        f"Уверенность: <b>{escape(lead_confidence(lead.score, lead.reasons, lead.geo_score))}</b>\n"
        f"Свежесть: <b>{freshness_label(lead.message_date)}</b>\n"
        f"Рекомендация: {escape(suggested_offer(lead.text, lead.category))}\n\n"
        + client_questions_text() + "\n\n" + sales_script_text()
    )


def format_daily_plan(stats: dict[str, object]) -> str:
    return daily_plan_text(stats)


def format_hidden_messages(rows) -> str:
    if not rows:
        return "🚫 <b>Скрытые сообщения</b>\n\nПока скрытых сообщений нет."
    parts = ["🚫 <b>Скрытые сообщения</b>\n\nЗдесь видно, что бот отсеял и почему. Если бот ошибся — нажми «✅ Это лид»."]
    for i, row in enumerate(rows, start=1):
        text = html.escape(" ".join(str(row.text).split())[:450])
        reasons = html.escape(str(row.reasons)[:300])
        title = html.escape(str(row.chat_title))
        parts.append(
            f"\n<b>{i}. {title}</b>\n"
            f"Оценка: <b>{row.score}/100</b> | Категория: <code>{html.escape(str(row.category))}</code>\n"
            f"Текст: {text}\n"
            f"Причина: <i>{reasons}</i>"
        )
    return "\n".join(parts)


def format_smart_reply(lead) -> str:
    text = (lead.text or "").lower()
    title = "вашей задаче"
    if any(w in text for w in ["курс", "школ", "урок", "обуч", "марафон"]):
        title = "онлайн-школе/курсам"
        offer = "запись учеников, оплату через Kaspi, выдачу материалов и уведомления"
    elif any(w in text for w in ["запись", "салон", "клиник", "мастер"]):
        title = "записи клиентов"
        offer = "выбор услуги, свободное время, уведомления администратору и напоминания клиентам"
    elif any(w in text for w in ["crm", "црм", "заявк", "воронк", "автоматизац"]):
        title = "автоматизации заявок"
        offer = "сбор заявок, CRM-воронку, уведомления менеджеру и таблицу/базу клиентов"
    elif any(w in text for w in ["сайт", "лендинг", "магазин"]):
        title = "сайту/лендингу"
        offer = "структуру, форму заявок, кнопки WhatsApp/Telegram и аналитику"
    elif any(w in text for w in ["smm", "смм", "реклам", "таргет"]):
        title = "продвижению"
        offer = "упаковку, контент, рекламную связку и обработку заявок"
    elif any(w in text for w in ["дизайн", "логотип", "баннер", "презентац"]):
        title = "дизайну"
        offer = "визуал, макеты, баннеры и оформление под вашу задачу"
    else:
        offer = "заявки, уведомления, базу клиентов и удобную обработку обращений"

    return (
        "💬 <b>Ответ под конкретный лид</b>\n\n"
        f"Здравствуйте! Увидел, что вам нужна помощь по {html.escape(title)}.\n"
        f"Могу помочь: {html.escape(offer)}.\n\n"
        "Чтобы точнее сориентировать по цене и срокам, подскажите, пожалуйста:\n"
        "1) для какого бизнеса это нужно?\n"
        "2) какие функции обязательны?\n"
        "3) есть ли пример, как должно работать?"
    )


def format_price_settings() -> str:
    return (
        "💰 <b>Базовые цены для расчёта</b>\n\n"
        "🤖 Простой бот: <b>от 50 000 ₸</b>\n"
        "💳 Бот с Kaspi/оплатой: <b>от 120 000 ₸</b>\n"
        "📦 CRM / автоматизация: <b>от 180 000 ₸</b>\n"
        "🌐 Сайт / лендинг: <b>от 100 000 ₸</b>\n"
        "📱 SMM / реклама: <b>от 80 000 ₸</b>\n"
        "🎨 Дизайн: <b>от 30 000 ₸</b>\n\n"
        "Для изменения цен пока используй текст в ответе/КП. Позже можно вынести редактирование цен в кнопки."
    )


def format_exact_scan_result(hours: int, saved_count: int) -> str:
    title = "⚡ <b>Проверка заявок за 1 час</b>" if hours == 1 else "🔎 <b>Проверка заявок за 24 часа</b>"
    return (
        f"{title}\n\n"
        f"Готово. Новых подходящих заявок сохранено: <b>{saved_count}</b>.\n\n"
        "Бот проверил текущие группы, где аккаунт уже состоит, и применил строгий фильтр:\n"
        "— только явные заявки;\n"
        "— только заказчики;\n"
        "— исполнители и реклама скрываются."
    )


def format_limits_status(blocked_until: str | None) -> str:
    if not blocked_until:
        return (
            "🛡 <b>Лимиты Telegram</b>\n\n"
            "Сейчас сохранённого Flood Wait нет.\n"
            "Можно сканировать текущие группы 24/7.\n\n"
            "Поиск новых публичных групп лучше запускать редко: 1–2 раза в день."
        )
    try:
        dt = datetime.fromisoformat(blocked_until)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return "🛡 <b>Лимиты Telegram</b>\n\nНе смог прочитать таймер Flood Wait."
    now = datetime.now(timezone.utc)
    if dt <= now:
        return "🛡 <b>Лимиты Telegram</b>\n\nFlood Wait уже должен быть завершён. Можно пробовать аккуратно."
    total = int((dt - now).total_seconds())
    hours = total // 3600
    minutes = (total % 3600) // 60
    return (
        "🛡 <b>Лимиты Telegram</b>\n\n"
        f"Поиск новых публичных групп временно ограничен.\n"
        f"Осталось примерно: <b>{hours}ч {minutes}м</b>.\n\n"
        "Что можно делать сейчас:\n"
        "✅ сканировать текущие группы;\n"
        "✅ искать заявки за час/24 часа;\n"
        "✅ работать с CRM и лидами.\n\n"
        "Что пока не нажимать:\n"
        "❌ ручной поиск новых публичных групп."
    )


def format_exact_mode_settings(settings_map: dict[str, str]) -> str:
    exact = settings_map.get("exact_leads_mode", "1") == "1"
    bots = settings_map.get("chatbot_only_mode", "0") == "1"
    return (
        "🎯 <b>Точный режим заявок</b>\n\n"
        f"Только явные заявки: {'✅ включено' if exact else '❌ выключено'}\n"
        f"Только чат-бот/автоматизация: {'✅ включено' if bots else '❌ выключено'}\n\n"
        "В точном режиме бот показывает сообщения только если человек реально ищет исполнителя:\n"
        "«нужен», «ищу», «кто сделает», «сколько стоит», «посоветуйте».\n\n"
        "Сообщения «делаю сайты», «ищу клиентов», «мои услуги» скрываются."
    )
