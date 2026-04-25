from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Iterable

from .filters import normalize_text, detect_city, estimate_budget

SPECIAL_SEGMENTS: dict[str, dict[str, object]] = {
    "kaspi": {
        "title": "💳 Kaspi-лиды",
        "keywords": ["kaspi", "каспи", "kaspi pay", "kaspi qr", "оплата", "онлайн оплата", "принимать оплату", "qr", "эквайринг"],
    },
    "school": {
        "title": "📚 Онлайн-школы / курсы",
        "keywords": ["курс", "курсы", "онлайн школа", "онлайн-школа", "обучение", "ученики", "уроки", "марафон", "вебинар", "домашка", "доступ к урокам", "оқыту", "мектеп"],
    },
    "booking": {
        "title": "📅 Запись клиентов",
        "keywords": ["запись", "онлайн запись", "клиенты записывались", "расписание", "бронь", "мастер", "салон", "клиника", "стоматолог", "услуги", "напоминание клиентам"],
    },
}

DEFAULT_CASES = [
    ("school_bot", "📚 Бот для онлайн-школы", "📚 Курсы / онлайн-школа", "Запись учеников, оплата через Kaspi, выдача материалов, напоминания, база учеников и рассылки."),
    ("salon_booking", "💇 Бот для записи клиентов", "💇 Услуги / запись", "Онлайн-запись, выбор услуги и мастера, напоминания клиентам, уведомление администратору и история клиентов."),
    ("kaspi_sales", "💳 Бот с Kaspi-сценарием", "🏪 Магазин / продажи", "Каталог, прием заказа, инструкция по оплате Kaspi/Kaspi Pay, уведомления менеджеру и таблица заказов."),
    ("crm_leads", "📦 CRM для заявок", "📦 CRM / заявки", "Сбор заявок из Telegram/Instagram, статусы, уведомления, Google Sheets/CRM и ежедневная статистика."),
    ("website_landing", "🌐 Лендинг / сайт", "🌐 Сайт / лендинг", "Структура оффера, форма заявки, адаптивная верстка, аналитика и быстрый запуск рекламы."),
    ("smm_ads", "📱 SMM / реклама", "📱 SMM / реклама", "Упаковка профиля, контент-план, рекламные креативы, запуск лидогенерации и отчетность."),
]


def contains_segment(text: str, segment: str) -> bool:
    data = SPECIAL_SEGMENTS.get(segment)
    if not data:
        return False
    cleaned = normalize_text(text)
    return any(str(k).lower() in cleaned for k in data["keywords"])


def segment_title(segment: str) -> str:
    return str(SPECIAL_SEGMENTS.get(segment, {}).get("title", "Лиды"))


def freshness_label(message_date: str) -> str:
    try:
        dt = datetime.fromisoformat(message_date)
    except Exception:
        return "неизвестно"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600
    if age_hours <= 1:
        return "🟢 свежий — до 1 часа"
    if age_hours <= 6:
        return "🟡 нормальный — до 6 часов"
    if age_hours <= 12:
        return "🟠 стареет — 6–12 часов"
    return "🔴 старый — 12+ часов"


def numeric_budget(text: str, category: str = "") -> int:
    budget = estimate_budget(text, category)
    if "200 000+" in budget:
        return 250_000
    if "80 000" in budget:
        return 120_000
    return 50_000


def lead_confidence(score: int, reasons: str, geo_score: int) -> str:
    reasons_low = normalize_text(reasons)
    confidence = score
    if "фразы заказчика" in reasons_low or "поиске исполнителя" in reasons_low:
        confidence += 5
    if geo_score > 0:
        confidence += 5
    confidence = max(0, min(100, confidence))
    if confidence >= 85:
        return f"{confidence}% — высокая уверенность, похоже на реальную заявку"
    if confidence >= 65:
        return f"{confidence}% — средняя уверенность, стоит проверить"
    return f"{confidence}% — слабая уверенность, лучше не тратить много времени"


def suggested_offer(text: str, category: str) -> str:
    combined = f"{text} {category}"
    if contains_segment(combined, "school"):
        return "Предложить бот для онлайн-школы: запись учеников, Kaspi-оплата, выдача уроков, напоминания и база учеников."
    if contains_segment(combined, "booking"):
        return "Предложить бот для записи: выбор услуги/мастера, расписание, уведомления и напоминания клиентам."
    if contains_segment(combined, "kaspi"):
        return "Предложить сценарий с Kaspi: прием заказа, инструкция оплаты, уведомления менеджеру и таблица заявок."
    if "сайт" in normalize_text(combined) or "лендинг" in normalize_text(combined):
        return "Предложить лендинг: структура оффера, форма заявки, адаптация под телефон и подключение аналитики."
    if "smm" in normalize_text(combined) or "смм" in normalize_text(combined):
        return "Предложить лидогенерацию: упаковка профиля, креативы, реклама и отчет по заявкам."
    return "Предложить Telegram-бота для заявок: меню, сбор контактов, уведомления, база клиентов и простая CRM."


def sales_script_text() -> str:
    return (
        "🧩 <b>Мини-скрипт продажи</b>\n\n"
        "1. Поздороваться и написать, что видел задачу.\n"
        "2. Коротко повторить проблему клиента.\n"
        "3. Предложить 1–2 понятных решения.\n"
        "4. Задать уточняющий вопрос: сроки, функции, оплата, примеры.\n"
        "5. После ответа назвать вилку цены и срок.\n"
        "6. Предложить показать пример или созвониться."
    )


def client_questions_text() -> str:
    return (
        "❓ <b>Что спросить у клиента</b>\n\n"
        "1. Для какого бизнеса нужен бот/сайт/автоматизация?\n"
        "2. Что клиент должен сделать внутри: оставить заявку, оплатить, записаться, получить материал?\n"
        "3. Нужна ли оплата через Kaspi или другой способ?\n"
        "4. Куда отправлять заявки: Telegram, Google Sheets, CRM?\n"
        "5. Нужна ли рассылка/напоминания?\n"
        "6. Есть ли пример, который нравится?\n"
        "7. Когда нужно запустить?"
    )


def daily_plan_text(stats: dict[str, object]) -> str:
    hot = int(stats.get("hot", 0) or 0)
    total = int(stats.get("total", 0) or 0)
    return (
        "🗓 <b>План на сегодня</b>\n\n"
        f"1. Написать 5 самым горячим лидам. Сейчас горячих: <b>{hot}</b>.\n"
        "2. Проверить очередь «Кому писать первым».\n"
        "3. Вернуться к лидам со статусом «Обсуждение» и «Ждет оплату».\n"
        "4. Отправить КП тем, кто просит цену.\n"
        "5. Почистить плохие группы и добавить 2–3 новые группы.\n\n"
        f"Всего лидов за 24 часа: <b>{total}</b>."
    )


def format_case_line(row, idx: int) -> str:
    return f"{idx}. <b>{escape(str(row['title']))}</b>\nКатегория: {escape(str(row['category']))}\n{escape(str(row['text']))}"
