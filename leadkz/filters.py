from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class LeadScore:
    is_lead: bool
    score: int
    category: str
    geo_score: int
    reasons: list[str]
    lead_hash: str
    niche: str = "bots"


BUYER_PHRASES = [
    "нужен бот", "нужен чат бот", "нужен чат-бот", "нужен телеграм бот", "нужен telegram bot",
    "кто сделает", "кто может сделать", "кто сможет сделать", "ищу разработчика", "ищу исполнителя",
    "нужно сделать бота", "заказать бота", "бот для курсов", "бот для школы", "бот для бизнеса",
    "нужна автоматизация", "нужно автоматизировать", "нужно принимать заявки", "прием заявок",
    "нужна запись клиентов", "онлайн запись", "нужна crm", "нужна црм", "нужна рассылка",
    "нужна оплата через kaspi", "оплата через каспи", "нужно сделать сайт", "нужен сайт",
    "кто делает сайты", "нужен smm", "нужен таргетолог", "нужен дизайн", "нужен логотип",
    "бот керек", "чат бот керек", "чат-бот керек", "телеграм бот керек", "кім жасап береді",
    "кім істеп береді", "әзірлеуші керек", "разработчик керек", "маған бот керек", "бизнеске бот керек",
]

BUYER_WORDS = [
    "нужен", "нужно", "надо", "ищу", "заказать", "сделать", "разработать", "создать",
    "автоматизировать", "заявки", "запись", "клиенты", "курсы", "школа", "бизнес",
    "срочно", "бюджет", "оплата", "тенге", "тг", "керек", "іздеймін", "требуется",
    "посоветуйте", "подскажите", "рекомендуйте", "кто", "кім",
]

SOLUTION_CONTEXT_WORDS = [
    "бот", "бота", "боты", "чатбот", "чат-бот", "чат бот", "telegram", "телеграм", "tg",
    "ai бот", "ии бот", "crm", "црм", "автоматизация", "автоматизац", "заявки", "запись",
    "онлайн запись", "онлайн-запись", "рассылка", "уведомления", "оплата", "kaspi", "каспи",
    "интеграция", "таблица", "google sheets", "воронка", "лиды", "клиентская база", "админка",
    "mini app", "мини app", "мини приложение", "мини-приложение", "whatsapp", "инстаграм", "instagram",
    "сайт", "лендинг", "landing", "интернет-магазин", "дизайн", "логотип", "smm", "смм", "таргет",
    "реклама", "маркетинг", "контент", "приложение", "мобильное приложение",
]

DEVELOPER_PHRASES = [
    "разрабатываю ботов", "делаю ботов", "создаю ботов", "создаем ботов", "помогу сделать бота",
    "наша команда делает", "услуги разработки", "разработка ботов", "боты под ключ", "портфолио",
    "ищу клиентов", "принимаю заказы", "напишите мне", "обращайтесь", "предлагаю услуги",
    "продаю бота", "закажите у нас", "скидка на бота", "сайт под ключ", "таргетолог",
    "smm услуги", "делаем сайты", "создание сайтов", "занимаюсь разработкой", "беру заказы",
    "готов сделать", "мои работы", "кейсы", "акция до", "скидка на разработку",
]

SPAM_PHRASES = [
    "казино", "ставки", "букмекер", "слоты", "крипта", "быстрый заработок", "пассивный доход",
    "инвестиции без риска", "подпишись", "розыгрыш", "розыгрываем", "дешевые подписчики",
    "накрутка", "18+", "эрот", "порно", "bet", "casino", "ставь плюс", "удаленная работа без опыта",
    "миллион за", "заработок в день", "кредит без", "микрозайм",
]

HOT_PHRASES = [
    "срочно", "сегодня", "завтра", "как можно быстрее", "кто свободен", "готов оплатить",
    "есть бюджет", "бюджет", "оплата", "нужно сейчас", "до вечера", "быстро", "горит",
    "в ближайшее время", "на этой неделе", "дедлайн",
]

QUESTION_PATTERNS = [
    r"кто\s+(может|сможет|умеет|делает|возьмется)",
    r"есть\s+кто",
    r"посоветуйте\s+(разработчика|специалиста|исполнителя|человека|команду)",
    r"подскажите\s+(разработчика|специалиста|исполнителя|человека|команду)",
    r"нуж(ен|на|но|ны)\s+.+(бот|сайт|автоматизац|crm|црм|запис|заявк|рассылк|интеграц|дизайн|smm|смм)",
    r"ищу\s+.+(разработчик|исполнитель|специалист|человек|команду)",
    r"кім\s+(жасап|істеп).*(беред|алады)",
]

CATEGORY_KEYWORDS = {
    "🤖 Чат-бот / автоматизация": ["бот", "чат бот", "чат-бот", "telegram", "телеграм", "mini app", "мини"],
    "📚 Курсы / онлайн-школа": ["курс", "курсы", "онлайн школа", "онлайн-школа", "урок", "обуч", "марафон", "вебинар", "мектеп", "оқыту"],
    "🏪 Магазин / продажи": ["магазин", "товар", "продаж", "каталог", "заказ", "kaspi", "каспи", "маркетплейс", "склад"],
    "🍽 Ресторан / доставка": ["ресторан", "кафе", "доставка", "меню", "еда", "заказ еды", "пицца", "суши"],
    "💇 Услуги / запись": ["запись", "салон", "барбер", "космет", "услуги", "мастер", "бронь", "забронировать", "клиника", "стоматолог"],
    "🏢 Бизнес-автоматизация": ["автоматизац", "бизнес", "процесс", "менеджер", "отдел продаж", "воронка"],
    "📦 CRM / заявки": ["crm", "црм", "заявки", "лиды", "amo", "битрикс", "интеграция", "таблица", "google sheets"],
    "📣 Рассылки / уведомления": ["рассылка", "уведомления", "напоминания", "push", "сообщения клиентам"],
    "🌐 Сайт / лендинг": ["сайт", "лендинг", "landing", "интернет-магазин", "веб", "web"],
    "📱 SMM / реклама": ["smm", "смм", "таргет", "реклама", "маркетинг", "сторис", "контент", "инстаграм", "instagram"],
    "🎨 Дизайн / бренд": ["дизайн", "логотип", "бренд", "фирменный стиль", "баннер", "презентация"],
}

NICHE_LABELS = {
    "bots": "🤖 Чат-боты",
    "automation": "📦 Автоматизация/CRM",
    "sites": "🌐 Сайты",
    "smm": "📱 SMM/реклама",
    "design": "🎨 Дизайн",
}

CATEGORY_TO_NICHE = {
    "🤖 Чат-бот / автоматизация": "bots",
    "📚 Курсы / онлайн-школа": "bots",
    "🏪 Магазин / продажи": "bots",
    "🍽 Ресторан / доставка": "bots",
    "💇 Услуги / запись": "bots",
    "🏢 Бизнес-автоматизация": "automation",
    "📦 CRM / заявки": "automation",
    "📣 Рассылки / уведомления": "automation",
    "🌐 Сайт / лендинг": "sites",
    "📱 SMM / реклама": "smm",
    "🎨 Дизайн / бренд": "design",
}

CITY_ALIASES: dict[str, list[str]] = {
    "Алматы": ["алматы", "almaty", "алма-ата"],
    "Астана": ["астана", "астане", "астану", "astana", "нур-султан", "nur-sultan"],
    "Шымкент": ["шымкент", "шымкенте", "shymkent"],
    "Караганда": ["караганда", "караганде", "karaganda"],
    "Актобе": ["актобе", "aktobe"],
    "Атырау": ["атырау", "atyrau"],
    "Актау": ["актау", "aktau"],
    "Костанай": ["костанай", "костанае", "kostanay"],
    "Павлодар": ["павлодар", "павлодаре", "pavlodar"],
    "Семей": ["семей", "семее", "semey"],
    "Усть-Каменогорск": ["усть-каменогорск", "oskemen", "өскемен", "ust-kamenogorsk"],
    "Кызылорда": ["кызылорда", "қызылорда", "kyzylorda"],
    "Тараз": ["тараз", "taraz"],
    "Петропавловск": ["петропавловск", "petropavl"],
    "Уральск": ["уральск", "oral", "орал"],
    "Кокшетау": ["кокшетау", "kokshetau"],
    "Туркестан": ["туркестан", "turkestan"],
    "Талдыкорган": ["талдыкорган", "taldykorgan"],
    "Сатпаев": ["сатпаев", "satbayev"],
    "Жезказган": ["жезказган", "zhezkazgan"],
    "Экибастуз": ["экибастуз", "ekibastuz"],
}

MEDIUM_BUDGET_MARKERS = ["оплата", "kaspi", "каспи", "crm", "црм", "интеграция", "рассылка", "таблица", "админка", "уведомления", "лендинг", "дизайн"]
HIGH_BUDGET_MARKERS = ["личный кабинет", "crm", "црм", "битрикс", "amo", "api", "интеграция", "mini app", "мини приложение", "склад", "много филиалов", "несколько филиалов", "аналитика", "оплата онлайн", "мобильное приложение"]


def normalize_text(text: str) -> str:
    text = (text or "").lower().replace("ё", "е")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"@\w+", " ", text)
    text = re.sub(r"[^a-zа-яәғқңөұүіһ0-9+#\-\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _contains_any(text: str, phrases: Iterable[str]) -> List[str]:
    return [phrase for phrase in phrases if phrase and phrase.lower().replace("ё", "е") in text]


def detect_business_category(cleaned_text: str) -> str:
    best_category = "🤖 Чат-бот / автоматизация"
    best_hits = 0
    for category, keywords in CATEGORY_KEYWORDS.items():
        hits = len(_contains_any(cleaned_text, keywords))
        if hits > best_hits:
            best_hits = hits
            best_category = category
    return best_category


def detect_niche(category: str, text: str = "") -> str:
    cleaned = normalize_text(text)
    if _contains_any(cleaned, ["сайт", "лендинг", "web", "landing"]):
        return "sites"
    if _contains_any(cleaned, ["smm", "смм", "таргет", "реклама", "маркетинг"]):
        return "smm"
    if _contains_any(cleaned, ["дизайн", "логотип", "бренд"]):
        return "design"
    if category in CATEGORY_TO_NICHE:
        return CATEGORY_TO_NICHE[category]
    if _contains_any(cleaned, ["crm", "црм", "автоматизац", "интеграция"]):
        return "automation"
    return "bots"


def detect_city(text: str) -> str:
    cleaned = normalize_text(text)
    for city, aliases in CITY_ALIASES.items():
        if _contains_any(cleaned, aliases):
            return city
    if _contains_any(cleaned, ["казахстан", "қазақстан", "kz", "кз", "kaspi", "каспи", "тенге", "тг", "тоо", "ип"]):
        return "Весь Казахстан / онлайн"
    return "Не определён"


def estimate_budget(text: str, category: str = "") -> str:
    cleaned = normalize_text(text + " " + category)
    high_hits = _contains_any(cleaned, HIGH_BUDGET_MARKERS)
    medium_hits = _contains_any(cleaned, MEDIUM_BUDGET_MARKERS)
    if len(high_hits) >= 2 or ("crm" in cleaned and "интегра" in cleaned) or "мобильное приложение" in cleaned:
        return "💰 200 000+ ₸ — сложная автоматизация"
    if high_hits or len(medium_hits) >= 2:
        return "💰 80 000–200 000 ₸ — бот/сайт с оплатой, CRM или интеграциями"
    return "💰 30 000–70 000 ₸ — простая версия"


def reply_risk(text: str, score: int, category: str = "") -> str:
    cleaned = normalize_text(text + " " + category)
    if _contains_any(cleaned, SPAM_PHRASES) or _contains_any(cleaned, DEVELOPER_PHRASES):
        return "🔴 Не писать — похоже на рекламу/спам"
    if score >= 80 and (_contains_any(cleaned, BUYER_PHRASES) or "кто" in cleaned or "ищу" in cleaned):
        return "🟢 Можно написать — человек сам ищет исполнителя"
    if score >= 60:
        return "🟡 Осторожно — лучше написать аккуратно и по делу"
    return "🔴 Не писать первым — слабый сигнал заказа"


def geo_score_for_text(text: str, geo_keywords: Iterable[str] | None = None) -> tuple[int, list[str]]:
    cleaned = normalize_text(text)
    keywords = list(geo_keywords or [])
    hits = _contains_any(cleaned, keywords)
    score = min(len(set(hits)), 4) * 15
    if re.search(r"(\+7|8)\s?7\d{2}", text or ""):
        score += 20
        hits.append("+7/87xx")
    return min(score, 100), sorted(set(hits))[:6]


def make_lead_hash(text: str) -> str:
    cleaned = normalize_text(text)
    cleaned = re.sub(r"\b(срочно|пожалуйста|добрый|день|здравствуйте|привет|салем)\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return hashlib.sha256(cleaned[:500].encode("utf-8")).hexdigest()


def score_message(text: str, min_score: int = 55, geo_keywords: Iterable[str] | None = None, geo_required: bool = False) -> LeadScore:
    original = text or ""
    cleaned = normalize_text(original)
    reasons: List[str] = []
    score = 0
    lead_hash = make_lead_hash(original)

    if not cleaned or len(cleaned) < 12:
        return LeadScore(False, 0, "too_short", 0, ["сообщение слишком короткое"], lead_hash)

    spam_hits = _contains_any(cleaned, SPAM_PHRASES)
    if spam_hits:
        return LeadScore(False, 0, "spam", 0, [f"спам-слова: {', '.join(spam_hits[:3])}"], lead_hash)

    developer_hits = _contains_any(cleaned, DEVELOPER_PHRASES)
    if developer_hits:
        score -= 65
        reasons.append(f"похоже на разработчика: {', '.join(developer_hits[:3])}")

    buyer_hits = _contains_any(cleaned, BUYER_PHRASES)
    buyer_word_hits = _contains_any(cleaned, BUYER_WORDS)
    solution_hits = _contains_any(cleaned, SOLUTION_CONTEXT_WORDS)
    hot_hits = _contains_any(cleaned, HOT_PHRASES)

    if buyer_hits:
        score += 45 + min(len(buyer_hits), 3) * 10
        reasons.append(f"фразы заказчика: {', '.join(buyer_hits[:3])}")
    if buyer_word_hits:
        score += min(len(set(buyer_word_hits)), 5) * 5
        reasons.append(f"слова спроса: {', '.join(sorted(set(buyer_word_hits))[:5])}")
    if solution_hits:
        score += min(len(set(solution_hits)), 5) * 7
        reasons.append(f"тема digital/автоматизации: {', '.join(sorted(set(solution_hits))[:5])}")
    if hot_hits:
        score += 15
        reasons.append(f"горячесть: {', '.join(hot_hits[:3])}")

    for pattern in QUESTION_PATTERNS:
        if re.search(pattern, cleaned):
            score += 20
            reasons.append("похоже на вопрос о поиске исполнителя")
            break

    if "?" in original:
        score += 5
    if len(cleaned) > 500:
        score -= 10
        reasons.append("длинное сообщение, возможная реклама")

    geo_score, geo_hits = geo_score_for_text(original, geo_keywords)
    if geo_hits:
        score += min(geo_score, 30)
        reasons.append(f"гео Казахстан: {', '.join(geo_hits[:4])}")
    elif geo_required:
        reasons.append("нет признаков гео Казахстана")

    has_question_intent = any(re.search(p, cleaned) for p in QUESTION_PATTERNS)
    has_intent = bool(buyer_hits or buyer_word_hits or has_question_intent)
    has_solution_context = bool(solution_hits)
    has_geo = geo_score > 0 or not geo_required
    category = detect_business_category(cleaned)
    niche = detect_niche(category, cleaned)
    is_lead = score >= min_score and has_intent and has_solution_context and has_geo and not developer_hits

    if developer_hits:
        category = "developer"
    elif spam_hits:
        category = "spam"
    elif not is_lead:
        category = "not_lead"

    if not reasons:
        reasons.append("нет явных признаков заказа")

    return LeadScore(is_lead=is_lead, score=max(min(score, 100), 0), category=category, geo_score=geo_score, reasons=reasons, lead_hash=lead_hash, niche=niche)
