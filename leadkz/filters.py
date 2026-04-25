from __future__ import annotations

import hashlib
import os
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

# ============================================================
# Buyer Only Mode
# Показываем только заказчиков, а не исполнителей.
# Работает по всем нишам: боты, CRM, сайты, SMM, дизайн.
# ============================================================

STRICT_BUYER_INTENT_PHRASES = [
    # Общий спрос / поиск исполнителя
    "нужен", "нужна", "нужно", "нужны", "надо", "требуется",
    "ищу разработчика", "ищу исполнителя", "ищу специалиста", "ищу человека", "ищу команду",
    "нужен разработчик", "нужен исполнитель", "нужен специалист", "нужен человек", "нужна команда",
    "кто сделает", "кто может сделать", "кто сможет сделать", "кто возьмется", "кто умеет",
    "кто занимается", "есть кто", "есть специалист", "посоветуйте", "подскажите", "порекомендуйте",
    "нужна помощь", "помогите найти", "хочу заказать", "заказать", "есть задача",
    "сколько стоит", "какая цена", "кто свободен", "готов оплатить", "есть бюджет",

    # Чат-боты
    "нужен бот", "нужна бот", "нужен чат бот", "нужен чат-бот", "нужен телеграм бот",
    "нужен telegram bot", "нужен бот в телеграм", "нужно сделать бота", "сделать бота",
    "ищу разработчика бота", "кто делает ботов", "кто сделает бота", "бот для курсов",
    "бот для школы", "бот для бизнеса", "бот для записи", "бот для заявок",
    "бот с оплатой", "бот с kaspi", "бот для клиентов", "бот для рассылки",

    # CRM / автоматизация
    "нужна crm", "нужна црм", "нужно crm", "нужна автоматизация", "нужно автоматизировать",
    "автоматизировать заявки", "автоматизация заявок", "нужно принимать заявки",
    "нужна интеграция", "подключить kaspi", "нужна оплата", "оплата через kaspi",
    "нужна рассылка", "нужны уведомления", "нужна база клиентов", "настроить воронку",

    # Сайты
    "нужен сайт", "нужно сделать сайт", "кто сделает сайт", "ищу веб разработчика",
    "ищу web разработчика", "нужен лендинг", "нужно сделать лендинг", "нужен интернет магазин",
    "сайт под бизнес", "сайт для бизнеса", "сделать лендинг", "создать сайт",

    # SMM / реклама
    "нужен smm", "нужен смм", "ищу smm", "ищу смм", "нужен таргетолог",
    "ищу таргетолога", "нужно настроить рекламу", "настроить рекламу", "нужна реклама",
    "нужен маркетолог", "ищу маркетолога", "вести instagram", "вести инстаграм",
    "нужен контент", "нужен контент план", "нужна упаковка профиля",

    # Дизайн
    "нужен дизайн", "нужен дизайнер", "ищу дизайнера", "кто сделает дизайн",
    "нужен логотип", "сделать логотип", "нужен баннер", "нужна презентация",
    "нужен фирменный стиль", "дизайн для instagram", "дизайн для инстаграм",

    # Қазақша
    "керек", "бот керек", "чат бот керек", "телеграм бот керек", "сайт керек",
    "дизайнер керек", "smm керек", "смм керек", "маман керек", "жасап береді",
    "кім жасап береді", "кім істеп береді", "іздеймін", "көмек керек",
]

SELLER_INTENT_PHRASES = [
    # Общая реклама услуг / исполнитель ищет клиентов
    "ищу клиентов", "ищем клиентов", "нужны клиенты", "ищу заказы", "ищу заказ",
    "беру заказы", "принимаю заказы", "набираю проекты", "ищу проекты", "ищу проект",
    "свободен для проектов", "есть свободные места", "возьму в работу",
    "мои услуги", "предлагаю услуги", "оказываю услуги", "услуги для бизнеса",
    "пишите в лс", "пишите в личку", "обращайтесь", "прайс", "портфолио",
    "кейсы", "отзывы клиентов", "скидка", "акция", "консультация бесплатно",
    "кому нужен", "кому нужна", "кому нужно", "кому нужны",
    "помогу сделать", "помогаю сделать", "поможем сделать", "готов сделать",
    "готова сделать", "готовы сделать", "сделаю для вас", "сделаем для вас",
    "закажите у нас", "заказать у меня",

    # Боты
    "делаю ботов", "создаю ботов", "разрабатываю ботов", "разработка ботов",
    "чат-боты под ключ", "чат боты под ключ", "боты под ключ",
    "telegram боты под ключ", "телеграм боты под ключ", "создание ботов",
    "разработка чат-ботов", "продаю бота", "готовый бот", "готовые боты",

    # CRM / автоматизация
    "настраиваю crm", "настраиваем crm", "внедряю crm", "внедрение crm",
    "автоматизирую бизнес", "автоматизация бизнеса под ключ", "настраиваю amo",
    "настраиваю битрикс", "интеграции под ключ",

    # Сайты
    "делаю сайты", "создаю сайты", "разрабатываю сайты", "разработка сайтов",
    "создание сайтов", "сайты под ключ", "лендинги под ключ", "делаю лендинги",
    "web разработчик", "веб разработчик", "сайт за", "лендинг за",

    # SMM / реклама
    "настраиваю рекламу", "таргетолог", "smm услуги", "смм услуги",
    "веду smm", "веду смм", "продвижение instagram", "продвижение инстаграм",
    "помогу с рекламой", "настройка таргета", "запуск рекламы",

    # Дизайн
    "делаю дизайн", "создаю дизайн", "дизайнер", "делаю логотипы",
    "создаю логотипы", "дизайн под ключ", "баннеры на заказ",

    # Қазақша seller
    "клиент іздеймін", "тапсырыс аламын", "бот жасап беремін", "сайт жасап беремін",
    "дизайн жасап беремін", "қызмет көрсетемін",
]

# Нишевой контекст. Одного контекста мало — нужен ещё buyer intent.
NICHE_CONTEXT_KEYWORDS_STRICT = [
    "бот", "бота", "боты", "чатбот", "чат-бот", "чат бот", "telegram", "телеграм", "tg",
    "mini app", "мини app", "мини приложение", "crm", "црм", "автоматизация", "автоматизац",
    "заявки", "заявка", "запись", "онлайн запись", "рассылка", "уведомления", "оплата",
    "kaspi", "каспи", "интеграция", "воронка", "клиентская база", "сайт", "лендинг",
    "landing", "интернет-магазин", "web", "веб", "smm", "смм", "таргет", "реклама",
    "маркетолог", "маркетинг", "контент", "дизайн", "логотип", "баннер", "презентация",
]



# Problem phrases сами по себе НЕ являются лидом, потому что их часто используют
# исполнители в рекламных постах: "у вас теряются заявки — я помогу".
# Поэтому боль клиента засчитывается только вместе с маркерами личной задачи.
SELF_NEED_MARKERS = [
    "у меня", "у нас", "мне", "нам", "моему бизнесу", "нашему бизнесу",
    "для моего бизнеса", "для нашей компании", "в моем бизнесе", "в нашем бизнесе",
    "для моей школы", "для нашей школы", "для моего курса", "для наших курсов",
    "для салона", "для магазина", "для клиники", "для студии", "для компании",
    "хочу", "хотим", "планирую", "планируем", "нужно нам", "нужна нам", "нужен нам",
]

PROBLEM_SELLER_CONTEXT_PHRASES = [
    "если у вас", "если у вас теряются", "у вас теряются", "помогу", "поможем",
    "решу проблему", "закрою эту боль", "автоматизирую", "настрою", "сделаю",
    "оставляйте заявку", "пишите в лс", "пишите в личку", "записывайтесь",
    "консультация бесплатно", "разберу ваш", "аудит бесплатно", "мой продукт",
    "наш продукт", "моя услуга", "наша услуга", "мы помогаем", "я помогаю",
]

# Боли клиента: человек может не писать "нужен бот", но описывает проблему,
# которую можно закрыть ботом/CRM/автоматизацией.
PROBLEM_TO_BOT_PHRASES = [
    "как принимать заявки", "принимать заявки", "заявки теряются", "теряются заявки",
    "клиенты теряются", "теряются клиенты", "не успеваем отвечать", "не успеваю отвечать",
    "много сообщений в директ", "много заявок в директ", "заявки из instagram", "заявки из инстаграм",
    "нужна запись клиентов", "клиенты сами записывались", "онлайн запись клиентов",
    "хочу автоматизировать запись", "как автоматизировать запись", "нужно вести учеников",
    "выдавать уроки", "доступ к урокам", "оплата за курс", "оплата через kaspi",
    "как подключить kaspi", "уведомления менеджеру", "база клиентов", "клиентская база",
    "рассылка клиентам", "напоминания клиентам", "таблица заявок", "заявки в таблицу",
    "воронка продаж", "автоматизировать продажи", "обработка заявок", "сбор заявок",
]

HOT_PHRASES = [
    "срочно", "сегодня", "завтра", "как можно быстрее", "кто свободен", "готов оплатить",
    "есть бюджет", "бюджет", "оплата", "нужно сейчас", "до вечера", "быстро", "горит",
    "в ближайшее время", "на этой неделе", "дедлайн",
]


# Явные заявки. Это самый строгий слой:
# сообщение должно быть не просто про услугу, а с явным запросом "нужен/ищу/кто сделает/сколько стоит".
EXACT_REQUEST_PHRASES = [
    "нужен", "нужна", "нужно", "нужны", "надо", "требуется",
    "ищу исполнителя", "ищу специалиста", "ищу разработчика", "ищу дизайнера",
    "ищу smm", "ищу смм", "ищу таргетолога", "ищу маркетолога",
    "кто сделает", "кто может сделать", "кто сможет сделать", "кто возьмется",
    "кто занимается", "есть кто", "посоветуйте", "подскажите", "порекомендуйте",
    "нужна помощь", "есть задача", "хочу заказать", "сколько стоит", "какая цена",
    "готов оплатить", "есть бюджет", "керек", "іздеймін", "кім жасап береді",
]

EXACT_REQUEST_PATTERNS = [
    r"\bнуж(ен|на|но|ны)\b.+\b(бот|чат.?бот|telegram|телеграм|crm|црм|автоматизац|сайт|лендинг|smm|смм|таргет|реклам|дизайн|логотип|заявк|запис|kaspi|каспи)\b",
    r"\bищу\b.+\b(исполнител|специалист|разработчик|дизайнер|smm|смм|таргетолог|маркетолог|человек|команд)\b",
    r"\bкто\b.+\b(сделает|может|сможет|возьмется|занимается|умеет)\b",
    r"\b(подскажите|посоветуйте|порекомендуйте)\b.+\b(кого|кто|специалист|разработчик|дизайнер|smm|смм|таргетолог)\b",
    r"\bсколько\b.+\b(стоит|будет стоить)\b",
    r"\bкім\b.+\b(жасап|істеп).*(беред|алады)\b",
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

    buyer_only_mode = os.getenv("BUYER_ONLY_MODE", "true").lower() in {"1", "true", "yes", "on"}
    reject_sellers = os.getenv("REJECT_SELLERS", "true").lower() in {"1", "true", "yes", "on"}
    require_buyer_intent = os.getenv("REQUIRE_BUYER_INTENT", "true").lower() in {"1", "true", "yes", "on"}
    exact_leads_mode = os.getenv("EXACT_LEADS_MODE", "true").lower() in {"1", "true", "yes", "on"}

    seller_hits = _contains_any(cleaned, SELLER_INTENT_PHRASES)
    developer_hits = _contains_any(cleaned, DEVELOPER_PHRASES)
    problem_seller_context_hits_for_reject = _contains_any(cleaned, PROBLEM_SELLER_CONTEXT_PHRASES)
    all_seller_hits = list(dict.fromkeys(seller_hits + developer_hits + problem_seller_context_hits_for_reject))

    strict_buyer_hits = _contains_any(cleaned, STRICT_BUYER_INTENT_PHRASES)
    buyer_hits = _contains_any(cleaned, BUYER_PHRASES)
    buyer_word_hits = _contains_any(cleaned, BUYER_WORDS)
    solution_hits = _contains_any(cleaned, SOLUTION_CONTEXT_WORDS)
    strict_solution_hits = _contains_any(cleaned, NICHE_CONTEXT_KEYWORDS_STRICT)
    hot_hits = _contains_any(cleaned, HOT_PHRASES)
    raw_problem_hits = _contains_any(cleaned, PROBLEM_TO_BOT_PHRASES)
    self_need_hits = _contains_any(cleaned, SELF_NEED_MARKERS)
    problem_seller_context_hits = _contains_any(cleaned, PROBLEM_SELLER_CONTEXT_PHRASES)
    question_intent_for_problem = "?" in text or any(p in cleaned for p in ["как ", "кто ", "где ", "подскажите", "посоветуйте"])
    # Боль засчитываем только если это похоже на личный запрос/вопрос, а не на рекламный пост исполнителя.
    problem_hits = raw_problem_hits if raw_problem_hits and (self_need_hits or question_intent_for_problem) and not problem_seller_context_hits else []

    exact_request_hits = _contains_any(cleaned, EXACT_REQUEST_PHRASES)
    exact_pattern_hits = [p for p in EXACT_REQUEST_PATTERNS if re.search(p, cleaned)]
    has_question_intent_early = any(re.search(p, cleaned) for p in QUESTION_PATTERNS)
    has_strict_buyer_intent = bool(strict_buyer_hits or buyer_hits or problem_hits or has_question_intent_early)
    has_strict_solution_context = bool(strict_solution_hits or solution_hits or problem_hits)

    if buyer_only_mode and reject_sellers and all_seller_hits:
        return LeadScore(
            False, 0, "seller_rejected", 0,
            [f"скрыто: это похоже на исполнителя/рекламу услуг: {', '.join(all_seller_hits[:4])}"],
            lead_hash
        )

    if buyer_only_mode and exact_leads_mode and not (exact_request_hits or exact_pattern_hits):
        return LeadScore(
            False, 0, "not_exact_request", 0,
            ["скрыто: это не явная заявка. Нужны сигналы: «нужен», «ищу», «кто сделает», «сколько стоит», «посоветуйте»."],
            lead_hash
        )

    if buyer_only_mode and require_buyer_intent and not has_strict_buyer_intent:
        return LeadScore(
            False, 0, "no_buyer_intent", 0,
            ["скрыто: нет явного намерения заказать/найти исполнителя. Боли типа «теряются заявки» считаются лидом только если человек пишет про свою задачу или задаёт вопрос."],
            lead_hash
        )

    if buyer_only_mode and not has_strict_solution_context:
        return LeadScore(
            False, 0, "no_niche_context", 0,
            ["скрыто: нет контекста нужной услуги/ниши"],
            lead_hash
        )

    if exact_request_hits or exact_pattern_hits:
        score += 20
        reasons.append("явная заявка: есть намерение найти исполнителя/заказать")
    if strict_buyer_hits:
        score += 35 + min(len(strict_buyer_hits), 4) * 8
        reasons.append(f"намерение заказчика: {', '.join(strict_buyer_hits[:4])}")
    if buyer_hits:
        score += 35 + min(len(buyer_hits), 3) * 8
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
    if problem_hits:
        score += 28 + min(len(problem_hits), 3) * 6
        reasons.append(f"личная боль клиента → можно предложить бота/CRM: {', '.join(problem_hits[:3])}")

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
    has_intent = bool(strict_buyer_hits or buyer_hits or problem_hits or has_question_intent)
    if not buyer_only_mode:
        has_intent = bool(buyer_hits or buyer_word_hits or has_question_intent)
    has_solution_context = bool(strict_solution_hits or solution_hits or problem_hits)
    has_geo = geo_score > 0 or not geo_required
    category = detect_business_category(cleaned)
    niche = detect_niche(category, cleaned)
    is_lead = score >= min_score and has_intent and has_solution_context and has_geo and not all_seller_hits

    if all_seller_hits:
        category = "seller_rejected"
    elif spam_hits:
        category = "spam"
    elif not is_lead:
        category = "not_lead"

    if not reasons:
        reasons.append("нет явных признаков заказа")

    return LeadScore(is_lead=is_lead, score=max(min(score, 100), 0), category=category, geo_score=geo_score, reasons=reasons, lead_hash=lead_hash, niche=niche)
