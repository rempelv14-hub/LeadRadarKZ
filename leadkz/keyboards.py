from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .database import GroupCandidate
from .filters import NICHE_LABELS
from .replies import template_key_for_category


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Лиды за час", callback_data="leads:1"), InlineKeyboardButton(text="📅 За 24 часа", callback_data="leads:24")],
        [InlineKeyboardButton(text="⭐ Избранные", callback_data="leads:favorites"), InlineKeyboardButton(text="🗂 Архив", callback_data="leads:archived")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats:24"), InlineKeyboardButton(text="🎯 Кому писать", callback_data="priority:queue")],
        [InlineKeyboardButton(text="✅ /check", callback_data="check:run"), InlineKeyboardButton(text="🛠 Диагностика", callback_data="diagnostics:show")],
        [InlineKeyboardButton(text="🧩 Мастер", callback_data="setup:wizard"), InlineKeyboardButton(text="🌐 Web-панель", callback_data="web:info")],
        [InlineKeyboardButton(text="📈 Воронка", callback_data="sales:funnel"), InlineKeyboardButton(text="💰 Доход", callback_data="sales:revenue")],
        [InlineKeyboardButton(text="💳 Kaspi", callback_data="leads:segment:kaspi"), InlineKeyboardButton(text="📚 Школы", callback_data="leads:segment:school")],
        [InlineKeyboardButton(text="📅 Запись", callback_data="leads:segment:booking"), InlineKeyboardButton(text="🏆 Рейтинг групп", callback_data="groups:rating")],
        [InlineKeyboardButton(text="🔎 Найти группы KZ", callback_data="discover:run"), InlineKeyboardButton(text="🔍 Похожие группы", callback_data="discover:similar")],
        [InlineKeyboardButton(text="📂 Все группы", callback_data="discover:list"), InlineKeyboardButton(text="🏆 Валидные группы", callback_data="discover:best")],
        [InlineKeyboardButton(text="🗑 Мусорные группы", callback_data="discover:trash"), InlineKeyboardButton(text="🚫 Плохие группы", callback_data="groups:bad")],
        [InlineKeyboardButton(text="🎯 Ниши", callback_data="niches:list"), InlineKeyboardButton(text="🧠 Обучение", callback_data="learning:list")],
        [InlineKeyboardButton(text="🧠 Слова", callback_data="words:report"), InlineKeyboardButton(text="🧹 Очистка", callback_data="cleanup:run")],
        [InlineKeyboardButton(text="📅 Отчёт дня", callback_data="daily:report"), InlineKeyboardButton(text="🚫 Отчёт групп", callback_data="groups:weekly")],
        [InlineKeyboardButton(text="📁 Кейсы", callback_data="cases:list"), InlineKeyboardButton(text="💰 Цена", callback_data="price:calc")],
        [InlineKeyboardButton(text="📥 Excel", callback_data="export:xlsx:24"), InlineKeyboardButton(text="📄 CSV", callback_data="export:csv:24")],
        [InlineKeyboardButton(text="💾 Бэкап", callback_data="backup:db"), InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings:show")],
        [InlineKeyboardButton(text="💬 Шаблоны", callback_data="templates:list"), InlineKeyboardButton(text="🗓 План", callback_data="plan:today")],
        [InlineKeyboardButton(text="⏰ Напоминания", callback_data="reminders:list"), InlineKeyboardButton(text="🔐 Защита", callback_data="security:info")],
        [InlineKeyboardButton(text="🛡 Антибан", callback_data="antiban"), InlineKeyboardButton(text="ℹ️ Как работает", callback_data="help")],
    ])


def lead_actions(chat_id: int, message_id: int, link: str | None, category: str = "") -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    if link:
        row.append(InlineKeyboardButton(text="Открыть", url=link))
    row.append(InlineKeyboardButton(text="Скрыть", callback_data=f"lead:status:hidden:{chat_id}:{message_id}"))
    buttons.append(row)
    buttons.append([
        InlineKeyboardButton(text="👍 Хороший", callback_data=f"lead:feedback:good:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="👎 Плохой", callback_data=f"lead:feedback:bad:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="🚫 Не показывать похожее", callback_data=f"lead:block_like:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="⭐ В избранное", callback_data=f"lead:status:favorite:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="💬 Шаблон", callback_data=f"lead:template:{template_key_for_category(category)}:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="❓ Вопросы", callback_data=f"lead:template:questions:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="🧭 Совет", callback_data=f"lead:advice:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="📄 PDF КП", callback_data=f"lead:pdf:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="⚡ Коротко", callback_data=f"lead:template:short:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="🇰🇿 Қазақша", callback_data=f"lead:template:kazakh:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="💰 С ценой", callback_data=f"lead:template:price:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="📄 КП текст", callback_data=f"lead:template:kp:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="⏰ 3ч", callback_data=f"lead:remind:3:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="⏰ завтра", callback_data=f"lead:remind:24:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="📝 Заметка", callback_data=f"lead:note:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="💬 Написал", callback_data=f"lead:status:contacted:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="🤝 Обсуждение", callback_data=f"lead:status:talking:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="💰 Ждет оплату", callback_data=f"lead:status:waiting_payment:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="✅ Закрыт", callback_data=f"lead:status:closed:{chat_id}:{message_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="❌ Не подошел", callback_data=f"lead:status:lost:{chat_id}:{message_id}"),
        InlineKeyboardButton(text="🗂 Архив", callback_data=f"lead:status:archived:{chat_id}:{message_id}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def groups_actions(groups: list[GroupCandidate]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for i, group in enumerate(groups[:8], start=1):
        buttons.append([
            InlineKeyboardButton(text=f"✅ {i}", callback_data=f"group:valid:{group.chat_id}"),
            InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"group:priority:{group.chat_id}"),
            InlineKeyboardButton(text=f"🚫 {i}", callback_data=f"group:blocked:{group.chat_id}"),
            InlineKeyboardButton(text=f"👁 {i}", callback_data=f"group:candidate:{group.chat_id}"),
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def settings_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇰🇿 Гео", callback_data="settings:toggle_geo"), InlineKeyboardButton(text="🔥 Горячие", callback_data="settings:toggle_hot")],
        [InlineKeyboardButton(text="🔕 Тихий", callback_data="settings:toggle_quiet"), InlineKeyboardButton(text="🕘 Раб. время", callback_data="settings:toggle_working")],
        [InlineKeyboardButton(text="✅ Только валидные", callback_data="settings:toggle_valid_groups")],
        [InlineKeyboardButton(text="➖ Балл", callback_data="settings:score_down"), InlineKeyboardButton(text="➕ Балл", callback_data="settings:score_up")],
        [InlineKeyboardButton(text="📤 Экспорт настроек", callback_data="settings:export")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])


def niches_actions() -> InlineKeyboardMarkup:
    rows = []
    for key, label in NICHE_LABELS.items():
        rows.append([InlineKeyboardButton(text=label, callback_data=f"niche:toggle:{key}")])
    rows.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wizard_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇰🇿 Гео вкл/выкл", callback_data="setup:toggle:geo_only"),
         InlineKeyboardButton(text="✅ Валидные группы", callback_data="setup:toggle:only_valid_groups")],
        [InlineKeyboardButton(text="🔥 Только горячие", callback_data="setup:toggle:hot_only"),
         InlineKeyboardButton(text="🔕 Тихий режим", callback_data="setup:toggle:quiet_mode")],
        [InlineKeyboardButton(text="🕘 Рабочее время", callback_data="setup:toggle:working_hours")],
        [InlineKeyboardButton(text="➖ Балл", callback_data="settings:score_down"),
         InlineKeyboardButton(text="➕ Балл", callback_data="settings:score_up")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])
