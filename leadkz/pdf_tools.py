from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .database import Lead
from .filters import estimate_budget


def _font_candidates() -> list[str]:
    return [
        "fonts/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/calibri.ttf",
    ]


def _lead_offer_text(lead: Lead) -> str:
    return (
        "Коммерческое предложение\n\n"
        f"Проект: {lead.category}\n"
        f"Ориентир бюджета: {estimate_budget(lead.text, lead.category)}\n"
        "Срок: 3–7 дней для простой версии, сложные интеграции оцениваются отдельно.\n\n"
        "Что можно сделать:\n"
        "• Telegram-бот для заявок, записи клиентов или продаж\n"
        "• Уведомления администратору\n"
        "• База клиентов и статусы заявок\n"
        "• Интеграция с Google Sheets/CRM при необходимости\n"
        "• Оплата/Kaspi-сценарии при необходимости\n\n"
        "Вопросы для уточнения:\n"
        "1. Для какого бизнеса нужен проект?\n"
        "2. Какие действия должен делать бот/сайт/CRM?\n"
        "3. Нужна ли оплата через Kaspi или CRM?\n"
        "4. Кто будет получать заявки?\n"
        "5. Есть ли пример, который нравится?\n\n"
        "Исходный лид:\n"
        f"{lead.text}\n"
    )


def create_offer_pdf(lead: Lead, export_dir: Path) -> Path:
    export_dir.mkdir(parents=True, exist_ok=True)
    path = export_dir / f"leadkz_offer_{lead.chat_id}_{lead.message_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    try:
        from fpdf import FPDF
    except Exception:
        txt_path = path.with_suffix(".txt")
        txt_path.write_text(_lead_offer_text(lead), encoding="utf-8")
        return txt_path

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    font_path = None
    for candidate in _font_candidates():
        p = Path(candidate)
        if p.exists():
            font_path = str(p)
            break
    if font_path:
        pdf.add_font("LeadKZ", "", font_path, uni=True)
        pdf.set_font("LeadKZ", size=12)
    else:
        pdf.set_font("Arial", size=12)
    for line in _lead_offer_text(lead).splitlines():
        pdf.multi_cell(0, 8, line)
    pdf.output(str(path))
    return path
