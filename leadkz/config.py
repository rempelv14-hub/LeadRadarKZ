from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _get_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_int(name: str, default: int | None = None) -> int:
    value = os.getenv(name, "").strip()
    if value == "" and default is not None:
        return default
    if value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if value == "":
        return default
    return value in {"1", "true", "yes", "y", "да", "on"}


def _get_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _get_int_list(name: str) -> List[int]:
    result: List[int] = []
    for item in _get_list(name):
        try:
            result.append(int(item))
        except ValueError:
            continue
    return result


DEFAULT_DISCOVERY_KEYWORDS = (
    "чат бот казахстан,чат-бот казахстан,telegram bot kazakhstan,телеграм бот казахстан,"
    "бот для бизнеса казахстан,автоматизация бизнеса казахстан,онлайн курсы казахстан,"
    "школа курсы казахстан,маркетинг казахстан,предприниматели казахстан,бизнес казахстан,"
    "стартап казахстан,алматы бизнес,астана бизнес,нур-султан бизнес,шымкент бизнес,"
    "караганда бизнес,актобе бизнес,атырау бизнес,костанай бизнес,павлодар бизнес,"
    "усть-каменогорск бизнес,семей бизнес,kz бизнес,kaspi бизнес,"
    "crm казахстан,автоматизация заявок казахстан,онлайн запись казахстан,"
    "предприниматели алматы,предприниматели астана,малый бизнес казахстан,"
    "нужен сайт казахстан,создание сайта казахстан,smm казахстан,таргет казахстан,"
    "дизайн казахстан,логотип казахстан,реклама казахстан,интернет магазин казахстан"
)

DEFAULT_GEO_KEYWORDS = (
    "казахстан,kazakhstan,қазақстан,казакстан,kz,кз,алматы,almaty,астана,astana,"
    "нур-султан,nur-sultan,шымкент,shymkent,караганда,karaganda,актобе,aktobe,"
    "атырау,atyrau,актау,aktau,костанай,kostanay,павлодар,pavlodar,семей,semey,"
    "усть-каменогорск,oskemen,өскемен,кызылорда,kyzylorda,тараз,taraz,"
    "петропавловск,petropavl,уральск,oral,орал,кокшетау,kokshetau,туркестан,turkestan,"
    "талдыкорган,taldykorgan,сатпаев,satbayev,жезказган,zhezkazgan,экибастуз,ekibastuz,"
    "+7,kaspi,каспи,тенге,тг,kzt,ип,тоо"
)


def _data_dir() -> Path:
    raw = os.getenv("DATA_DIR", "").strip() or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    path = Path(raw) if raw else BASE_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


DATA_DIR = _data_dir()


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_id: int
    admin_ids: List[int]
    api_id: int
    api_hash: str
    tg_session: str
    tg_session_string: str | None
    scan_interval_seconds: int
    lead_max_age_hours: int
    hot_lead_age_minutes: int
    min_lead_score: int
    discovery_interval_hours: int
    discovery_limit_per_keyword: int
    discovery_keywords: List[str]
    monitored_chat_ids: List[int]
    geo_only_kazakhstan: bool
    geo_keywords: List[str]
    database_path: Path
    export_dir: Path
    daily_report_enabled: bool
    daily_report_hour: int
    autobackup_enabled: bool
    autobackup_hour: int
    auto_discovery_enabled: bool
    auto_discovery_min_group_score: int
    auto_discovery_history_limit: int
    auto_monitor_valid_groups: bool
    auto_hide_bad_groups: bool
    only_valid_groups: bool
    health_server_enabled: bool
    health_port: int
    max_notifications_per_hour: int
    weekly_report_enabled: bool
    weekly_report_day: int
    cleanup_enabled: bool
    cleanup_hour: int
    web_dashboard_enabled: bool
    brand_name: str
    brand_phone: str
    brand_telegram: str


settings = Settings(
    bot_token=_get_required("BOT_TOKEN"),
    admin_id=_get_int("ADMIN_ID"),
    admin_ids=_get_int_list("ADMIN_IDS") or [_get_int("ADMIN_ID")],
    api_id=_get_int("API_ID"),
    api_hash=_get_required("API_HASH"),
    tg_session=os.getenv("TG_SESSION", "leadkz_session").strip() or "leadkz_session",
    tg_session_string=os.getenv("TG_SESSION_STRING", "").strip() or None,
    scan_interval_seconds=_get_int("SCAN_INTERVAL_SECONDS", 300),
    lead_max_age_hours=_get_int("LEAD_MAX_AGE_HOURS", 24),
    hot_lead_age_minutes=_get_int("HOT_LEAD_AGE_MINUTES", 60),
    min_lead_score=_get_int("MIN_LEAD_SCORE", 55),
    discovery_interval_hours=_get_int("DISCOVERY_INTERVAL_HOURS", 12),
    discovery_limit_per_keyword=_get_int("DISCOVERY_LIMIT_PER_KEYWORD", 15),
    discovery_keywords=_get_list("DISCOVERY_KEYWORDS", DEFAULT_DISCOVERY_KEYWORDS),
    monitored_chat_ids=_get_int_list("MONITORED_CHAT_IDS"),
    geo_only_kazakhstan=_get_bool("GEO_ONLY_KAZAKHSTAN", True),
    geo_keywords=_get_list("GEO_KEYWORDS", DEFAULT_GEO_KEYWORDS),
    database_path=DATA_DIR / "leadkz.sqlite3",
    export_dir=DATA_DIR / "exports",
    daily_report_enabled=_get_bool("DAILY_REPORT_ENABLED", True),
    daily_report_hour=_get_int("DAILY_REPORT_HOUR", 21),
    autobackup_enabled=_get_bool("AUTOBACKUP_ENABLED", True),
    autobackup_hour=_get_int("AUTOBACKUP_HOUR", 23),
    auto_discovery_enabled=_get_bool("AUTO_DISCOVERY_ENABLED", True),
    auto_discovery_min_group_score=_get_int("AUTO_DISCOVERY_MIN_GROUP_SCORE", 75),
    auto_discovery_history_limit=_get_int("AUTO_DISCOVERY_HISTORY_LIMIT", 120),
    auto_monitor_valid_groups=_get_bool("AUTO_MONITOR_VALID_GROUPS", True),
    auto_hide_bad_groups=_get_bool("AUTO_HIDE_BAD_GROUPS", True),
    only_valid_groups=_get_bool("ONLY_VALID_GROUPS", True),
    health_server_enabled=_get_bool("HEALTH_SERVER_ENABLED", True),
    health_port=_get_int("PORT", 8080),
    max_notifications_per_hour=_get_int("MAX_NOTIFICATIONS_PER_HOUR", 10),
    weekly_report_enabled=_get_bool("WEEKLY_REPORT_ENABLED", True),
    weekly_report_day=_get_int("WEEKLY_REPORT_DAY", 6),
    cleanup_enabled=_get_bool("CLEANUP_ENABLED", True),
    cleanup_hour=_get_int("CLEANUP_HOUR", 4),
    web_dashboard_enabled=_get_bool("WEB_DASHBOARD_ENABLED", True),
    brand_name=os.getenv("BRAND_NAME", "LeadKZ").strip() or "LeadKZ",
    brand_phone=os.getenv("BRAND_PHONE", "").strip(),
    brand_telegram=os.getenv("BRAND_TELEGRAM", "").strip(),
)
