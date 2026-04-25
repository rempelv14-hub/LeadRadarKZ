from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

api_id = int(os.getenv("API_ID", "0"))
api_hash = os.getenv("API_HASH", "").strip()
session_name = os.getenv("TG_SESSION", "leadkz_session").strip() or "leadkz_session"
session_file = BASE_DIR / f"{session_name}.session"

if not api_id or not api_hash:
    raise SystemExit("Заполни API_ID и API_HASH в .env перед запуском.")


async def main() -> None:
    if session_file.exists():
        print(f"Нашёл готовую сессию: {session_file.name}")
        client = TelegramClient(str(BASE_DIR / session_name), api_id, api_hash)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise SystemExit("Сессия есть, но она не авторизована. Запусти create_session.py.")
            session_string = StringSession.save(client.session)
        finally:
            await client.disconnect()
    else:
        print("Локальной сессии нет. Создаю новую StringSession для Railway.")
        print("Вводи номер Telegram. Если попросит пароль — это пароль 2FA Telegram.")
        client = TelegramClient(StringSession(), api_id, api_hash)
        await client.start()
        session_string = client.session.save()
        await client.disconnect()

    out = BASE_DIR / "session_string.txt"
    out.write_text("TG_SESSION_STRING=" + session_string + "\n", encoding="utf-8")
    print("\nГотово: session_string.txt")
    print("Скопируй TG_SESSION_STRING в Railway Variables.")


if __name__ == "__main__":
    asyncio.run(main())
