from __future__ import annotations

import asyncio
import logging

from telethon import TelegramClient

from leadkz.config import settings

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    print("Создаём Telegram user session для поиска групп и чтения сообщений.")
    print("Важно: вводи номер своего Telegram-аккаунта, не токен бота.")
    client = TelegramClient(settings.tg_session, settings.api_id, settings.api_hash)
    await client.start()
    me = await client.get_me()
    print(f"Готово. Сессия создана: {settings.tg_session}.session")
    print(f"Аккаунт: {getattr(me, 'username', None) or me.id}")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
