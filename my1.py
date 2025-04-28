# -*- coding: utf-8 -*-
import asyncio
import sys
import io
import os
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
import pandas as pd
import logging
from datetime import datetime

# Налаштування кодування для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_parser.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

async def parse_telegram_group():
    # Отримуємо конфігурацію від користувача
    try:
        api_id = int(input("🔑 Введіть ваш api_id (ціле число): ").strip())
    except ValueError:
        logging.error("❌ Помилка: api_id має бути числом.")
        return
    api_hash = input("🔒 Введіть ваш api_hash: ").strip()
    phone = input("📱 Введіть номер телефону (включно з кодом країни, напр. +380...): ").strip()

    # Запитуємо лінк на групу або канал
    group_link = input("🔗 Введіть посилання на групу/канал: ").strip()

    client = TelegramClient(
        'tg_session',
        api_id,
        api_hash,
        system_version="4.16.30-vxCUSTOM"
    )

    try:
        logging.info("🔐 Підключення до Telegram...")
        await client.start(phone)

        logging.info("🔍 Отримання інформації про групу...")
        try:
            group = await client.get_entity(group_link)
            logging.info(f"📌 Група: {group.title} (ID: {group.id})")
        except ValueError as e:
            logging.error(f"❌ Не вдалося знайти групу: {e}")
            return
        except ChannelPrivateError:
            logging.error("❌ Група є приватною або у вас немає доступу")
            return

        logging.info("⏳ Початок парсингу учасників...")
        participants = []

        try:
            async for user in client.iter_participants(group, aggressive=True):
                try:
                    user_info = {
                        'Група': group.title,
                        'ID': user.id,
                        'Ім\'я': user.first_name or '',
                        'Прізвище': user.last_name or '',
                        'Юзернейм': f"@{user.username}" if user.username else '',
                        'Телефон': user.phone or '',
                        'Бот': 'Так' if user.bot else 'Ні',
                        'Дата парсингу': datetime.now().strftime('%Y-%m-%d %H:%M')
                    }
                    participants.append(user_info)

                    if len(participants) % 10 == 0:
                        logging.info(f"👥 Оброблено {len(participants)} учасників...")

                except Exception as e:
                    logging.warning(f"⚠️ Помилка обробки користувача: {e}")
                    continue

        except FloodWaitError as e:
            logging.warning(f"⏳ Telegram обмеження. Чекаємо {e.seconds} секунд...")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logging.error(f"❌ Помилка парсингу: {e}")

        # Збереження результатів
        if participants:
            current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            safe_title = ''.join(c for c in group.title if c.isalnum() or c in (' ', '_')).rstrip()
            filename = f"Учасники_{safe_title}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            filepath = os.path.join(current_dir, filename)

            try:
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    pd.DataFrame(participants).to_excel(writer, index=False)

                logging.info(f"✅ Успішно збережено {len(participants)} учасників у файлі {filepath}")

            except Exception as e:
                logging.error(f"❌ Помилка збереження файлу: {e}")

                try:
                    csv_filepath = filepath.replace('.xlsx', '.csv')
                    pd.DataFrame(participants).to_csv(csv_filepath, index=False, encoding='utf-8-sig')
                    logging.info(f"✅ Збережено резервну копію у {csv_filepath}")
                except Exception as e:
                    logging.error(f"❌ Помилка збереження резервної копії: {e}")
        else:
            logging.warning("⚠️ Не знайдено жодного учасника для збереження")

    except Exception as e:
        logging.critical(f"‼️ Критична помилка: {e}")
    finally:
        await client.disconnect()
        logging.info("🔌 Відключення від Telegram")

if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(parse_telegram_group())
