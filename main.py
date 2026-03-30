import asyncio
import logging
import os
from threading import Thread

from fastapi import FastAPI
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден в переменных окружения")

app = FastAPI()


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Бот подключен. Доступ будет выдаваться автоматически после оплаты."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Этот бот нужен для автоматического доступа в закрытый канал."
    )


async def run_bot() -> None:
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    while True:
        await asyncio.sleep(3600)


def start_bot_in_background() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_bot())


@app.on_event("startup")
def startup_event() -> None:
    thread = Thread(target=start_bot_in_background, daemon=True)
    thread.start()


@app.get("/")
def root() -> dict:
    return {"status": "ok", "message": "Telegram bot is running"}
