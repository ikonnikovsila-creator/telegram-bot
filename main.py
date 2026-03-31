import asyncio
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from threading import Thread
from typing import Optional

import psycopg2
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден в переменных окружения")

LAVA_WEBHOOK_API_KEY = os.getenv("LAVA_WEBHOOK_API_KEY")
if not LAVA_WEBHOOK_API_KEY:
    raise RuntimeError("LAVA_WEBHOOK_API_KEY не найден в переменных окружения")

LAVA_PUBLIC_API_KEY = os.getenv("LAVA_PUBLIC_API_KEY")
if not LAVA_PUBLIC_API_KEY:
    raise RuntimeError("LAVA_PUBLIC_API_KEY не найден в переменных окружения")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не найден в переменных окружения")

PUBLIC_BASE_URL = "https://zooming-acceptance-production-b914.up.railway.app"

LAVA_INVOICE_API_URL = "https://gate.lava.top/api/v3/invoice"
LAVA_SUBSCRIPTION_OFFER_ID = "70ca1de2-4073-4ca4-abb8-a964003fe500"
DEFAULT_CURRENCY = "USD"
DEFAULT_PAYMENT_PROVIDER = "UNLIMIT"
DEFAULT_PAYMENT_METHOD = "CARD"
DEFAULT_PERIODICITY = "MONTHLY"

ALLOWED_CURRENCIES = {"USD", "EUR", "RUB"}

app = FastAPI()


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,
                webhook_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        conn.commit()
        cur.close()
    finally:
        conn.close()


def save_user(update: Update) -> None:
    if not update.effective_user:
        return

    user = update.effective_user

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (telegram_user_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
            """,
            (user.id, user.username, user.first_name),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def save_payment_webhook(webhook_type: str, payload: dict) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO payments (webhook_type, payload)
            VALUES (%s, %s)
            """,
            (webhook_type, json.dumps(payload)),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def is_valid_email(email: str) -> bool:
    email = email.strip()
    pattern = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
    return bool(re.fullmatch(pattern, email))


def create_lava_invoice(email: str, currency: str = DEFAULT_CURRENCY) -> dict:
    currency = currency.upper().strip()

    if currency not in ALLOWED_CURRENCIES:
        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемая валюта: {currency}",
        )

    payload = {
        "email": email,
        "offerId": LAVA_SUBSCRIPTION_OFFER_ID,
        "currency": currency,
        "paymentProvider": DEFAULT_PAYMENT_PROVIDER,
        "paymentMethod": DEFAULT_PAYMENT_METHOD,
        "periodicity": DEFAULT_PERIODICITY,
    }

    req = urllib.request.Request(
        LAVA_INVOICE_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Api-Key": LAVA_PUBLIC_API_KEY,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
            result = json.loads(raw) if raw else {}

            logging.info("Lava invoice created successfully: %s", result)
            return result

    except urllib.error.HTTPError as e:
        raw_error = e.read().decode("utf-8", errors="replace")
        logging.exception("Lava invoice HTTP error: status=%s body=%s", e.code, raw_error)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Ошибка создания invoice в Lava",
                "status_code": e.code,
                "response_body": raw_error,
            },
        )

    except urllib.error.URLError as e:
        logging.exception("Lava invoice URL error: %s", str(e))
        raise HTTPException(
            status_code=502,
            detail=f"Не удалось соединиться с Lava: {str(e)}",
        )

    except Exception as e:
        logging.exception("Unexpected Lava invoice error: %s", str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Неожиданная ошибка при создании invoice: {str(e)}",
        )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    save_user(update)

    context.user_data["awaiting_email"] = True

    if update.message:
        await update.message.reply_text(
            "Отправь свой email для оформления оплаты. После этого я пришлю кнопку оплаты."
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "Нажми /start, отправь email и получи кнопку оплаты."
        )


async def handle_email_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    if not context.user_data.get("awaiting_email"):
        return

    email = update.message.text.strip()

    if not is_valid_email(email):
        await update.message.reply_text(
            "Это не похоже на корректный email. Отправь нормальный email ещё раз."
        )
        return

    context.user_data["awaiting_email"] = False
    context.user_data["email"] = email

    payment_link = (
        f"{PUBLIC_BASE_URL}/create-payment?"
        f"email={urllib.parse.quote(email)}&currency={DEFAULT_CURRENCY}"
    )

    keyboard = [
        [
            InlineKeyboardButton(
                "Оплатить доступ",
                url=payment_link,
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Отлично. Теперь нажми кнопку ниже, чтобы перейти к оплате.",
        reply_markup=reply_markup,
    )


async def run_bot() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email_message)
    )

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
    init_db()
    thread = Thread(target=start_bot_in_background, daemon=True)
    thread.start()


@app.get("/")
def root() -> dict:
    return {"status": "ok", "message": "Telegram bot is running"}


@app.get("/create-payment")
def create_payment(email: str, currency: str = DEFAULT_CURRENCY):
    result = create_lava_invoice(email=email, currency=currency)

    payment_url = result.get("paymentUrl")
    if not payment_url:
        raise HTTPException(
            status_code=502,
            detail="Lava не вернула paymentUrl",
        )

    return RedirectResponse(url=payment_url, status_code=302)


async def handle_lava_webhook(
    webhook_type: str,
    request: Request,
    x_api_key: Optional[str],
) -> dict:
    if x_api_key != LAVA_WEBHOOK_API_KEY:
        logging.warning(
            "Lava webhook unauthorized: type=%s ip=%s x_api_key=%s",
            webhook_type,
            request.client.host if request.client else "unknown",
            x_api_key,
        )
        raise HTTPException(status_code=401, detail="Unauthorized")

    raw_body = await request.body()
    raw_text = raw_body.decode("utf-8", errors="replace")

    logging.info("Lava webhook received: type=%s", webhook_type)
    logging.info("Lava webhook headers: %s", dict(request.headers))
    logging.info("Lava webhook raw body: %s", raw_text)

    try:
        payload = json.loads(raw_text) if raw_text else {}
    except json.JSONDecodeError:
        payload = {"raw_body": raw_text}

    logging.info("Lava webhook parsed payload: %s", payload)

    save_payment_webhook(webhook_type, payload)

    return {"ok": True, "webhook_type": webhook_type}


@app.post("/webhooks/lava/payment")
async def lava_payment_webhook(
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key"),
) -> dict:
    return await handle_lava_webhook("payment", request, x_api_key)


@app.post("/webhooks/lava/recurring")
async def lava_recurring_webhook(
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key"),
) -> dict:
    return await handle_lava_webhook("recurring", request, x_api_key)
