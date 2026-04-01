import asyncio
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Any, Optional

import psycopg2
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ChatJoinRequestHandler,
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

PUBLIC_BASE_URL = os.getenv(
    "PUBLIC_BASE_URL",
    "https://zooming-acceptance-production-b914.up.railway.app",
)

PRIVATE_CHANNEL_CHAT_ID = os.getenv("PRIVATE_CHANNEL_CHAT_ID")
if not PRIVATE_CHANNEL_CHAT_ID:
    raise RuntimeError("PRIVATE_CHANNEL_CHAT_ID не найден в переменных окружения")

PUBLIC_CHANNEL_CHAT_ID = os.getenv("PUBLIC_CHANNEL_CHAT_ID")
if not PUBLIC_CHANNEL_CHAT_ID:
    raise RuntimeError("PUBLIC_CHANNEL_CHAT_ID не найден в переменных окружения")

TELEGRAM_BOT_USERNAME = "tochka_opory_access_bot"
TELEGRAM_BOT_URL = f"https://t.me/{TELEGRAM_BOT_USERNAME}"
TELEGRAM_BOT_START_FROM_VITRINA_URL = f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=from_vitrina"

LAVA_INVOICE_API_URL = "https://gate.lava.top/api/v3/invoice"
LAVA_SUBSCRIPTION_OFFER_ID = "70ca1de2-4073-4ca4-abb8-a964003fe500"
DEFAULT_PERIODICITY = "MONTHLY"

ALLOWED_CURRENCIES = {"USD", "EUR", "RUB"}
ALLOWED_PAYMENT_ROUTES = {"CARD", "PAYPAL", "SBP"}

PUBLIC_ENTRY_POST_TEXT = (
    "Здесь не утешают, здесь проясняют.\n\n"
    "Закрытый канал с доступом по подписке.\n"
    "Внутри - тексты и разборы о тревоге, внимании, мыслях, отношениях и внутренней собранности.\n\n"
    "Нажми на кнопку ниже.\n"
    "Дальше бот сам проведёт тебя по шагам."
)

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
                email TEXT,
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
                lava_invoice_id TEXT,
                contract_id TEXT,
                buyer_email TEXT,
                event_type TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS invoices (
                id BIGSERIAL PRIMARY KEY,
                telegram_user_id BIGINT,
                email TEXT,
                buyer_email TEXT,
                currency TEXT,
                payment_route TEXT,
                payment_route_label TEXT,
                lava_invoice_id TEXT UNIQUE,
                contract_id TEXT UNIQUE,
                product_id TEXT,
                product_title TEXT,
                payment_url TEXT,
                status TEXT,
                last_event_type TEXT,
                access_invite_sent_at TIMESTAMP,
                access_granted_at TIMESTAMP,
                pending_access_invite_link TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                last_webhook_type TEXT,
                last_webhook_payload JSONB
            )
            """
        )

        cur.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS email TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS buyer_email TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS contract_id TEXT UNIQUE
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS product_id TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS product_title TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS last_event_type TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS access_invite_sent_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS access_granted_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS pending_access_invite_link TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE payments
            ADD COLUMN IF NOT EXISTS contract_id TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE payments
            ADD COLUMN IF NOT EXISTS buyer_email TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE payments
            ADD COLUMN IF NOT EXISTS event_type TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE payments
            ADD COLUMN IF NOT EXISTS status TEXT
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


def save_user_email(telegram_user_id: int, email: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET email = %s
            WHERE telegram_user_id = %s
            """,
            (email, telegram_user_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def save_payment_webhook(
    webhook_type: str,
    payload: dict,
    lava_invoice_id: Optional[str],
    contract_id: Optional[str],
    buyer_email: Optional[str],
    event_type: Optional[str],
    status: Optional[str],
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO payments (
                webhook_type,
                payload,
                lava_invoice_id,
                contract_id,
                buyer_email,
                event_type,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                webhook_type,
                json.dumps(payload),
                lava_invoice_id,
                contract_id,
                buyer_email,
                event_type,
                status,
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def save_invoice_record(
    telegram_user_id: int,
    email: str,
    currency: str,
    payment_route: str,
    payment_route_label: str,
    lava_invoice_id: str,
    payment_url: str,
    status: str,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO invoices (
                telegram_user_id,
                email,
                currency,
                payment_route,
                payment_route_label,
                lava_invoice_id,
                payment_url,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (lava_invoice_id)
            DO UPDATE SET
                telegram_user_id = EXCLUDED.telegram_user_id,
                email = EXCLUDED.email,
                currency = EXCLUDED.currency,
                payment_route = EXCLUDED.payment_route,
                payment_route_label = EXCLUDED.payment_route_label,
                payment_url = EXCLUDED.payment_url,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            (
                telegram_user_id,
                email,
                currency,
                payment_route,
                payment_route_label,
                lava_invoice_id,
                payment_url,
                status,
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def is_valid_email(email: str) -> bool:
    email = email.strip()
    pattern = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
    return bool(re.fullmatch(pattern, email))


def normalize_currency(currency: str) -> str:
    normalized = currency.upper().strip()
    if normalized not in ALLOWED_CURRENCIES:
        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемая валюта: {currency}",
        )
    return normalized


def normalize_payment_route(payment_route: Optional[str]) -> str:
    route = (payment_route or "CARD").upper().strip()
    if route not in ALLOWED_PAYMENT_ROUTES:
        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемый маршрут оплаты: {payment_route}",
        )
    return route


def get_payment_route_label(currency: str, payment_route: str) -> str:
    currency = normalize_currency(currency)
    payment_route = normalize_payment_route(payment_route)

    if payment_route == "SBP":
        return "СБП"
    if payment_route == "PAYPAL":
        return "PayPal"
    if currency == "RUB":
        return "RUB / карта"
    if currency == "EUR":
        return "EUR / карта"
    return "USD / карта"


def build_payment_link(
    telegram_user_id: int,
    email: str,
    currency: str,
    payment_route: str,
) -> str:
    query = urllib.parse.urlencode(
        {
            "tg_user_id": telegram_user_id,
            "email": email,
            "currency": currency,
            "route": payment_route,
        }
    )
    return f"{PUBLIC_BASE_URL}/create-payment?{query}"


def create_payment_keyboard(telegram_user_id: int, email: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "Оплатить в RUB",
                url=build_payment_link(telegram_user_id, email, "RUB", "CARD"),
            )
        ],
        [
            InlineKeyboardButton(
                "Оплатить через СБП",
                url=build_payment_link(telegram_user_id, email, "RUB", "SBP"),
            )
        ],
        [
            InlineKeyboardButton(
                "Оплатить в USD",
                url=build_payment_link(telegram_user_id, email, "USD", "CARD"),
            )
        ],
        [
            InlineKeyboardButton(
                "Оплатить в EUR",
                url=build_payment_link(telegram_user_id, email, "EUR", "CARD"),
            )
        ],
        [
            InlineKeyboardButton(
                "Оплатить через PayPal",
                url=build_payment_link(telegram_user_id, email, "USD", "PAYPAL"),
            )
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def find_first_value(data: Any, target_keys: set[str]) -> Optional[Any]:
    if isinstance(data, dict):
        for key, value in data.items():
            if key in target_keys:
                return value
        for value in data.values():
            found = find_first_value(value, target_keys)
            if found is not None:
                return found

    if isinstance(data, list):
        for item in data:
            found = find_first_value(item, target_keys)
            if found is not None:
                return found

    return None


def extract_lava_invoice_id(payload: dict) -> Optional[str]:
    direct = find_first_value(
        payload,
        {
            "invoiceId",
            "invoice_id",
            "invoiceID",
            "receiptInvoice",
            "receipt_invoice",
        },
    )
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    payment_settings = find_first_value(payload, {"paymentSettings", "payment_settings"})
    if isinstance(payment_settings, dict):
        invoice_type = payment_settings.get("type")
        invoice_id = payment_settings.get("id")
        if invoice_type == "invoice" and isinstance(invoice_id, str) and invoice_id.strip():
            return invoice_id.strip()

    return None


def extract_status(payload: dict) -> Optional[str]:
    status = find_first_value(
        payload,
        {
            "status",
            "paymentStatus",
            "payment_status",
            "invoiceStatus",
            "invoice_status",
        },
    )
    if status is None:
        return None
    return str(status)


def extract_event_type(payload: dict) -> Optional[str]:
    value = find_first_value(payload, {"eventType", "event_type", "type"})
    if value is None:
        return None
    return str(value)


def extract_contract_id(payload: dict) -> Optional[str]:
    value = find_first_value(payload, {"contractId", "contract_id"})
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def extract_buyer_email(payload: dict) -> Optional[str]:
    buyer = payload.get("buyer")
    if isinstance(buyer, dict):
        email = buyer.get("email")
        if isinstance(email, str) and email.strip():
            return email.strip().lower()

    value = find_first_value(payload, {"email", "buyerEmail", "buyer_email"})
    if isinstance(value, str) and value.strip():
        return value.strip().lower()

    return None


def extract_product_id(payload: dict) -> Optional[str]:
    product = payload.get("product")
    if isinstance(product, dict):
        value = product.get("id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def extract_product_title(payload: dict) -> Optional[str]:
    product = payload.get("product")
    if isinstance(product, dict):
        value = product.get("title")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def build_invoice_payload(email: str, currency: str, payment_route: str) -> dict:
    currency = normalize_currency(currency)
    payment_route = normalize_payment_route(payment_route)

    if currency == "RUB" and payment_route == "PAYPAL":
        raise HTTPException(
            status_code=400,
            detail="PayPal недоступен для RUB-маршрута",
        )

    if currency in {"USD", "EUR"} and payment_route == "SBP":
        raise HTTPException(
            status_code=400,
            detail="СБП доступен только для RUB-маршрута",
        )

    payload = {
        "email": email,
        "offerId": LAVA_SUBSCRIPTION_OFFER_ID,
        "currency": currency,
        "periodicity": DEFAULT_PERIODICITY,
    }

    if currency == "RUB":
        payload["paymentProvider"] = "SMART_GLOCAL"
    else:
        payload["paymentProvider"] = "UNLIMIT"

    return payload


def create_lava_invoice(email: str, currency: str, payment_route: str) -> dict:
    payload = build_invoice_payload(
        email=email,
        currency=currency,
        payment_route=payment_route,
    )

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
        logging.exception(
            "Lava invoice HTTP error: status=%s body=%s",
            e.code,
            raw_error,
        )
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


def is_successful_payment(event_type: Optional[str], status: Optional[str]) -> bool:
    normalized_event = (event_type or "").strip().lower()
    normalized_status = (status or "").strip().lower()

    success_events = {
        "payment.success",
        "payment_success",
        "subscription.payment.success",
    }

    success_statuses = {
        "success",
        "succeeded",
        "paid",
        "completed",
        "active",
        "finished",
        "subscription-active",
    }

    return normalized_event in success_events or normalized_status in success_statuses


def send_telegram_api_request(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read().decode("utf-8")
        result = json.loads(raw) if raw else {}

    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error in {method}: {result}")

    return result


def send_telegram_text(chat_id: int, text: str) -> None:
    send_telegram_api_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
        },
    )


def send_public_entry_post() -> dict:
    return send_telegram_api_request(
        "sendMessage",
        {
            "chat_id": int(PUBLIC_CHANNEL_CHAT_ID),
            "text": PUBLIC_ENTRY_POST_TEXT,
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {
                            "text": "Оформить доступ",
                            "url": TELEGRAM_BOT_START_FROM_VITRINA_URL,
                        }
                    ]
                ]
            },
        },
    )


def create_personal_join_request_link(label: str) -> str:
    expire_at = int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp())

    result = send_telegram_api_request(
        "createChatInviteLink",
        {
            "chat_id": int(PRIVATE_CHANNEL_CHAT_ID),
            "creates_join_request": True,
            "name": label[:32],
            "expire_date": expire_at,
        },
    )
    return result["result"]["invite_link"]


def approve_join_request(user_id: int) -> None:
    send_telegram_api_request(
        "approveChatJoinRequest",
        {
            "chat_id": int(PRIVATE_CHANNEL_CHAT_ID),
            "user_id": user_id,
        },
    )


def decline_join_request(user_id: int) -> None:
    send_telegram_api_request(
        "declineChatJoinRequest",
        {
            "chat_id": int(PRIVATE_CHANNEL_CHAT_ID),
            "user_id": user_id,
        },
    )


def resolve_invoice_row(
    lava_invoice_id: Optional[str],
    contract_id: Optional[str],
    buyer_email: Optional[str],
) -> Optional[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()

        if lava_invoice_id:
            cur.execute(
                """
                SELECT id, telegram_user_id, status, access_invite_sent_at, pending_access_invite_link
                FROM invoices
                WHERE lava_invoice_id = %s
                LIMIT 1
                """,
                (lava_invoice_id,),
            )
            row = cur.fetchone()
            if row:
                cur.close()
                return {
                    "id": row[0],
                    "telegram_user_id": row[1],
                    "status": row[2],
                    "access_invite_sent_at": row[3],
                    "pending_access_invite_link": row[4],
                }

        if contract_id:
            cur.execute(
                """
                SELECT id, telegram_user_id, status, access_invite_sent_at, pending_access_invite_link
                FROM invoices
                WHERE contract_id = %s
                LIMIT 1
                """,
                (contract_id,),
            )
            row = cur.fetchone()
            if row:
                cur.close()
                return {
                    "id": row[0],
                    "telegram_user_id": row[1],
                    "status": row[2],
                    "access_invite_sent_at": row[3],
                    "pending_access_invite_link": row[4],
                }

        if buyer_email:
            cur.execute(
                """
                SELECT id, telegram_user_id, status, access_invite_sent_at, pending_access_invite_link
                FROM invoices
                WHERE LOWER(email) = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (buyer_email.lower(),),
            )
            row = cur.fetchone()
            if row:
                cur.close()
                return {
                    "id": row[0],
                    "telegram_user_id": row[1],
                    "status": row[2],
                    "access_invite_sent_at": row[3],
                    "pending_access_invite_link": row[4],
                }

        cur.close()
        return None
    finally:
        conn.close()


def update_invoice_from_webhook_resolved(
    invoice_db_id: int,
    webhook_type: str,
    payload: dict,
    status: Optional[str],
    event_type: Optional[str],
    lava_invoice_id: Optional[str],
    contract_id: Optional[str],
    buyer_email: Optional[str],
    product_id: Optional[str],
    product_title: Optional[str],
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE invoices
            SET status = COALESCE(%s, status),
                last_event_type = COALESCE(%s, last_event_type),
                lava_invoice_id = COALESCE(%s, lava_invoice_id),
                contract_id = COALESCE(%s, contract_id),
                buyer_email = COALESCE(%s, buyer_email),
                product_id = COALESCE(%s, product_id),
                product_title = COALESCE(%s, product_title),
                last_webhook_type = %s,
                last_webhook_payload = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                status,
                event_type,
                lava_invoice_id,
                contract_id,
                buyer_email,
                product_id,
                product_title,
                webhook_type,
                json.dumps(payload),
                invoice_db_id,
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def mark_access_invite_sent(invoice_db_id: int, invite_link: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE invoices
            SET access_invite_sent_at = NOW(),
                pending_access_invite_link = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (invite_link, invoice_db_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def mark_access_granted(invoice_db_id: int) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE invoices
            SET access_granted_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (invoice_db_id,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def get_invoice_by_pending_link(invite_link: str) -> Optional[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, telegram_user_id, status, pending_access_invite_link
            FROM invoices
            WHERE pending_access_invite_link = %s
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (invite_link,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "id": row[0],
            "telegram_user_id": row[1],
            "status": row[2],
            "pending_access_invite_link": row[3],
        }
    finally:
        conn.close()


def send_access_request_link_if_paid(
    invoice_row: dict,
    event_type: Optional[str],
    status: Optional[str],
    contract_id: Optional[str],
) -> None:
    if not is_successful_payment(event_type, status):
        logging.info(
            "Webhook is not treated as successful payment yet: event_type=%s status=%s",
            event_type,
            status,
        )
        return

    if invoice_row.get("access_invite_sent_at"):
        logging.info("Access request link already sent for invoice db id=%s", invoice_row["id"])
        return

    telegram_user_id = invoice_row.get("telegram_user_id")
    if not telegram_user_id:
        logging.warning("telegram_user_id not found for invoice db id=%s", invoice_row["id"])
        return

    label = f"paid-{contract_id or invoice_row['id']}"
    invite_link = create_personal_join_request_link(label)

    send_telegram_text(
        telegram_user_id,
        "Оплата подтверждена.\n\n"
        "Вот твоя персональная ссылка для входа в закрытый канал:\n"
        f"{invite_link}\n\n"
        "Важно:\n"
        "- ссылка привязана к заявке на вход;\n"
        "- если её откроет другой аккаунт, вход не будет одобрен;\n"
        "- открой её со своего Telegram-аккаунта.",
    )

    mark_access_invite_sent(invoice_row["id"], invite_link)

    logging.info(
        "Access join-request link sent: invoice_db_id=%s telegram_user_id=%s",
        invoice_row["id"],
        telegram_user_id,
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    save_user(update)
    context.user_data["awaiting_email"] = True

    if update.message:
        await update.message.reply_text(
            "Отправь свой email для оформления оплаты.\n\n"
            "После этого я пришлю кнопки оплаты:\n"
            "- RUB\n"
            "- СБП\n"
            "- USD\n"
            "- EUR\n"
            "- PayPal"
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "Нажми /start, отправь email и выбери удобный способ оплаты."
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

    save_user(update)
    save_user_email(update.effective_user.id, email)

    context.user_data["awaiting_email"] = False
    context.user_data["email"] = email

    reply_markup = create_payment_keyboard(update.effective_user.id, email)

    await update.message.reply_text(
        "Отлично. Теперь выбери способ оплаты.\n\n"
        "Подсказка:\n"
        "- RUB или СБП - для РФ\n"
        "- USD / EUR - для зарубежных карт\n"
        "- PayPal - для тех, у кого удобнее этот маршрут\n\n"
        "Если один вариант не проходит, попробуй другой.",
        reply_markup=reply_markup,
    )


async def handle_chat_join_request(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    join_request = update.chat_join_request
    if not join_request:
        return

    chat_id = join_request.chat.id
    user_id = join_request.from_user.id
    invite_link_obj = join_request.invite_link
    invite_link = invite_link_obj.invite_link if invite_link_obj else None

    logging.info(
        "Chat join request received: chat_id=%s user_id=%s invite_link=%s",
        chat_id,
        user_id,
        invite_link,
    )

    if str(chat_id) != str(PRIVATE_CHANNEL_CHAT_ID):
        logging.info("Join request ignored: wrong chat_id=%s", chat_id)
        return

    if not invite_link:
        decline_join_request(user_id)
        logging.warning("Join request declined: no invite_link user_id=%s", user_id)
        return

    invoice_row = get_invoice_by_pending_link(invite_link)
    if not invoice_row:
        decline_join_request(user_id)
        logging.warning("Join request declined: invite link not found user_id=%s", user_id)
        return

    expected_user_id = invoice_row["telegram_user_id"]
    status = (invoice_row.get("status") or "").strip().lower()

    if expected_user_id != user_id:
        decline_join_request(user_id)
        logging.warning(
            "Join request declined: wrong user expected=%s actual=%s",
            expected_user_id,
            user_id,
        )
        return

    if status not in {"subscription-active", "paid", "completed", "active", "success"}:
        decline_join_request(user_id)
        logging.warning(
            "Join request declined: invoice status is not active enough status=%s user_id=%s",
            status,
            user_id,
        )
        return

    approve_join_request(user_id)
    mark_access_granted(invoice_row["id"])

    logging.info(
        "Join request approved: invoice_db_id=%s user_id=%s",
        invoice_row["id"],
        user_id,
    )


async def run_bot() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
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


@app.get("/success", response_class=HTMLResponse)
def success_page() -> str:
    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Оплата почти завершена</title>
        <style>
            body {{
                margin: 0;
                padding: 0;
                background: #0f0f10;
                color: #f5f5f5;
                font-family: Arial, sans-serif;
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
            }}
            .card {{
                width: 100%;
                max-width: 560px;
                padding: 32px 24px;
                border-radius: 20px;
                background: #18181b;
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.35);
                text-align: center;
                box-sizing: border-box;
            }}
            h1 {{
                margin: 0 0 16px;
                font-size: 28px;
            }}
            p {{
                margin: 0 0 14px;
                color: #d1d5db;
                line-height: 1.5;
                font-size: 16px;
            }}
            .btn {{
                display: inline-block;
                margin-top: 18px;
                padding: 14px 22px;
                border-radius: 12px;
                background: #ffffff;
                color: #111111;
                text-decoration: none;
                font-weight: 700;
                font-size: 16px;
            }}
            .note {{
                margin-top: 16px;
                font-size: 14px;
                color: #a1a1aa;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>Оплата почти завершена.</h1>
            <p>Если платёж уже прошёл, вернись в Telegram.</p>
            <p>После подтверждения оплаты бот пришлёт персональную ссылку на вход в закрытый канал.</p>
            <a class="btn" href="{TELEGRAM_BOT_URL}">Открыть Telegram</a>
            <div class="note">Если Telegram не открылся автоматически, нажми кнопку ещё раз.</div>
        </div>
    </body>
    </html>
    """


@app.get("/open-bot")
def open_bot():
    return RedirectResponse(url=TELEGRAM_BOT_URL, status_code=302)


@app.get("/publish-public-entry-post")
def publish_public_entry_post():
    result = send_public_entry_post()
    return {
        "ok": True,
        "chat_id": PUBLIC_CHANNEL_CHAT_ID,
        "message_id": result["result"]["message_id"],
    }


@app.get("/create-payment")
def create_payment(
    tg_user_id: int,
    email: str,
    currency: str,
    route: Optional[str] = "CARD",
):
    if not is_valid_email(email):
        raise HTTPException(status_code=400, detail="Некорректный email")

    normalized_currency = normalize_currency(currency)
    normalized_route = normalize_payment_route(route)
    payment_route_label = get_payment_route_label(
        normalized_currency,
        normalized_route,
    )

    result = create_lava_invoice(
        email=email,
        currency=normalized_currency,
        payment_route=normalized_route,
    )

    lava_invoice_id = result.get("id")
    payment_url = result.get("paymentUrl")
    status = result.get("status", "new")

    if not payment_url:
        raise HTTPException(
            status_code=502,
            detail="Lava не вернула paymentUrl",
        )

    save_invoice_record(
        telegram_user_id=tg_user_id,
        email=email,
        currency=normalized_currency,
        payment_route=normalized_route,
        payment_route_label=payment_route_label,
        lava_invoice_id=lava_invoice_id,
        payment_url=payment_url,
        status=status,
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

    lava_invoice_id = extract_lava_invoice_id(payload)
    status = extract_status(payload)
    event_type = extract_event_type(payload)
    contract_id = extract_contract_id(payload)
    buyer_email = extract_buyer_email(payload)
    product_id = extract_product_id(payload)
    product_title = extract_product_title(payload)

    logging.info("Lava webhook parsed payload: %s", payload)
    logging.info(
        "Lava webhook extracted invoice_id=%s contract_id=%s buyer_email=%s event_type=%s status=%s",
        lava_invoice_id,
        contract_id,
        buyer_email,
        event_type,
        status,
    )

    save_payment_webhook(
        webhook_type=webhook_type,
        payload=payload,
        lava_invoice_id=lava_invoice_id,
        contract_id=contract_id,
        buyer_email=buyer_email,
        event_type=event_type,
        status=status,
    )

    invoice_row = resolve_invoice_row(
        lava_invoice_id=lava_invoice_id,
        contract_id=contract_id,
        buyer_email=buyer_email,
    )

    if not invoice_row:
        logging.warning(
            "Invoice row not resolved for webhook: invoice_id=%s contract_id=%s buyer_email=%s",
            lava_invoice_id,
            contract_id,
            buyer_email,
        )
        return {
            "ok": True,
            "warning": "invoice_not_resolved",
            "event_type": event_type,
            "status": status,
        }

    update_invoice_from_webhook_resolved(
        invoice_db_id=invoice_row["id"],
        webhook_type=webhook_type,
        payload=payload,
        status=status,
        event_type=event_type,
        lava_invoice_id=lava_invoice_id,
        contract_id=contract_id,
        buyer_email=buyer_email,
        product_id=product_id,
        product_title=product_title,
    )

    send_access_request_link_if_paid(
        invoice_row=invoice_row,
        event_type=event_type,
        status=status,
        contract_id=contract_id,
    )

    return {
        "ok": True,
        "invoice_db_id": invoice_row["id"],
        "event_type": event_type,
        "status": status,
        "contract_id": contract_id,
    }


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
