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
).rstrip("/")

PRIVATE_CHANNEL_CHAT_ID = os.getenv("PRIVATE_CHANNEL_CHAT_ID")
if not PRIVATE_CHANNEL_CHAT_ID:
    raise RuntimeError("PRIVATE_CHANNEL_CHAT_ID не найден в переменных окружения")

PRIVATE_DISCUSSION_CHAT_ID = os.getenv("PRIVATE_DISCUSSION_CHAT_ID")
if not PRIVATE_DISCUSSION_CHAT_ID:
    raise RuntimeError("PRIVATE_DISCUSSION_CHAT_ID не найден в переменных окружения")

PUBLIC_CHANNEL_CHAT_ID = os.getenv("PUBLIC_CHANNEL_CHAT_ID")
if not PUBLIC_CHANNEL_CHAT_ID:
    raise RuntimeError("PUBLIC_CHANNEL_CHAT_ID не найден в переменных окружения")

TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "tochka_opory_access_bot")
TELEGRAM_BOT_URL = f"https://t.me/{TELEGRAM_BOT_USERNAME}"
TELEGRAM_BOT_START_FROM_VITRINA_URL = f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=from_vitrina"

LAVA_INVOICE_API_URL = "https://gate.lava.top/api/v3/invoice"
LAVA_SUBSCRIPTION_OFFER_ID = os.getenv(
    "LAVA_SUBSCRIPTION_OFFER_ID",
    "70ca1de2-4073-4ca4-abb8-a964003fe500",
)
DEFAULT_PERIODICITY = "MONTHLY"

# Эти env нужны только для отображения стоимости на экране подтверждения.
# Если оставить пустыми - строка "Стоимость" просто не покажется.
DISPLAY_PRICE_RF = os.getenv("DISPLAY_PRICE_RF", "")
DISPLAY_PRICE_FOREIGN = os.getenv("DISPLAY_PRICE_FOREIGN", "")
DISPLAY_PRICE_PAYPAL = os.getenv("DISPLAY_PRICE_PAYPAL", "")

ALLOWED_CURRENCIES = {"USD", "RUB"}
ALLOWED_PAYMENT_ROUTES = {"CARD", "PAYPAL"}
ALLOWED_PAYMENT_METHODS = {"rf_card", "foreign_card", "paypal"}

ACCESS_STATUS_VALUES = {"subscription-active", "paid", "completed", "active", "success"}

CHANNEL_KIND = "channel"
CHAT_KIND = "chat"

PUBLIC_ENTRY_POST_TEXT = (
    "Здесь не утешают, здесь проясняют.\n\n"
    "Закрытый доступ по подписке.\n"
    "Внутри - канал с текстами и разборами, закрытый чат для общения и эфиры только для подписчиков.\n\n"
    "Нажми на кнопку ниже.\n"
    "Дальше бот сам проведёт тебя по шагам."
)

START_TEXT = (
    "Точка опоры - подписка на 1 месяц.\n\n"
    "После оплаты бот пришлёт 2 персональные ссылки:\n"
    "1. в закрытый канал\n"
    "2. в закрытый чат\n\n"
    "Нажми на кнопку ниже.\n"
    "Откроется экран выбора оплаты."
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

                channel_access_invite_sent_at TIMESTAMP,
                channel_access_granted_at TIMESTAMP,
                channel_access_revoked_at TIMESTAMP,
                pending_channel_invite_link TEXT,

                chat_access_invite_sent_at TIMESTAMP,
                chat_access_granted_at TIMESTAMP,
                chat_access_revoked_at TIMESTAMP,
                pending_chat_invite_link TEXT,

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
            ADD COLUMN IF NOT EXISTS channel_access_invite_sent_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS channel_access_granted_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS channel_access_revoked_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS pending_channel_invite_link TEXT
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS chat_access_invite_sent_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS chat_access_granted_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS chat_access_revoked_at TIMESTAMP
            """
        )

        cur.execute(
            """
            ALTER TABLE invoices
            ADD COLUMN IF NOT EXISTS pending_chat_invite_link TEXT
            """
        )

        # Старые поля оставляем, чтобы не падать на уже существующей базе.
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
            ADD COLUMN IF NOT EXISTS access_revoked_at TIMESTAMP
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
            INSERT INTO users (telegram_user_id, email)
            VALUES (%s, %s)
            ON CONFLICT (telegram_user_id)
            DO UPDATE SET email = EXCLUDED.email
            """,
            (telegram_user_id, email),
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
        raise HTTPException(status_code=400, detail=f"Неподдерживаемая валюта: {currency}")
    return normalized


def normalize_payment_route(payment_route: Optional[str]) -> str:
    route = (payment_route or "CARD").upper().strip()
    if route not in ALLOWED_PAYMENT_ROUTES:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый маршрут оплаты: {payment_route}")
    return route


def normalize_payment_method(method: str) -> str:
    normalized = method.strip().lower()
    if normalized not in ALLOWED_PAYMENT_METHODS:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый способ оплаты: {method}")
    return normalized


def map_payment_method(method: str) -> dict:
    normalized = normalize_payment_method(method)

    if normalized == "rf_card":
        return {
            "currency": "RUB",
            "route": "CARD",
            "label": "Оплата картами РФ",
            "display_price": DISPLAY_PRICE_RF,
        }

    if normalized == "foreign_card":
        return {
            "currency": "USD",
            "route": "CARD",
            "label": "Оплата любой другой картой",
            "display_price": DISPLAY_PRICE_FOREIGN,
        }

    return {
        "currency": "USD",
        "route": "PAYPAL",
        "label": "PayPal",
        "display_price": DISPLAY_PRICE_PAYPAL,
    }


def get_payment_route_label(currency: str, payment_route: str) -> str:
    currency = normalize_currency(currency)
    payment_route = normalize_payment_route(payment_route)

    if payment_route == "PAYPAL":
        return "PayPal"
    if currency == "RUB":
        return "Оплата картами РФ"
    return "Оплата любой другой картой"


def build_checkout_url(telegram_user_id: int) -> str:
    query = urllib.parse.urlencode({"tg_user_id": telegram_user_id})
    return f"{PUBLIC_BASE_URL}/checkout?{query}"


def build_start_keyboard(telegram_user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Открыть доступ", url=build_checkout_url(telegram_user_id))]]
    )


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
        {"status", "paymentStatus", "payment_status", "invoiceStatus", "invoice_status"},
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
        raise HTTPException(status_code=400, detail="PayPal недоступен для рублёвого маршрута")

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
    payload = build_invoice_payload(email=email, currency=currency, payment_route=payment_route)

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
        raise HTTPException(status_code=502, detail=f"Не удалось соединиться с Lava: {str(e)}")

    except Exception as e:
        logging.exception("Unexpected Lava invoice error: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при создании invoice: {str(e)}")


def is_successful_payment(event_type: Optional[str], status: Optional[str]) -> bool:
    normalized_event = (event_type or "").strip().lower()
    normalized_status = (status or "").strip().lower()

    success_events = {"payment.success", "payment_success", "subscription.payment.success"}
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


def is_failed_or_inactive_payment(event_type: Optional[str], status: Optional[str]) -> bool:
    normalized_event = (event_type or "").strip().lower()
    normalized_status = (status or "").strip().lower()

    failed_events = {"payment.failed", "payment_failed", "subscription.payment.failed"}
    failed_statuses = {
        "subscription-failed",
        "failed",
        "declined",
        "expired",
        "inactive",
        "cancelled",
        "canceled",
    }

    return normalized_event in failed_events or normalized_status in failed_statuses


def send_telegram_api_request(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read().decode("utf-8")
        result = json.loads(raw) if raw else {}

    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error in {method}: {result}")

    return result


def send_telegram_text(chat_id: int, text: str) -> None:
    send_telegram_api_request("sendMessage", {"chat_id": chat_id, "text": text})


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


def get_access_chat_id(access_kind: str) -> int:
    if access_kind == CHANNEL_KIND:
        return int(PRIVATE_CHANNEL_CHAT_ID)
    if access_kind == CHAT_KIND:
        return int(PRIVATE_DISCUSSION_CHAT_ID)
    raise ValueError(f"Неизвестный access_kind: {access_kind}")


def create_personal_join_request_link(access_kind: str, label: str) -> str:
    expire_at = int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp())

    result = send_telegram_api_request(
        "createChatInviteLink",
        {
            "chat_id": get_access_chat_id(access_kind),
            "creates_join_request": True,
            "name": label[:32],
            "expire_date": expire_at,
        },
    )
    return result["result"]["invite_link"]


def approve_join_request(chat_id: int, user_id: int) -> None:
    send_telegram_api_request(
        "approveChatJoinRequest",
        {"chat_id": chat_id, "user_id": user_id},
    )


def decline_join_request(chat_id: int, user_id: int) -> None:
    send_telegram_api_request(
        "declineChatJoinRequest",
        {"chat_id": chat_id, "user_id": user_id},
    )


def remove_member(chat_id: int, user_id: int) -> None:
    until_date = int((datetime.now(timezone.utc) + timedelta(seconds=60)).timestamp())

    try:
        send_telegram_api_request(
            "banChatMember",
            {
                "chat_id": chat_id,
                "user_id": user_id,
                "until_date": until_date,
                "revoke_messages": False,
            },
        )
    except Exception:
        logging.exception(
            "Не удалось удалить пользователя: chat_id=%s user_id=%s",
            chat_id,
            user_id,
        )

    try:
        send_telegram_api_request(
            "unbanChatMember",
            {
                "chat_id": chat_id,
                "user_id": user_id,
                "only_if_banned": True,
            },
        )
    except Exception:
        logging.exception(
            "Не удалось снять временный бан после удаления: chat_id=%s user_id=%s",
            chat_id,
            user_id,
        )


def resolve_invoice_row(
    lava_invoice_id: Optional[str],
    contract_id: Optional[str],
    buyer_email: Optional[str],
) -> Optional[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()

        select_sql = """
            SELECT
                id,
                telegram_user_id,
                status,

                channel_access_invite_sent_at,
                channel_access_granted_at,
                channel_access_revoked_at,
                pending_channel_invite_link,

                chat_access_invite_sent_at,
                chat_access_granted_at,
                chat_access_revoked_at,
                pending_chat_invite_link
            FROM invoices
            WHERE {condition}
            LIMIT 1
        """

        def row_to_dict(row: tuple) -> dict:
            return {
                "id": row[0],
                "telegram_user_id": row[1],
                "status": row[2],
                "channel_access_invite_sent_at": row[3],
                "channel_access_granted_at": row[4],
                "channel_access_revoked_at": row[5],
                "pending_channel_invite_link": row[6],
                "chat_access_invite_sent_at": row[7],
                "chat_access_granted_at": row[8],
                "chat_access_revoked_at": row[9],
                "pending_chat_invite_link": row[10],
            }

        if lava_invoice_id:
            cur.execute(select_sql.format(condition="lava_invoice_id = %s"), (lava_invoice_id,))
            row = cur.fetchone()
            if row:
                cur.close()
                return row_to_dict(row)

        if contract_id:
            cur.execute(select_sql.format(condition="contract_id = %s"), (contract_id,))
            row = cur.fetchone()
            if row:
                cur.close()
                return row_to_dict(row)

        if buyer_email:
            cur.execute(
                """
                SELECT
                    id,
                    telegram_user_id,
                    status,

                    channel_access_invite_sent_at,
                    channel_access_granted_at,
                    channel_access_revoked_at,
                    pending_channel_invite_link,

                    chat_access_invite_sent_at,
                    chat_access_granted_at,
                    chat_access_revoked_at,
                    pending_chat_invite_link
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
                return row_to_dict(row)

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


def mark_access_invite_sent(invoice_db_id: int, access_kind: str, invite_link: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        if access_kind == CHANNEL_KIND:
            cur.execute(
                """
                UPDATE invoices
                SET channel_access_invite_sent_at = NOW(),
                    pending_channel_invite_link = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (invite_link, invoice_db_id),
            )
        elif access_kind == CHAT_KIND:
            cur.execute(
                """
                UPDATE invoices
                SET chat_access_invite_sent_at = NOW(),
                    pending_chat_invite_link = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (invite_link, invoice_db_id),
            )
        else:
            raise ValueError(f"Неизвестный access_kind: {access_kind}")

        conn.commit()
        cur.close()
    finally:
        conn.close()


def mark_access_granted(invoice_db_id: int, access_kind: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        if access_kind == CHANNEL_KIND:
            cur.execute(
                """
                UPDATE invoices
                SET channel_access_granted_at = NOW(),
                    channel_access_revoked_at = NULL,
                    pending_channel_invite_link = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (invoice_db_id,),
            )
        elif access_kind == CHAT_KIND:
            cur.execute(
                """
                UPDATE invoices
                SET chat_access_granted_at = NOW(),
                    chat_access_revoked_at = NULL,
                    pending_chat_invite_link = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (invoice_db_id,),
            )
        else:
            raise ValueError(f"Неизвестный access_kind: {access_kind}")

        conn.commit()
        cur.close()
    finally:
        conn.close()


def mark_all_access_revoked(invoice_db_id: int) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE invoices
            SET channel_access_revoked_at = NOW(),
                channel_access_granted_at = NULL,
                channel_access_invite_sent_at = NULL,
                pending_channel_invite_link = NULL,

                chat_access_revoked_at = NOW(),
                chat_access_granted_at = NULL,
                chat_access_invite_sent_at = NULL,
                pending_chat_invite_link = NULL,

                access_revoked_at = NOW(),
                access_granted_at = NULL,
                access_invite_sent_at = NULL,
                pending_access_invite_link = NULL,

                updated_at = NOW()
            WHERE id = %s
            """,
            (invoice_db_id,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def get_invoice_by_pending_link(access_kind: str, invite_link: str) -> Optional[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()

        if access_kind == CHANNEL_KIND:
            cur.execute(
                """
                SELECT
                    id,
                    telegram_user_id,
                    status,
                    pending_channel_invite_link,
                    pending_chat_invite_link
                FROM invoices
                WHERE pending_channel_invite_link = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (invite_link,),
            )
        elif access_kind == CHAT_KIND:
            cur.execute(
                """
                SELECT
                    id,
                    telegram_user_id,
                    status,
                    pending_channel_invite_link,
                    pending_chat_invite_link
                FROM invoices
                WHERE pending_chat_invite_link = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (invite_link,),
            )
        else:
            raise ValueError(f"Неизвестный access_kind: {access_kind}")

        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        return {
            "id": row[0],
            "telegram_user_id": row[1],
            "status": row[2],
            "pending_channel_invite_link": row[3],
            "pending_chat_invite_link": row[4],
        }
    finally:
        conn.close()


def build_paid_access_message(
    channel_invite_link: Optional[str],
    chat_invite_link: Optional[str],
) -> str:
    parts = ["Оплата подтверждена.\n"]

    if channel_invite_link:
        parts.append(
            "1. Твоя персональная ссылка для входа в закрытый канал:\n"
            f"{channel_invite_link}\n"
        )

    if chat_invite_link:
        parts.append(
            "2. Твоя персональная ссылка для входа в закрытый чат:\n"
            f"{chat_invite_link}\n"
        )

    parts.append(
        "Важно:\n"
        "- каждая ссылка привязана к заявке на вход;\n"
        "- если её откроет другой аккаунт, вход не будет одобрен;\n"
        "- открой обе ссылки со своего Telegram-аккаунта;\n"
        "- сначала зайди в канал, потом в чат."
    )

    return "\n".join(parts)


def send_access_request_links_if_paid(
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

    telegram_user_id = invoice_row.get("telegram_user_id")
    if not telegram_user_id:
        logging.warning("telegram_user_id not found for invoice db id=%s", invoice_row["id"])
        return

    channel_invite_link = invoice_row.get("pending_channel_invite_link")
    chat_invite_link = invoice_row.get("pending_chat_invite_link")

    created_any_link = False

    if not invoice_row.get("channel_access_granted_at") and not channel_invite_link:
        label = f"channel-{contract_id or invoice_row['id']}"
        channel_invite_link = create_personal_join_request_link(CHANNEL_KIND, label)
        mark_access_invite_sent(invoice_row["id"], CHANNEL_KIND, channel_invite_link)
        created_any_link = True

    if not invoice_row.get("chat_access_granted_at") and not chat_invite_link:
        label = f"chat-{contract_id or invoice_row['id']}"
        chat_invite_link = create_personal_join_request_link(CHAT_KIND, label)
        mark_access_invite_sent(invoice_row["id"], CHAT_KIND, chat_invite_link)
        created_any_link = True

    already_fully_granted = bool(
        invoice_row.get("channel_access_granted_at") and invoice_row.get("chat_access_granted_at")
    )
    if already_fully_granted:
        logging.info("Access already fully granted for invoice db id=%s", invoice_row["id"])
        return

    if not created_any_link:
        logging.info(
            "Access links already created and sent earlier for invoice db id=%s",
            invoice_row["id"],
        )
        return

    send_telegram_text(
        telegram_user_id,
        build_paid_access_message(
            channel_invite_link=channel_invite_link,
            chat_invite_link=chat_invite_link,
        ),
    )

    logging.info(
        "Access links sent: invoice_db_id=%s telegram_user_id=%s",
        invoice_row["id"],
        telegram_user_id,
    )


def revoke_access_if_needed(invoice_row: dict, event_type: Optional[str], status: Optional[str]) -> None:
    if not is_failed_or_inactive_payment(event_type, status):
        return

    telegram_user_id = invoice_row.get("telegram_user_id")
    if not telegram_user_id:
        return

    had_access_or_pending = bool(
        invoice_row.get("channel_access_granted_at")
        or invoice_row.get("channel_access_invite_sent_at")
        or invoice_row.get("chat_access_granted_at")
        or invoice_row.get("chat_access_invite_sent_at")
    )

    if not had_access_or_pending:
        logging.info(
            "Failure webhook received, but access was not granted yet: invoice_db_id=%s",
            invoice_row["id"],
        )
        return

    remove_member(int(PRIVATE_CHANNEL_CHAT_ID), telegram_user_id)
    remove_member(int(PRIVATE_DISCUSSION_CHAT_ID), telegram_user_id)
    mark_all_access_revoked(invoice_row["id"])

    try:
        send_telegram_text(
            telegram_user_id,
            "Подписка не продлена.\n"
            "Доступ в закрытый канал и закрытый чат остановлен.\n\n"
            "Чтобы вернуться, снова открой бота и оформи подписку заново.",
        )
    except Exception:
        logging.exception(
            "Не удалось отправить уведомление о снятии доступа: telegram_user_id=%s",
            telegram_user_id,
        )

    logging.info(
        "Access revoked after failed/inactive payment: invoice_db_id=%s telegram_user_id=%s",
        invoice_row["id"],
        telegram_user_id,
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    save_user(update)

    if not update.message or not update.effective_user:
        return

    await update.message.reply_text(
        START_TEXT,
        reply_markup=build_start_keyboard(update.effective_user.id),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    await update.message.reply_text(
        START_TEXT,
        reply_markup=build_start_keyboard(update.effective_user.id),
    )
async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    chat = update.effective_chat
    user = update.effective_user

    text = (
        f"Chat title: {chat.title or '—'}\n"
        f"Chat type: {chat.type}\n"
        f"Chat ID: {chat.id}"
    )

    try:
        await context.bot.send_message(chat_id=user.id, text=text)
    except Exception:
        pass

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    save_user(update)

    chat = update.effective_chat
    if not chat:
        return

    # В группах и каналах бот молчит.
    # Чтобы не засорять чат и не светить кнопку подписки всем участникам.
    if chat.type in {"group", "supergroup", "channel"}:
        return

    await update.message.reply_text(
        "Нажми кнопку ниже, чтобы открыть доступ в канал и чат.",
        reply_markup=build_start_keyboard(update.effective_user.id),
    )
    if not update.message or not update.effective_user:
        return

    save_user(update)

    await update.message.reply_text(
        "Нажми кнопку ниже, чтобы открыть доступ в канал и чат.",
        reply_markup=build_start_keyboard(update.effective_user.id),
    )


def resolve_access_kind_by_chat_id(chat_id: int) -> Optional[str]:
    if str(chat_id) == str(PRIVATE_CHANNEL_CHAT_ID):
        return CHANNEL_KIND
    if str(chat_id) == str(PRIVATE_DISCUSSION_CHAT_ID):
        return CHAT_KIND
    return None


async def handle_chat_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    access_kind = resolve_access_kind_by_chat_id(chat_id)
    if not access_kind:
        logging.info("Join request ignored: wrong chat_id=%s", chat_id)
        return

    if not invite_link:
        decline_join_request(chat_id, user_id)
        logging.warning("Join request declined: no invite_link user_id=%s", user_id)
        return

    invoice_row = get_invoice_by_pending_link(access_kind, invite_link)
    if not invoice_row:
        decline_join_request(chat_id, user_id)
        logging.warning(
            "Join request declined: invite link not found access_kind=%s user_id=%s",
            access_kind,
            user_id,
        )
        return

    expected_user_id = invoice_row["telegram_user_id"]
    status = (invoice_row.get("status") or "").strip().lower()

    if expected_user_id != user_id:
        decline_join_request(chat_id, user_id)
        logging.warning(
            "Join request declined: wrong user expected=%s actual=%s access_kind=%s",
            expected_user_id,
            user_id,
            access_kind,
        )
        return

    if status not in ACCESS_STATUS_VALUES:
        decline_join_request(chat_id, user_id)
        logging.warning(
            "Join request declined: invoice status is not active enough status=%s user_id=%s access_kind=%s",
            status,
            user_id,
            access_kind,
        )
        return

    approve_join_request(chat_id, user_id)
    mark_access_granted(invoice_row["id"], access_kind)

    try:
        if access_kind == CHANNEL_KIND:
            send_telegram_text(
                user_id,
                "Вход в закрытый канал одобрен.\n"
                "Теперь открой персональную ссылку на чат и отправь заявку туда.",
            )
        else:
            send_telegram_text(
                user_id,
                "Вход в закрытый чат одобрен.\n"
                "Доступ полностью открыт.",
            )
    except Exception:
        logging.exception(
            "Не удалось отправить уведомление после approve: user_id=%s access_kind=%s",
            user_id,
            access_kind,
        )

    logging.info(
        "Join request approved: invoice_db_id=%s user_id=%s access_kind=%s",
        invoice_row["id"],
        user_id,
        access_kind,
    )


async def run_bot() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
                            application.add_handler(CommandHandler("chatid", chatid_command))
    application.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

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


@app.get("/checkout", response_class=HTMLResponse)
def checkout_page(tg_user_id: int) -> str:
    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1.0">
        <title>Точка опоры - подписка на 1 месяц</title>
        <style>
            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                min-height: 100vh;
                background: #050505;
                color: #f5f5f5;
                font-family: Arial, sans-serif;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 24px;
            }}

            .card {{
                position: relative;
                width: 100%;
                max-width: 860px;
                background: #0b0b0c;
                border: 1px solid rgba(255, 255, 255, 0.07);
                border-radius: 30px;
                padding: 36px 42px 28px;
                overflow: hidden;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
            }}

            .logo {{
                position: absolute;
                top: 24px;
                left: 50%;
                transform: translateX(-50%);
                width: 132px;
                height: 132px;
                pointer-events: none;
            }}

            .content {{
                position: relative;
                z-index: 1;
            }}

            h1 {{
                margin: 128px 0 14px;
                text-align: center;
                font-size: 38px;
                line-height: 1.08;
                font-weight: 800;
                letter-spacing: -0.02em;
                color: #ffffff;
            }}

            .subtitle {{
                margin: 0 0 28px;
                text-align: center;
                color: #d1d5db;
                font-size: 18px;
                line-height: 1.45;
            }}

            .step {{
                display: none;
            }}

            .step.active {{
                display: block;
            }}

            .stack {{
                display: grid;
                gap: 18px;
                max-width: 740px;
                margin: 0 auto;
            }}

            .option-btn,
            .action-btn,
            .secondary-btn {{
                appearance: none;
                width: 100%;
                border: none;
                border-radius: 20px;
                cursor: pointer;
                transition: transform 0.15s ease, border-color 0.15s ease, opacity 0.15s ease;
                font-size: 16px;
                line-height: 1.2;
                font-family: inherit;
            }}

            .option-btn:hover,
            .action-btn:hover,
            .secondary-btn:hover {{
                transform: translateY(-1px);
            }}

            .option-btn {{
                text-align: center;
                padding: 28px 24px;
                background: #121316;
                color: #ffffff;
                border: 1px solid rgba(255, 255, 255, 0.08);
                font-weight: 800;
                font-size: 24px;
                box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
            }}

            .label {{
                display: block;
                margin: 0 auto 10px;
                max-width: 740px;
                font-size: 16px;
                color: #e5e7eb;
            }}

            .input {{
                width: 100%;
                max-width: 740px;
                display: block;
                margin: 0 auto;
                padding: 18px 18px;
                border-radius: 18px;
                border: 1px solid rgba(255, 255, 255, 0.10);
                background: #101114;
                color: #ffffff;
                outline: none;
                font-size: 17px;
            }}

            .input:focus {{
                border-color: rgba(255, 255, 255, 0.24);
            }}

            .hint {{
                max-width: 740px;
                margin: 12px auto 0;
                color: #8f95a3;
                font-size: 14px;
                line-height: 1.45;
            }}

            .summary {{
                display: grid;
                gap: 12px;
                background: #101114;
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 20px;
                padding: 18px;
                margin: 0 auto 20px;
                max-width: 740px;
            }}

            .summary-row {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
                font-size: 16px;
            }}

            .summary-label {{
                color: #9ca3af;
            }}

            .summary-value {{
                text-align: right;
                color: #ffffff;
                font-weight: 700;
                word-break: break-word;
            }}

            .action-btn {{
                max-width: 740px;
                display: block;
                margin: 0 auto;
                padding: 18px 20px;
                background: #f3f4f6;
                color: #111111;
                font-weight: 800;
                font-size: 18px;
            }}

            .secondary-btn {{
                max-width: 740px;
                display: block;
                margin: 14px auto 0;
                padding: 15px 18px;
                background: transparent;
                color: #cbd5e1;
                border: 1px solid rgba(255, 255, 255, 0.10);
                font-size: 16px;
            }}

            .error {{
                min-height: 18px;
                margin: 10px auto 0;
                max-width: 740px;
                color: #fda4af;
                font-size: 13px;
                text-align: center;
            }}

            .footnote {{
                max-width: 740px;
                margin: 22px auto 0;
                text-align: center;
                color: #9ca3af;
                font-size: 14px;
                line-height: 1.5;
            }}

            @media (max-width: 900px) {{
                .card {{
                    max-width: 620px;
                    padding: 32px 26px 24px;
                }}

                .logo {{
                    width: 118px;
                    height: 118px;
                }}

                h1 {{
                    font-size: 34px;
                    margin-top: 118px;
                }}

                .subtitle {{
                    font-size: 17px;
                }}

                .option-btn {{
                    font-size: 22px;
                    padding: 24px 20px;
                }}
            }}

            @media (max-width: 640px) {{
                body {{
                    padding: 16px;
                }}

                .card {{
                    max-width: 100%;
                    padding: 26px 18px 20px;
                    border-radius: 24px;
                }}

                .logo {{
                    width: 104px;
                    height: 104px;
                    top: 20px;
                }}

                h1 {{
                    margin-top: 104px;
                    font-size: 28px;
                }}

                .subtitle {{
                    font-size: 16px;
                    margin-bottom: 22px;
                }}

                .option-btn {{
                    font-size: 20px;
                    padding: 22px 16px;
                }}

                .footnote {{
                    font-size: 13px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <svg class="logo" viewBox="0 0 200 200" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
                <circle cx="100" cy="100" r="12" fill="white"/>
                <path d="M100 24 A76 76 0 0 1 176 100" stroke="white" stroke-width="14" stroke-linecap="round"/>
                <path d="M176 100 A76 76 0 0 1 100 176" stroke="white" stroke-width="14" stroke-linecap="round"/>
                <path d="M100 176 A76 76 0 0 1 24 100" stroke="white" stroke-width="14" stroke-linecap="round"/>
                <path d="M24 100 A76 76 0 0 1 100 24" stroke="white" stroke-width="14" stroke-linecap="round"/>
            </svg>

            <div class="content">
                <h1>Точка опоры - подписка на 1 месяц</h1>
                <p class="subtitle">После оплаты бот пришлёт доступ в закрытый канал и закрытый чат</p>

                <div id="step-method" class="step active">
                    <div class="stack">
                        <button class="option-btn" onclick="chooseMethod('rf_card')">
                            Оплата картами РФ
                        </button>

                        <button class="option-btn" onclick="chooseMethod('foreign_card')">
                            Оплата любой другой картой
                        </button>

                        <button class="option-btn" onclick="chooseMethod('paypal')">
                            PayPal
                        </button>
                    </div>

                    <div class="footnote">
                        * При оплате картой оформляется автосписание каждые 30 дней.
                    </div>
                </div>

                <div id="step-email" class="step">
                    <label class="label" for="email">Введите ваш email</label>
                    <input id="email" class="input" type="email" placeholder="name@example.com" autocomplete="email">
                    <div class="hint">
                        На этот email будет привязана оплата. После подтверждения бот пришлёт 2 персональные ссылки - в закрытый канал и в закрытый чат.
                    </div>
                    <div class="error" id="email-error"></div>
                    <button class="action-btn" style="margin-top: 18px;" onclick="goToConfirm()">Далее</button>
                    <button class="secondary-btn" onclick="showStep('step-method')">Назад</button>
                </div>

                <div id="step-confirm" class="step">
                    <div class="summary">
                        <div class="summary-row">
                            <div class="summary-label">Продукт</div>
                            <div class="summary-value">Точка опоры - подписка на 1 месяц</div>
                        </div>
                        <div class="summary-row">
                            <div class="summary-label">Что входит</div>
                            <div class="summary-value">Закрытый канал + закрытый чат</div>
                        </div>
                        <div class="summary-row">
                            <div class="summary-label">Способ оплаты</div>
                            <div class="summary-value" id="summary-method"></div>
                        </div>
                        <div class="summary-row">
                            <div class="summary-label">Email</div>
                            <div class="summary-value" id="summary-email"></div>
                        </div>
                        <div class="summary-row" id="summary-price-row" style="display:none;">
                            <div class="summary-label">Стоимость</div>
                            <div class="summary-value" id="summary-price"></div>
                        </div>
                    </div>

                    <button class="action-btn" onclick="goToPayment()">Перейти к оплате</button>
                    <button class="secondary-btn" onclick="showStep('step-email')">Вернуться назад</button>
                </div>
            </div>
        </div>

        <script>
            const tgUserId = {tg_user_id};
            const methods = {{
                rf_card: {{
                    label: "Оплата картами РФ",
                    displayPrice: {json.dumps(DISPLAY_PRICE_RF)},
                }},
                foreign_card: {{
                    label: "Оплата любой другой картой",
                    displayPrice: {json.dumps(DISPLAY_PRICE_FOREIGN)},
                }},
                paypal: {{
                    label: "PayPal",
                    displayPrice: {json.dumps(DISPLAY_PRICE_PAYPAL)},
                }},
            }};

            let selectedMethod = null;

            function showStep(stepId) {{
                document.querySelectorAll(".step").forEach((el) => el.classList.remove("active"));
                document.getElementById(stepId).classList.add("active");
            }}

            function chooseMethod(method) {{
                selectedMethod = method;
                document.getElementById("email-error").textContent = "";
                showStep("step-email");
            }}

            function validateEmail(email) {{
                const pattern = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$/;
                return pattern.test(String(email).trim());
            }}

            function goToConfirm() {{
                const emailInput = document.getElementById("email");
                const email = emailInput.value.trim();
                const errorBox = document.getElementById("email-error");

                if (!selectedMethod) {{
                    errorBox.textContent = "Сначала выбери способ оплаты.";
                    showStep("step-method");
                    return;
                }}

                if (!validateEmail(email)) {{
                    errorBox.textContent = "Введите корректный email.";
                    return;
                }}

                errorBox.textContent = "";
                document.getElementById("summary-method").textContent = methods[selectedMethod].label;
                document.getElementById("summary-email").textContent = email;

                const priceRow = document.getElementById("summary-price-row");
                const priceValue = document.getElementById("summary-price");
                if (methods[selectedMethod].displayPrice) {{
                    priceRow.style.display = "flex";
                    priceValue.textContent = methods[selectedMethod].displayPrice;
                }} else {{
                    priceRow.style.display = "none";
                    priceValue.textContent = "";
                }}

                showStep("step-confirm");
            }}

            function goToPayment() {{
                const email = document.getElementById("email").value.trim();
                const query = new URLSearchParams({{
                    tg_user_id: String(tgUserId),
                    email: email,
                    method: selectedMethod,
                }});
                window.location.href = "{PUBLIC_BASE_URL}/create-payment?" + query.toString();
            }}
        </script>
    </body>
    </html>
    """


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
                background: #050505;
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
                border-radius: 22px;
                background: #0b0b0c;
                border: 1px solid rgba(255,255,255,0.08);
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
            <p>После подтверждения оплаты бот пришлёт персональные ссылки на вход в закрытый канал и закрытый чат.</p>
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
    method: Optional[str] = None,
    currency: Optional[str] = None,
    route: Optional[str] = None,
):
    if not is_valid_email(email):
        raise HTTPException(status_code=400, detail="Некорректный email")

    if method:
        mapped = map_payment_method(method)
        normalized_currency = mapped["currency"]
        normalized_route = mapped["route"]
        payment_route_label = mapped["label"]
    else:
        if not currency:
            raise HTTPException(status_code=400, detail="Не передан способ оплаты")
        normalized_currency = normalize_currency(currency)
        normalized_route = normalize_payment_route(route)
        payment_route_label = get_payment_route_label(normalized_currency, normalized_route)

    result = create_lava_invoice(
        email=email,
        currency=normalized_currency,
        payment_route=normalized_route,
    )

    lava_invoice_id = result.get("id")
    payment_url = result.get("paymentUrl")
    status = result.get("status", "new")

    if not payment_url:
        raise HTTPException(status_code=502, detail="Lava не вернула paymentUrl")

    save_user_email(tg_user_id, email)

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

    revoke_access_if_needed(
        invoice_row=invoice_row,
        event_type=event_type,
        status=status,
    )

    send_access_request_links_if_paid(
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
