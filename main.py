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

DISPLAY_PRICE_RF = os.getenv("DISPLAY_PRICE_RF", "")
DISPLAY_PRICE_FOREIGN = os.getenv("DISPLAY_PRICE_FOREIGN", "")
DISPLAY_PRICE_PAYPAL = os.getenv("DISPLAY_PRICE_PAYPAL", "")

ALLOWED_CURRENCIES = {"USD", "RUB"}
ALLOWED_PAYMENT_ROUTES = {"CARD", "PAYPAL"}
ALLOWED_PAYMENT_METHODS = {"rf_card", "foreign_card", "paypal"}

PUBLIC_ENTRY_POST_TEXT = (
    "Здесь не утешают, здесь проясняют.\n\n"
    "Закрытый канал с доступом по подписке.\n"
    "Внутри - тексты и разборы о тревоге, внимании, мыслях, отношениях и внутренней собранности.\n\n"
    "Нажми на кнопку ниже.\n"
    "Дальше бот сам проведёт тебя по шагам."
)

START_TEXT = (
    "Доступ в Точку опоры.\n\n"
    "Нажми на кнопку ниже.\n"
    "Откроется тёмный экран выбора оплаты.\n"
    "После оплаты бот сам пришлёт персональную ссылку на вход в закрытый канал."
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
                access_revoked_at TIMESTAMP,
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
            detail="PayPal недоступен для рублёвого маршрута",
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


def is_failed_or_inactive_payment(event_type: Optional[str], status: Optional[str]) -> bool:
    normalized_event = (event_type or "").strip().lower()
    normalized_status = (status or "").strip().lower()

    failed_events = {
        "payment.failed",
        "payment_failed",
        "subscription.payment.failed",
    }

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


def remove_member_from_private_channel(user_id: int) -> None:
    until_date = int((datetime.now(timezone.utc) + timedelta(seconds=60)).timestamp())

    try:
        send_telegram_api_request(
            "banChatMember",
            {
                "chat_id": int(PRIVATE_CHANNEL_CHAT_ID),
                "user_id": user_id,
                "until_date": until_date,
                "revoke_messages": False,
            },
        )
    except Exception:
        logging.exception("Не удалось удалить пользователя из канала: user_id=%s", user_id)

    try:
        send_telegram_api_request(
            "unbanChatMember",
            {
                "chat_id": int(PRIVATE_CHANNEL_CHAT_ID),
                "user_id": user_id,
                "only_if_banned": True,
            },
        )
    except Exception:
        logging.exception("Не удалось снять временный бан после удаления: user_id=%s", user_id)


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
                access_invite_sent_at,
                access_granted_at,
                access_revoked_at,
                pending_access_invite_link
            FROM invoices
            WHERE {condition}
            LIMIT 1
        """

        if lava_invoice_id:
            cur.execute(select_sql.format(condition="lava_invoice_id = %s"), (lava_invoice_id,))
            row = cur.fetchone()
            if row:
                cur.close()
                return {
                    "id": row[0],
                    "telegram_user_id": row[1],
                    "status": row[2],
                    "access_invite_sent_at": row[3],
                    "access_granted_at": row[4],
                    "access_revoked_at": row[5],
                    "pending_access_invite_link": row[6],
                }

        if contract_id:
            cur.execute(select_sql.format(condition="contract_id = %s"), (contract_id,))
            row = cur.fetchone()
            if row:
                cur.close()
                return {
                    "id": row[0],
                    "telegram_user_id": row[1],
                    "status": row[2],
                    "access_invite_sent_at": row[3],
                    "access_granted_at": row[4],
                    "access_revoked_at": row[5],
                    "pending_access_invite_link": row[6],
                }

        if buyer_email:
            cur.execute(
                """
                SELECT
                    id,
                    telegram_user_id,
                    status,
                    access_invite_sent_at,
                    access_granted_at,
                    access_revoked_at,
                    pending_access_invite_link
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
                    "access_granted_at": row[4],
                    "access_revoked_at": row[5],
                    "pending_access_invite_link": row[6],
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
                access_revoked_at = NULL,
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


def mark_access_revoked(invoice_db_id: int) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE invoices
            SET access_revoked_at = NOW(),
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

    if invoice_row.get("access_granted_at"):
        logging.info("Access already granted for invoice db id=%s", invoice_row["id"])
        return

    if invoice_row.get("access_invite_sent_at") and invoice_row.get("pending_access_invite_link"):
        logging.info("Access join-request link already sent for invoice db id=%s", invoice_row["id"])
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


def revoke_access_if_needed(
    invoice_row: dict,
    event_type: Optional[str],
    status: Optional[str],
) -> None:
    if not is_failed_or_inactive_payment(event_type, status):
        return

    telegram_user_id = invoice_row.get("telegram_user_id")
    if not telegram_user_id:
        return

    has_any_access_state = bool(
        invoice_row.get("access_granted_at") or invoice_row.get("access_invite_sent_at")
    )
    if not has_any_access_state:
        logging.info(
            "Failure webhook received, but access was not granted yet: invoice_db_id=%s",
            invoice_row["id"],
        )
        return

    remove_member_from_private_channel(telegram_user_id)
    mark_access_revoked(invoice_row["id"])

    try:
        send_telegram_text(
            telegram_user_id,
            "Подписка не продлена.\n"
            "Доступ в канал остановлен.\n\n"
            "Чтобы вернуться, снова открой бота и оформи доступ заново.",
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


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    save_user(update)

    await update.message.reply_text(
        "Нажми кнопку ниже, чтобы открыть доступ.",
        reply_markup=build_start_keyboard(update.effective_user.id),
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
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message)
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


@app.get("/checkout", response_class=HTMLResponse)
def checkout_page(tg_user_id: int) -> str:
    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1.0">
        <title>Доступ в Точку опоры</title>
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
                max-width: 520px;
                background: #0b0b0c;
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 24px;
                padding: 28px 22px 22px;
                overflow: hidden;
                box-shadow: 0 18px 50px rgba(0, 0, 0, 0.45);
            }}

            .watermark {{
                position: absolute;
                top: 18px;
                left: 50%;
                transform: translateX(-50%);
                width: 160px;
                height: 160px;
                opacity: 0.12;
                pointer-events: none;
            }}

            .content {{
                position: relative;
                z-index: 1;
            }}

            .eyebrow {{
                margin: 96px 0 10px;
                text-align: center;
                font-size: 12px;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #9ca3af;
            }}

            h1 {{
                margin: 0 0 12px;
                text-align: center;
                font-size: 30px;
                line-height: 1.12;
                font-weight: 700;
            }}

            .subtitle {{
                margin: 0 0 24px;
                text-align: center;
                color: #b5b8be;
                font-size: 15px;
                line-height: 1.5;
            }}

            .step {{
                display: none;
            }}

            .step.active {{
                display: block;
            }}

            .stack {{
                display: grid;
                gap: 12px;
            }}

            .option-btn,
            .action-btn,
            .secondary-btn {{
                appearance: none;
                width: 100%;
                border: none;
                border-radius: 16px;
                cursor: pointer;
                transition: transform 0.15s ease, opacity 0.15s ease, border-color 0.15s ease;
                font-size: 16px;
                line-height: 1.25;
            }}

            .option-btn:hover,
            .action-btn:hover,
            .secondary-btn:hover {{
                transform: translateY(-1px);
            }}

            .option-btn {{
                text-align: left;
                padding: 18px 18px;
                background: #121316;
                color: #ffffff;
                border: 1px solid rgba(255, 255, 255, 0.08);
            }}

            .option-btn .title {{
                display: block;
                font-weight: 700;
                margin-bottom: 4px;
            }}

            .option-btn .desc {{
                display: block;
                color: #a1a1aa;
                font-size: 13px;
            }}

            .label {{
                display: block;
                margin-bottom: 8px;
                font-size: 14px;
                color: #d1d5db;
            }}

            .input {{
                width: 100%;
                padding: 16px 16px;
                border-radius: 16px;
                border: 1px solid rgba(255, 255, 255, 0.10);
                background: #101114;
                color: #ffffff;
                outline: none;
                font-size: 16px;
            }}

            .input:focus {{
                border-color: rgba(255, 255, 255, 0.25);
            }}

            .hint {{
                margin-top: 10px;
                color: #8f95a3;
                font-size: 13px;
                line-height: 1.45;
            }}

            .summary {{
                display: grid;
                gap: 10px;
                background: #101114;
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 18px;
                padding: 16px;
                margin-bottom: 18px;
            }}

            .summary-row {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
                font-size: 15px;
            }}

            .summary-label {{
                color: #9ca3af;
            }}

            .summary-value {{
                text-align: right;
                color: #ffffff;
                font-weight: 600;
                word-break: break-word;
            }}

            .action-btn {{
                padding: 16px 18px;
                background: #f3f4f6;
                color: #111111;
                font-weight: 700;
            }}

            .secondary-btn {{
                padding: 14px 18px;
                background: transparent;
                color: #cbd5e1;
                border: 1px solid rgba(255, 255, 255, 0.10);
                margin-top: 12px;
            }}

            .error {{
                min-height: 18px;
                margin-top: 10px;
                color: #fda4af;
                font-size: 13px;
                text-align: center;
            }}

            .footnote {{
                margin-top: 18px;
                text-align: center;
                color: #7d8593;
                font-size: 12px;
                line-height: 1.45;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <svg class="watermark" viewBox="0 0 200 200" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
                <circle cx="100" cy="100" r="10" fill="white"/>
                <circle
                    cx="100"
                    cy="100"
                    r="62"
                    stroke="white"
                    stroke-width="14"
                    stroke-linecap="round"
                    stroke-dasharray="72 26 72 26 72 26 72 26"
                    transform="rotate(-45 100 100)"
                />
            </svg>

            <div class="content">
                <div class="eyebrow">точка ясности</div>
                <h1>Доступ в Точку опоры</h1>
                <p class="subtitle">
                    Выбери способ оплаты. Дальше система сама переведёт тебя на нужный маршрут.
                </p>

                <div id="step-method" class="step active">
                    <div class="stack">
                        <button class="option-btn" onclick="chooseMethod('rf_card')">
                            <span class="title">Оплата картами РФ</span>
                            <span class="desc">Рублёвый маршрут для карт российских банков.</span>
                        </button>

                        <button class="option-btn" onclick="chooseMethod('foreign_card')">
                            <span class="title">Оплата любой другой картой</span>
                            <span class="desc">Маршрут для зарубежных карт с автоматической конвертацией.</span>
                        </button>

                        <button class="option-btn" onclick="chooseMethod('paypal')">
                            <span class="title">PayPal</span>
                            <span class="desc">Отдельный маршрут через PayPal.</span>
                        </button>
                    </div>
                </div>

                <div id="step-email" class="step">
                    <label class="label" for="email">Введите ваш email</label>
                    <input id="email" class="input" type="email" placeholder="name@example.com" autocomplete="email">
                    <div class="hint">
                        На этот email будет привязана оплата. После подтверждения бот пришлёт ссылку на вход в закрытый канал.
                    </div>
                    <div class="error" id="email-error"></div>
                    <button class="action-btn" style="margin-top: 18px;" onclick="goToConfirm()">Далее</button>
                    <button class="secondary-btn" onclick="showStep('step-method')">Назад</button>
                </div>

                <div id="step-confirm" class="step">
                    <div class="summary">
                        <div class="summary-row">
                            <div class="summary-label">Продукт</div>
                            <div class="summary-value">Доступ в Точку опоры</div>
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

                <div class="footnote">
                    Если после оплаты что-то пошло не так, просто вернись в Telegram и открой бота ещё раз.
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
                const pattern = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{{2,}}$/;
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
