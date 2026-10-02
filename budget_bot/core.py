"""Budget rules and durable, idempotent journal writes (no network imports)."""
import json
import re
import sqlite3
import threading
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

CATEGORIES = ["Оренда (з комуналкою)", "Розстрочки", "Зв'язок / інтернет",
              "Підписки", "Їжа / продукти", "Кафе / доставка", "Розваги / дозвілля",
              "Транспорт", "Гігієна / побут", "Здоров'я / ліки", "Одяг", "Масаж",
              "Подарунки", "Благодійність", "Інше"]
ACCOUNTS = ["Монобанк картка", "Монобанк банка", "ПриватБанк зп", "ПриватБанк кр",
            "Пумб", "Сільпо банк", "Готівка", "Інше"]
HEADER = ["Дата", "Тип", "Категорія", "Опис", "Сума, грн", "Рахунок", "Куди (для переказу)"]


def amount(text):
    text = re.sub(r"\s", "", text).replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d{1,2})?", text):
        raise ValueError("Введи суму, наприклад 125 або 125,50 — без знака мінус.")
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise ValueError("Не вдалося прочитати суму.") from None
    if not Decimal("0") < value <= Decimal("10000000"):
        raise ValueError("Сума має бути більшою за нуль і не більшою за 10 млн грн.")
    return str(value.quantize(Decimal("0.01")))


def journal_row(draft, year, month):
    day = date.fromisoformat(draft["date"])
    if (day.year, day.month) != (year, month):
        raise ValueError("Ця дата не належить місяцю підключеної таблиці.")
    kind = draft["kind"]
    if kind not in ("Витрата", "Надходження", "Переказ"):
        raise ValueError("Невідомий тип операції.")
    account, target = draft["account"], draft.get("target", "")
    category = draft.get("category", "")
    if account not in ACCOUNTS:
        raise ValueError("Невідомий рахунок.")
    if kind == "Витрата" and category not in CATEGORIES:
        raise ValueError("Обери категорію витрат.")
    if kind == "Переказ" and (target not in ACCOUNTS or target == account):
        raise ValueError("Для переказу обери інший рахунок отримувача.")
    description = draft.get("description", "").strip()
    if len(description) > 300:
        raise ValueError("Опис — до 300 символів.")
    return [(day - date(1899, 12, 30)).days, kind,
            category if kind == "Витрата" else "", description,
            float(amount(draft["amount"])), account, target if kind == "Переказ" else ""]


def reminder_due(now, last_sent):
    """Catch up only within two hours; wall time follows Kyiv DST."""
    slots = [("morning", 9, 1), ("evening", 23, 0)]
    for name, hour, minute in slots:
        scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        # Evening's window includes midnight after a brief restart.
        if name == "evening" and now.hour < 1:
            scheduled -= timedelta(days=1)
        key = f"{scheduled.date()}:{name}"
        if timedelta(0) <= now - scheduled < timedelta(hours=2) and key not in last_sent:
            yield key, name, scheduled.date()


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.local = threading.local()
        self.connections = []
        self.conn.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS drafts (owner INTEGER PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS operations (
          id TEXT PRIMARY KEY, payload TEXT NOT NULL, sheet TEXT NOT NULL,
          row_number INTEGER, state TEXT NOT NULL DEFAULT 'pending');
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS reminders (key TEXT PRIMARY KEY);
        """)

    @property
    def conn(self):
        # Sheets work runs off the event loop. Use an independent SQLite connection
        # per thread so a draft write cannot commit another thread's transaction.
        if not hasattr(self.local, 'conn'):
            connection = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
            connection.row_factory = sqlite3.Row
            self.local.conn = connection
            self.connections.append(connection)
        return self.local.conn

    def close(self):
        for connection in self.connections:
            connection.close()

    def draft(self, owner):
        row = self.conn.execute("SELECT payload FROM drafts WHERE owner=?", (owner,)).fetchone()
        return json.loads(row[0]) if row else None

    def save_draft(self, owner, data):
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO drafts VALUES (?,?)", (owner, json.dumps(data)))

    def clear_draft(self, owner):
        with self.conn:
            self.conn.execute("DELETE FROM drafts WHERE owner=?", (owner,))

    def setting(self, key, default=""):
        row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_setting(self, key, value):
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, str(value)))

    def enqueue(self, token, payload, sheet):
        existing = self.conn.execute("SELECT * FROM operations WHERE id=?", (token,)).fetchone()
        if existing and (json.loads(existing["payload"]) != payload or existing["sheet"] != sheet):
            raise ValueError("Це підтвердження вже належить іншому запису.")
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO operations(id,payload,sheet) VALUES (?,?,?)",
                              (token, json.dumps(payload), sheet))

    def pending(self):
        return self.conn.execute("SELECT id FROM operations WHERE state='pending' ORDER BY rowid").fetchall()

    def commit(self, token, sheets):
        op = self.conn.execute("SELECT * FROM operations WHERE id=?", (token,)).fetchone()
        if not op:
            raise ValueError("Операцію не знайдено.")
        if op["state"] == "done":
            return op["row_number"]
        if op["sheet"] != sheets.spreadsheet_id:
            raise ValueError("Є незавершена операція для іншої таблиці. Спершу заверши її.")
        payload = json.loads(op["payload"])
        number = op["row_number"]
        rows = sheets.journal()
        # Operation markers survive a lost response and row moves in the Sheet.
        for index, (values, marker) in enumerate(rows, 5):
            if marker == token:
                if values != payload:
                    raise ValueError("Запис у таблиці змінено вручну. Перевір його перед повторенням.")
                with self.conn:
                    self.conn.execute("UPDATE operations SET row_number=?,state='done' WHERE id=?", (index, token))
                return index
        if number is None:
            reserved = {r[0] for r in self.conn.execute(
                "SELECT row_number FROM operations WHERE state='pending' AND sheet=? AND row_number IS NOT NULL",
                (sheets.spreadsheet_id,))}
            number = next((i for i, (values, marker) in enumerate(rows, 5)
                           if not any(v != "" for v in values) and not marker and i not in reserved), None)
            if number is None:
                raise ValueError("Журнал до рядка 308 заповнений. Потрібно розширити формули й валідації.")
            with self.conn:
                self.conn.execute("UPDATE operations SET row_number=? WHERE id=?", (number, token))
        values, marker = sheets.row(number)
        if any(v != "" for v in values) or marker:
            raise ValueError("Рядок зайнятий іншим записом. Нічого не перезаписано.")
        sheets.write(number, payload, token)
        values, marker = sheets.row(number)
        if values != payload or marker != token:
            raise RuntimeError("Запис ще не підтверджений Google. Повторна перевірка буде автоматичною.")
        with self.conn:
            self.conn.execute("UPDATE operations SET state='done' WHERE id=?", (token,))
        return number


def new_draft(kind, day):
    return {"id": uuid.uuid4().hex, "kind": kind, "date": day.isoformat(), "step": "amount"}
