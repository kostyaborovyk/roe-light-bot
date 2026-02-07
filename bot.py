import asyncio
import os
import re
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ---------------- CONFIG ----------------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # твій Telegram user id (через @userinfobot)

URL = "https://www.roe.vsei.ua/disconnections/"

# інтервали на сайті — це години ВІДКЛЮЧЕННЯ
TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")
UPDATE_RE = re.compile(r"Оновлено:\s*\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}")

SITE_CHECK_EVERY_SECONDS = 300   # 5 хв
NOTICE_MINUTES = 10              # за 10 хв
PREALERT_WINDOW_SECONDS = 120    # 2 хв вікно

# Київський час (незалежно від UTC на Render)
KYIV_TZ = ZoneInfo("Europe/Kyiv")

# Broadcast subscribers
SUBSCRIBERS_FILE = "subscribers.txt"


def now_kiev() -> datetime:
    return datetime.now(KYIV_TZ)


def load_subscribers() -> set[int]:
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            return {int(line.strip()) for line in f if line.strip().isdigit()}
    except FileNotFoundError:
        return set()


def save_subscriber(chat_id: int) -> None:
    subs = load_subscribers()
    if chat_id in subs:
        return
    subs.add(chat_id)
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        for cid in sorted(subs):
            f.write(f"{cid}\n")


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# in-memory storage
USER_SUBQUEUE: dict[int, str] = {}
USER_LAST_HASH: dict[int, str] = {}
USER_LAST_SCHEDULE: dict[int, list[tuple[str, str]]] = {}
USER_NOTIFIED_KEYS: dict[int, set[str]] = {}

# ---------------- UI ----------------


def keyboard_choose_subqueue():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1.1", callback_data="sq:1.1"),
         InlineKeyboardButton(text="1.2", callback_data="sq:1.2")],
        [InlineKeyboardButton(text="2.1", callback_data="sq:2.1"),
         InlineKeyboardButton(text="2.2", callback_data="sq:2.2")],
        [InlineKeyboardButton(text="3.1", callback_data="sq:3.1"),
         InlineKeyboardButton(text="3.2", callback_data="sq:3.2")],
        [InlineKeyboardButton(text="4.1", callback_data="sq:4.1"),
         InlineKeyboardButton(text="4.2", callback_data="sq:4.2")],
        [InlineKeyboardButton(text="5.1", callback_data="sq:5.1"),
         InlineKeyboardButton(text="5.2", callback_data="sq:5.2")],
        [InlineKeyboardButton(text="6.1", callback_data="sq:6.1"),
         InlineKeyboardButton(text="6.2", callback_data="sq:6.2")],
    ])


def keyboard_manage():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Змінити підчергу", callback_data="change")],
        [InlineKeyboardButton(text="❌ Скасувати сповіщення", callback_data="stop")],
    ])

# ---------------- FETCH ----------------


async def fetch_html() -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(URL, timeout=25) as r:
            r.raise_for_status()
            return await r.text()


def _find_update_marker(full_text: str) -> str | None:
    m = UPDATE_RE.search(full_text)
    return m.group(0) if m else None

# ---------------- PARSE (robust table -> matrix) ----------------


def _html_table_to_matrix(table) -> list[list[str]]:
    """
    Перетворює HTML-таблицю в матрицю з урахуванням rowspan/colspan.
    """
    rows = table.find_all("tr")
    grid: list[list[str]] = []
    span_map: dict[tuple[int, int], dict] = {}

    for r_idx, tr in enumerate(rows):
        cells = tr.find_all(["th", "td"])
        grid_row: list[str] = []
        c_idx = 0

        def fill_spans_until_free():
            nonlocal c_idx
            while (r_idx, c_idx) in span_map:
                info = span_map[(r_idx, c_idx)]
                grid_row.append(info["text"])
                if info["rows_left"] > 1:
                    span_map[(r_idx + 1, c_idx)] = {"text": info["text"], "rows_left": info["rows_left"] - 1}
                del span_map[(r_idx, c_idx)]
                c_idx += 1

        for cell in cells:
            fill_spans_until_free()

            text = cell.get_text(" ", strip=True)
            rowspan = int(cell.get("rowspan", 1))
            colspan = int(cell.get("colspan", 1))

            for _ in range(colspan):
                grid_row.append(text)
                c_idx += 1

            if rowspan > 1:
                for col in range(c_idx - colspan, c_idx):
                    span_map[(r_idx + 1, col)] = {"text": text, "rows_left": rowspan - 1}

        fill_spans_until_free()
        grid.append(grid_row)

    max_cols = max((len(r) for r in grid), default=0)
    for r in grid:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))

    return grid


def parse_schedule_for_subqueue(html: str, subqueue: str) -> tuple[str | None, list[tuple[str, str]]]:
    """
    Повертає (update_marker, intervals) для підчерги.
    intervals — інтервали ВІДКЛЮЧЕННЯ.
    """
    soup = BeautifulSoup(html, "lxml")
    full_text = soup.get_text("\n", strip=True)
    update_marker = _find_update_marker(full_text)

    table = None
    for t in soup.find_all("table"):
        tt = t.get_text(" ", strip=True)
        if "Підчерга" in tt and "1.1" in tt and "6.2" in tt:
            table = t
            break

    if table is None:
        return update_marker, []

    matrix = _html_table_to_matrix(table)

    header_row_idx = None
    col_idx = None
    for r_i, row in enumerate(matrix):
        if any(subqueue == (cell or "").strip() for cell in row):
            header_row_idx = r_i
            col_idx = next((i for i, cell in enumerate(row) if (cell or "").strip() == subqueue), None)
            break

    if header_row_idx is None or col_idx is None:
        return update_marker, []

    intervals: list[tuple[str, str]] = []
    for r in matrix[header_row_idx + 1:]:
        cell_text = (r[col_idx] or "").strip()
        if not cell_text:
            continue
        if "Очікується" in cell_text:
            continue
        for a, b in TIME_RANGE_RE.findall(cell_text):
            intervals.append((a, b))

    # прибираємо дублікати, зберігаючи порядок
    uniq: list[tuple[str, str]] = []
    seen = set()
    for it in intervals:
        if it not in seen:
            uniq.append(it)
            seen.add(it)

    return update_marker, uniq


def schedule_hash(intervals: list[tuple[str, str]]) -> str:
    raw = "|".join([f"{a}-{b}" for a, b in intervals])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def format_schedule(subqueue: str, intervals: list[tuple[str, str]], update_marker: str | None) -> str:
    today = now_kiev().strftime("%d.%m.%Y")
    if not intervals:
        msg = (
            f"Графік (ВІДКЛЮЧЕННЯ) для {subqueue} на {today}:\n"
            f"⚠️ Інтервали не знайдено (можливо “Очікується” або змінилась таблиця)."
        )
    else:
        lines = [f"Графік (ВІДКЛЮЧЕННЯ) для {subqueue} на {today}:"]
        for a, b in intervals:
            lines.append(f"• {a}–{b}")
        msg = "\n".join(lines)

    if update_marker:
        msg += f"\n\n{update_marker}"
    return msg

# ---------------- TIME LOGIC ----------------


def _dt_today(hhmm: str) -> datetime:
    hh, mm = hhmm.split(":")
    now = now_kiev()
    return now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)


def is_off_now(intervals: list[tuple[str, str]], now: datetime) -> bool:
    for a, b in intervals:
        st = _dt_today(a)
        en = _dt_today(b)
        if b == "23:59":
            en = en.replace(second=59)
        if st <= now <= en:
            return True
    return False


def next_event(intervals: list[tuple[str, str]], now: datetime) -> tuple[datetime | None, str | None]:
    # якщо зараз у відключенні -> найближче ON (кінець інтервалу)
    for a, b in intervals:
        st = _dt_today(a)
        en = _dt_today(b)
        if b == "23:59":
            en = en.replace(second=59)
        if st <= now <= en:
            return en, "ON"

    # якщо зараз світло є -> найближче OFF (початок наступного інтервалу)
    future = []
    for a, _b in intervals:
        st = _dt_today(a)
        if st > now:
            future.append(st)

    if future:
        return min(future), "OFF"

    return None, None

# ---------------- LOOPS ----------------


async def site_watcher_loop():
    while True:
        try:
            if not USER_SUBQUEUE:
                await asyncio.sleep(SITE_CHECK_EVERY_SECONDS)
                continue

            html = await fetch_html()

            for chat_id, subqueue in list(USER_SUBQUEUE.items()):
                update_marker, intervals = parse_schedule_for_subqueue(html, subqueue)

                new_hash = schedule_hash(intervals)
                old_hash = USER_LAST_HASH.get(chat_id)

                USER_LAST_SCHEDULE[chat_id] = intervals

                if old_hash is not None and new_hash != old_hash:
                    USER_LAST_HASH[chat_id] = new_hash
                    USER_NOTIFIED_KEYS[chat_id] = set()

                    await bot.send_message(
                        chat_id,
                        f"🔄 Оновився графік по підчерзі {subqueue}\n\n{format_schedule(subqueue, intervals, update_marker)}",
                        reply_markup=keyboard_manage()
                    )

                if old_hash is None:
                    USER_LAST_HASH[chat_id] = new_hash
                    USER_NOTIFIED_KEYS.setdefault(chat_id, set())

        except Exception:
            pass

        await asyncio.sleep(SITE_CHECK_EVERY_SECONDS)


async def reminders_loop():
    while True:
        try:
            now = now_kiev()
            for chat_id, subqueue in list(USER_SUBQUEUE.items()):
                intervals = USER_LAST_SCHEDULE.get(chat_id, [])
                if not intervals:
                    continue

                notified = USER_NOTIFIED_KEYS.setdefault(chat_id, set())
                day_key = now.strftime("%Y-%m-%d")

                event_dt, event_type = next_event(intervals, now)
                if not event_dt or not event_type:
                    continue

                notify_time = event_dt - timedelta(minutes=NOTICE_MINUTES)

                if notify_time <= now < notify_time + timedelta(seconds=PREALERT_WINDOW_SECONDS):
                    key = f"{day_key}|{subqueue}|{event_type}|{event_dt.strftime('%H:%M')}"
                    if key not in notified:
                        notified.add(key)

                        if event_type == "OFF":
                            text = f"⛔️ За {NOTICE_MINUTES} хв очікується ВІДКЛЮЧЕННЯ світла (о {event_dt.strftime('%H:%M')})"
                        else:
                            text = f"💡 За {NOTICE_MINUTES} хв очікується ВІДНОВЛЕННЯ світла (о {event_dt.strftime('%H:%M')})"

                        await bot.send_message(chat_id, text, reply_markup=keyboard_manage())
        except Exception:
            pass

        await asyncio.sleep(60)

# ---------------- COMMANDS ----------------


@dp.message(F.text == "/start")
async def start(message: Message):
    save_subscriber(message.chat.id)
    await message.answer(
        "Оберіть вашу підчергу.\n"
        "👇 Натисни кнопку:",
        reply_markup=keyboard_choose_subqueue()
    )


@dp.message(F.text == "/schedule")
async def cmd_schedule(message: Message):
    save_subscriber(message.chat.id)
    chat_id = message.chat.id
    if chat_id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return
    subqueue = USER_SUBQUEUE[chat_id]
    intervals = USER_LAST_SCHEDULE.get(chat_id, [])
    await message.answer(format_schedule(subqueue, intervals, None), reply_markup=keyboard_manage())


@dp.message(F.text == "/status")
async def cmd_status(message: Message):
    save_subscriber(message.chat.id)
    chat_id = message.chat.id
    if chat_id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return

    subqueue = USER_SUBQUEUE[chat_id]
    intervals = USER_LAST_SCHEDULE.get(chat_id, [])
    if not intervals:
        await message.answer("Немає інтервалів (можливо 'Очікується').", reply_markup=keyboard_manage())
        return

    now = now_kiev()
    off = is_off_now(intervals, now)
    ev_dt, ev_type = next_event(intervals, now)

    txt = "❌ ЗАРАЗ ВІДКЛЮЧЕННЯ" if off else "✅ ЗАРАЗ Є СВІТЛО"
    tail = ""
    if ev_dt and ev_type:
        if ev_type == "OFF":
            tail = f"\nНайближче: відключення о {ev_dt.strftime('%H:%M')}"
        else:
            tail = f"\nНайближче: відновлення о {ev_dt.strftime('%H:%M')}"

    await message.answer(f"{txt}\nПідчерга: {subqueue}{tail}", reply_markup=keyboard_manage())


@dp.message(F.text == "/time")
async def cmd_time(message: Message):
    save_subscriber(message.chat.id)
    await message.answer(f"Server time: {now_kiev().strftime('%d.%m.%Y %H:%M:%S')}")


@dp.message(F.text.startswith("/broadcast"))
async def cmd_broadcast(message: Message):
    # тільки адмін
    if ADMIN_ID == 0 or message.from_user.id != ADMIN_ID:
        return

    text = message.text.replace("/broadcast", "", 1).strip()
    if not text:
        await message.answer("Формат: /broadcast твій текст")
        return

    subs = load_subscribers()
    ok = 0
    fail = 0

    for chat_id in subs:
        try:
            await bot.send_message(chat_id, text)
            ok += 1
        except Exception:
            fail += 1

    await message.answer(f"Розсилка: ✅{ok} ❌{fail}")

# ---------------- BUTTONS ----------------


@dp.callback_query(F.data == "change")
async def change_subqueue(cb: CallbackQuery):
    save_subscriber(cb.message.chat.id)
    await cb.answer()
    await cb.message.answer("Ок, обери нову підчергу 👇", reply_markup=keyboard_choose_subqueue())


@dp.callback_query(F.data == "stop")
async def stop_notifications(cb: CallbackQuery):
    save_subscriber(cb.message.chat.id)
    chat_id = cb.message.chat.id
    USER_SUBQUEUE.pop(chat_id, None)
    USER_LAST_HASH.pop(chat_id, None)
    USER_LAST_SCHEDULE.pop(chat_id, None)
    USER_NOTIFIED_KEYS.pop(chat_id, None)

    await cb.answer("Сповіщення вимкнено")
    await cb.message.answer("Сповіщення вимкнув ✅\nЩоб знову увімкнути — натисни /start")


@dp.callback_query(F.data.startswith("sq:"))
async def choose(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    save_subscriber(chat_id)

    subqueue = cb.data.split(":", 1)[1]
    USER_SUBQUEUE[chat_id] = subqueue
    USER_NOTIFIED_KEYS.setdefault(chat_id, set())
    await cb.answer()

    try:
        html = await fetch_html()
        update_marker, intervals = parse_schedule_for_subqueue(html, subqueue)

        USER_LAST_HASH[chat_id] = schedule_hash(intervals)
        USER_LAST_SCHEDULE[chat_id] = intervals
        USER_NOTIFIED_KEYS[chat_id] = set()

        text = f"✅ Ви обрали підчергу {subqueue}\n\n{format_schedule(subqueue, intervals, update_marker)}"
    except Exception:
        text = (
            f"✅ Ви обрали підчергу {subqueue}\n\n"
            "⚠️ Не зміг зараз отримати графік із сайту. Спробуй ще раз через хвилину."
        )

    await cb.message.answer(text, reply_markup=keyboard_manage())

# ---------------- TESTS ----------------


@dp.message(F.text == "/test_off")
async def test_off(message: Message):
    if message.chat.id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return
    await message.answer("⛔️ За 10 хв очікується ВІДКЛЮЧЕННЯ світла (тест)", reply_markup=keyboard_manage())


@dp.message(F.text == "/test_on")
async def test_on(message: Message):
    if message.chat.id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return
    await message.answer("💡 За 10 хв очікується ВІДНОВЛЕННЯ світла (тест)", reply_markup=keyboard_manage())


# ---------------- MAIN ----------------


async def main():
    asyncio.create_task(site_watcher_loop())
    asyncio.create_task(reminders_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
