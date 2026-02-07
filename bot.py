import asyncio
import os
import re
import hashlib
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

URL = "https://www.roe.vsei.ua/disconnections/"
TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")

SITE_CHECK_EVERY_SECONDS = 300   # 5 хв
NOTICE_MINUTES = 10              # за 10 хв

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

USER_SUBQUEUE: dict[int, str] = {}
USER_LAST_HASH: dict[int, str] = {}
USER_LAST_SCHEDULE: dict[int, list[tuple[str, str]]] = {}
USER_NOTIFIED_KEYS: dict[int, set[str]] = {}


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


async def fetch_html() -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(URL, timeout=25) as r:
            r.raise_for_status()
            return await r.text()


def _find_update_marker(full_text: str) -> str | None:
    m = re.search(r"Оновлено:\s*\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}", full_text)
    return m.group(0) if m else None


def _html_table_to_matrix(table) -> list[list[str]]:
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

    max_cols = max(len(r) for r in grid) if grid else 0
    for r in grid:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))

    return grid


def parse_schedule_for_subqueue(html: str, subqueue: str) -> tuple[str | None, list[tuple[str, str]]]:
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
        for a, b in TIME_RANGE_RE.findall(cell_text):
            intervals.append((a, b))

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
    today = datetime.now().strftime("%d.%m.%Y")
    if not intervals:
        msg = (
            f"Графік для {subqueue} на {today}:\n"
            f"⚠️ Інтервали не знайдено (можливо на сайті ще “Очікується” або змінилась таблиця)."
        )
    else:
        lines = [f"Графік для {subqueue} на {today}:"]
        for a, b in intervals:
            lines.append(f"• {a}–{b}")
        msg = "\n".join(lines)

    if update_marker:
        msg += f"\n\n{update_marker}"
    return msg


def _dt_today(hhmm: str) -> datetime:
    hh, mm = hhmm.split(":")
    now = datetime.now()
    return now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)


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
            now = datetime.now()
            for chat_id, subqueue in list(USER_SUBQUEUE.items()):
                intervals = USER_LAST_SCHEDULE.get(chat_id, [])
                if not intervals:
                    continue

                notified = USER_NOTIFIED_KEYS.setdefault(chat_id, set())
                day_key = now.strftime("%Y-%m-%d")

                for a, b in intervals:
                    start_dt = _dt_today(a)
                    end_dt = _dt_today(b)

                    off_notify_time = start_dt - timedelta(minutes=NOTICE_MINUTES)
                    on_notify_time = end_dt - timedelta(minutes=NOTICE_MINUTES)

                    if off_notify_time <= now < off_notify_time + timedelta(seconds=60):
                        key = f"{day_key}|{subqueue}|OFF|{a}"
                        if key not in notified:
                            notified.add(key)
                            await bot.send_message(
                                chat_id,
                                "За 10 хвилин можливе відключення світла",
                                reply_markup=keyboard_manage()
                            )

                    if on_notify_time <= now < on_notify_time + timedelta(seconds=60):
                        key = f"{day_key}|{subqueue}|ON|{b}"
                        if key not in notified:
                            notified.add(key)
                            await bot.send_message(
                                chat_id,
                                "За 10 хвилин очікується відновлення світла",
                                reply_markup=keyboard_manage()
                            )
        except Exception:
            pass

        await asyncio.sleep(60)


@dp.message(F.text == "/start")
async def start(message: Message):
    await message.answer(
        "Оберіть вашу підчергу.\n"
        "Де дізнатись підчергу:\n"
        "https://www.roe.vsei.ua/disconnections/\n\n"
        "👇 Натисни кнопку:",
        reply_markup=keyboard_choose_subqueue()
    )


@dp.callback_query(F.data == "change")
async def change_subqueue(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer("Ок, обери нову підчергу 👇", reply_markup=keyboard_choose_subqueue())


@dp.callback_query(F.data == "stop")
async def stop_notifications(cb: CallbackQuery):
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


# --- ТЕСТИ ---

@dp.message(F.text == "/test_off")
async def test_off(message: Message):
    if message.chat.id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return
    await message.answer("За 10 хвилин можливе відключення світла", reply_markup=keyboard_manage())


@dp.message(F.text == "/test_on")
async def test_on(message: Message):
    if message.chat.id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return
    await message.answer("За 10 хвилин очікується відновлення світла", reply_markup=keyboard_manage())


@dp.message(F.text == "/test_update")
async def test_update(message: Message):
    """
    Симуляція "оновився графік": показує, як виглядатиме push при зміні інтервалів.
    """
    chat_id = message.chat.id
    if chat_id not in USER_SUBQUEUE:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return

    subqueue = USER_SUBQUEUE[chat_id]

    # умовний "новий графік" (для демонстрації)
    demo_intervals = [("06:00", "13:00"), ("15:00", "21:00"), ("23:00", "23:59")]
    demo_marker = f"Оновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}"

    await message.answer(
        f"🔄 Оновився графік по підчерзі {subqueue}\n\n{format_schedule(subqueue, demo_intervals, demo_marker)}",
        reply_markup=keyboard_manage()
    )


async def main():
    asyncio.create_task(site_watcher_loop())
    asyncio.create_task(reminders_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
