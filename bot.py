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
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

URL = "https://www.roe.vsei.ua/disconnections/"

TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")
UPDATE_RE = re.compile(r"Оновлено:\s*\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}")

SITE_CHECK_EVERY_SECONDS = 300
NOTICE_MINUTES = 10
PREALERT_WINDOW_SECONDS = 120

KYIV_TZ = ZoneInfo("Europe/Kyiv")

SUBSCRIBERS_FILE = "subscribers.txt"

# ---------------- BOT ----------------

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

USER_SUBQUEUE = {}
USER_LAST_HASH = {}
USER_LAST_SCHEDULE = {}
USER_NOTIFIED_KEYS = {}

# ---------------- HELPERS ----------------

def now_kiev():
    return datetime.now(KYIV_TZ)

def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            return {int(x.strip()) for x in f if x.strip().isdigit()}
    except FileNotFoundError:
        return set()

def save_subscriber(chat_id: int):
    subs = load_subscribers()
    if chat_id in subs:
        return
    subs.add(chat_id)
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        for cid in sorted(subs):
            f.write(f"{cid}\n")

def keyboard_choose_subqueue():
    rows = []
    for i in range(1, 7):
        rows.append([
            InlineKeyboardButton(text=f"{i}.1", callback_data=f"sq:{i}.1"),
            InlineKeyboardButton(text=f"{i}.2", callback_data=f"sq:{i}.2"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def keyboard_manage():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Змінити підчергу", callback_data="change")],
        [InlineKeyboardButton(text="❌ Скасувати сповіщення", callback_data="stop")],
    ])

async def fetch_html():
    async with aiohttp.ClientSession() as session:
        async with session.get(URL, timeout=25) as r:
            r.raise_for_status()
            return await r.text()

def _dt_today(hhmm: str):
    hh, mm = hhmm.split(":")
    return now_kiev().replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)

# ---------------- PARSER ----------------

def parse_schedule_for_subqueue(html: str, subqueue: str):
    soup = BeautifulSoup(html, "lxml")
    full_text = soup.get_text("\n", strip=True)
    update_marker = UPDATE_RE.search(full_text)
    update_marker = update_marker.group(0) if update_marker else None

    table = None
    for t in soup.find_all("table"):
        if "Підчерга" in t.get_text(" ", strip=True):
            table = t
            break

    if not table:
        return update_marker, []

    rows = table.find_all("tr")
    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    if subqueue not in headers:
        return update_marker, []

    col = headers.index(subqueue)
    intervals = []

    for r in rows[1:]:
        cells = r.find_all(["td", "th"])
        if col >= len(cells):
            continue
        txt = cells[col].get_text(" ", strip=True)
        if "Очікується" in txt:
            continue
        for a, b in TIME_RANGE_RE.findall(txt):
            intervals.append((a, b))

    return update_marker, list(dict.fromkeys(intervals))

def schedule_hash(intervals):
    return hashlib.sha256("|".join(f"{a}-{b}" for a, b in intervals).encode()).hexdigest()

# ---------------- LOGIC ----------------

def is_off_now(intervals, now):
    for a, b in intervals:
        st = _dt_today(a)
        en = _dt_today(b)
        if st <= now <= en:
            return True
    return False

def next_event(intervals, now):
    for a, b in intervals:
        st = _dt_today(a)
        en = _dt_today(b)
        if st <= now <= en:
            return en, "ON"
    future = [_dt_today(a) for a, _ in intervals if _dt_today(a) > now]
    if future:
        return min(future), "OFF"
    return None, None

# ---------------- BACKGROUND ----------------

async def site_watcher_loop():
    while True:
        try:
            html = await fetch_html()
            for chat_id, subqueue in USER_SUBQUEUE.items():
                update, intervals = parse_schedule_for_subqueue(html, subqueue)
                h = schedule_hash(intervals)
                if USER_LAST_HASH.get(chat_id) != h:
                    USER_LAST_HASH[chat_id] = h
                    USER_LAST_SCHEDULE[chat_id] = intervals
                    USER_NOTIFIED_KEYS[chat_id] = set()
                    await bot.send_message(
                        chat_id,
                        f"🔄 Оновився графік по підчерзі {subqueue}",
                        reply_markup=keyboard_manage()
                    )
        except:
            pass
        await asyncio.sleep(SITE_CHECK_EVERY_SECONDS)

async def reminders_loop():
    while True:
        try:
            now = now_kiev()
            for chat_id, intervals in USER_LAST_SCHEDULE.items():
                ev_dt, ev_type = next_event(intervals, now)
                if not ev_dt:
                    continue
                notify_time = ev_dt - timedelta(minutes=NOTICE_MINUTES)
                if notify_time <= now < notify_time + timedelta(seconds=PREALERT_WINDOW_SECONDS):
                    key = f"{chat_id}|{ev_type}|{ev_dt}"
                    sent = USER_NOTIFIED_KEYS.setdefault(chat_id, set())
                    if key not in sent:
                        sent.add(key)
                        text = (
                            f"⛔️ За 10 хв очікується ВІДКЛЮЧЕННЯ ({ev_dt.strftime('%H:%M')})"
                            if ev_type == "OFF"
                            else f"💡 За 10 хв очікується ВІДНОВЛЕННЯ ({ev_dt.strftime('%H:%M')})"
                        )
                        await bot.send_message(chat_id, text, reply_markup=keyboard_manage())
        except:
            pass
        await asyncio.sleep(60)

# ---------------- COMMANDS ----------------

@dp.message(F.text == "/start")
async def start(message: Message):
    save_subscriber(message.chat.id)
    await message.answer("Оберіть вашу підчергу 👇", reply_markup=keyboard_choose_subqueue())

@dp.message(F.text.startswith("/broadcast"))
async def broadcast(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    text = message.text.replace("/broadcast", "", 1).strip()
    if not text:
        await message.answer("Формат: /broadcast текст")
        return
    ok = fail = 0
    for cid in load_subscribers():
        try:
            await bot.send_message(cid, text)
            ok += 1
        except:
            fail += 1
    await message.answer(f"Розіслано: ✅{ok} ❌{fail}")

@dp.callback_query(F.data.startswith("sq:"))
async def choose(cb: CallbackQuery):
    save_subscriber(cb.message.chat.id)
    USER_SUBQUEUE[cb.message.chat.id] = cb.data.split(":")[1]
    await cb.message.answer(f"✅ Підчерга {USER_SUBQUEUE[cb.message.chat.id]} обрана", reply_markup=keyboard_manage())

@dp.callback_query(F.data == "stop")
async def stop(cb: CallbackQuery):
    USER_SUBQUEUE.pop(cb.message.chat.id, None)
    await cb.message.answer("Сповіщення вимкнено")

# ---------------- MAIN ----------------

async def main():
    asyncio.create_task(site_watcher_loop())
    asyncio.create_task(reminders_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
