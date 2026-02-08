import asyncio
import os
import re
import json
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")

URL = "https://www.roe.vsei.ua/disconnections/"

TZ = ZoneInfo("Europe/Kyiv")

SITE_CHECK_EVERY_SECONDS = 300          # 5 хв
PREALERT_WINDOW_SECONDS = 120           # 2 хв вікно
DEFAULT_NOTICE_MINUTES = 10
ALLOWED_NOTICE = {5, 10, 30}

STATE_FILE = "state.json"               # json state

# Парсинг
TIME_RANGE_RE = re.compile(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})")
UPDATE_RE = re.compile(r"Оновлено:\s*\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}")
DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")

# =========================
# BOT INIT
# =========================
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add BOT_TOKEN to environment variables.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =========================
# IN-MEMORY STATE
# =========================
USER_SUBQUEUE: dict[int, str] = {}  # chat_id -> subqueue
USER_NOTICE: dict[int, int] = {}    # chat_id -> notice minutes

USER_LAST_HASH: dict[int, str] = {}  # chat_id -> hash schedule
USER_LAST_SCHEDULE: dict[int, dict[str, list[tuple[str, str]]]] = {}  # chat_id -> {date: [(a,b)]}
USER_LAST_UPDATE_MARKER: dict[int, str | None] = {}  # chat_id -> "Оновлено: ..."

USER_NOTIFIED_KEYS: dict[int, set[str]] = {}  # chat_id -> set(keys)
ALL_USERS: set[int] = set()                   # known users for broadcast/stats

# cached last global parse
_last_global_schedules: dict[str, dict[str, list[tuple[str, str]]]] = {}
_last_global_update_marker: str | None = None


# =========================
# PERSISTENCE
# =========================
def load_state() -> None:
    global USER_SUBQUEUE, USER_NOTICE, ALL_USERS
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        users = data.get("users", {})
        for chat_id_str, u in users.items():
            try:
                cid = int(chat_id_str)
            except ValueError:
                continue

            ALL_USERS.add(cid)

            sq = (u.get("subqueue") or "").strip()
            if sq:
                USER_SUBQUEUE[cid] = sq

            notice = u.get("notice")
            if isinstance(notice, int) and notice in ALLOWED_NOTICE:
                USER_NOTICE[cid] = notice
            else:
                USER_NOTICE.setdefault(cid, DEFAULT_NOTICE_MINUTES)

    except Exception as e:
        print(f"[STATE] load_state failed: {e}")


def save_state() -> None:
    try:
        users_obj: dict[str, dict] = {}
        for cid in ALL_USERS:
            users_obj[str(cid)] = {
                "subqueue": USER_SUBQUEUE.get(cid),
                "notice": USER_NOTICE.get(cid, DEFAULT_NOTICE_MINUTES),
            }
        data = {"users": users_obj}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE] save_state failed: {e}")


def register_user(chat_id: int) -> None:
    ALL_USERS.add(chat_id)
    USER_NOTICE.setdefault(chat_id, DEFAULT_NOTICE_MINUTES)
    save_state()


# =========================
# UI (KEYBOARDS)
# =========================
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


def keyboard_main():
    # кнопки керування (без /next та /schedule)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Поточний стан", callback_data="main:status")],
        [InlineKeyboardButton(text="🔔 Налаштувати сповіщення", callback_data="main:notice")],
        [InlineKeyboardButton(text="🔁 Змінити підчергу", callback_data="main:change"),
         InlineKeyboardButton(text="❌ Вимкнути сповіщення", callback_data="main:stop")],
    ])


def keyboard_notice(cur: int | None = None):
    def btn(val: int):
        mark = " ✅" if cur == val else ""
        return InlineKeyboardButton(text=f"⏱ {val} хв{mark}", callback_data=f"notice:{val}")

    return InlineKeyboardMarkup(inline_keyboard=[
        [btn(5), btn(10), btn(30)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main:back")],
    ])


async def send_main_menu(chat_id: int, text: str):
    await bot.send_message(chat_id, text, reply_markup=keyboard_main())


# =========================
# HTTP / PARSING
# =========================
async def fetch_html() -> str:
    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(URL) as r:
            r.raise_for_status()
            return await r.text()


def _find_update_marker(full_text: str) -> str | None:
    m = UPDATE_RE.search(full_text)
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

    max_cols = max((len(r) for r in grid), default=0)
    for r in grid:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))

    return grid


def _parse_date_from_row(row: list[str]) -> str | None:
    for cell in row:
        if not cell:
            continue
        m = DATE_RE.search(cell)
        if m:
            return m.group(1)
    return None


def parse_all_schedules(html: str) -> tuple[str | None, dict[str, dict[str, list[tuple[str, str]]]]]:
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
        return update_marker, {}

    matrix = _html_table_to_matrix(table)
    if not matrix:
        return update_marker, {}

    subqueues = [f"{i}.{j}" for i in range(1, 7) for j in (1, 2)]
    col_map: dict[str, int] = {}
    header_row_idx = None

    # шукаємо рядок з підчергами
    for r_i, row in enumerate(matrix):
        found = []
        for sq in subqueues:
            for c_i, cell in enumerate(row):
                if (cell or "").strip() == sq:
                    found.append((sq, c_i))
        if len(found) >= 6:
            header_row_idx = r_i
            for sq, c_i in found:
                col_map[sq] = c_i
            break

    if header_row_idx is None or not col_map:
        return update_marker, {}

    schedules: dict[str, dict[str, list[tuple[str, str]]]] = {sq: {} for sq in col_map.keys()}

    current_date: str | None = None
    for row in matrix[header_row_idx + 1:]:
        row_date = _parse_date_from_row(row)
        if row_date:
            current_date = row_date

        if not current_date:
            continue

        for sq, c_i in col_map.items():
            if c_i >= len(row):
                continue
            cell_text = (row[c_i] or "").strip()
            if not cell_text:
                continue
            if "Очікується" in cell_text:
                continue

            intervals = TIME_RANGE_RE.findall(cell_text)
            if not intervals:
                continue

            day_map = schedules[sq].setdefault(current_date, [])
            for a, b in intervals:
                day_map.append((a, b))

    # дедуп по днях
    for sq, day_map in schedules.items():
        for d, intervals in list(day_map.items()):
            uniq = []
            seen = set()
            for it in intervals:
                if it not in seen:
                    uniq.append(it)
                    seen.add(it)
            day_map[d] = uniq

    schedules = {sq: dm for sq, dm in schedules.items() if any(dm.values())}
    return update_marker, schedules


def _date_sort_key(d: str) -> tuple[int, int, int]:
    try:
        dd, mm, yy = d.split(".")
        return (int(yy), int(mm), int(dd))
    except Exception:
        return (9999, 99, 99)


def schedule_hash(schedule_by_day: dict[str, list[tuple[str, str]]]) -> str:
    parts = []
    for d in sorted(schedule_by_day.keys(), key=_date_sort_key):
        parts.append(d)
        for a, b in schedule_by_day[d]:
            parts.append(f"{a}-{b}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def format_schedule_all_days(subqueue: str, schedule_by_day: dict[str, list[tuple[str, str]]], update_marker: str | None) -> str:
    if not schedule_by_day:
        msg = (
            f"Графік (ВІДКЛЮЧЕННЯ) для {subqueue}:\n"
            f"⚠️ Інтервали не знайдено (можливо “Очікується” або змінилась таблиця)."
        )
        if update_marker:
            msg += f"\n\n{update_marker}"
        return msg

    lines = []
    for d in sorted(schedule_by_day.keys(), key=_date_sort_key):
        lines.append(f"{d} (ВІДКЛЮЧЕННЯ):")
        for a, b in schedule_by_day[d]:
            lines.append(f"• {a}–{b}")
        lines.append("")

    msg = "\n".join(lines).strip()
    if update_marker:
        msg += f"\n\n{update_marker}"
    return msg


def _dt_for_date(d_str: str, hhmm: str) -> datetime:
    dd, mm, yy = d_str.split(".")
    hh, mn = hhmm.split(":")
    return datetime(int(yy), int(mm), int(dd), int(hh), int(mn), 0, tzinfo=TZ)


def _interval_end_dt(d_str: str, hhmm: str) -> datetime:
    dt = _dt_for_date(d_str, hhmm)
    if hhmm == "23:59":
        dt = dt.replace(second=59)
    return dt


def is_off_now(schedule_by_day: dict[str, list[tuple[str, str]]], now: datetime) -> bool:
    today_str = now.strftime("%d.%m.%Y")
    intervals = schedule_by_day.get(today_str, [])
    for a, b in intervals:
        st = _dt_for_date(today_str, a)
        en = _interval_end_dt(today_str, b)
        if st <= now <= en:
            return True
    return False


def next_event(schedule_by_day: dict[str, list[tuple[str, str]]], now: datetime) -> tuple[datetime | None, str | None]:
    today_str = now.strftime("%d.%m.%Y")

    # якщо зараз OFF - найближче ON (кінець поточного інтервалу)
    today_intervals = schedule_by_day.get(today_str, [])
    for a, b in today_intervals:
        st = _dt_for_date(today_str, a)
        en = _interval_end_dt(today_str, b)
        if st <= now <= en:
            return en, "ON"

    # інакше шукаємо найближчий старт OFF у майбутньому (по всіх днях)
    candidates: list[datetime] = []
    for d in schedule_by_day.keys():
        for a, _b in schedule_by_day[d]:
            st = _dt_for_date(d, a)
            if st > now:
                candidates.append(st)

    if candidates:
        return min(candidates), "OFF"

    return None, None


# =========================
# LOOPS
# =========================
async def process_site_once(send_updates: bool = True) -> None:
    global _last_global_schedules, _last_global_update_marker

    html = await fetch_html()
    update_marker, schedules_all = parse_all_schedules(html)

    _last_global_schedules = schedules_all
    _last_global_update_marker = update_marker

    for chat_id, subqueue in list(USER_SUBQUEUE.items()):
        schedule_by_day = schedules_all.get(subqueue, {})
        USER_LAST_SCHEDULE[chat_id] = schedule_by_day
        USER_LAST_UPDATE_MARKER[chat_id] = update_marker

        new_hash = schedule_hash(schedule_by_day)
        old_hash = USER_LAST_HASH.get(chat_id)

        if old_hash is None:
            USER_LAST_HASH[chat_id] = new_hash
            USER_NOTIFIED_KEYS.setdefault(chat_id, set())
            continue

        if send_updates and new_hash != old_hash:
            USER_LAST_HASH[chat_id] = new_hash
            USER_NOTIFIED_KEYS[chat_id] = set()

            text = (
                f"🔄 Оновився графік по підчерзі {subqueue}\n\n"
                f"{format_schedule_all_days(subqueue, schedule_by_day, update_marker)}"
            )
            await send_main_menu(chat_id, text)


async def site_watcher_loop():
    while True:
        try:
            if USER_SUBQUEUE:
                await process_site_once(send_updates=True)
        except Exception as e:
            print(f"[WATCHER] loop error: {e}")
        await asyncio.sleep(SITE_CHECK_EVERY_SECONDS)


async def reminders_loop():
    while True:
        try:
            now = datetime.now(TZ)

            for chat_id, subqueue in list(USER_SUBQUEUE.items()):
                schedule_by_day = USER_LAST_SCHEDULE.get(chat_id) or _last_global_schedules.get(subqueue, {})
                if not schedule_by_day:
                    continue

                notice = USER_NOTICE.get(chat_id, DEFAULT_NOTICE_MINUTES)
                if notice not in ALLOWED_NOTICE:
                    notice = DEFAULT_NOTICE_MINUTES
                    USER_NOTICE[chat_id] = notice

                notified = USER_NOTIFIED_KEYS.setdefault(chat_id, set())
                day_key = now.strftime("%Y-%m-%d")

                event_dt, event_type = next_event(schedule_by_day, now)
                if not event_dt or not event_type:
                    continue

                notify_time = event_dt - timedelta(minutes=notice)

                if notify_time <= now < notify_time + timedelta(seconds=PREALERT_WINDOW_SECONDS):
                    key = f"{day_key}|{subqueue}|{event_type}|{event_dt.isoformat()}|{notice}"
                    if key in notified:
                        continue
                    notified.add(key)

                    hhmm = event_dt.strftime("%H:%M")
                    if event_type == "OFF":
                        text = f"⛔️ За {notice} хвилин можливе відключення світла (о {hhmm})"
                    else:
                        text = f"💡 За {notice} хвилин очікується відновлення світла (о {hhmm})"

                    await bot.send_message(chat_id, text)

        except Exception as e:
            print(f"[REMINDERS] loop error: {e}")

        await asyncio.sleep(60)


# =========================
# USER COMMANDS
# =========================
@dp.message(F.text == "/start")
async def start(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)

    await message.answer(
        "Оберіть вашу підчергу.\n"
        "👇 Натисни кнопку:",
        reply_markup=keyboard_choose_subqueue()
    )


@dp.message(F.text == "/status")
async def cmd_status(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)
    text = build_status_text(chat_id)
    await send_main_menu(chat_id, text)


@dp.message(F.text == "/notice")
async def cmd_notice(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)

    cur = USER_NOTICE.get(chat_id, DEFAULT_NOTICE_MINUTES)
    await message.answer(
        f"Оберіть за скільки хвилин попереджати.\nПоточне: {cur} хв",
        reply_markup=keyboard_notice(cur)
    )


@dp.message(F.text == "/change")
async def cmd_change(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)
    await message.answer("Ок, обери нову підчергу 👇", reply_markup=keyboard_choose_subqueue())


@dp.message(F.text == "/stop")
async def cmd_stop(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)

    USER_SUBQUEUE.pop(chat_id, None)
    USER_LAST_HASH.pop(chat_id, None)
    USER_LAST_SCHEDULE.pop(chat_id, None)
    USER_LAST_UPDATE_MARKER.pop(chat_id, None)
    USER_NOTIFIED_KEYS.pop(chat_id, None)

    save_state()
    await message.answer("Сповіщення вимкнув ✅\nЩоб знову увімкнути — натисни /start")


# залишаємо технічні команди (без кнопок)
@dp.message(F.text == "/schedule")
async def cmd_schedule(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)

    subqueue = USER_SUBQUEUE.get(chat_id)
    if not subqueue:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return

    schedule_by_day = USER_LAST_SCHEDULE.get(chat_id) or _last_global_schedules.get(subqueue, {})
    update_marker = USER_LAST_UPDATE_MARKER.get(chat_id) or _last_global_update_marker
    await send_main_menu(chat_id, format_schedule_all_days(subqueue, schedule_by_day, update_marker))


@dp.message(F.text == "/next")
async def cmd_next(message: Message):
    chat_id = message.chat.id
    register_user(chat_id)

    subqueue = USER_SUBQUEUE.get(chat_id)
    if not subqueue:
        await message.answer("⚠️ Спочатку обери підчергу через /start")
        return

    schedule_by_day = USER_LAST_SCHEDULE.get(chat_id) or _last_global_schedules.get(subqueue, {})
    if not schedule_by_day:
        await message.answer("⚠️ Немає даних по графіку (можливо 'Очікується').")
        return

    now = datetime.now(TZ)
    ev_dt, ev_type = next_event(schedule_by_day, now)
    if not ev_dt or not ev_type:
        await message.answer("⚠️ Немає наступних подій у доступному графіку.")
        return

    hhmm = ev_dt.strftime("%H:%M")
    dstr = ev_dt.strftime("%d.%m.%Y")
    if ev_type == "OFF":
        await send_main_menu(chat_id, f"⛔️ Наступне відключення: {dstr} о {hhmm}")
    else:
        await send_main_menu(chat_id, f"💡 Наступне відновлення: {dstr} о {hhmm}")


def build_status_text(chat_id: int) -> str:
    subqueue = USER_SUBQUEUE.get(chat_id)
    if not subqueue:
        return "⚠️ Спочатку обери підчергу через /start"

    schedule_by_day = USER_LAST_SCHEDULE.get(chat_id) or _last_global_schedules.get(subqueue, {})
    if not schedule_by_day:
        return "⚠️ Зараз немає даних по графіку (можливо 'Очікується'). Спробуй /schedule пізніше."

    now = datetime.now(TZ)
    off = is_off_now(schedule_by_day, now)
    ev_dt, ev_type = next_event(schedule_by_day, now)

    txt = "❌ ЗАРАЗ ВІДКЛЮЧЕННЯ" if off else "✅ ЗАРАЗ Є СВІТЛО"
    tail = ""
    if ev_dt and ev_type:
        hhmm = ev_dt.strftime("%H:%M")
        if ev_type == "OFF":
            tail = f"\nНайближче: відключення о {hhmm}"
        else:
            tail = f"\nНайближче: відновлення о {hhmm}"

    return f"{txt}\nПідчерга: {subqueue}{tail}"


# =========================
# CALLBACKS
# =========================
@dp.callback_query(F.data.startswith("sq:"))
async def choose_subqueue(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    register_user(chat_id)

    subqueue = cb.data.split(":", 1)[1].strip()
    USER_SUBQUEUE[chat_id] = subqueue
    USER_NOTIFIED_KEYS.setdefault(chat_id, set())
    save_state()

    await cb.answer()

    try:
        # оновити кеш з сайту (без пушів)
        await process_site_once(send_updates=False)

        schedule_by_day = _last_global_schedules.get(subqueue, {})
        update_marker = _last_global_update_marker

        USER_LAST_SCHEDULE[chat_id] = schedule_by_day
        USER_LAST_UPDATE_MARKER[chat_id] = update_marker
        USER_LAST_HASH[chat_id] = schedule_hash(schedule_by_day)
        USER_NOTIFIED_KEYS[chat_id] = set()

        notice = USER_NOTICE.get(chat_id, DEFAULT_NOTICE_MINUTES)

        text = (
            f"✅ Підчерга {subqueue} обрана\n"
            f"⏱ Попередження: за {notice} хв\n\n"
            f"{format_schedule_all_days(subqueue, schedule_by_day, update_marker)}"
        )
        await send_main_menu(chat_id, text)

    except Exception as e:
        print(f"[CHOOSE] failed: {e}")
        await send_main_menu(
            chat_id,
            f"✅ Підчерга {subqueue} обрана\n\n⚠️ Не зміг зараз отримати графік із сайту. Спробуй ще раз через хвилину."
        )


@dp.callback_query(F.data.startswith("main:"))
async def main_buttons(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    register_user(chat_id)
    action = cb.data.split(":", 1)[1]
    await cb.answer()

    if action == "status":
        await send_main_menu(chat_id, build_status_text(chat_id))

    elif action == "notice":
        cur = USER_NOTICE.get(chat_id, DEFAULT_NOTICE_MINUTES)
        await cb.message.answer(
            f"Оберіть за скільки хвилин попереджати.\nПоточне: {cur} хв",
            reply_markup=keyboard_notice(cur)
        )

    elif action == "change":
        await cb.message.answer("Ок, обери нову підчергу 👇", reply_markup=keyboard_choose_subqueue())

    elif action == "stop":
        USER_SUBQUEUE.pop(chat_id, None)
        USER_LAST_HASH.pop(chat_id, None)
        USER_LAST_SCHEDULE.pop(chat_id, None)
        USER_LAST_UPDATE_MARKER.pop(chat_id, None)
        USER_NOTIFIED_KEYS.pop(chat_id, None)
        save_state()
        await cb.message.answer("

Сповіщення вимкнув ✅\nЩоб знову увімкнути — натисни /start")

    elif action == "back":
        # просто показуємо статус як "домашній" екран
        await send_main_menu(chat_id, build_status_text(chat_id))


@dp.callback_query(F.data.startswith("notice:"))
async def choose_notice(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    register_user(chat_id)

    try:
        val = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer("Помилка", show_alert=True)
        return

    if val not in ALLOWED_NOTICE:
        await cb.answer("Доступно: 5/10/30", show_alert=True)
        return

    USER_NOTICE[chat_id] = val
    save_state()

    await cb.answer("Збережено ✅")
    await send_main_menu(chat_id, f"✅ Ок. Попереджатиму за {val} хв до події.")


# =========================
# ADMIN COMMANDS
# =========================
def is_admin(message: Message) -> bool:
    return ADMIN_ID != 0 and message.from_user and message.from_user.id == ADMIN_ID


@dp.message(F.text.startswith("/bc"))
async def admin_broadcast(message: Message):
    if not is_admin(message):
        return

    text = message.text.replace("/bc", "", 1).strip()
    if not text:
        await message.answer("Формат:\n/bc ваш текст повідомлення")
        return

    ok, fail = 0, 0
    for cid in list(ALL_USERS):
        try:
            await bot.send_message(cid, text)
            ok += 1
        except Exception:
            fail += 1

    await message.answer(f"Розсилка: ✅{ok} ❌{fail}")


@dp.message(F.text == "/stats")
async def admin_stats(message: Message):
    if not is_admin(message):
        return

    total = len(ALL_USERS)
    active = len(USER_SUBQUEUE)
    await message.answer(
        f"📊 Статистика:\n"
        f"👥 Всього користувачів: {total}\n"
        f"🔔 З активними сповіщеннями: {active}"
    )


@dp.message(F.text == "/force")
async def admin_force(message: Message):
    if not is_admin(message):
        return

    await message.answer("⏳ Ок, перевіряю сайт зараз...")
    try:
        await process_site_once(send_updates=True)
        await message.answer("✅ Готово.")
    except Exception as e:
        await message.answer(f"⚠️ Помилка: {e}")


@dp.message(F.text == "/time")
async def admin_time(message: Message):
    if not is_admin(message):
        return
    now = datetime.now(TZ)
    await message.answer(f"Server time: {now.strftime('%d.%m.%Y %H:%M:%S')} (Europe/Kyiv)")


# =========================
# MAIN
# =========================
async def main():
    load_state()

    # Initial fetch (non-fatal)
    try:
        await process_site_once(send_updates=False)
    except Exception as e:
        print(f"[INIT] initial fetch failed: {e}")

    asyncio.create_task(site_watcher_loop())
    asyncio.create_task(reminders_loop())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
