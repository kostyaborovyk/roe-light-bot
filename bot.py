"""ROE light bot v2. Run with Python 3.10+; credentials come from environment."""
import asyncio
import contextlib
import io
import json
import logging
import os
import re
import secrets
import shutil
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, BufferedInputFile, BotCommand
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

load_dotenv()
TZ = ZoneInfo("Europe/Kyiv")
URL = "https://www.roe.vsei.ua/disconnections/"
SUBQUEUES = tuple(f"{i}.{j}" for i in range(1, 7) for j in (1, 2))
NOTICES = (5, 10, 30)
MAX_PLACES, CHECK_SECONDS, STALE_SECONDS, REPORT_TTL = 5, 300, 900, 600
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data" if Path("/var/data").is_dir() else "data"))
LEGACY_FILE = Path(os.getenv("STATE_FILE", str(DATA_DIR / "state.json")))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_NAME = os.getenv("BOT_NAME", "Світло Рівненщини")
ABOUT = "Бот для сповіщень про відключення та відновлення електроенергії за вашою підчергою."
DONATION_URL = os.getenv("DONATION_URL", "https://send.monobank.ua/jar/3bqEWHMcDB").strip()
log = logging.getLogger("light-bot")
def now_ts(): return int(time.time())
def local_time(ts=None): return datetime.fromtimestamp(ts if ts is not None else time.time(), TZ)
def jdump(value): return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
def stamp(ts): return local_time(ts).strftime("%d.%m %H:%M") if ts else "ще не перевірено"

class Store:
    def __init__(self, directory=DATA_DIR, legacy=LEGACY_FILE):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "bot.sqlite3"
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users(chat_id INTEGER PRIMARY KEY,created INTEGER NOT NULL,
          blocked INTEGER NOT NULL DEFAULT 0,subscribed INTEGER NOT NULL DEFAULT 1,selected INTEGER,name TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS places(id INTEGER PRIMARY KEY AUTOINCREMENT,
          chat_id INTEGER NOT NULL REFERENCES users(chat_id),name TEXT NOT NULL,sq TEXT NOT NULL,
          notices TEXT NOT NULL DEFAULT '[10]',enabled INTEGER NOT NULL DEFAULT 1,
          updates INTEGER NOT NULL DEFAULT 1,off_alert INTEGER NOT NULL DEFAULT 1,on_alert INTEGER NOT NULL DEFAULT 1);
        CREATE INDEX IF NOT EXISTS places_chat ON places(chat_id);
        CREATE TABLE IF NOT EXISTS schedules(sq TEXT PRIMARY KEY,payload TEXT NOT NULL,fetched INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER NOT NULL,kind TEXT NOT NULL,
          payload TEXT NOT NULL,dedupe TEXT NOT NULL UNIQUE,place_id INTEGER,event_ts INTEGER,notice INTEGER,
          status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,due INTEGER NOT NULL,
          expires INTEGER NOT NULL,sent INTEGER,error TEXT,created INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status,due,id);
        CREATE TABLE IF NOT EXISTS reports(chat_id INTEGER NOT NULL,sq TEXT NOT NULL,is_on INTEGER NOT NULL,
          created INTEGER NOT NULL,PRIMARY KEY(chat_id,sq));
        CREATE TABLE IF NOT EXISTS sessions(chat_id INTEGER PRIMARY KEY,kind TEXT NOT NULL,payload TEXT NOT NULL,expires INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS drafts(token TEXT PRIMARY KEY,chat_id INTEGER NOT NULL,target TEXT NOT NULL,text TEXT NOT NULL,expires INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS ui_cards(chat_id INTEGER PRIMARY KEY,message_id INTEGER,revision TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS feedback(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER NOT NULL,text TEXT NOT NULL,created INTEGER NOT NULL);
        """)
        try:
            self.migrate(legacy)
        except Exception:
            self.conn.close()
            raise
    def all(self, sql, params=()): return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
    def one(self, sql, params=()):
        r = self.conn.execute(sql, params).fetchone()
        return dict(r) if r else None
    def run(self, sql, params=()):
        with self.conn: return self.conn.execute(sql, params)
    def get_meta(self, key, default=None):
        r = self.one("SELECT value FROM meta WHERE key=?", (key,))
        return r["value"] if r else default
    def set_meta(self, key, value):
        self.run("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    def migrate(self, legacy):
        if self.get_meta("legacy_imported"): return
        legacy = Path(legacy)
        data = {"users": {}}
        if legacy.exists():
            data = json.loads(legacy.read_text(encoding="utf-8-sig"))
            if not isinstance(data, dict) or not isinstance(data.get("users"), dict):
                raise ValueError("Invalid state.json; restore legacy backup before startup")
            shutil.copy2(legacy, self.directory / f"state-before-v2-{now_ts()}.json")
        with self.conn:
            for cid, u in data["users"].items():
                cid = int(cid)
                if not isinstance(u, dict) or (u.get("subqueue") is not None and u["subqueue"] not in SUBQUEUES):
                    raise ValueError("Invalid legacy user")
                sq, notice = u.get("subqueue"), u.get("notice", 10)
                notice = notice if notice in NOTICES else 10
                self.conn.execute("INSERT OR IGNORE INTO users(chat_id,created,subscribed) VALUES(?,?,?)", (cid, now_ts(), int(bool(sq))))
                if sq and not self.one("SELECT id FROM places WHERE chat_id=?", (cid,)):
                    cur = self.conn.execute("INSERT INTO places(chat_id,name,sq,notices) VALUES(?,?,?,?)", (cid, "Дім", sq, jdump([notice])))
                    self.conn.execute("UPDATE users SET selected=? WHERE chat_id=?", (cur.lastrowid, cid))
            self.conn.execute("INSERT INTO meta VALUES('legacy_imported','1')")
            self.conn.execute("INSERT INTO meta VALUES('legacy_imported_at',?)", (str(now_ts()),))
        log.info("Imported %s legacy users", len(data["users"]))
    def user(self, cid, name=""):
        self.run("INSERT INTO users(chat_id,created,name) VALUES(?,?,?) ON CONFLICT(chat_id) DO UPDATE SET blocked=0,name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE users.name END", (cid, now_ts(), name[:100]))
        return self.one("SELECT * FROM users WHERE chat_id=?", (cid,))
    def places(self, cid): return self.all("SELECT * FROM places WHERE chat_id=? ORDER BY id", (cid,))
    def place(self, cid, pid=None):
        if pid is None:
            u = self.one("SELECT selected FROM users WHERE chat_id=?", (cid,))
            pid = u["selected"] if u else None
            found = self.one("SELECT * FROM places WHERE chat_id=? AND id=?", (cid, pid))
            return found or next(iter(self.places(cid)), None)
        return self.one("SELECT * FROM places WHERE chat_id=? AND id=?", (cid, pid))
    def add_place(self, cid, name, sq):
        if sq not in SUBQUEUES or len(self.places(cid)) >= MAX_PLACES: raise ValueError("Invalid place")
        r = self.run("INSERT INTO places(chat_id,name,sq) VALUES(?,?,?)", (cid, name[:40], sq))
        self.run("UPDATE users SET selected=?,subscribed=1 WHERE chat_id=?", (r.lastrowid, cid))
        return self.place(cid, r.lastrowid)
    def schedule(self, sq):
        r = self.one("SELECT * FROM schedules WHERE sq=?", (sq,))
        return (json.loads(r["payload"]), r["fetched"]) if r else ({"days": {}, "pending": []}, 0)
    def enqueue(self, cid, text, kind="service", dedupe=None, pid=None, event=None, notice=None, expires=None, markup=None):
        payload = {"text": text}
        if markup: payload["markup"] = markup.model_dump(mode="json", exclude_none=True)
        execute = self.conn.execute if self.conn.in_transaction else self.run
        return execute("INSERT OR IGNORE INTO outbox(chat_id,kind,payload,dedupe,place_id,event_ts,notice,due,expires,created) VALUES(?,?,?,?,?,?,?,?,?,?)",
          (cid, kind, jdump(payload), dedupe or secrets.token_hex(16), pid, event, notice, now_ts(), expires or now_ts()+3600, now_ts())).rowcount
    def session(self, cid, kind=None, payload=None):
        if kind is not None:
            self.run("INSERT INTO sessions VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET kind=excluded.kind,payload=excluded.payload,expires=excluded.expires", (cid, kind, jdump(payload or {}), now_ts()+1800))
            return
        r = self.one("SELECT * FROM sessions WHERE chat_id=? AND expires>?", (cid, now_ts()))
        if r: r["payload"] = json.loads(r["payload"])
        return r
    def clear_session(self, cid): self.run("DELETE FROM sessions WHERE chat_id=?", (cid,))
    def backup(self):
        folder = self.directory / "backups"
        folder.mkdir(exist_ok=True)
        p = folder / f"bot-{local_time().strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(2)}.sqlite3"
        with contextlib.closing(sqlite3.connect(p)) as destination:
            self.conn.backup(destination)
        return p

TIME_RANGE = re.compile(r"((?:[01]\d|2[0-4]):[0-5]\d)\s*[–—−-]\s*((?:[01]\d|2[0-4]):[0-5]\d)")
DATE_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
def table_matrix(table):
    occupied, grid = {}, []
    for ri, tr in enumerate(table.find_all("tr")):
        ci = 0
        for cell in tr.find_all(["td", "th"], recursive=False):
            while (ri, ci) in occupied: ci += 1
            text = cell.get_text(" ", strip=True)
            rs, cs = int(cell.get("rowspan", 1)), int(cell.get("colspan", 1))
            if not 1 <= rs <= 100 or not 1 <= cs <= 100: raise ValueError("Unexpected table span")
            for r in range(ri, ri+rs):
                for c in range(ci, ci+cs): occupied[r, c] = text
            ci += cs
        width = max((c+1 for r, c in occupied if r == ri), default=0)
        grid.append([occupied.get((ri, c), "") for c in range(width)])
    return grid
def parse_schedules(html):
    soup = BeautifulSoup(html, "lxml")
    m = re.search(r"Оновлено:\s*\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}", soup.get_text(" ", strip=True))
    for table in soup.find_all("table"):
        matrix = table_matrix(table)
        for ri, row in enumerate(matrix):
            cols = {sq: row.index(sq) for sq in SUBQUEUES if sq in row}
            if len(cols) != 12: continue
            out = {sq: {"days": {}, "pending": []} for sq in SUBQUEUES}
            day = None
            for values in matrix[ri+1:]:
                dates = [d for v in values for d in DATE_RE.findall(v)]
                if dates:
                    day = dates[0]
                    datetime.strptime(day, "%d.%m.%Y")
                if not day: continue
                for sq, col in cols.items():
                    text = values[col] if col < len(values) else ""
                    pairs = TIME_RANGE.findall(text)
                    if any(v.startswith("24:") and v != "24:00" for pair in pairs for v in pair):
                        raise ValueError("Invalid midnight time")
                    if pairs: out[sq]["days"].setdefault(day, []).extend(pairs)
                    elif re.search(r"немає|відсутні|не\s+плану|без\s+відключ", text, re.I):
                        out[sq]["days"].setdefault(day, [])
                    elif not text.strip() or text.strip() in ("-", "—", "–") or re.search(r"очіку|уточню|буде\s+опублі|буде\s+оприлюд", text, re.I):
                        out[sq]["pending"].append(day)
                    else:
                        raise ValueError("Unrecognized schedule cell; keeping last valid snapshot")
            for sq in SUBQUEUES:
                for d, pairs in out[sq]["days"].items():
                    out[sq]["days"][d] = [list(p) for p in sorted(set(tuple(x) for x in pairs))]
                out[sq]["pending"] = sorted(set(out[sq]["pending"]) - set(out[sq]["days"]))
            if not any(v["days"] or v["pending"] for v in out.values()): raise ValueError("No schedule dates")
            return m.group(0) if m else "", out
    raise ValueError("Schedule table with all 12 subqueues not found")
def clock_dt(day, hhmm):
    dt = datetime.strptime(day, "%d.%m.%Y").replace(tzinfo=TZ)
    h, m = map(int, hhmm.split(":"))
    if not 0 <= h <= 24 or not 0 <= m < 60 or (h == 24 and m): raise ValueError("Invalid time")
    return dt + timedelta(hours=h, minutes=m)
def intervals_for(payload):
    pairs = []
    for day, values in payload["days"].items():
        for a, b in values:
            start, end = clock_dt(day, a), clock_dt(day, b)
            if b == "23:59": end += timedelta(minutes=1)
            if end < start: end += timedelta(days=1)
            if end > start: pairs.append((start, end))
    merged = []
    for a, b in sorted(pairs):
        if merged and a <= merged[-1][1]: merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
        else: merged.append((a, b))
    return merged
def next_event(payload, now):
    for a, b in intervals_for(payload):
        if a <= now < b: return b, "on"
        if a > now: return a, "off"
    return None, None
def duration_text(seconds):
    mins = max(0, int(seconds//60))
    return f"{mins//60} год {mins%60} хв" if mins >= 60 else f"{mins} хв"

db = None
bot = None
dp = Dispatcher()
fetch_lock, delivery_lock = asyncio.Lock(), asyncio.Lock()
last_delivery, chat_delivery = 0.0, {}
ui_locks = {}
def buttons(rows):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=c) for t,c in row] for row in rows])
def main_keyboard(cid=None):
    rows=[[("📅 Графік","graph")],[("💡 Повідомити про світло","reports")],[("⚙️ Налаштування","settings")]]
    if cid is not None and len(db.places(cid))>1:
        p=db.place(cid)
        rows.insert(0,[(f"🏠 {p['name'][:30]} ▾","switchplace")])
    return buttons(rows)
async def api_call(method, cid, **kwargs):
    global last_delivery
    async with delivery_lock:
        delay = max(0, .10-(time.monotonic()-last_delivery), 1.05-(time.monotonic()-chat_delivery.get(cid,-100)))
        if delay: await asyncio.sleep(delay)
        try: return await getattr(bot, method)(chat_id=cid, **kwargs)
        finally:
            last_delivery = time.monotonic()
            chat_delivery[cid] = last_delivery
            if len(chat_delivery)>10000:
                for old in [k for k,v in chat_delivery.items() if v<last_delivery-60]: chat_delivery.pop(old,None)
async def deliver_ui(cid,text,markup,revision):
    card=db.one("SELECT * FROM ui_cards WHERE chat_id=?",(cid,))
    if not card or card["revision"]!=revision:
        return None
    if card["message_id"]:
        try:
            return await api_call("edit_message_text",cid,message_id=card["message_id"],text=text,reply_markup=markup)
        except TelegramBadRequest as exc:
            message=str(exc).lower()
            if "message is not modified" in message:
                return None
            if not any(x in message for x in ("message to edit not found","message can't be edited","message can not be edited","there is no text")):
                raise
    result=await api_call("send_message",cid,text=text,reply_markup=markup)
    message_id=getattr(result,"message_id",None)
    if isinstance(message_id,int):
        db.run("UPDATE ui_cards SET message_id=? WHERE chat_id=? AND revision=?",(message_id,cid,revision))
    return result

async def say(cid,text,markup=None):
    text=text[:4000]
    async with ui_locks.setdefault(cid,asyncio.Lock()):
        revision=secrets.token_hex(8)
        db.run("INSERT INTO ui_cards(chat_id,revision) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET revision=excluded.revision",(cid,revision))
        try:
            return await deliver_ui(cid,text,markup,revision)
        except TelegramForbiddenError:
            db.run("UPDATE users SET blocked=1 WHERE chat_id=?",(cid,))
        except (TelegramRetryAfter,TelegramNetworkError,TelegramServerError) as exc:
            if isinstance(exc,TelegramRetryAfter):
                db.set_meta("telegram_pause",now_ts()+int(exc.retry_after)+1)
            dedupe=f"ui:{cid}:{revision}"
            db.enqueue(cid,text,kind="ui",dedupe=dedupe,markup=markup)
            row=db.one("SELECT payload FROM outbox WHERE dedupe=?",(dedupe,))
            payload=json.loads(row["payload"])
            payload["revision"]=revision
            db.run("UPDATE outbox SET payload=? WHERE dedupe=?",(jdump(payload),dedupe))
        except TelegramBadRequest:
            log.warning("Invalid reply for chat=%s",cid)
    return None
def report_summary(sq):
    rows = db.all("SELECT is_on,created FROM reports WHERE sq=? AND created>?", (sq,now_ts()-REPORT_TTL))
    if not rows: return ""
    on = sum(r["is_on"] for r in rows)
    return (f"👥 За останні 10 хв: світло є — {on}, немає — {len(rows)-on}.\n"
      f"Останнє повідомлення: {stamp(max(r['created'] for r in rows))}.\n"
      "Це відгуки з різних місць підчерги, а не перевірка вашого будинку.")
def source_update(marker=None):
    marker=marker if marker is not None else db.get_meta("marker","")
    return marker.strip() if marker else ""

def event_label(event,now=None):
    now=now or local_time()
    if event.date()==now.date(): return f"о {event:%H:%M}"
    if event.date()==(now+timedelta(days=1)).date(): return f"завтра о {event:%H:%M}"
    return f"{event:%d.%m} о {event:%H:%M}"

def status_text(cid):
    p=db.place(cid)
    if not p:
        return "👋 Додайте адресу в «Налаштування → Мої адреси». Потрібні лише назва та підчерга."
    payload,fetched=db.schedule(p["sq"])
    now=local_time()
    day=now.strftime("%d.%m.%Y")
    lines=[f"🏠 {p['name']} · {p['sq']}"]
    if not fetched or now_ts()-fetched>STALE_SECONDS:
        lines.append("⚪ Графік поки недоступний")
    elif day not in payload["days"]:
        lines.append("⚪ Графік поки очікується")
    else:
        off=any(a<=now<b for a,b in intervals_for(payload))
        lines.append("🔴 За графіком зараз відключення" if off else "🟢 За графіком зараз має бути світло")
        event,kind=next_event(payload,now)
        if event:
            remaining=duration_text(event.timestamp()-now.timestamp())
            if kind=="on":
                lines.append(f"🟢 Очікуване відновлення {event_label(event,now)} — через {remaining}")
            else:
                end=next(b for a,b in intervals_for(payload) if a==event)
                lines.append(f"🔴 Наступне відключення {event_label(event,now)} — через {remaining}")
                lines.append(f"⏳ До {end:%H:%M}, тривалість {duration_text(end.timestamp()-event.timestamp())}")
        else:
            lines.append("Наступних відключень у доступному графіку немає")
    marker=source_update()
    if marker: lines.extend(["",marker])
    user=db.one("SELECT subscribed FROM users WHERE chat_id=?",(cid,))
    if not user["subscribed"] or not p["enabled"]:
        lines.append("🔕 Сповіщення вимкнені")
    return "\n".join(lines)
def graph_keyboard(offset=0):
    return buttons([[("✓ Сьогодні" if offset==0 else "Сьогодні","day:0"),("✓ Завтра" if offset==1 else "Завтра","day:1")],
                    [("🖼 Графік-картинка",f"image:{offset}")],[("← Назад","home")]])

def day_lines(payload,day):
    title="сьогодні" if day==local_time().strftime("%d.%m.%Y") else "завтра" if day==(local_time()+timedelta(days=1)).strftime("%d.%m.%Y") else ""
    lines=[f"📅 {day}"+(f" · {title}" if title else "")]
    if day not in payload["days"]:
        return lines+["⚪ Графік поки очікується"]
    values=payload["days"][day]
    lines.extend(f"🔴 {a}–{b}" for a,b in values)
    if not values: lines.append("🟢 Відключень за графіком не заплановано")
    start,finish=clock_dt(day,"00:00"),clock_dt(day,"24:00")
    total=sum(max(0,min(b,finish).timestamp()-max(a,start).timestamp()) for a,b in intervals_for(payload))
    lines.append(f"Загалом за графіком: {duration_text(total)} без світла")
    return lines

def day_text(p,offset=0):
    day=(local_time()+timedelta(days=offset)).strftime("%d.%m.%Y")
    payload,fetched=db.schedule(p["sq"])
    lines=[f"🏠 {p['name']} · {p['sq']}",""]
    if not fetched or now_ts()-fetched>STALE_SECONDS:
        lines.append("⚪ Графік поки недоступний")
    else:
        lines.extend(day_lines(payload,day))
    marker=source_update()
    if marker: lines.extend(["",marker])
    return "\n".join(lines)

def full_graph(p,payload,marker):
    today=local_time().date()
    days=sorted((d for d in set(payload["days"])|set(payload["pending"]) if datetime.strptime(d,"%d.%m.%Y").date()>=today),
                key=lambda d:datetime.strptime(d,"%d.%m.%Y"))
    lines=[f"🔄 Оновився графік · {p['name']} · {p['sq']}"]
    for day in days:
        lines.extend([""]+day_lines(payload,day))
    if marker: lines.extend(["",source_update(marker)])
    return "\n".join(lines)[:3900]

def schedule_changed(old,new):
    today=local_time().date()
    # A site outage or a waiting cell is not an explicit cancellation.
    for day,values in new["days"].items():
        if datetime.strptime(day,"%d.%m.%Y").date()<today: continue
        if day not in old["days"] or set(map(tuple,values))!=set(map(tuple,old["days"][day])):
            return True
    return False
def render_chart(p,payload,fetched,offset=0):
    day = (local_time()+timedelta(days=offset)).strftime("%d.%m.%Y")
    font = lambda n: ImageFont.truetype(str(Path(__file__).with_name("DejaVuSans.ttf")),n)
    image = Image.new("RGB",(1200,690),"#111827")
    draw = ImageDraw.Draw(image)
    draw.text((60,40),BOT_NAME[:40],font=font(34),fill="#f8fafc")
    draw.text((60,99),f"{p['name'][:32]}  /  підчерга {p['sq']}",font=font(26),fill="#cbd5e1")
    draw.text((60,145),f"Графік на {day}",font=font(24),fill="#94a3b8")
    known,start = day in payload["days"] and bool(fetched) and now_ts()-fetched<=STALE_SECONDS,clock_dt(day,"00:00")
    for row in range(2):
        y=235+row*130
        for cell in range(12):
            hour,x=row*12+cell,60+cell*90
            draw.text((x,y-36),f"{hour:02d}",font=font(18),fill="#cbd5e1")
            draw.rounded_rectangle((x,y,x+82,y+62),radius=7,fill="#22c55e" if known else "#64748b")
            hs=start+timedelta(hours=hour)
            if known:
                for a,b in intervals_for(payload):
                    lo,hi=max(a,hs),min(b,hs+timedelta(hours=1))
                    if lo<hi: draw.rectangle((x+82*(lo-hs).total_seconds()/3600,y+2,x+82*(hi-hs).total_seconds()/3600,y+60),fill="#ef4444")
    draw.rectangle((60,491,80,511),fill="#22c55e")
    draw.text((91,486),"Світло за графіком",font=font(19),fill="#e2e8f0")
    draw.rectangle((390,491,410,511),fill="#ef4444")
    draw.text((421,486),"Відключення",font=font(19),fill="#e2e8f0")
    if not known: draw.text((60,535),"Графік поки недоступний" if not fetched or now_ts()-fetched>STALE_SECONDS else "Графік поки очікується",font=font(20),fill="#cbd5e1")
    draw.text((60,582),source_update() or "Джерело: Рівнеобленерго",font=font(19),fill="#94a3b8")
    draw.text((60,625),"Фактичне електропостачання може відрізнятися від графіка.",font=font(18),fill="#94a3b8")
    output=io.BytesIO()
    image.save(output,"PNG")
    return output.getvalue()


async def refresh_site():
    async with fetch_lock:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "ROE-Light-Bot/2.0"}) as session:
            async with session.get(URL) as response:
                response.raise_for_status()
                if response.content_length and response.content_length > 4_000_000:
                    raise ValueError("Unexpectedly large source page")
                raw = await response.content.read(4_000_001)
                if len(raw)>4_000_000: raise ValueError("Unexpectedly large source page")
                html = raw.decode(response.charset or "utf-8", errors="replace")
        marker, schedules = await asyncio.to_thread(parse_schedules, html)
        fetched = now_ts()
        # A single transaction updates both the snapshot and its outgoing change messages.
        with db.conn:
            for sq, payload in schedules.items():
                old, previous = db.schedule(sq)
                changed = bool(previous and schedule_changed(old,payload))
                db.conn.execute("INSERT INTO schedules VALUES(?,?,?) ON CONFLICT(sq) DO UPDATE SET payload=excluded.payload,fetched=excluded.fetched", (sq,jdump(payload),fetched))
                if changed:
                    for p in db.all("SELECT p.* FROM places p JOIN users u ON u.chat_id=p.chat_id WHERE p.sq=? AND p.enabled=1 AND p.updates=1 AND u.subscribed=1 AND u.blocked=0",(sq,)):
                        db.enqueue(p["chat_id"],full_graph(p,payload,marker),
                          kind="update",dedupe=f"change:{p['id']}:{fetched}",pid=p["id"],expires=fetched+3600,markup=buttons([[("📅 Відкрити графік",f"open:{p['id']}")]]))
            db.conn.execute("INSERT INTO meta VALUES('last_good',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(fetched),))
            db.conn.execute("INSERT INTO meta VALUES('marker',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(marker,))
            db.conn.execute("INSERT INTO meta VALUES('site_failures','0') ON CONFLICT(key) DO UPDATE SET value='0'")
        if db.get_meta("site_alerted")=="1":
            db.set_meta("site_alerted","0")
            if ADMIN_ID: db.enqueue(ADMIN_ID,"✅ Перевірка сайту знову працює.",kind="admin")
        return True

async def site_loop():
    while True:
        try:
            await refresh_site()
        except asyncio.CancelledError: raise
        except Exception as exc:
            log.warning("Site refresh failed: %s", type(exc).__name__)
            failures=int(db.get_meta("site_failures","0"))+1
            db.set_meta("site_failures",failures)
            db.set_meta("site_error",type(exc).__name__)
            if failures>=3 and db.get_meta("site_alerted")!="1":
                db.set_meta("site_alerted","1")
                if ADMIN_ID:
                    db.enqueue(ADMIN_ID,f"⚠️ Не вдалося отримати графік {failures} рази поспіль.\nОстання успішна перевірка: {stamp(int(db.get_meta('last_good','0')))}.\nБот показує збережені дані з позначкою часу.",kind="admin")
        await asyncio.sleep(CHECK_SECONDS)


def reminder_text(p,event_ts,kind,ts=None):
    ts=now_ts() if ts is None else ts
    event=local_time(event_ts)
    remaining=duration_text(event_ts-ts)
    if kind=="on":
        return f"🟢 За {remaining} очікується відновлення\n🏠 {p['name']} · {p['sq']}\n{event_label(event)}, за графіком."
    payload,_=db.schedule(p["sq"])
    end=next((b for a,b in intervals_for(payload) if int(a.timestamp())==event_ts),None)
    interval=f"{event:%H:%M}–{end:%H:%M} · {duration_text(end.timestamp()-event_ts)}" if end else f"{event:%H:%M}"
    date=f"{event:%d.%m} · " if event.date()!=local_time(ts).date() else ""
    return f"🔴 За {remaining} — можливе відключення\n🏠 {p['name']} · {p['sq']}\n{date}{interval}\nЗа графіком Рівнеобленерго."

def queue_reminders(ts=None):
    ts=now_ts() if ts is None else ts
    now=local_time(ts)
    for p in db.all("SELECT p.* FROM places p JOIN users u ON p.chat_id=u.chat_id WHERE p.enabled=1 AND u.subscribed=1 AND u.blocked=0"):
        payload,fetched=db.schedule(p["sq"])
        if not fetched or ts-fetched>STALE_SECONDS: continue
        for a,b in intervals_for(payload):
            for event,kind in ((a,"off"),(b,"on")):
                if not p["off_alert" if kind=="off" else "on_alert"]: continue
                event_ts=int(event.timestamp())
                for notice in json.loads(p["notices"]):
                    notify=event_ts-notice*60
                    # Catch up short restarts, but never send a reminder after its event.
                    grace=min(notice*60,600)
                    if not notify<=ts<event_ts or ts-notify>grace: continue
                    text=reminder_text(p,event_ts,kind,ts)
                    db.enqueue(p["chat_id"],text,kind=f"reminder_{kind}",pid=p["id"],event=event_ts,notice=notice,
                      expires=event_ts,dedupe=f"reminder:{p['id']}:{kind}:{event_ts}:{notice}")

async def reminder_loop():
    while True:
        try: queue_reminders()
        except asyncio.CancelledError: raise
        except Exception: log.exception("Reminder scheduling failed")
        await asyncio.sleep(20)

def eligible(row):
    user=db.one("SELECT * FROM users WHERE chat_id=?",(row["chat_id"],))
    if user and user["blocked"]: return False
    if row["kind"]=="ui":
        payload=json.loads(row["payload"])
        card=db.one("SELECT revision FROM ui_cards WHERE chat_id=?",(row["chat_id"],))
        return bool(card and card["revision"]==payload.get("revision"))
    if row["kind"]=="broadcast":
        return bool(user and user["subscribed"])
    if not row["place_id"]: return True
    p=db.place(row["chat_id"],row["place_id"])
    if not p or not p["enabled"] or not user or not user["subscribed"]: return False
    if row["kind"]=="update": return bool(p["updates"])
    if row["kind"].startswith("reminder_"):
        kind=row["kind"].split("_",1)[1]
        if not p["off_alert" if kind=="off" else "on_alert"] or row["notice"] not in json.loads(p["notices"]): return False
        payload,fetched=db.schedule(p["sq"])
        if not fetched or now_ts()-fetched>STALE_SECONDS: return None
        return any(int((a if kind=="off" else b).timestamp())==row["event_ts"] for a,b in intervals_for(payload))
    return True

async def deliver_once():
    ts=now_ts()
    db.run("UPDATE outbox SET status='expired' WHERE status='pending' AND expires<=?",(ts,))
    row=db.one("SELECT * FROM outbox WHERE status='pending' AND due<=? ORDER BY CASE WHEN kind LIKE 'reminder_%' THEN 0 ELSE 1 END,id LIMIT 1",(ts,))
    if not row: return False
    allowed=eligible(row)
    if allowed is None:
        db.run("UPDATE outbox SET due=? WHERE id=?",(ts+30,row["id"]))
        return True
    if not allowed:
        db.run("UPDATE outbox SET status='cancelled' WHERE id=?",(row["id"],))
        return True
    payload=json.loads(row["payload"])
    markup=InlineKeyboardMarkup.model_validate(payload["markup"]) if payload.get("markup") else None
    try:
        if row["kind"]=="ui":
            async with ui_locks.setdefault(row["chat_id"],asyncio.Lock()):
                await deliver_ui(row["chat_id"],payload["text"],markup,payload["revision"])
        else:
            text=payload["text"]
            if row["kind"].startswith("reminder_"):
                place=db.place(row["chat_id"],row["place_id"])
                text=reminder_text(place,row["event_ts"],row["kind"].split("_",1)[1])
            await api_call("send_message",row["chat_id"],text=text,reply_markup=markup)
    except TelegramForbiddenError:
        with db.conn:
            db.conn.execute("UPDATE users SET blocked=1 WHERE chat_id=?",(row["chat_id"],))
            db.conn.execute("UPDATE outbox SET status='failed',error='forbidden' WHERE chat_id=? AND status='pending'",(row["chat_id"],))
    except TelegramRetryAfter as exc:
        db.set_meta("telegram_pause",now_ts()+int(exc.retry_after)+1)
        db.run("UPDATE outbox SET due=?,attempts=attempts+1,error='rate_limit' WHERE id=?",(now_ts()+int(exc.retry_after)+1,row["id"]))
    except (TelegramNetworkError,TelegramServerError,TimeoutError,OSError):
        attempts=row["attempts"]+1
        db.run("UPDATE outbox SET attempts=?,due=?,error='temporary' WHERE id=?",(attempts,now_ts()+min(300,2**min(attempts,8)),row["id"]))
    except TelegramBadRequest:
        db.run("UPDATE outbox SET status='failed',error='bad_request' WHERE id=?",(row["id"],))
        log.warning("Invalid queued delivery: id=%s",row["id"])
    else:
        db.run("UPDATE outbox SET status='sent',sent=?,error=NULL WHERE id=?",(now_ts(),row["id"]))
    return True

async def outbox_loop():
    while True:
        try:
            if int(db.get_meta("telegram_pause","0"))>now_ts():
                await asyncio.sleep(1)
                continue
            worked=await deliver_once()
        except asyncio.CancelledError: raise
        except Exception:
            log.exception("Outbox worker failed")
            worked=False
        if not worked: await asyncio.sleep(.5)

async def maintenance_loop():
    while True:
        try:
            date=local_time().strftime("%Y-%m-%d")
            if db.get_meta("last_backup")!=date:
                db.backup()
                db.set_meta("last_backup",date)
            cutoff=now_ts()-14*86400
            db.run("DELETE FROM outbox WHERE status<>'pending' AND created<?",(cutoff,))
            db.run("DELETE FROM reports WHERE created<?",(now_ts()-REPORT_TTL,))
            db.run("DELETE FROM sessions WHERE expires<?",(now_ts(),))
            db.run("DELETE FROM drafts WHERE expires<?",(now_ts(),))
            db.run("DELETE FROM feedback WHERE created<?",(now_ts()-90*86400,))
            for path in (db.directory/"backups").glob("bot-*.sqlite3"):
                if path.stat().st_mtime<cutoff: path.unlink()
            for path in db.directory.glob("state-before-v2-*.json"):
                if path.stat().st_mtime<cutoff: path.unlink()
            imported=int(db.get_meta("legacy_imported_at",str(now_ts())))
            if imported<cutoff and LEGACY_FILE.exists():
                # The active database was successfully imported at least 14 days ago.
                LEGACY_FILE.unlink()
        except asyncio.CancelledError: raise
        except Exception:
            log.exception("Maintenance failed")
            if ADMIN_ID: db.enqueue(ADMIN_ID,"⚠️ Не вдалося виконати резервне копіювання або очищення. Перевірте Logs у Render.",kind="admin",dedupe=f"backup-error:{local_time().date()}")
        await asyncio.sleep(3600)

def settings_keyboard(p):
    notices=set(json.loads(p["notices"]))
    rows=[[(f"{'✅ ' if v in notices else ''}{v} хв",f"notice2:{p['id']}:{v}") for v in NOTICES]]
    for field,label in (("updates","Оновлення графіка"),("off_alert","Попередження про відключення"),("on_alert","Попередження про відновлення"),("enabled","Сповіщення адреси")):
        rows.append([(f"{'✅' if p[field] else '🔕'} {label}",f"toggle:{p['id']}:{field}")])
    u=db.one("SELECT subscribed FROM users WHERE chat_id=?",(p["chat_id"],))
    rows.extend([[(f"{'🔕 Вимкнути' if u['subscribed'] else '🔔 Увімкнути'} всі сповіщення","subscription")],[("← Назад","settings")]])
    return buttons(rows)

def settings_menu():
    rows=[[("📍 Мої адреси","places")],[("🔔 Сповіщення","alerts")],[("ℹ️ Про бота","about")],[("✍️ Написати відгук","feedback")]]
    if DONATION_URL.startswith(("https://","http://")):
        markup=buttons(rows+[[("← Назад","home")]])
        markup.inline_keyboard.insert(-1,[InlineKeyboardButton(text="💛 Підтримати ініціативу",url=DONATION_URL)])
        return markup
    return buttons(rows+[[("← Назад","home")]])

async def show_settings(cid):
    await say(cid,"⚙️ Налаштування",settings_menu())

async def show_alerts(cid):
    p=db.place(cid)
    if not p: return await show_places(cid)
    await say(cid,f"🔔 {p['name']} · {p['sq']}\nПопереджати за:\nМожна вибрати кілька часів.",settings_keyboard(p))

async def show_places(cid):
    places=db.places(cid)
    selected=db.place(cid)
    rows=[[(f"{'✅ ' if selected and p['id']==selected['id'] else ''}{p['name']} · {p['sq']}",f"select:{p['id']}")] for p in places]
    if len(places)<MAX_PLACES: rows.append([("➕ Додати адресу","add")])
    if selected:
        rows.extend([[("✏️ Назва",f"rename:{selected['id']}"),("🔁 Підчерга",f"change:{selected['id']}")],
                     [("🗑 Видалити адресу",f"removeask:{selected['id']}")]])
    rows.append([("← Назад","settings")])
    await say(cid,f"📍 Мої адреси · {len(places)}/{MAX_PLACES}",buttons(rows))

def queue_keyboard(prefix):
    rows=[[(sq,f"{prefix}:{sq}") for sq in SUBQUEUES[i:i+2]] for i in range(0,12,2)]
    rows.append([("Скасувати","cancel")])
    return buttons(rows)

def admin_keyboard():
    return buttons([[("📊 Статистика","admin:stats"),("🩺 Стан бота","admin:health")],
      [("📣 Розсилка","admin:broadcast"),("💾 Резервна копія","admin:backup")],
      [("✍️ Відгуки","admin:feedback"),("🔄 Перевірити сайт","admin:force")],
      [("⬅️ Головне меню","home")]])

def stats_text():
    total=db.one("SELECT COUNT(*) n FROM users")["n"]
    active=db.one("SELECT COUNT(DISTINCT u.chat_id) n FROM users u JOIN places p ON p.chat_id=u.chat_id WHERE u.subscribed=1 AND u.blocked=0 AND p.enabled=1")["n"]
    blocked=db.one("SELECT COUNT(*) n FROM users WHERE blocked=1")["n"]
    week=db.one("SELECT COUNT(*) n FROM users WHERE created>=?",(now_ts()-7*86400,))["n"]
    lines=[f"📊 Користувачів: {total}",f"🔔 Активні підписники: {active}",f"📍 Адрес: {db.one('SELECT COUNT(*) n FROM places')['n']}",f"🚫 Заблокували бота: {blocked}",f"Нових за 7 днів: {week} (дата для старих користувачів — дата перенесення)",""]
    for r in db.all("SELECT sq,COUNT(*) n FROM places GROUP BY sq ORDER BY sq"):
        lines.append(f"Підчерга {r['sq']}: {r['n']} адрес")
    return "\n".join(lines)

def health_text():
    pending=db.one("SELECT COUNT(*) n FROM outbox WHERE status='pending'")["n"]
    failed=db.one("SELECT COUNT(*) n FROM outbox WHERE status='failed' AND created>?",(now_ts()-86400,))["n"]
    return (f"🩺 Бот v2.1\nОстання перевірка: {stamp(int(db.get_meta('last_good','0')))}\n"
      f"Послідовних помилок сайту: {db.get_meta('site_failures','0')}\n"
      f"Повідомлень у черзі: {pending}\nНевдалих доставок за добу: {failed}\n"
      f"Остання щоденна копія: {db.get_meta('last_backup','ще немає')}\n"
      f"Час сервера: {stamp(now_ts())} (Київ)")

async def show_about(cid):
    rows=[[InlineKeyboardButton(text="🌐 Джерело графіків",url=URL)]]
    if DONATION_URL.startswith(("https://","http://")):
        rows.append([InlineKeyboardButton(text="💛 Підтримати ініціативу",url=DONATION_URL)])
    rows.extend([[InlineKeyboardButton(text="✍️ Написати відгук",callback_data="feedback")],
                 [InlineKeyboardButton(text="⬅️ Головне меню",callback_data="home")]])
    await say(cid,f"💡 {BOT_NAME}\n\n{ABOUT}\n\nСоціальна ініціатива. Підтримка добровільна.\n"
      "Графіки беремо з сайту Рівнеобленерго. Це планові дані, а не вимірювання електропостачання.\n"
      "Зберігаємо Telegram ID, назви ваших адрес, підчерги та налаштування. Повідомлення «світло є / немає» показуємо лише у зведенні без імен.\n"
      "Команда /delete_me видаляє ваші дані з робочої бази. Резервні копії зберігаються до 14 днів.",InlineKeyboardMarkup(inline_keyboard=rows))

async def send_chart(cid,offset):
    p=db.place(cid)
    if not p: return await show_places(cid)
    payload,fetched=db.schedule(p["sq"])
    try:
        png=await asyncio.to_thread(render_chart,p,payload,fetched,offset)
        await api_call("send_photo",cid,photo=BufferedInputFile(png,filename=f"schedule-{p['sq']}.png"),
          caption=f"🏠 {p['name']} · {p['sq']}"+("\n"+source_update() if source_update() else ""),
          reply_markup=buttons([[("← До графіка",f"graph:{offset}")]]))
    except TelegramForbiddenError: db.run("UPDATE users SET blocked=1 WHERE chat_id=?",(cid,))
    except (OSError,TelegramRetryAfter,TelegramNetworkError,TelegramServerError,TelegramBadRequest):
        log.warning("Chart unavailable")
        await say(cid,"⚠️ Картинка зараз недоступна. Надсилаю текстовий графік.\n\n"+day_text(p,offset),main_keyboard(cid))


def recipients(target):
    sql="SELECT u.chat_id FROM users u WHERE u.blocked=0 AND u.subscribed=1 AND EXISTS(SELECT 1 FROM places p WHERE p.chat_id=u.chat_id AND p.enabled=1"
    params=()
    if target!="all":
        sql+=" AND p.sq=?"
        params=(target,)
    return db.all(sql+")",params)

async def broadcast_preview(cid,target,text):
    if cid!=ADMIN_ID: return
    if not text.strip() or len(text)>3000: return await say(cid,"Текст має містити від 1 до 3000 символів.")
    token=secrets.token_hex(8)
    db.run("INSERT INTO drafts VALUES(?,?,?,?,?)",(token,cid,target,text,now_ts()+1800))
    count=len(recipients(target))
    await say(cid,f"📣 Попередній перегляд\nАудиторія: {'усі активні підписники' if target=='all' else 'підчерга '+target}\nОдержувачів зараз: {count}\n\n{text}",
      buttons([[("✅ Надіслати",f"bcconfirm:{token}"),("❌ Скасувати",f"bccancel:{token}")]]))

async def admin_action(cid,action):
    if cid!=ADMIN_ID: return
    if action=="stats": await say(cid,stats_text(),admin_keyboard())
    elif action=="health": await say(cid,health_text(),admin_keyboard())
    elif action=="broadcast":
        rows=[[("Усі активні підписники","bctarget:all")]]
        rows.extend([[(sq,f"bctarget:{sq}") for sq in SUBQUEUES[i:i+2]] for i in range(0,12,2)])
        rows.append([("Скасувати","cancel")])
        await say(cid,"📣 Оберіть аудиторію розсилки.",buttons(rows))
    elif action=="backup":
        try:
            path=db.backup()
            if path.stat().st_size>48*1024*1024:
                await say(cid,"Копію створено на диску, але вона завелика для надсилання через бота.")
            else:
                from aiogram.types import FSInputFile
                await api_call("send_document",cid,document=FSInputFile(path),
                  caption="Резервна копія SQLite. Збережіть приватно; вона містить дані користувачів.")
        except (OSError,sqlite3.Error,TelegramRetryAfter,TelegramNetworkError,TelegramServerError,TelegramBadRequest):
            log.warning("Backup export failed")
            await say(cid,"⚠️ Не вдалося надіслати копію. Перевірте стан бота та спробуйте пізніше.")
    elif action=="feedback":
        rows=db.all("SELECT * FROM feedback ORDER BY id DESC LIMIT 5")
        text="\n\n".join(f"#{r['id']} · {stamp(r['created'])}\n{r['text'][:500]}" for r in rows)
        await say(cid,"✍️ Останні відгуки\n\n"+(text or "Поки немає."),admin_keyboard())
    elif action=="force":
        if now_ts()-int(db.get_meta("last_good","0"))<30:
            return await say(cid,"Сайт щойно перевірено. Зачекайте 30 секунд.",admin_keyboard())
        await say(cid,"⏳ Перевіряю сайт…")
        try:
            await refresh_site()
            await say(cid,"✅ Графік перевірено.",admin_keyboard())
        except Exception:
            await say(cid,"⚠️ Сайт недоступний або таблицю не вдалося розібрати. Останні дані збережено.",admin_keyboard())

@dp.message(F.text)
async def messages(message: Message):
    if message.chat.type!="private": return
    cid=message.chat.id
    db.user(cid,message.from_user.full_name if message.from_user else "")
    text=message.text.strip()
    command=text.split(maxsplit=1)[0].split("@",1)[0].lower() if text.startswith("/") else ""
    if command=="/cancel":
        db.clear_session(cid)
        return await say(cid,"Дію скасовано.",main_keyboard(cid))
    if command:
        db.run("DELETE FROM ui_cards WHERE chat_id=?",(cid,))
        db.clear_session(cid)
        if command=="/start":
            db.run("UPDATE users SET subscribed=1 WHERE chat_id=?",(cid,))
            if not db.places(cid):
                db.session(cid,"addqueue",{"name":"Дім"})
                await say(cid,"👋 Оберіть підчергу для першої адреси «Дім». Пізніше можна перейменувати її та додати інші.",queue_keyboard("sqnew"))
            else: await say(cid,status_text(cid),main_keyboard(cid))
        elif command in ("/status","/next","/menu"): await say(cid,status_text(cid),main_keyboard(cid))
        elif command=="/schedule":
            p=db.place(cid)
            if p: await say(cid,day_text(p),graph_keyboard())
            else: await show_places(cid)
        elif command=="/notice": await show_alerts(cid)
        elif command=="/change":
            p=db.place(cid)
            if p: await say(cid,"Оберіть нову підчергу.",queue_keyboard(f"sqchange:{p['id']}"))
            else: await show_places(cid)
        elif command=="/stop":
            db.run("UPDATE users SET subscribed=0 WHERE chat_id=?",(cid,))
            await say(cid,"🔕 Усі сповіщення вимкнено. Адреси збережено. Увімкнути знову можна через налаштування або /start.",main_keyboard(cid))
        elif command=="/about": await show_about(cid)
        elif command=="/feedback":
            db.session(cid,"feedback")
            await say(cid,"Напишіть відгук одним текстовим повідомленням, до 2000 символів. Його побачить автор бота. Скасувати: /cancel.")
        elif command=="/delete_me":
            await say(cid,"Видалити всі ваші адреси, налаштування та повідомлення про світло?",
              buttons([[("Видалити мої дані","deleteconfirm"),("Скасувати","cancel")]]))
        elif command in ("/admin","/stats","/force","/time","/backup","/bc"):
            if cid!=ADMIN_ID: return
            if command=="/admin": await say(cid,"Керування ботом",admin_keyboard())
            elif command=="/stats": await admin_action(cid,"stats")
            elif command=="/force": await admin_action(cid,"force")
            elif command=="/time": await admin_action(cid,"health")
            elif command=="/backup": await admin_action(cid,"backup")
            elif command=="/bc":
                content=text.split(maxsplit=1)
                if len(content)==2: await broadcast_preview(cid,"all",content[1])
                else: await admin_action(cid,"broadcast")
        else: await say(cid,"Оберіть дію в меню або використайте /start.",main_keyboard(cid))
        return
    state=db.session(cid)
    if not state: return await say(cid,"Оберіть дію в меню.",main_keyboard(cid))
    kind,data=state["kind"],state["payload"]
    if kind in ("addname","rename"):
        if not 1<=len(text)<=40: return await say(cid,"Назва має містити 1–40 символів. Наприклад: Дім, Батьки або Робота.")
        if kind=="addname":
            db.session(cid,"addqueue",{"name":text})
            await say(cid,f"Оберіть підчергу для «{text}».",queue_keyboard("sqnew"))
        else:
            if db.place(cid,data["pid"]):
                db.run("UPDATE places SET name=? WHERE id=? AND chat_id=?",(text,data["pid"],cid))
            db.clear_session(cid)
            await show_places(cid)
    elif kind=="feedback":
        if len(text)>2000: return await say(cid,"Скоротіть відгук до 2000 символів.")
        last=db.one("SELECT MAX(created) ts FROM feedback WHERE chat_id=?",(cid,))
        if last["ts"] and now_ts()-last["ts"]<60: return await say(cid,"Відгук уже отримано. Наступний можна надіслати через хвилину.")
        r=db.run("INSERT INTO feedback(chat_id,text,created) VALUES(?,?,?)",(cid,text,now_ts()))
        db.clear_session(cid)
        if ADMIN_ID: db.enqueue(ADMIN_ID,f"✍️ Новий відгук #{r.lastrowid}\nВід: {cid}\n\n{text}",kind="admin")
        await say(cid,"Дякую! Відгук збережено для автора бота.",main_keyboard(cid))
    elif kind=="broadcast":
        if cid==ADMIN_ID:
            await broadcast_preview(cid,data["target"],text)
            if len(text)<=3000: db.clear_session(cid)
    else: await say(cid,"Оберіть підчергу кнопкою або скасуйте дію командою /cancel.")

@dp.callback_query()
async def callbacks(cb: CallbackQuery):
    if not isinstance(cb.message,Message) or cb.message.chat.type!="private" or cb.message.chat.id!=cb.from_user.id:
        with contextlib.suppress(TelegramBadRequest): await cb.answer("Відкрийте приватний чат із ботом.",show_alert=True)
        return
    cid=cb.from_user.id
    db.user(cid,cb.from_user.full_name)
    with contextlib.suppress(TelegramBadRequest,TelegramNetworkError,TelegramRetryAfter): await cb.answer()
    data=cb.data or ""
    parts=data.split(":")
    action=parts[0]
    if action in ("open","graph") or cb.message.photo:
        # Opening a schedule from a notification never edits that notification.
        if action=="open" or cb.message.photo:
            db.run("DELETE FROM ui_cards WHERE chat_id=?",(cid,))
    else:
        revision=secrets.token_hex(8)
        db.run("INSERT INTO ui_cards VALUES(?,?,?) ON CONFLICT(chat_id) DO UPDATE SET message_id=excluded.message_id,revision=excluded.revision",(cid,cb.message.message_id,revision))
    if action=="open" and len(parts)==2 and parts[1].isdigit():
        p=db.place(cid,int(parts[1]))
        if not p: return await say(cid,"Ця адреса вже недоступна.",main_keyboard(cid))
        db.run("UPDATE users SET selected=? WHERE chat_id=?",(p["id"],cid))
        return await say(cid,day_text(p),graph_keyboard())
    if action=="graph":
        offset=int(parts[1]) if len(parts)==2 and parts[1] in ("0","1") else 0
        p=db.place(cid)
        if not p: return await show_places(cid)
        return await say(cid,day_text(p,offset),graph_keyboard(offset))
    if data=="switchplace":
        rows=[[(f"{p['name']} · {p['sq']}",f"select:{p['id']}")] for p in db.places(cid)]
        return await say(cid,"🏠 Оберіть адресу",buttons(rows+[[("← Назад","home")]]))
    if data=="alerts": return await show_alerts(cid)
    if data=="home":
        db.clear_session(cid)
        return await say(cid,status_text(cid),main_keyboard(cid))
    if data=="cancel":
        db.clear_session(cid)
        return await say(cid,"Дію скасовано.",main_keyboard(cid))
    if data=="places": return await show_places(cid)
    if data=="settings": return await show_settings(cid)
    if data=="about": return await show_about(cid)
    if data=="feedback":
        db.session(cid,"feedback")
        return await say(cid,"Напишіть відгук одним текстовим повідомленням (до 2000 символів). Скасувати: /cancel.")
    if data=="subscription":
        db.run("UPDATE users SET subscribed=1-subscribed WHERE chat_id=?",(cid,))
        return await show_alerts(cid)
    if data=="add":
        if len(db.places(cid))>=MAX_PLACES: return await say(cid,"Можна зберегти до 5 адрес.",main_keyboard(cid))
        db.session(cid,"addname")
        return await say(cid,"Напишіть назву адреси: Дім, Батьки, Робота… До 40 символів. Точна адреса не потрібна. Скасувати: /cancel.")
    if action in ("day","image") and len(parts)==2 and parts[1] in ("0","1"):
        p=db.place(cid)
        if not p: return await show_places(cid)
        offset=int(parts[1])
        if action=="image": return await send_chart(cid,offset)
        return await say(cid,day_text(p,offset),graph_keyboard(offset))
    if action=="sqnew" and len(parts)==2 and parts[1] in SUBQUEUES:
        state=db.session(cid)
        if not state or state["kind"]!="addqueue": return await say(cid,"Цей вибір уже завершено. Відкрийте «Мої адреси».",main_keyboard(cid))
        if len(db.places(cid))>=MAX_PLACES: return await show_places(cid)
        db.add_place(cid,state["payload"]["name"],parts[1])
        db.clear_session(cid)
        return await say(cid,status_text(cid),main_keyboard(cid))
    if action in ("select","rename","change","removeask","remove","sqchange","notice2","toggle") and len(parts)>=2 and parts[1].isdigit():
        pid=int(parts[1])
        p=db.place(cid,pid)
        if not p: return await say(cid,"Ця адреса вже недоступна.",main_keyboard(cid))
        if action=="select":
            db.run("UPDATE users SET selected=? WHERE chat_id=?",(pid,cid))
            return await say(cid,status_text(cid),main_keyboard(cid))
        if action=="rename":
            db.session(cid,"rename",{"pid":pid})
            return await say(cid,"Напишіть нову назву, до 40 символів. Скасувати: /cancel.")
        if action=="change": return await say(cid,f"Нова підчерга для «{p['name']}»:",queue_keyboard(f"sqchange:{pid}"))
        if action=="sqchange" and len(parts)==3 and parts[2] in SUBQUEUES:
            db.run("UPDATE places SET sq=? WHERE id=? AND chat_id=?",(parts[2],pid,cid))
            db.run("UPDATE outbox SET status='cancelled' WHERE place_id=? AND status='pending'",(pid,))
            return await say(cid,status_text(cid),main_keyboard(cid))
        if action=="removeask":
            return await say(cid,f"Видалити «{p['name']}»?",buttons([[("Видалити",f"remove:{pid}"),("Скасувати","places")]]))
        if action=="remove":
            with db.conn:
                db.conn.execute("DELETE FROM places WHERE id=? AND chat_id=?",(pid,cid))
                db.conn.execute("UPDATE outbox SET status='cancelled' WHERE place_id=? AND status='pending'",(pid,))
                first=next(iter(db.places(cid)),None)
                db.conn.execute("UPDATE users SET selected=? WHERE chat_id=?",(first["id"] if first else None,cid))
            return await show_places(cid)
        if action=="notice2" and len(parts)==3 and parts[2].isdigit() and int(parts[2]) in NOTICES:
            notice=int(parts[2])
            values=set(json.loads(p["notices"]))
            values.remove(notice) if notice in values else values.add(notice)
            db.run("UPDATE places SET notices=? WHERE id=? AND chat_id=?",(jdump(sorted(values)),pid,cid))
            return await show_alerts(cid)
        if action=="toggle" and len(parts)==3 and parts[2] in ("updates","off_alert","on_alert","enabled"):
            field=parts[2] # Whitelisted SQL identifier; values remain bound parameters.
            db.run(f"UPDATE places SET {field}=1-{field} WHERE id=? AND chat_id=?",(pid,cid))
            return await show_alerts(cid)
    if data=="reports":
        p=db.place(cid)
        if not p: return await show_places(cid)
        return await say(cid,f"💡 {p['name']} · {p['sq']}\nЧи є світло у вас зараз?\n\n"+report_summary(p["sq"]),
          buttons([[("🟢 Є світло",f"report:{p['id']}:1"),("🔴 Немає",f"report:{p['id']}:0")],[("⬅️ Меню","home")]]))
    if action=="report" and len(parts)==3 and parts[1].isdigit() and parts[2] in ("0","1"):
        p=db.place(cid,int(parts[1]))
        if not p: return await show_places(cid)
        last=db.one("SELECT * FROM reports WHERE chat_id=? AND sq=?",(cid,p["sq"]))
        if last and now_ts()-last["created"]<30:
            return await say(cid,"Повідомлення вже враховано. Оновити його можна через 30 секунд.",main_keyboard(cid))
        db.run("INSERT INTO reports VALUES(?,?,?,?) ON CONFLICT(chat_id,sq) DO UPDATE SET is_on=excluded.is_on,created=excluded.created",(cid,p["sq"],int(parts[2]),now_ts()))
        return await say(cid,"Дякую! Повідомлення враховується протягом 10 хвилин.\n\n"+report_summary(p["sq"]),main_keyboard(cid))
    if action=="admin" and len(parts)==2:
        if cid==ADMIN_ID: return await admin_action(cid,parts[1])
        return
    if action=="bctarget" and len(parts)==2 and cid==ADMIN_ID and parts[1] in ("all",*SUBQUEUES):
        db.session(cid,"broadcast",{"target":parts[1]})
        return await say(cid,"Напишіть текст розсилки (до 3000 символів). Перед відправленням буде попередній перегляд. Скасувати: /cancel.")
    if action in ("bcconfirm","bccancel") and len(parts)==2 and cid==ADMIN_ID:
        draft=db.one("SELECT * FROM drafts WHERE token=? AND chat_id=? AND expires>?",(parts[1],cid,now_ts()))
        if not draft: return await say(cid,"Розсилку вже підтверджено, скасовано або час підтвердження минув.",admin_keyboard())
        if action=="bccancel":
            db.run("DELETE FROM drafts WHERE token=?",(draft["token"],))
            return await say(cid,"Розсилку скасовано.",admin_keyboard())
        targets=recipients(draft["target"])
        with db.conn:
            for target in targets:
                db.enqueue(target["chat_id"],draft["text"],kind="broadcast",
                  dedupe=f"broadcast:{draft['token']}:{target['chat_id']}",expires=now_ts()+86400)
            db.conn.execute("DELETE FROM drafts WHERE token=?",(draft["token"],))
        return await say(cid,f"✅ У чергу додано {len(targets)} повідомлень. Доставка відбувається поступово.",admin_keyboard())
    if data=="deleteconfirm":
        with db.conn:
            for table in ("places","outbox","reports","sessions","drafts","feedback","ui_cards"):
                db.conn.execute(f"DELETE FROM {table} WHERE chat_id=?",(cid,))
            db.conn.execute("DELETE FROM users WHERE chat_id=?",(cid,))
        return await say(cid,"Ваші дані видалено з робочої бази. Резервні копії зберігаються до 14 днів. Почати заново: /start.")
    # Older keyboards continue to work after migration.
    if action=="sq" and len(parts)==2 and parts[1] in SUBQUEUES:
        p=db.place(cid)
        if p:
            db.run("UPDATE places SET sq=? WHERE id=?",(parts[1],p["id"]))
            db.run("UPDATE outbox SET status='cancelled' WHERE place_id=? AND status='pending'",(p["id"],))
        else: db.add_place(cid,"Дім",parts[1])
        db.run("UPDATE users SET subscribed=1 WHERE chat_id=?",(cid,))
        return await say(cid,status_text(cid),main_keyboard(cid))
    if action=="notice" and len(parts)==2 and parts[1].isdigit() and int(parts[1]) in NOTICES:
        p=db.place(cid)
        if p: db.run("UPDATE places SET notices=? WHERE id=?",(jdump([int(parts[1])]),p["id"]))
        return await show_alerts(cid)
    if action=="main" and len(parts)==2:
        if parts[1]=="notice": return await show_alerts(cid)
        if parts[1]=="change": return await show_places(cid)
        if parts[1]=="stop": db.run("UPDATE users SET subscribed=0 WHERE chat_id=?",(cid,))
        return await say(cid,status_text(cid),main_keyboard(cid))
    await say(cid,"Кнопка застаріла. Відкрийте актуальне меню.",main_keyboard(cid))

@dp.errors()
async def handler_error(event):
    log.error("Handler failed: %s",type(event.exception).__name__)
    if ADMIN_ID and db:
        db.enqueue(ADMIN_ID,"⚠️ Виникла помилка обробки дії. Перевірте Logs у Render.",
          kind="admin",dedupe=f"handler-error:{now_ts()//3600}")
    return True

async def main():
    global db,bot
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not BOT_TOKEN: raise RuntimeError("Add BOT_TOKEN in Render Environment")
    db=Store()
    bot=Bot(BOT_TOKEN)
    tasks=[]
    try:
        # Discard no queued Telegram updates. Never start a second polling instance.
        await bot.delete_webhook(drop_pending_updates=False)
        with contextlib.suppress(TelegramBadRequest,TelegramNetworkError,TelegramRetryAfter,TelegramServerError):
            await bot.set_my_commands([BotCommand(command=c,description=d) for c,d in [
              ("start","Почати / увімкнути сповіщення"),("status","Поточний стан"),
              ("schedule","Графік на сьогодні"),("notice","Налаштування сповіщень"),
              ("stop","Вимкнути сповіщення"),("about","Про бота та підтримка"),("feedback","Написати відгук")]])
        tasks=[asyncio.create_task(fn(),name=fn.__name__) for fn in (site_loop,reminder_loop,outbox_loop,maintenance_loop)]
        log.info("Light bot v2.1 started; users=%s",db.one("SELECT COUNT(*) n FROM users")["n"])
        await dp.start_polling(bot)
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await bot.session.close()
        db.conn.close()

if __name__=="__main__":
    asyncio.run(main())




