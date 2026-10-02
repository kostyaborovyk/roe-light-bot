"""Private Telegram budget entry bot. Configure secrets in Render, never in code."""
import asyncio
import contextlib
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery
from core import ACCOUNTS, CATEGORIES, Store, amount, journal_row, new_draft, reminder_due
from sheets import Sheets

TZ = ZoneInfo("Europe/Kyiv")
log = logging.getLogger("budget")


def keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=text, callback_data=data) for text, data in row] for row in rows])


def menu():
    return keyboard([[('➖ Витрата', 'new:expense:today'), ('➕ Надходження', 'new:income:today')],
                     [('🔁 Переказ', 'new:transfer:today'), ('📊 Залишки', 'summary')],
                     [('📅 Витрата за вчора', 'new:expense:yesterday')],
                     [('🔔 Увімкнути нагадування', 'reminders:on'), ('🔕 Вимкнути', 'reminders:off')]])


def choice(draft, action, values):
    return keyboard([[(text, f'd:{draft["id"]}:{action}:{i}')] for i, text in enumerate(values)]
                    + [[('Скасувати', f'd:{draft["id"]}:cancel:0')]])


class BudgetBot:
    def __init__(self, owner, sheets, store, year, month):
        self.owner, self.sheets, self.store = owner, sheets, store
        self.year, self.month = year, month
        # One lock includes both the Sheets session and all journal reservations.
        self.lock = asyncio.Lock()
        self.dp = Dispatcher()
        self.dp.message.register(self.start, Command('start', 'help'))
        self.dp.message.register(self.stop, Command('stop'))
        self.dp.message.register(self.cancel, Command('cancel'))
        self.dp.message.register(self.status, Command('status'))
        self.dp.callback_query.register(self.callback, F.data)
        self.dp.message.register(self.text, F.text)

    def allowed(self, event):
        user = event.from_user
        chat = event.chat if isinstance(event, Message) else getattr(event.message, 'chat', None)
        return bool(user and user.id == self.owner and chat and chat.type == 'private'
                    and chat.id == self.owner)

    async def start(self, message):
        if not self.allowed(message):
            return
        self.store.set_setting('reminders_enabled', '1')
        await message.answer('💳 Особистий бюджет\nОбери операцію. Зберігаю тільки після підтвердження.\n'
                             'Нагадування: 23:00 за день і 09:01 за вчора, за Києвом.\n'
                             'Бот підключений до одного місяця; наприкінці місяця таблицю потрібно перемкнути.',
                             reply_markup=menu())
        draft = self.store.draft(self.owner)
        if draft:
            await message.answer('Є незавершений запис. Продовжуй із останнього кроку або /cancel.')

    async def stop(self, message):
        if self.allowed(message):
            self.store.set_setting('reminders_enabled', '0')
            await message.answer('Нагадування вимкнено. /start — увімкнути знову.', reply_markup=menu())

    async def cancel(self, message):
        if self.allowed(message):
            self.store.clear_draft(self.owner)
            await message.answer('Чернетку скасовано. Підтверджені записи залишаються в журналі.', reply_markup=menu())

    async def status(self, message):
        if self.allowed(message):
            try:
                async with self.lock:
                    text = await asyncio.to_thread(self.sheets.summary)
                await message.answer(text, reply_markup=menu())
            except Exception as exc:
                log.warning('Summary failed: %s', type(exc).__name__)
                await message.answer('Не вдалося прочитати таблицю. Спробуй трохи пізніше.')

    async def review(self, message, draft):
        row = journal_row(draft, self.year, self.month)
        text = (f'Перевір запис:\n{draft["date"]} · {row[1]}\n{row[4]:.2f} грн\n'
                f'{row[2] or "Без категорії"}\n{row[5]}'
                + (f' → {row[6]}' if row[6] else '') + f'\n{row[3]}')
        draft['step'] = 'confirm'
        self.store.save_draft(self.owner, draft)
        await message.answer(text, reply_markup=keyboard([
            [('✅ Зберегти', f'd:{draft["id"]}:confirm:0')],
            [('❌ Скасувати', f'd:{draft["id"]}:cancel:0')]]))

    async def callback(self, callback: CallbackQuery):
        if not self.allowed(callback):
            await callback.answer('Це приватний бот.', show_alert=True)
            return
        await callback.answer()
        msg = callback.message
        data = callback.data
        try:
            if data == 'summary':
                async with self.lock:
                    text = await asyncio.to_thread(self.sheets.summary)
                await msg.answer(text, reply_markup=menu())
                return
            if data.startswith('reminders:'):
                enabled = data.endswith(':on')
                self.store.set_setting('reminders_enabled', '1' if enabled else '0')
                await msg.answer('Нагадування увімкнено.' if enabled else 'Нагадування вимкнено.')
                return
            if data.startswith('new:'):
                _, kind, when = data.split(':')
                kinds = {'expense': 'Витрата', 'income': 'Надходження', 'transfer': 'Переказ'}
                day = datetime.now(TZ).date() - timedelta(days=1 if when == 'yesterday' else 0)
                if (day.year, day.month) != (self.year, self.month):
                    raise ValueError('Ця дата поза місяцем таблиці. Спочатку підключи потрібний місяць.')
                if self.store.draft(self.owner):
                    raise ValueError('Є незавершений запис. Спершу заверши його або /cancel.')
                draft = new_draft(kinds[kind], day)
                self.store.save_draft(self.owner, draft)
                await msg.answer(f'{draft["kind"]}, дата {day:%d.%m.%Y}.\nВведи суму, наприклад 125,50.')
                return
            prefix, token, action, index = data.split(':')
            draft = self.store.draft(self.owner)
            if not draft or draft['id'] != token:
                await msg.answer('Цей запис уже завершено або кнопка застаріла.', reply_markup=menu())
                return
            if action == 'cancel':
                self.store.clear_draft(self.owner)
                await msg.answer('Чернетку скасовано.', reply_markup=menu())
                return
            i = int(index)
            if action == 'category' and draft['step'] == 'category':
                if not 0 <= i < len(CATEGORIES):
                    raise ValueError('Невідома категорія.')
                draft.update(category=CATEGORIES[i], step='account')
                self.store.save_draft(self.owner, draft)
                await msg.answer('Обери рахунок:', reply_markup=choice(draft, 'account', ACCOUNTS))
            elif action == 'account' and draft['step'] == 'account':
                if not 0 <= i < len(ACCOUNTS):
                    raise ValueError('Невідомий рахунок.')
                draft['account'] = ACCOUNTS[i]
                if draft['kind'] == 'Переказ':
                    draft['step'] = 'target'
                    self.store.save_draft(self.owner, draft)
                    await msg.answer('Куди переказуєш?', reply_markup=choice(draft, 'target', ACCOUNTS))
                else:
                    draft['step'] = 'description'
                    self.store.save_draft(self.owner, draft)
                    await msg.answer('Короткий опис покупки/надходження. Без опису — надішли «-».')
            elif action == 'target' and draft['step'] == 'target':
                if not 0 <= i < len(ACCOUNTS) or ACCOUNTS[i] == draft['account']:
                    raise ValueError('Обери інший рахунок отримувача.')
                draft.update(target=ACCOUNTS[i], step='description')
                self.store.save_draft(self.owner, draft)
                await msg.answer('Короткий опис переказу. Без опису — «-».')
            elif action == 'confirm' and draft['step'] == 'confirm':
                payload = journal_row(draft, self.year, self.month)
                async with self.lock:
                    self.store.enqueue(token, payload, self.sheets.spreadsheet_id)
                    self.store.clear_draft(self.owner)
                    try:
                        number = await asyncio.to_thread(self.store.commit, token, self.sheets)
                    except Exception as exc:
                        log.warning('Journal write pending: %s', type(exc).__name__)
                        await msg.answer('Запис підтверджено й збережено в черзі. Google ще не підтвердив '
                                         'внесення. Бот повторить перевірку; не вводь цю операцію знову.')
                        return
                await msg.answer(f'✅ Збережено в «Журнал», рядок {number}.', reply_markup=menu())
            else:
                await msg.answer('Ця кнопка вже не відповідає поточному кроку.')
        except ValueError as exc:
            await msg.answer(str(exc))
        except Exception as exc:
            log.warning('Callback failed: %s', type(exc).__name__)
            await msg.answer('Зараз не вдалося виконати дію. Чернетка збережена; спробуй пізніше.')

    async def text(self, message):
        if not self.allowed(message):
            return
        draft = self.store.draft(self.owner)
        if not draft:
            await message.answer('Обери операцію:', reply_markup=menu())
            return
        try:
            if message.text.startswith('/date '):
                day = date.fromisoformat(message.text.split(maxsplit=1)[1])
                if (day.year, day.month) != (self.year, self.month):
                    raise ValueError('Дата має бути в межах підключеного місяця.')
                if day > datetime.now(TZ).date():
                    raise ValueError('Майбутні операції не записуємо — тільки факт.')
                draft['date'] = day.isoformat()
                self.store.save_draft(self.owner, draft)
                await message.answer(f'Дата змінена на {day:%d.%m.%Y}.')
                if draft['step'] == 'confirm':
                    await self.review(message, draft)
            elif draft['step'] == 'amount':
                draft['amount'] = amount(message.text)
                if draft['kind'] == 'Витрата':
                    draft['step'] = 'category'
                    self.store.save_draft(self.owner, draft)
                    await message.answer('Обери категорію:', reply_markup=choice(draft, 'category', CATEGORIES))
                else:
                    draft['step'] = 'account'
                    self.store.save_draft(self.owner, draft)
                    await message.answer('Обери рахунок:', reply_markup=choice(draft, 'account', ACCOUNTS))
            elif draft['step'] == 'description':
                draft['description'] = '' if message.text.strip() == '-' else message.text.strip()
                await self.review(message, draft)
            else:
                await message.answer('Продовжуй кнопками. Змінити дату: /date 2026-10-01. Скасувати: /cancel.')
        except ValueError as exc:
            await message.answer(str(exc))

    async def background(self, bot):
        while True:
            try:
                async with self.lock:
                    for item in self.store.pending():
                        try:
                            number = await asyncio.to_thread(self.store.commit, item['id'], self.sheets)
                            await bot.send_message(self.owner, f'✅ Запис із черги внесено, рядок {number}.')
                        except Exception as exc:
                            log.warning('Pending retry failed: %s', type(exc).__name__)
                            if isinstance(exc, ValueError) and not self.store.setting('warn:' + item['id']):
                                await bot.send_message(self.owner, f'⚠️ Запис очікує перевірки: {exc}')
                                self.store.set_setting('warn:' + item['id'], '1')
                if self.store.setting('reminders_enabled') == '1':
                    now = datetime.now(TZ)
                    sent = {r[0] for r in self.store.conn.execute('SELECT key FROM reminders')}
                    for key, kind, scheduled_day in reminder_due(now, sent):
                        target = scheduled_day if kind == 'evening' else scheduled_day - timedelta(days=1)
                        if (target.year, target.month) != (self.year, self.month):
                            text = '📅 Потрібно підключити бюджетну таблицю нового місяця.'
                            buttons = menu()
                        else:
                            async with self.lock:
                                expenses = await asyncio.to_thread(self.sheets.day_expenses, target)
                            total = sum(v[4] for v in expenses)
                            intro = 'Внеси витрати за день' if kind == 'evening' else 'Перевір витрати за вчора'
                            text = (f'🔔 {intro} ({target:%d.%m.%Y}).\n'
                                    f'У журналі: {len(expenses)} витрат на {total:.2f} грн.\n'
                                    'Перевір, чи нічого не пропустив.' if expenses else
                                    f'🔔 {intro} ({target:%d.%m.%Y}).\nЗаписів витрат немає. '
                                    'Якщо витрачав — додай; якщо ні — усе гаразд.')
                            when = 'yesterday' if target < now.date() else 'today'
                            buttons = keyboard([[('➖ Додати витрату', f'new:expense:{when}')],
                                                [('📊 Залишки', 'summary')]])
                        await bot.send_message(self.owner, text, reply_markup=buttons)
                        with self.store.conn:
                            self.store.conn.execute('INSERT OR IGNORE INTO reminders VALUES (?)', (key,))
            except Exception as exc:
                log.warning('Background tick failed: %s', type(exc).__name__)
            await asyncio.sleep(30)


async def main():
    required = ['BUDGET_BOT_TOKEN', 'BUDGET_OWNER_ID', 'GOOGLE_SERVICE_ACCOUNT_JSON', 'BUDGET_SPREADSHEET_ID']
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError('Додай в Render Environment: ' + ', '.join(missing))
    owner = int(os.environ['BUDGET_OWNER_ID'])
    if owner <= 0:
        raise RuntimeError('BUDGET_OWNER_ID має бути додатним числом.')
    year, month = int(os.getenv('BUDGET_YEAR', '2026')), int(os.getenv('BUDGET_MONTH', '10'))
    date(year, month, 1)
    directory = Path(os.getenv('BUDGET_DATA_DIR', str(Path(os.getenv('DATA_DIR', '/var/data')) / 'budget')))
    sheets = Sheets(os.environ['BUDGET_SPREADSHEET_ID'], os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'],
                    os.getenv('BUDGET_MONTH_TAB', 'Жовтень'))
    await asyncio.to_thread(sheets.validate)
    store = Store(directory / 'budget.sqlite3')
    app = BudgetBot(owner, sheets, store, year, month)
    bot = Bot(os.environ['BUDGET_BOT_TOKEN'])
    background = None
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        background = asyncio.create_task(app.background(bot))
        await app.dp.start_polling(bot, handle_as_tasks=False)
    finally:
        if background:
            background.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await background
        await bot.session.close()
        sheets.session.close()
        store.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    asyncio.run(main())
