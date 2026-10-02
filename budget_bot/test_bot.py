"""Telegram flows with real aiogram types and fake Sheet; never send messages."""
import tempfile
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo
from aiogram.types import User, Chat, Message, CallbackQuery
from budget_bot import BudgetBot
from core import Store, new_draft, reminder_due
from test_core import FakeSheets
from sheets import Sheets


class FlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = Path(__file__).parent / ('test-data-' + uuid.uuid4().hex)
        self.directory.mkdir()
        self.store = Store(self.directory / 'budget.sqlite3')
        self.sheet = FakeSheets()
        self.app = BudgetBot(123, self.sheet, self.store, 2026, 10)
        self.answer_patch = patch.object(Message, 'answer', new_callable=AsyncMock)
        self.answer = self.answer_patch.start()
        self.callback_patch = patch.object(CallbackQuery, 'answer', new_callable=AsyncMock)
        self.callback_answer = self.callback_patch.start()

    async def asyncTearDown(self):
        self.answer_patch.stop()
        self.callback_patch.stop()
        self.store.close()
        for file in self.directory.iterdir():
            file.unlink()
        self.directory.rmdir()

    def message(self, text='', owner=123, chat_type='private'):
        return Message(message_id=1, date=datetime.now(), chat=Chat(id=owner, type=chat_type),
                       from_user=User(id=owner, is_bot=False, first_name='test'), text=text)

    def callback(self, data):
        return CallbackQuery(id='1', from_user=User(id=123, is_bot=False, first_name='test'),
                             chat_instance='1', data=data, message=self.message())

    async def test_stranger_and_groups_cannot_read_or_start(self):
        await self.app.start(self.message('/start', owner=456))
        await self.app.start(self.message('/start', chat_type='group'))
        self.answer.assert_not_awaited()
        self.assertEqual(self.store.setting('reminders_enabled'), '')

    async def test_expense_flow_and_duplicate_confirmation(self):
        draft = new_draft('Витрата', datetime(2026, 10, 2).date())
        self.store.save_draft(123, draft)
        await self.app.text(self.message('125,50'))
        token = draft['id']
        await self.app.callback(self.callback(f'd:{token}:category:4'))
        await self.app.callback(self.callback(f'd:{token}:account:0'))
        await self.app.text(self.message('продукти'))
        self.assertEqual(self.store.draft(123)['step'], 'confirm')
        self.assertEqual(self.sheet.writes, 0)
        await self.app.callback(self.callback(f'd:{token}:confirm:0'))
        await self.app.callback(self.callback(f'd:{token}:confirm:0'))
        self.assertEqual(self.sheet.writes, 1)
        self.assertEqual(self.sheet.rows[0][0][2:6], ['Їжа / продукти', 'продукти', 125.5, 'Монобанк картка'])

    async def test_offline_google_retains_confirmed_queue(self):
        draft = new_draft('Надходження', datetime(2026, 10, 2).date())
        draft.update(amount='100', account='Готівка', description='факт', step='confirm')
        self.store.save_draft(123, draft)
        self.sheet.lose_response = True
        await self.app.callback(self.callback(f'd:{draft["id"]}:confirm:0'))
        self.assertIsNone(self.store.draft(123))
        self.assertEqual(len(self.store.pending()), 1)
        self.assertIn('в черзі', self.answer.await_args.args[0])
        self.store.commit(draft['id'], self.sheet)
        self.assertEqual(self.sheet.writes, 1)

    async def test_old_draft_buttons_cannot_change_new_draft(self):
        draft = new_draft('Витрата', datetime(2026, 10, 2).date())
        self.store.save_draft(123, draft)
        await self.app.callback(self.callback('d:old:cancel:0'))
        self.assertEqual(self.store.draft(123), draft)

    async def test_start_stop_reminders(self):
        await self.app.start(self.message('/start'))
        self.assertEqual(self.store.setting('reminders_enabled'), '1')
        await self.app.stop(self.message('/stop'))
        self.assertEqual(self.store.setting('reminders_enabled'), '0')


class AdapterTests(unittest.TestCase):
    def test_write_uses_only_literals_and_preserves_validation(self):
        adapter = Sheets.__new__(Sheets)
        adapter.journal_id = 1155952684
        calls = []
        adapter.request = lambda *args, **kwargs: calls.append(kwargs['json'])
        adapter.write(5, [46296, 'Витрата', 'Інше', '=1+2', 12.5, 'Готівка', ''], 'abc')
        requests = calls[0]['requests']
        self.assertEqual(requests[0]['updateCells']['fields'], 'userEnteredValue')
        self.assertEqual(requests[0]['updateCells']['rows'][0]['values'][3]['userEnteredValue'],
                         {'stringValue': '=1+2'})
        self.assertEqual(requests[1]['updateCells']['fields'], 'note')
        self.assertEqual(requests[1]['updateCells']['start']['columnIndex'], 3)

    def test_kyiv_reminders_after_dst_change(self):
        tz = ZoneInfo('Europe/Kyiv')
        for day in [24, 25, 26]:
            now = datetime(2026, 10, day, 9, 1, tzinfo=tz)
            self.assertEqual(list(reminder_due(now, set()))[0][1], 'morning')
        self.assertNotEqual(datetime(2026, 10, 24, 9, tzinfo=tz).utcoffset(),
                            datetime(2026, 10, 26, 9, tzinfo=tz).utcoffset())


if __name__ == '__main__':
    unittest.main()
