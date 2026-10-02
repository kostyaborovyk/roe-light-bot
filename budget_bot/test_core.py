import json
import uuid
import unittest
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from core import Store, amount, journal_row, reminder_due, new_draft


class FakeSheets:
    spreadsheet_id = 'october'

    def __init__(self):
        self.rows = [([''] * 7, '') for _ in range(304)]
        self.writes = 0
        self.lose_response = False
        self.before_write = None

    def journal(self):
        return self.rows

    def row(self, number):
        return self.rows[number - 5]

    def write(self, number, values, token):
        self.writes += 1
        self.rows[number - 5] = (values[:], token)
        if self.lose_response:
            self.lose_response = False
            raise TimeoutError('response lost after Google committed')


class RulesTests(unittest.TestCase):
    def test_amounts(self):
        for text, expected in [('125', '125.00'), ('1 250,50', '1250.50'), ('0,01', '0.01')]:
            self.assertEqual(amount(text), expected)

    def test_bad_amounts(self):
        for text in ['0', '-1', 'nan', 'Infinity', '=1+2', '1.234', '10000001', '1,2,3']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                amount(text)

    def draft(self, **fields):
        d = new_draft('Витрата', date(2026, 10, 1))
        d.update(amount='125,50', account='Монобанк картка', category='Їжа / продукти',
                 description='кава')
        d.update(fields)
        return d

    def test_excel_serial_and_expense(self):
        row = journal_row(self.draft(), 2026, 10)
        self.assertEqual(row, [46296, 'Витрата', 'Їжа / продукти', 'кава', 125.5, 'Монобанк картка', ''])

    def test_transfer_not_expense(self):
        row = journal_row(self.draft(kind='Переказ', target='Монобанк банка'), 2026, 10)
        self.assertEqual(row[1], 'Переказ')
        self.assertEqual(row[2], '')
        self.assertEqual(row[6], 'Монобанк банка')

    def test_same_account_rejected(self):
        with self.assertRaises(ValueError):
            journal_row(self.draft(kind='Переказ', target='Монобанк картка'), 2026, 10)

    def test_wrong_month_rejected(self):
        with self.assertRaises(ValueError):
            journal_row(self.draft(date='2026-09-30'), 2026, 10)

    def test_income_no_planned_category(self):
        self.assertEqual(journal_row(self.draft(kind='Надходження'), 2026, 10)[2], '')

    def test_description_is_literal(self):
        self.assertEqual(journal_row(self.draft(description='=IMPORTXML("url")'), 2026, 10)[3], '=IMPORTXML("url")')

    def test_invalid_category_or_account(self):
        for fields in [{'category': 'unknown'}, {'account': 'unknown'}, {'description': 'x' * 301}]:
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                journal_row(self.draft(**fields), 2026, 10)

    def test_reminders_exact_times_and_no_early_send(self):
        t = datetime(2026, 10, 2, 9, 0)
        self.assertEqual(list(reminder_due(t, set())), [])
        self.assertEqual(list(reminder_due(t.replace(minute=1), set())),
                         [('2026-10-02:morning', 'morning', date(2026, 10, 2))])
        self.assertEqual(list(reminder_due(t.replace(hour=23, minute=0), set()))[0][1], 'evening')

    def test_reminders_dedupe_and_missed_window(self):
        t = datetime(2026, 10, 2, 9, 2)
        self.assertEqual(list(reminder_due(t, {'2026-10-02:morning'})), [])
        self.assertEqual(list(reminder_due(t.replace(hour=12), set())), [])

    def test_evening_recovery_after_midnight(self):
        t = datetime(2026, 10, 3, 0, 10)
        self.assertEqual(list(reminder_due(t, set())),
                         [('2026-10-02:evening', 'evening', date(2026, 10, 2))])


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(__file__).parent / ('test-data-' + uuid.uuid4().hex)
        self.directory.mkdir()
        self.path = self.directory / 'budget.sqlite3'
        self.store = Store(self.path)
        self.sheet = FakeSheets()
        self.payload = [46296, 'Витрата', 'Їжа / продукти', 'кава', 125.5, 'Монобанк картка', '']

    def tearDown(self):
        self.store.close()
        for file in self.directory.iterdir():
            file.unlink()
        self.directory.rmdir()

    def queue(self, token='a'):
        self.store.enqueue(token, self.payload, self.sheet.spreadsheet_id)

    def test_duplicate_confirm_only_one_write(self):
        self.queue()
        self.assertEqual(self.store.commit('a', self.sheet), 5)
        self.queue()
        self.assertEqual(self.store.commit('a', self.sheet), 5)
        self.assertEqual(self.sheet.writes, 1)

    def test_lost_google_response_no_duplicate_after_restart(self):
        self.queue()
        self.sheet.lose_response = True
        with self.assertRaises(TimeoutError):
            self.store.commit('a', self.sheet)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.commit('a', self.sheet), 5)
        self.assertEqual(self.sheet.writes, 1)

    def test_manual_row_is_preserved(self):
        self.sheet.rows[0] = ([46296, 'Витрата', '', 'manual', 20, 'Готівка', ''], '')
        self.queue()
        self.assertEqual(self.store.commit('a', self.sheet), 6)
        self.assertEqual(self.sheet.rows[0][0][3], 'manual')

    def test_pending_reserved_row_conflict_never_overwrites(self):
        self.queue()
        with self.store.conn:
            self.store.conn.execute("UPDATE operations SET row_number=5 WHERE id='a'")
        self.sheet.rows[0] = ([46296, 'Витрата', '', 'manual', 20, 'Готівка', ''], '')
        with self.assertRaises(ValueError):
            self.store.commit('a', self.sheet)
        self.assertEqual(self.sheet.writes, 0)

    def test_moved_marker_dedupes(self):
        self.queue()
        self.sheet.rows[8] = (self.payload, 'a')
        self.assertEqual(self.store.commit('a', self.sheet), 13)
        self.assertEqual(self.sheet.writes, 0)

    def test_changed_marker_payload_is_not_overwritten(self):
        self.queue()
        changed = self.payload[:]
        changed[4] = 200
        self.sheet.rows[0] = (changed, 'a')
        with self.assertRaises(ValueError):
            self.store.commit('a', self.sheet)
        self.assertEqual(self.sheet.writes, 0)

    def test_full_journal_stops_at_formula_boundary(self):
        self.sheet.rows = [(['occupied'] * 7, '') for _ in range(304)]
        self.queue()
        with self.assertRaises(ValueError):
            self.store.commit('a', self.sheet)
        self.assertEqual(self.sheet.writes, 0)

    def test_sheet_switch_cannot_move_pending_expense(self):
        self.queue()
        self.sheet.spreadsheet_id = 'november'
        with self.assertRaises(ValueError):
            self.store.commit('a', self.sheet)

    def test_two_pending_records_and_reserved_rows(self):
        self.queue('a')
        self.queue('b')
        with self.store.conn:
            self.store.conn.execute("UPDATE operations SET row_number=5 WHERE id='a'")
        self.assertEqual(self.store.commit('b', self.sheet), 6)
        self.assertEqual(self.store.commit('a', self.sheet), 5)

    def test_draft_persists_restart(self):
        draft = new_draft('Витрата', date(2026, 10, 1))
        self.store.save_draft(123, draft)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.draft(123), draft)

    def test_same_token_different_payload_rejected(self):
        self.queue()
        with self.assertRaises(ValueError):
            self.store.enqueue('a', ['other'], self.sheet.spreadsheet_id)


if __name__ == '__main__':
    unittest.main()
