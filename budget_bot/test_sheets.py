import unittest
from datetime import date
from urllib.parse import unquote
from core import ACCOUNTS, CATEGORIES, HEADER
from sheets import Sheets
from test_reporting import entry


class ReadOnlySheets(Sheets):
    def __init__(self):
        self.main_tab = None
        self.today = lambda: date(2026, 11, 5)
        self.journal_last = 308
        self.requests = []
        self.tabs = {title: {'title': title, 'sheetId': i, 'gridProperties': {'rowCount': 1004}}
                     for i, title in enumerate(['Журнал', 'Борги', 'Жовтень', 'Листопад'])}
        self.data = {"'Журнал'!A4:G4": [HEADER], "'Борги'!A4:F53": [],
                     "'Журнал'!A5:G1004": [entry(date(2026, 11, 1), category=CATEGORIES[4], value=100, account=ACCOUNTS[0])]}
        for title in ['Жовтень', 'Листопад']:
            self.data[f"'{title}'!A1:A1"] = [[f'Особистий бюджет — {title.lower()} 2026']]
            self.data[f"'{title}'!A16:E30"] = [['Фіксовані' if i < 4 else 'Гнучкі', c, 0, 1000]
                                                            for i, c in enumerate(CATEGORIES)]
            self.data[f"'{title}'!A36:E43"] = [[a, 'Накопичення' if i == 1 else 'Операційний',
                                               1000, 'Ні' if i == 1 else 'Так'] for i, a in enumerate(ACCOUNTS)]
            self.data[f"'{title}'!K21:K21"] = [[0]]

    def request(self, method, suffix='', **kwargs):
        self.requests.append((method, suffix, kwargs))
        if suffix.startswith('/values/'):
            self.assert_render(kwargs)
            return {'values': self.data[unquote(suffix[len('/values/'):])]}
        if kwargs.get('params', {}).get('includeGridData') == 'true':
            return {'sheets': [{'data': [{'rowData': []}]}]}
        return {'sheets': [{'properties': props} for props in self.tabs.values()]}

    def assert_render(self, kwargs):
        if kwargs['params']['valueRenderOption'] != 'UNFORMATTED_VALUE':
            raise AssertionError('Numeric values are required')


class SheetTests(unittest.TestCase):
    def setUp(self):
        self.sheet = ReadOnlySheets()

    def test_live_layout_and_actual_cash_are_read_without_writes(self):
        self.sheet.validate()
        snap = self.sheet.snapshot()
        self.assertEqual(snap.cash, 6900)
        self.assertEqual(snap.spent, 100)
        self.assertEqual(snap.reserve, 4000)
        self.assertEqual(snap.source, 'Листопад')
        self.assertEqual(snap.inherited, False)
        self.assertTrue(all(method == 'GET' for method, _, _ in self.sheet.requests))

    def test_new_month_and_new_year_do_not_require_restart(self):
        self.sheet.validate()
        self.sheet.today = lambda: date(2026, 12, 1)
        snap = self.sheet.snapshot()
        self.assertEqual(snap.spent, 0)
        self.assertEqual(snap.cash, 6900)
        self.assertTrue(snap.inherited)
        self.sheet.today = lambda: date(2027, 1, 1)
        self.assertIn('Січень 2027', self.sheet.summary())

    def test_yesterday_and_old_drafts_remain_in_same_journal(self):
        self.sheet.today = lambda: date(2026, 11, 1)
        self.sheet.validate_date(date(2026, 10, 31))
        with self.assertRaises(ValueError):
            self.sheet.validate_date(date(2026, 11, 2))
        with self.assertRaises(ValueError):
            self.sheet.validate_date(date(2026, 9, 30))

    def test_new_dated_template_supersedes_inherited_limits(self):
        self.sheet.tabs['Грудень 2026'] = {'title': 'Грудень 2026', 'sheetId': 99}
        self.sheet.data["'Грудень 2026'!A1:A1"] = [['Особистий бюджет — грудень 2026']]
        self.sheet.data["'Грудень 2026'!A16:E30"] = self.sheet.data["'Листопад'!A16:E30"]
        self.sheet.data["'Грудень 2026'!A36:E43"] = self.sheet.data["'Листопад'!A36:E43"]
        self.sheet.today = lambda: date(2026, 12, 1)
        start, title, _, _ = self.sheet.template(self.sheet.today())
        self.assertEqual((start, title), (date(2026, 12, 1), 'Грудень 2026'))

    def test_header_schema_and_duplicate_month_fail_closed(self):
        self.sheet.data["'Журнал'!A4:G4"] = [['wrong']]
        with self.assertRaises(ValueError):
            self.sheet.validate()
        self.sheet.data["'Журнал'!A4:G4"] = [HEADER]
        self.sheet.data["'Листопад'!A16:E30"][0][1] = 'wrong'
        with self.assertRaises(ValueError):
            self.sheet.validate()

    def test_dynamic_journal_keeps_markers_after_row_308(self):
        rows = self.sheet.journal()
        self.assertEqual(len(rows), 1000)
        self.assertEqual(self.sheet.requests[-1][2]['params']['ranges'], "'Журнал'!A5:G1004")

    def test_duplicate_dated_templates_are_rejected(self):
        self.sheet.tabs['2026-11'] = {'title': '2026-11', 'sheetId': 99}
        self.sheet.data["'2026-11'!A1:A1"] = self.sheet.data["'Листопад'!A1:A1"]
        with self.assertRaisesRegex(ValueError, 'кілька'):
            self.sheet.template(self.sheet.today())

    def test_invalid_money_does_not_generate_safe_allowance(self):
        self.sheet.data["'Листопад'!A36:E43"][0][2] = '#REF!'
        with self.assertRaises(ValueError):
            self.sheet.summary()

    def test_http_errors_never_expose_body(self):
        from unittest.mock import Mock
        sheet = Sheets.__new__(Sheets)
        sheet.base = 'https://sheets.googleapis.com/test'
        sheet.session = Mock()
        sheet.session.request.return_value.status_code = 403
        sheet.session.request.return_value.text = 'private response'
        with self.assertRaisesRegex(RuntimeError, '^Google Sheets HTTP 403$'):
            sheet.request('GET')


if __name__ == '__main__':
    unittest.main()
