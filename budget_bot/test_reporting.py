import unittest
from datetime import date
from decimal import Decimal
from reporting import Category, calculate, serial_day


def entry(day, kind='Витрата', category='Їжа', value=100, account='Картка', target=''):
    return [(day - date(1899, 12, 30)).days, kind, category, '', value, account, target]


class ReportingTests(unittest.TestCase):
    def snapshot(self, day=date(2026, 11, 5), start=date(2026, 11, 1), entries=(), debts=(), cash=10000,
                 other=0, food_limit=5000, fixed_limit=3000):
        return calculate(day, start, 'Листопад',
                         [['Фіксовані', 'Оренда', 0, fixed_limit], ['Гнучкі', 'Їжа', 0, food_limit]],
                         [['Картка', 'Операційний', cash, 'Так'], ['Банка', 'Накопичення', 50000, 'Ні']],
                         entries, debts, other)

    def test_actual_cash_reserve_and_flexible_cap(self):
        snap = self.snapshot(entries=[entry(date(2026, 11, 2), value=500)])
        self.assertEqual(snap.cash, 9500)
        self.assertEqual(snap.reserve, 3000)
        self.assertEqual(snap.daily, Decimal('173.07'))  # 4500 / 26, rounded down
        self.assertLessEqual(snap.daily * snap.days_left, 4500)

    def test_savings_transfers_and_actual_income(self):
        snap = self.snapshot(entries=[entry(date(2026, 11, 2), 'Переказ', '', 2000, target='Банка'),
                                      entry(date(2026, 11, 3), 'Надходження', '', 500)])
        self.assertEqual(snap.cash, 8500)
        self.assertEqual(snap.spent, 0)
        self.assertEqual(snap.income, 500)
        self.assertEqual(snap.daily, Decimal('192.30'))

    def test_cash_is_binding_and_never_counts_unreceived_partner_help(self):
        snap = self.snapshot(cash=3500)
        self.assertEqual(snap.daily, Decimal('19.23'))
        self.assertEqual(self.snapshot(cash=2000).daily, 0)
        self.assertIn('Бракує', self.snapshot(cash=-1).risks())

    def test_partial_fixed_payments_reduce_reserve_without_pace_alarm(self):
        snap = self.snapshot(entries=[entry(date(2026, 11, 1), category='Оренда', value=2000)])
        self.assertEqual(snap.reserve, 1000)
        self.assertNotIn('Оренда:', snap.risks())
        self.assertIn('📌 регулярні', snap.category_text())

    def test_flexible_pace_near_limit_and_overspend(self):
        day = date(2026, 11, 5)
        for spent, expected in [(1000, 'темп'), (4000, 'близько'), (5001, 'понад')]:
            with self.subTest(spent=spent):
                self.assertIn(expected, Category('Їжа', False, Decimal(5000), Decimal(spent)).risk(day))
        self.assertIn('без ліміту', Category('Інше', False, Decimal(0), Decimal(1)).risk(day))
        self.assertIn('понад', Category('Оренда', True, Decimal(100), Decimal(101)).risk(day))

    def test_empty_categories_and_show_all(self):
        snap = self.snapshot()
        self.assertIn('Витрат ще немає', snap.category_text())
        self.assertNotIn('Оренда:', snap.category_text())
        self.assertIn('Оренда:', snap.category_text(True))
        self.assertLess(len(snap.summary().splitlines()), 8)

    def test_rollover_and_old_entries_are_not_month_expenses(self):
        snap = self.snapshot(day=date(2026, 12, 1), entries=[entry(date(2026, 11, 30), value=1000),
                                                           entry(date(2026, 12, 1), value=500)])
        self.assertEqual(snap.cash, 8500)
        self.assertEqual(snap.spent, 500)
        self.assertEqual(snap.categories[1].spent, 500)
        self.assertTrue(snap.inherited)
        self.assertIn('Грудень 2026', snap.summary())
        self.assertIn('перенесені', snap.summary())

    def test_december_january_and_leap_year(self):
        snap = self.snapshot(day=date(2027, 1, 1), entries=[entry(date(2026, 12, 31), value=500)])
        self.assertEqual(snap.spent, 0)
        self.assertEqual(snap.cash, 9500)
        self.assertEqual(snap.days_left, 31)
        leap = self.snapshot(day=date(2028, 2, 29))
        self.assertEqual(leap.days_left, 1)
        self.assertEqual(leap.daily, 5000)

    def test_overdue_and_undated_debts_reserved_receivables_excluded(self):
        def debt(kind, due, value=100, status='Активний'):
            return [kind, '', value, '', (due - date(1899, 12, 30)).days if due else '', status]
        debts = [debt('Я винен', date(2026, 10, 1)), debt('Я винен', date(2026, 11, 30)),
                 debt('Я винен', None), debt('Я винен', date(2026, 12, 1)),
                 debt('Мені винні', None, 7000), debt('Я винен', None, status='Закритий')]
        self.assertEqual(self.snapshot(debts=debts, other=50).reserve, 3350)

    def test_unclassified_expenses_reduce_flexible_resource(self):
        snap = self.snapshot(entries=[entry(date(2026, 11, 1), category='невідома', value=100)])
        self.assertEqual(snap.unclassified, 100)
        self.assertEqual(snap.flexible_left, 4900)
        self.assertIn('Поза категоріями', snap.risks())

    def test_future_entries_are_excluded(self):
        snap = self.snapshot(entries=[entry(date(2026, 11, 6), 'Надходження', '', 99999)])
        self.assertEqual(snap.cash, 10000)
        self.assertEqual(snap.income, 0)

    def test_malformed_inputs_fail_closed(self):
        for fields in [{'cash': '#REF!'}, {'other': -1}, {'food_limit': '5000'},
                       {'entries': [['not a date', 'Витрата']]},
                       {'entries': [entry(date(2026, 11, 1), account='невідомий')]},
                       {'entries': [entry(date(2026, 11, 1), value=float('nan'))]}]:
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.snapshot(**fields)
        with self.assertRaises(ValueError):
            serial_day(1000000000)


if __name__ == '__main__':
    unittest.main()
