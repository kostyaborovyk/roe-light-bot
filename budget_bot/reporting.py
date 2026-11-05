"""Read-only budget calculations; expected income is never a cash resource."""
import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_DOWN

MONTHS = ('Січень', 'Лютий', 'Березень', 'Квітень', 'Травень', 'Червень',
          'Липень', 'Серпень', 'Вересень', 'Жовтень', 'Листопад', 'Грудень')
EPOCH = date(1899, 12, 30)


def number(value, blank=False):
    if blank and value in ('', None):
        return Decimal(0)
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError('У таблиці очікується число. Перевір формули та значення.')
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Некоректне число в таблиці.')
    return result


def serial_day(value):
    value = number(value)
    if value != int(value):
        raise ValueError('Дата в Журналі має бути без часу.')
    try:
        return EPOCH + timedelta(days=int(value))
    except (OverflowError, ValueError):
        raise ValueError('Некоректна дата в Журналі.') from None


def cell(row, index):
    return row[index] if len(row) > index else ''


def money(value):
    return f'{value:,.2f}'.replace(',', ' ') + ' ₴'


@dataclass(frozen=True)
class Category:
    label: str
    fixed: bool
    limit: Decimal
    spent: Decimal

    def risk(self, day):
        if self.spent > self.limit:
            return '🔴 понад ліміт' if self.limit else '🔴 без ліміту'
        if self.fixed:
            return '📌 регулярні'  # Paying rent early is not overspending pace.
        if self.limit and self.spent / self.limit >= Decimal('.8'):
            return '🟡 близько до ліміту'
        if self.limit and self.spent / self.limit > Decimal(day.day) / calendar.monthrange(day.year, day.month)[1]:
            return '🟡 випереджає темп'
        return '🟢 у темпі' if self.limit else '—'


@dataclass
class Snapshot:
    day: date
    source: str
    inherited: bool
    cash: Decimal
    income: Decimal
    spent: Decimal
    categories: list
    reserve: Decimal
    unclassified: Decimal

    @property
    def title(self):
        return f'{MONTHS[self.day.month - 1]} {self.day.year}'

    @property
    def limit(self):
        return sum((c.limit for c in self.categories), Decimal(0))

    @property
    def flexible_left(self):
        return sum((c.limit - c.spent for c in self.categories if not c.fixed), Decimal(0)) - self.unclassified

    @property
    def days_left(self):
        return calendar.monthrange(self.day.year, self.day.month)[1] - self.day.day + 1

    @property
    def daily(self):
        available = max(Decimal(0), min(self.cash - self.reserve, self.flexible_left))
        return (available / self.days_left).quantize(Decimal('.01'), rounding=ROUND_DOWN)

    def footer(self):
        return f'\n↪ Ліміти перенесені з «{self.source}»; перевір їх.' if self.inherited else ''

    def summary(self):
        used = f'{self.spent / self.limit:.1%}' if self.limit else 'без ліміту'
        return (f'💰 Бюджет · {self.title}\n'
                f'Доступно зараз: {money(self.cash)}\n'
                f'Витрачено: {money(self.spent)} · {used}\n'
                f'Залишок лімітів: {money(self.limit - self.spent)}\n'
                f'Безпечно на день: {money(self.daily)}\n'
                f'Резерв платежів: {money(self.reserve)}' + self.footer())

    def category_text(self, show_all=False):
        lines = [f'📈 {self.title} · ' + ('усі категорії' if show_all else 'категорії з витратами')]
        for c in self.categories:
            if show_all or c.spent:
                lines.append(f'{c.label}: {money(c.spent)} / {money(c.limit)} · {c.risk(self.day)}')
        if len(lines) == 1:
            lines.append('Витрат ще немає.')
        if self.unclassified:
            lines.append(f'⚠️ Поза категоріями: {money(self.unclassified)}')
        return '\n'.join(lines) + self.footer()

    def risks(self):
        lines = [f'⚠️ Ризики · {self.title}']
        if self.cash < self.reserve:
            lines.append(f'Бракує на обов’язкові: {money(self.reserve - self.cash)}')
        for c in self.categories:
            risk = c.risk(self.day)
            if risk.startswith(('🔴', '🟡')):
                lines.append(f'{c.label}: {risk}')
        if self.unclassified:
            lines.append(f'Поза категоріями: {money(self.unclassified)}')
        if len(lines) == 1:
            lines.append('Перевищень лімітів і темпу не виявлено.')
        lines.append('Темп — лише для гнучких витрат. Регулярні перевіряю за лімітом.')
        return '\n'.join(lines) + self.footer()

    def forecast(self):
        return (f'📅 Прогноз · {self.title}\n'
                f'Фактичні кошти: {money(self.cash)}\n'
                f'Обов’язкові попереду: {money(self.reserve)}\n'
                f'Після резерву: {money(self.cash - self.reserve)}\n'
                f'Залишок гнучких лімітів: {money(self.flexible_left)}\n'
                f'Днів із сьогодні: {self.days_left}\n'
                f'Безпечно на день: {money(self.daily)}\n'
                'Резерв — залишки фіксованих лімітів, інші обов’язкові й активні борги до кінця місяця.\n'
                'Це оцінка без календаря платежів. Банка, очікувані надходження й борги мені не враховані.'
                + self.footer())


def calculate(day, start, source, category_rows, account_rows, journal, debts, other=0):
    """Roll account openings forward through the single journal, then isolate this month."""
    if start > day or start.day != 1:
        raise ValueError('Немає початкових залишків для цієї дати.')
    balances, included = {}, set()
    for row in account_rows:
        name = cell(row, 0)
        balances[name] = number(cell(row, 2))
        if cell(row, 3) not in ('Так', 'Ні'):
            raise ValueError('Перевір позначки «В бюджет?» рахунків.')
        if cell(row, 3) == 'Так' and cell(row, 1) != 'Накопичення':
            included.add(name)
    spent_by, spent, income = {}, Decimal(0), Decimal(0)
    first = day.replace(day=1)
    for row in journal:
        if not any(v != '' for v in row):
            continue
        entry_day = serial_day(cell(row, 0))
        if not start <= entry_day <= day:
            continue
        kind, category, account, target = cell(row, 1), cell(row, 2), cell(row, 5), cell(row, 6)
        value = number(cell(row, 4))
        if value < 0 or account not in balances or kind not in ('Витрата', 'Надходження', 'Переказ'):
            raise ValueError('Перевір тип, суму та рахунок операції в Журналі.')
        if kind == 'Надходження':
            balances[account] += value
            if entry_day >= first:
                income += value
        else:
            balances[account] -= value
            if kind == 'Переказ':
                if target not in balances or account == target:
                    raise ValueError('Перевір рахунок отримувача переказу.')
                balances[target] += value
            elif entry_day >= first:
                spent += value
                spent_by[category] = spent_by.get(category, Decimal(0)) + value
    categories = []
    for row in category_rows:
        group, label, limit = cell(row, 0), cell(row, 1), number(cell(row, 3), blank=True)
        if group not in ('Фіксовані', 'Гнучкі') or limit < 0:
            raise ValueError('Перевір групи та ліміти категорій.')
        categories.append(Category(label, group == 'Фіксовані', limit, spent_by.pop(label, Decimal(0))))
    reserve = sum((max(Decimal(0), c.limit - c.spent) for c in categories if c.fixed), Decimal(0))
    reserve += number(other, blank=True)
    if reserve < 0 or number(other, blank=True) < 0:
        raise ValueError('Резерв обов’язкових платежів не може бути від’ємним.')
    end = day.replace(day=calendar.monthrange(day.year, day.month)[1]) + timedelta(days=1)
    for row in debts:
        if cell(row, 0) == 'Я винен' and cell(row, 5) == 'Активний':
            due = cell(row, 4)
            # Undated debt is conservatively reserved too; overdue debt stays reserved.
            if due in ('', None, 0) or serial_day(due) < end:
                debt = number(cell(row, 2))
                if debt < 0:
                    raise ValueError('Від’ємна сума боргу.')
                reserve += debt
    return Snapshot(day, source, start != first,
                    sum((balances[a] for a in included), Decimal(0)), income, spent,
                    categories, reserve, sum(spent_by.values(), Decimal(0)))
