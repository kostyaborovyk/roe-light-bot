"""Write literal cells atomically with an operation note; retain all formatting."""
import json
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote
from core import HEADER, ACCOUNTS, CATEGORIES
from reporting import MONTHS, calculate


class Sheets:
    def __init__(self, spreadsheet_id, credentials_json, main_tab=None):
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import AuthorizedSession
        credentials = Credentials.from_service_account_info(
            json.loads(credentials_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
        self.session = AuthorizedSession(credentials, refresh_timeout=30)
        self.spreadsheet_id = spreadsheet_id
        self.main_tab = main_tab
        self.base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
        self.journal_id = None
        self.journal_last = 308
        self.today = lambda: datetime.now(ZoneInfo('Europe/Kyiv')).date()

    def request(self, method, suffix="", **kwargs):
        response = self.session.request(method, self.base + suffix, timeout=30, **kwargs)
        if response.status_code >= 400:
            # Never log response bodies (they can include private finance data).
            raise RuntimeError(f"Google Sheets HTTP {response.status_code}")
        return response.json()

    def values(self, range_name):
        return self.request("GET", "/values/" + quote(range_name, safe=""),
                            params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])

    def metadata(self):
        meta = self.request("GET", params={"fields": "sheets(properties(sheetId,title,gridProperties))"})
        return {s["properties"]["title"]: s["properties"] for s in meta["sheets"]}

    def validate(self):
        tabs = self.metadata()
        required = {"Журнал", "Борги"}
        missing = required - set(tabs)
        if missing:
            raise ValueError("Не знайдено вкладки: " + ", ".join(sorted(missing))
                             + ". Перевір назви та BUDGET_MONTH_TAB у Render.")
        self.journal_id = tabs["Журнал"]["sheetId"]
        self.journal_last = tabs["Журнал"]["gridProperties"]["rowCount"]
        if tabs["Журнал"]["gridProperties"]["rowCount"] < 308:
            raise ValueError("У журналі замало рядків.")
        if self.values("'Журнал'!A4:G4") != [HEADER]:
            raise ValueError("Заголовки журналу не відповідають структурі бюджету.")
        self.template(self.today(), tabs)

    def template(self, day, tabs=None):
        """Use a dated opening balance, never a month name alone (December -> January)."""
        tabs = tabs if tabs is not None else self.metadata()
        candidates = []
        for title in tabs:
            if title == self.main_tab or any(title.casefold().startswith(m.casefold()) for m in MONTHS) or re.fullmatch(r'\d{4}-\d{2}', title):
                name = title.replace("'", "''")
                heading = self.values(f"'{name}'!A1:A1")
                text = str(heading[0][0]) if heading and heading[0] else ''
                matches = [(i + 1, re.search(r'\b(20\d{2})\b', text)) for i, m in enumerate(MONTHS) if m.casefold() in text.casefold()]
                if len(matches) != 1 or not matches[0][1]:
                    continue
                month, year_match = matches[0]
                start = date(int(year_match[1]), month, 1)
                if start <= day:
                    candidates.append((start, title))
        if not candidates:
            raise ValueError('Немає датованої вкладки бюджету для цієї дати. Перевір заголовок A1: місяць і рік.')
        latest = max(s for s, _ in candidates)
        choices = [(s, t) for s, t in candidates if s == latest]
        if len(choices) != 1:
            raise ValueError('Знайдено кілька бюджетів одного місяця. Залиш одну датовану вкладку.')
        start, title = choices[0]
        name = title.replace("'", "''")
        categories = self.values(f"'{name}'!A16:E30")
        accounts = self.values(f"'{name}'!A36:E43")
        if [r[1] if len(r) > 1 else '' for r in categories] != CATEGORIES:
            raise ValueError("Категорії таблиці змінилися; онови налаштування бота.")
        if [r[0] if r else '' for r in accounts] != ACCOUNTS:
            raise ValueError("Рахунки таблиці змінилися; онови налаштування бота.")
        return start, title, categories, accounts

    def validate_date(self, day):
        if day > self.today():
            raise ValueError('Майбутні операції не записуємо — тільки факт.')
        self.template(day)

    def snapshot(self):
        day = self.today()
        tabs = self.metadata()
        self.journal_last = tabs['Журнал']['gridProperties']['rowCount']
        start, title, categories, accounts = self.template(day, tabs)
        name = title.replace("'", "''")
        other = self.values(f"'{name}'!K21:K21")
        return calculate(day, start, title, categories, accounts,
                         self.values(f"'Журнал'!A5:G{self.journal_last}"),
                         self.values("'Борги'!A4:F53"), other[0][0] if other and other[0] else 0)

    def cells(self, first, last):
        data = self.request("GET", params={"ranges": f"'Журнал'!A{first}:G{last}",
            "includeGridData": "true", "fields": "sheets(data(rowData(values(userEnteredValue,note))))"})
        raw = data["sheets"][0].get("data", [{}])[0].get("rowData", [])
        rows = []
        for i in range(last - first + 1):
            cells = raw[i].get("values", []) if i < len(raw) else []
            values = []
            for j in range(7):
                value = cells[j].get("userEnteredValue", {}) if j < len(cells) else {}
                # A formula is occupied; never overwrite it as an empty row.
                values.append(next(iter(value.values()), ""))
            note = cells[3].get("note", "") if len(cells) > 3 else ""
            marker = note.removeprefix("budget-bot:") if note.startswith("budget-bot:") else note
            rows.append((values, marker))
        return rows

    def journal(self):
        # Grid size may have been expanded manually; markers remain in the same journal.
        tabs = self.metadata()
        self.journal_last = tabs['Журнал']['gridProperties']['rowCount']
        return self.cells(5, self.journal_last)

    def row(self, number):
        return self.cells(number, number)[0]

    def write(self, number, values, token):
        cells = [{"userEnteredValue": {"numberValue": v} if isinstance(v, (int, float))
                  else {"stringValue": v}} for v in values]
        # A string beginning '=' stays a string, never a formula.
        cells[3]["note"] = "budget-bot:" + token
        self.request("POST", ":batchUpdate", json={"requests": [
            {"updateCells": {"start": {"sheetId": self.journal_id, "rowIndex": number - 1,
                                       "columnIndex": 0},
                             "rows": [{"values": cells}], "fields": "userEnteredValue"}},
            {"updateCells": {"start": {"sheetId": self.journal_id, "rowIndex": number - 1,
                                       "columnIndex": 3},
                             "rows": [{"values": [{"note": "budget-bot:" + token}]}], "fields": "note"}}
        ]})

    def summary(self):
        return self.snapshot().summary()

    def categories_summary(self, show_all=False):
        return self.snapshot().category_text(show_all)

    def risks(self):
        return self.snapshot().risks()

    def forecast(self):
        return self.snapshot().forecast()

    def day_expenses(self, day):
        from datetime import date
        serial = (day - date(1899, 12, 30)).days
        return [v for v, marker in self.journal() if v[0] == serial and v[1] == "Витрата"]
