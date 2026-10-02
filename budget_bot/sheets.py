"""Write literal cells atomically with an operation note; retain all formatting."""
import json
from urllib.parse import quote
from core import HEADER, ACCOUNTS, CATEGORIES


class Sheets:
    def __init__(self, spreadsheet_id, credentials_json, main_tab):
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import AuthorizedSession
        credentials = Credentials.from_service_account_info(
            json.loads(credentials_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
        self.session = AuthorizedSession(credentials, refresh_timeout=30)
        self.spreadsheet_id = spreadsheet_id
        self.main_tab = main_tab
        self.base = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
        self.journal_id = None

    def request(self, method, suffix="", **kwargs):
        response = self.session.request(method, self.base + suffix, timeout=30, **kwargs)
        if response.status_code >= 400:
            # Never log response bodies (they can include private finance data).
            raise RuntimeError(f"Google Sheets HTTP {response.status_code}")
        return response.json()

    def values(self, range_name):
        return self.request("GET", "/values/" + quote(range_name, safe=""),
                            params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])

    def validate(self):
        meta = self.request("GET", params={"fields": "sheets(properties(sheetId,title,gridProperties))"})
        tabs = {s["properties"]["title"]: s["properties"] for s in meta["sheets"]}
        if set(tabs) != {self.main_tab, "Журнал", "Борги"}:
            raise ValueError("Очікуються тільки вкладки місяця, Журнал і Борги.")
        self.journal_id = tabs["Журнал"]["sheetId"]
        if tabs["Журнал"]["gridProperties"]["rowCount"] < 308:
            raise ValueError("У журналі замало рядків.")
        if self.values("'Журнал'!A4:G4") != [HEADER]:
            raise ValueError("Заголовки журналу не відповідають структурі бюджету.")
        name = self.main_tab.replace("'", "''")
        if [r[0] for r in self.values(f"'{name}'!B16:B30")] != CATEGORIES:
            raise ValueError("Категорії таблиці змінилися; онови налаштування бота.")
        if [r[0] for r in self.values(f"'{name}'!A36:A43")] != ACCOUNTS:
            raise ValueError("Рахунки таблиці змінилися; онови налаштування бота.")

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
        return self.cells(5, 308)

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
        name = self.main_tab.replace("'", "''")
        rows = self.values(f"'{name}'!A5:H10")
        def n(r, c):
            return rows[r][c] if len(rows) > r and len(rows[r]) > c else 0
        return (f"📊 {self.main_tab}\nОпераційно: {n(1, 1):,.2f} грн\n"
                f"Банка: {n(2, 1):,.2f} грн\nЛіміти: {n(1, 4):,.2f} грн\n"
                f"Надходження: {n(3, 1):,.2f} грн\nВитрати: {n(4, 1):,.2f} грн\n"
                f"Нерозподілено: {n(2, 4):,.2f} грн\n"
                f"Мені винні: {n(5, 1):,.2f} грн — поза ресурсом бюджету.")

    def day_expenses(self, day):
        from datetime import date
        serial = (day - date(1899, 12, 30)).days
        return [v for v, marker in self.journal() if v[0] == serial and v[1] == "Витрата"]
