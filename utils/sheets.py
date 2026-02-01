import os
import json
import time
import base64
import random
from typing import Any, Callable, Optional

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GC = None
_SS_CACHE = {}
_WS_CACHE = {}

def _load_service_account_info() -> dict:
    json_str = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or os.environ.get("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if json_str:
        return json.loads(json_str)

    b64 = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_B64") or os.environ.get("GOOGLE_CREDENTIALS_B64") or "").strip()
    if b64:
        raw = base64.b64decode(b64.encode("utf-8")).decode("utf-8")
        return json.loads(raw)

    path = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales Google. Define GOOGLE_SERVICE_ACCOUNT_JSON o GOOGLE_CREDENTIALS_JSON (o GOOGLE_APPLICATION_CREDENTIALS).")

def get_gspread_client() -> gspread.Client:
    global _GC
    if _GC is not None:
        return _GC
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _GC = gspread.authorize(creds)
    return _GC

def open_spreadsheet(name: str) -> gspread.Spreadsheet:
    if not name:
        raise RuntimeError("GOOGLE_SHEET_NAME vacío.")
    if name in _SS_CACHE:
        return _SS_CACHE[name]
    gc = get_gspread_client()
    sh = gc.open(name)
    _SS_CACHE[name] = sh
    return sh

def open_worksheet(sh: gspread.Spreadsheet, tab_name: str) -> gspread.Worksheet:
    key = f"{sh.id}:{tab_name}"
    if key in _WS_CACHE:
        return _WS_CACHE[key]
    ws = sh.worksheet(tab_name)
    _WS_CACHE[key] = ws
    return ws

def with_backoff(fn: Callable[..., Any], *args, **kwargs) -> Any:
    max_tries = int(os.environ.get("SHEETS_MAX_TRIES", "5"))
    base = float(os.environ.get("SHEETS_BACKOFF_BASE", "0.6"))
    for i in range(max_tries):
        try:
            return fn(*args, **kwargs)
        except Exception:
            if i == max_tries - 1:
                raise
            sleep = base * (2 ** i) + random.random() * 0.25
            time.sleep(sleep)

def get_all_values_safe(ws: gspread.Worksheet):
    return with_backoff(ws.get_all_values)

def header_map(header_row: list[str]) -> dict[str, int]:
    m = {}
    for i, h in enumerate(header_row, start=1):
        key = (h or "").strip()
        if not key:
            continue
        if key not in m:
            m[key] = i
    return m

def row_to_dict(header: list[str], row: list[str]) -> dict:
    d = {}
    for i, h in enumerate(header):
        k = (h or "").strip()
        if not k:
            continue
        d[k] = row[i] if i < len(row) else ""
    return d

def find_row_by_col_value(values: list[list[str]], col_name: str, needle: str) -> Optional[int]:
    if not values or len(values) < 2:
        return None
    header = values[0]
    hmap = header_map(header)
    if col_name not in hmap:
        return None
    j = hmap[col_name] - 1
    needle = (needle or "").strip()
    for i, row in enumerate(values[1:], start=1):
        v = row[j].strip() if j < len(row) else ""
        if v == needle:
            return i  # índice real en values (incluye header)
    return None

def update_row_cells(ws: gspread.Worksheet, values_index: int, updates: dict[str, Any]):
    all_values = get_all_values_safe(ws)
    if not all_values or not all_values[0]:
        raise RuntimeError("Worksheet sin encabezados.")
    hdr = all_values[0]
    hmap = header_map(hdr)

    row_number = values_index + 1  # gspread es 1-based
    cells = []
    for col, val in updates.items():
        if col not in hmap:
            continue
        col_number = hmap[col]
        cells.append(gspread.Cell(row_number, col_number, str(val)))
    if cells:
        with_backoff(ws.update_cells, cells, value_input_option="USER_ENTERED")

