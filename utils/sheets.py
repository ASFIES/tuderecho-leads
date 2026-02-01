import os
import json
import time
import random
from typing import Dict, Any, Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GC: Optional[gspread.Client] = None
_SHEET_CACHE: Dict[str, gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str], gspread.Worksheet] = {}

def _load_service_account_info() -> Dict[str, Any]:
    raw = (
        os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    )
    if raw:
        return json.loads(raw)

    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        "Faltan credenciales Google. Define GOOGLE_CREDENTIALS_JSON o "
        "GOOGLE_SERVICE_ACCOUNT_JSON o GOOGLE_APPLICATION_CREDENTIALS."
    )

def get_gspread_client() -> gspread.Client:
    global _GC
    if _GC is not None:
        return _GC
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _GC = gspread.authorize(creds)
    return _GC

def open_spreadsheet(sheet_name: str) -> gspread.Spreadsheet:
    if not sheet_name:
        raise RuntimeError("GOOGLE_SHEET_NAME está vacío.")
    if sheet_name in _SHEET_CACHE:
        return _SHEET_CACHE[sheet_name]
    gc = get_gspread_client()
    sh = gc.open(sheet_name)
    _SHEET_CACHE[sheet_name] = sh
    return sh

def open_worksheet(sh: gspread.Spreadsheet, tab_name: str) -> gspread.Worksheet:
    key = (sh.id, tab_name)
    if key in _WS_CACHE:
        return _WS_CACHE[key]
    ws = sh.worksheet(tab_name)
    _WS_CACHE[key] = ws
    return ws

def _is_rate_limit_error(err: Exception) -> bool:
    if isinstance(err, APIError):
        try:
            code = err.response.status_code
            return code in (429, 503)
        except Exception:
            return False
    return False

def with_backoff(fn, *args, **kwargs):
    max_tries = 6
    base = 0.8
    for i in range(max_tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if _is_rate_limit_error(e):
                sleep_s = base * (2 ** i) + random.random()
                time.sleep(min(sleep_s, 6.0))
                continue
            raise

def build_header_map(ws: gspread.Worksheet) -> Dict[str, int]:
    headers = with_backoff(ws.row_values, 1)
    clean = []
    for h in headers:
        clean.append((h or "").strip())

    # Detecta vacíos / duplicados (esto es CLAVE para evitar errores raros)
    if any(h == "" for h in clean):
        raise RuntimeError(f"En la hoja '{ws.title}' hay encabezados vacíos en la fila 1.")
    seen = set()
    dups = []
    for h in clean:
        if h in seen:
            dups.append(h)
        seen.add(h)
    if dups:
        raise RuntimeError(f"En la hoja '{ws.title}' hay encabezados duplicados: {sorted(set(dups))}")

    return {h: i + 1 for i, h in enumerate(clean)}

def col_idx(hmap: Dict[str, int], name: str) -> Optional[int]:
    return hmap.get((name or "").strip())

def find_row_by_value(ws: gspread.Worksheet, col_name: str, value: str) -> Optional[int]:
    """
    Busca value exacto en la columna col_name.
    Retorna el número de fila (1-based) o None.
    """
    h = build_header_map(ws)
    c = col_idx(h, col_name)
    if not c:
        return None
    col_vals = with_backoff(ws.col_values, c)
    target = (value or "").strip()
    for i, v in enumerate(col_vals, start=1):
        if (v or "").strip() == target:
            return i
    return None

def update_row_cells(ws: gspread.Worksheet, row_num: int, updates: Dict[str, Any]):
    """
    Batch update por headers (atómico a nivel de request).
    """
    h = build_header_map(ws)
    cell_list = []
    for k, v in updates.items():
        k = (k or "").strip()
        if k in h:
            cell_list.append(gspread.Cell(row_num, h[k], str(v)))
    if cell_list:
        with_backoff(ws.update_cells, cell_list, value_input_option="USER_ENTERED")


