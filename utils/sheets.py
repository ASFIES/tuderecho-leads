import os
import json
import time
from functools import lru_cache
from typing import Dict, Any, Optional, List

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]



def _sleep_backoff(attempt: int):
    # 0, 1, 2, 4, 8 segundos aprox
    time.sleep(min(8, (2 ** max(0, attempt - 1))))


def _is_quota_error(e: Exception) -> bool:
    msg = str(e)
    return "429" in msg or "Quota exceeded" in msg or "Read requests" in msg


@lru_cache(maxsize=1)
def get_gspread_client():
    """
    Usa credenciales desde:
    - GOOGLE_CREDENTIALS_JSON (json completo)
    - o GOOGLE_APPLICATION_CREDENTIALS (path)
    """
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)

    if creds_path:
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Faltan credenciales: GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS.")


def open_spreadsheet(sheet_name: str):
    gc = get_gspread_client()
    for attempt in range(1, 6):
        try:
            return gc.open(sheet_name)
        except APIError as e:
            if _is_quota_error(e) and attempt < 6:
                _sleep_backoff(attempt)
                continue
            raise


def open_worksheet(sh, title: str):
    for attempt in range(1, 6):
        try:
            return sh.worksheet(title)
        except APIError as e:
            if _is_quota_error(e) and attempt < 6:
                _sleep_backoff(attempt)
                continue
            raise


# -------------------------
# Cache simple por worksheet
# -------------------------
_cache = {}  # key -> (expires_at, data)


def get_all_records_cached(ws, ttl_seconds: int = 30) -> List[Dict[str, Any]]:
    key = f"{ws.spreadsheet.id}:{ws.title}"
    now = time.time()
    hit = _cache.get(key)
    if hit and hit[0] > now:
        return hit[1]

    for attempt in range(1, 6):
        try:
            data = ws.get_all_records()
            _cache[key] = (now + ttl_seconds, data)
            return data
        except APIError as e:
            if _is_quota_error(e) and attempt < 6:
                _sleep_backoff(attempt)
                continue
            raise


def find_row_by_value(ws, header_name: str, value: str) -> Optional[int]:
    """
    Busca la fila (index 2..n) donde header_name == value
    Minimiza lecturas: obtiene headers + columna completa.
    """
    headers = ws.row_values(1)
    if header_name not in headers:
        return None
    col = headers.index(header_name) + 1

    for attempt in range(1, 6):
        try:
            col_vals = ws.col_values(col)
            break
        except APIError as e:
            if _is_quota_error(e) and attempt < 6:
                _sleep_backoff(attempt)
                continue
            raise

    target = str(value).strip()
    for i, v in enumerate(col_vals, start=1):
        if i == 1:
            continue
        if str(v).strip() == target:
            return i
    return None


def safe_update_cells(ws, row_idx: int, updates: Dict[str, Any]):
    """
    Actualiza varias columnas en 1 batch (menos cuota).
    updates: {"ColName": "valor"}
    """
    headers = ws.row_values(1)
    cells = []
    for k, v in updates.items():
        if k not in headers:
            continue
        col = headers.index(k) + 1
        cells.append(gspread.Cell(row_idx, col, str(v)))

    if not cells:
        return

    for attempt in range(1, 6):
        try:
            ws.update_cells(cells, value_input_option="USER_ENTERED")
            return
        except APIError as e:
            if _is_quota_error(e) and attempt < 6:
                _sleep_backoff(attempt)
                continue
            raise
