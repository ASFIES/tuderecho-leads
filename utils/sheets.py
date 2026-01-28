# utils/sheets.py
import os
import json
import base64
import time
from typing import Dict, Any, Tuple, List

import gspread
from google.oauth2.service_account import Credentials


GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_env_creds_dict() -> Dict[str, Any]:
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        if raw.lstrip().startswith("{"):
            return json.loads(raw)
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)

    if GOOGLE_CREDENTIALS_PATH:
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales: GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH")


def get_gspread_client():
    info = _get_env_creds_dict()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def open_spreadsheet(gc):
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME")
    return gc.open(GOOGLE_SHEET_NAME)


def build_header_map(headers: List[str]) -> Dict[str, int]:
    m = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if not key:
            continue
        if key not in m:
            m[key] = i
        low = key.lower()
        if low not in m:
            m[low] = i
    return m


def col_idx(header_map: Dict[str, int], name: str):
    return header_map.get(name) or header_map.get((name or "").lower())


def with_backoff(func, max_tries=6, base_sleep=0.8):
    """
    Backoff simple para errores 429/5xx de Google.
    """
    last_err = None
    for i in range(max_tries):
        try:
            return func()
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            # Detecta 429 / quota / rate limit
            if ("429" in msg) or ("quota" in msg) or ("rate" in msg):
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last_err


def read_row_range(ws, row_idx: int, min_col: int, max_col: int) -> List[str]:
    """
    Lee una sola fila por rango A1. 1 request.
    """
    a1 = gspread.utils.rowcol_to_a1(row_idx, min_col)
    b1 = gspread.utils.rowcol_to_a1(row_idx, max_col)
    rng = f"{a1}:{b1}"

    def _do():
        values = ws.get(rng)  # retorna [[...]] o []
        return values[0] if values else []

    return with_backoff(_do)


def batch_update_row(ws, row_idx: int, updates: Dict[int, Any]):
    """
    updates: {col_number: value}
    1 request (batch_update).
    """
    data = []
    for col_num, val in updates.items():
        a1 = gspread.utils.rowcol_to_a1(row_idx, col_num)
        data.append({"range": a1, "values": [[val]]})

    if not data:
        return

    def _do():
        ws.batch_update(data)

    with_backoff(_do)

