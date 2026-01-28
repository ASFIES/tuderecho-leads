import os
import json
import time
import random
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# SCOPES (CLAVE PARA 403)
# =========================
# Esto evita: "Request had insufficient authentication scopes"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Cache en memoria (por proceso)
_GC: Optional[gspread.Client] = None
_SHEET_CACHE: Dict[str, gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str], gspread.Worksheet] = {}

def _load_service_account_info() -> Dict[str, Any]:
    """
    Lee credenciales desde:
    - GOOGLE_SERVICE_ACCOUNT_JSON (JSON completo como string)
    o
    - GOOGLE_APPLICATION_CREDENTIALS (path a archivo json)
    """
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        return json.loads(raw)

    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        "Faltan credenciales Google. Define GOOGLE_SERVICE_ACCOUNT_JSON "
        "o GOOGLE_APPLICATION_CREDENTIALS."
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
    """
    Abre por NOMBRE (como ya lo usas).
    Tip: asegúrate de compartir el Sheet con el email del Service Account.
    """
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
    """
    Backoff para evitar que el Worker se muera con 429.
    """
    max_tries = 6
    base = 0.8
    for i in range(max_tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if _is_rate_limit_error(e):
                sleep_s = base * (2 ** i) + random.random()
                time.sleep(sleep_s)
                continue
            raise

def get_all_records_cached(ws: gspread.Worksheet, cache_seconds: int = 15) -> List[Dict[str, Any]]:
    """
    Cache corto para lecturas repetidas del worker (reduce 429).
    """
    now = time.time()
    cache_key = (ws.id, "all_records")
    if not hasattr(get_all_records_cached, "_cache"):
        get_all_records_cached._cache = {}  # type: ignore

    c = get_all_records_cached._cache  # type: ignore
    if cache_key in c:
        ts, data = c[cache_key]
        if now - ts <= cache_seconds:
            return data

    data = with_backoff(ws.get_all_records)
    c[cache_key] = (now, data)
    return data
