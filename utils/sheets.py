# utils/sheets.py
import os
import json
import time
import random
import base64
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Cache en memoria (por proceso)
_GC: Optional[gspread.Client] = None
_SHEET_CACHE: Dict[str, gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str], gspread.Worksheet] = {}


def _try_json(raw: str) -> Optional[Dict[str, Any]]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _try_b64_json(raw: str) -> Optional[Dict[str, Any]]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="ignore")
        return _try_json(decoded)
    except Exception:
        return None


def _load_service_account_info() -> Dict[str, Any]:
    """
    Lee credenciales desde:
    - GOOGLE_CREDENTIALS_JSON (Render actual)
    - GOOGLE_SERVICE_ACCOUNT_JSON (estándar)
    - GOOGLE_CREDENTIALIALS_JSON (compat typo)
    - GOOGLE_APPLICATION_CREDENTIALS (path)
    - o variantes B64
    """
    # JSON directo (varios nombres posibles)
    for key in ("GOOGLE_CREDENTIALS_JSON", "GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_CREDENTIALIALS_JSON"):
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_json(raw)
            if info:
                return info
            info = _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(
                f"La variable {key} existe pero NO es JSON válido (ni base64). "
                "Pega el JSON completo del Service Account."
            )

    # Base64 explícito
    for key in ("GOOGLE_CREDENTIALS_B64", "GOOGLE_SERVICE_ACCOUNT_B64", "GOOGLE_CREDENTIALIALS_B64"):
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(f"La variable {key} existe pero no es base64 JSON válido.")

    # Path a archivo
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        "Faltan credenciales Google. Define:\n"
        "- GOOGLE_CREDENTIALS_JSON (recomendado en tu Render)\n"
        "- o GOOGLE_SERVICE_ACCOUNT_JSON\n"
        "- o GOOGLE_APPLICATION_CREDENTIALS (ruta a json)\n"
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
                time.sleep(sleep_s)
                continue
            raise


def get_all_records_cached(ws: gspread.Worksheet, cache_seconds: int = 15) -> List[Dict[str, Any]]:
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
