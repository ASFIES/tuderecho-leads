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

_GC: Optional[gspread.Client] = None
_SHEET_CACHE: Dict[str, gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str], gspread.Worksheet] = {}
_RECORDS_CACHE: Dict[Tuple[str, int], Tuple[float, List[Dict[str, Any]]]] = {}

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
    for key in ("GOOGLE_CREDENTIALS_JSON", "GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_CREDENTIALIALS_JSON"):
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_json(raw) or _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(f"{key} existe pero no es JSON válido (ni base64).")

    for key in ("GOOGLE_CREDENTIALS_B64", "GOOGLE_SERVICE_ACCOUNT_B64", "GOOGLE_CREDENTIALIALS_B64"):
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(f"{key} existe pero no es base64 JSON válido.")

    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        "Faltan credenciales Google. Define GOOGLE_CREDENTIALS_JSON o GOOGLE_SERVICE_ACCOUNT_JSON "
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
            return err.response.status_code in (429, 503)
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
                time.sleep(base * (2 ** i) + random.random())
                continue
            raise

def _make_unique_headers(headers: List[str]) -> Tuple[List[str], List[str]]:
    seen: Dict[str, int] = {}
    unique: List[str] = []
    dups: List[str] = []

    for idx, h in enumerate(headers, start=1):
        name = (h or "").strip() or f"COL_{idx}"
        base = name
        if base in seen:
            seen[base] += 1
            name = f"{base}__{seen[base]}"
            dups.append(base)
        else:
            seen[base] = 1
        unique.append(name)

    dups = sorted(list(set(dups)))
    return unique, dups

def get_records(ws: gspread.Worksheet, header_row: int = 1) -> List[Dict[str, Any]]:
    values = with_backoff(ws.get_all_values)
    if not values or len(values) < header_row:
        return []

    headers_raw = values[header_row - 1]
    headers, dups = _make_unique_headers(headers_raw)

    if dups:
        print(f"[sheets] WARNING: headers duplicados en '{ws.title}': {dups}")

    out: List[Dict[str, Any]] = []
    for row in values[header_row:]:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[: len(headers)]

        rec = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        if any(str(v).strip() for v in rec.values()):
            out.append(rec)
    return out

def get_records_cached(ws: gspread.Worksheet, cache_seconds: int = 10) -> List[Dict[str, Any]]:
    key = (ws.id, 1)
    now = time.time()
    if key in _RECORDS_CACHE:
        ts, data = _RECORDS_CACHE[key]
        if now - ts <= cache_seconds:
            return data
    data = get_records(ws, header_row=1)
    _RECORDS_CACHE[key] = (now, data)
    return data


