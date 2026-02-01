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

# Cache por proceso
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
    """
    Soporta distintos nombres de env (Render / legacy):
    - GOOGLE_CREDENTIALS_JSON (tu Render actual)
    - GOOGLE_SERVICE_ACCOUNT_JSON (estándar)
    - GOOGLE_CREDENTIALIALS_JSON (por si quedó typo)
    - GOOGLE_APPLICATION_CREDENTIALS (path)
    y variantes B64.
    """
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

