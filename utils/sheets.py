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

# =========================
# SCOPES (CLAVE PARA 403)
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Cache en memoria (por proceso)
_GC: Optional[gspread.Client] = None
_SHEET_CACHE: Dict[str, gspread.Spreadsheet] = {}
_WS_CACHE: Dict[Tuple[str, str], gspread.Worksheet] = {}

# ==========================================================
# CREDENCIALES: soporta múltiples nombres de variables ENV
# ==========================================================
ENV_JSON_CANDIDATES = [
    "GOOGLE_SERVICE_ACCOUNT_JSON",   # ✅ estándar nuevo
    "GOOGLE_CREDENTIALS_JSON",       # ✅ el que tú tienes en Render
    "GOOGLE_CREDENTIALIALS_JSON",    # ✅ por si quedó con typo en Render
    "GOOGLE_CREDE​NTIALS_JSON",       # (cualquier variante rara: se ignora si no existe)
]

ENV_B64_CANDIDATES = [
    "GOOGLE_SERVICE_ACCOUNT_B64",
    "GOOGLE_CREDENTIALS_B64",
    "GOOGLE_CREDENTIALIALS_B64",
]

def _try_json_load(raw: str) -> Optional[Dict[str, Any]]:
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
        return _try_json_load(decoded)
    except Exception:
        return None

def _load_service_account_info() -> Dict[str, Any]:
    """
    Lee credenciales desde:
    - GOOGLE_SERVICE_ACCOUNT_JSON (JSON completo como string)
    - GOOGLE_CREDENTIALS_JSON (compat)
    - GOOGLE_CREDENTIALIALS_JSON (compat typo)
    - GOOGLE_APPLICATION_CREDENTIALS (path a archivo json)
    - o variantes B64 (base64)
    """

    # 1) JSON directo (varios nombres posibles)
    for key in ENV_JSON_CANDIDATES:
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_json_load(raw)
            if info:
                return info
            # a veces lo guardan como base64 sin querer
            info = _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(
                f"La variable {key} existe pero NO es JSON válido (ni base64 JSON). "
                "Verifica que pegaste el JSON completo del Service Account."
            )

    # 2) Base64 explícito
    for key in ENV_B64_CANDIDATES:
        raw = os.environ.get(key, "").strip()
        if raw:
            info = _try_b64_json(raw)
            if info:
                return info
            raise RuntimeError(
                f"La variable {key} existe pero NO se pudo decodificar como base64 JSON."
            )

    # 3) Path a archivo
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # 4) Nada encontrado
    raise RuntimeError(
        "Faltan credenciales Google. Define una de estas variables:\n"
        "- GOOGLE_CREDENTIALS_JSON (tu Render actual)\n"
        "- GOOGLE_SERVICE_ACCOUNT_JSON\n"
        "- GOOGLE_APPLICATION_CREDENTIALS (ruta a json)\n"
        "- GOOGLE_CREDENTIALS_B64 / GOOGLE_SERVICE_ACCOUNT_B64\n"
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
    Abre por NOMBRE.
    Asegúrate de compartir el Google Sheet con el email del Service Account.
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
