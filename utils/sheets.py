# utils/sheets.py
import os
import re
import time
import json
import base64
import ast
from typing import Any, Dict, Optional, List, Callable

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


# =========================
# Config
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GSPREAD_CLIENT: Optional[gspread.Client] = None


# =========================
# Backoff
# =========================
def with_backoff(fn: Callable, *args, retries: int = 6, base_sleep: float = 0.6, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            time.sleep(min(base_sleep * (2 ** i), 6.0))
    raise last_err


# =========================
# Credentials parsing
# =========================
def _looks_like_base64(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 40:
        return False
    if s.startswith("eyJ"):  # típico JSON base64
        return True
    if re.fullmatch(r"[A-Za-z0-9+/=\s]+", s) and ("{" not in s):
        return True
    return False


def _try_b64_decode(raw: str) -> Optional[str]:
    try:
        b = base64.b64decode((raw or "").strip())
        return b.decode("utf-8", errors="strict")
    except Exception:
        return None


def _unescape_if_needed(raw: str) -> str:
    """
    Convierte secuencias escapadas \" \\n \\' a caracteres reales.
    """
    raw = raw or ""
    if any(x in raw for x in ("\\n", '\\"', "\\'")):
        try:
            return bytes(raw, "utf-8").decode("unicode_escape")
        except Exception:
            return raw
    return raw


def _normalize_private_key(info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asegura que private_key sea PEM válido con saltos reales.
    """
    pk = info.get("private_key")
    if not pk or not isinstance(pk, str):
        raise RuntimeError(
            "Credenciales inválidas: falta 'private_key' en GOOGLE_CREDENTIALS_JSON. "
            "Asegúrate de pegar el JSON de *Service Account* (no OAuth client)."
        )

    pk = pk.strip()

    # Caso común: quedó con \\n literal
    if "\\n" in pk:
        pk = pk.replace("\\n", "\n")

    # Caso: PEM aplastado (sin saltos)
    if "-----BEGIN PRIVATE KEY-----" in pk and "-----END PRIVATE KEY-----" in pk and "\n" not in pk:
        pk = pk.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n")
        pk = pk.replace("-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----\n")

    # Validación mínima
    if "-----BEGIN PRIVATE KEY-----" not in pk:
        raise RuntimeError(
            "private_key no parece PEM válido (no contiene 'BEGIN PRIVATE KEY'). "
            "Esto ocurre cuando el JSON se pegó incompleto o modificado en Render."
        )

    info["private_key"] = pk
    return info


def _parse_service_account(raw: str) -> Dict[str, Any]:
    if not raw or not raw.strip():
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON está vacío.")

    raw0 = raw.strip()

    # 1) Base64 (opcional)
    if _looks_like_base64(raw0):
        dec = _try_b64_decode(raw0)
        if dec:
            raw0 = dec.strip()

    # 2) Quitar comillas externas si las trae
    if (raw0.startswith('"') and raw0.endswith('"')) or (raw0.startswith("'") and raw0.endswith("'")):
        raw0 = raw0[1:-1].strip()

    # 3) Des-escapar si aplica
    raw1 = _unescape_if_needed(raw0)

    # 4) Intentar JSON
    try:
        data = json.loads(raw1)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # 5) Reparación común
    raw2 = raw1.replace("\\'", "'").replace('\\"', '"')

    try:
        data = json.loads(raw2)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # 6) Dict estilo Python
    try:
        data = ast.literal_eval(raw2)
        if isinstance(data, dict):
            return data
    except Exception as e:
        snippet = raw2[:220].replace("\n", "\\n")
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON no se pudo parsear como JSON.\n"
            "TIP: pega el JSON del service account tal cual (con comillas dobles).\n"
            f"Detalle parse: {e}\n"
            f"Inicio del valor: {snippet}"
        )


def _load_service_account_info() -> Dict[str, Any]:
    raw = (
        os.environ.get("GOOGLE_CREDENTIALS_JSON")
        or os.environ.get("GOOGLE_CREDENTIALS")
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        or ""
    ).strip()

    info = _parse_service_account(raw)

    # Validación/normalización de llave
    info = _normalize_private_key(info)

    # Campos típicos requeridos por google-auth
    for k in ("client_email", "token_uri"):
        if not info.get(k):
            raise RuntimeError(f"Credenciales inválidas: falta '{k}' en GOOGLE_CREDENTIALS_JSON.")

    return info


# =========================
# Gspread
# =========================
def get_gspread_client() -> gspread.Client:
    global _GSPREAD_CLIENT
    if _GSPREAD_CLIENT is not None:
        return _GSPREAD_CLIENT

    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _GSPREAD_CLIENT = gspread.authorize(creds)
    return _GSPREAD_CLIENT


def open_spreadsheet(sheet_name_or_key: str):
    if not sheet_name_or_key or not sheet_name_or_key.strip():
        raise RuntimeError("Falta GOOGLE_SHEET_NAME (nombre/id del spreadsheet).")

    s = sheet_name_or_key.strip()

    # Si es URL, extraer key
    if "docs.google.com" in s and "/spreadsheets/d/" in s:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
        if m:
            s = m.group(1)

    gc = get_gspread_client()

    # Si parece key, abrir por key
    if re.fullmatch(r"[a-zA-Z0-9-_]{25,}", s):
        return with_backoff(gc.open_by_key, s)

    return with_backoff(gc.open, s)


def open_worksheet(spreadsheet, tab_name: str):
    if not tab_name or not tab_name.strip():
        raise RuntimeError("Nombre de pestaña vacío.")
    return with_backoff(spreadsheet.worksheet, tab_name.strip())


# =========================
# Sheet utils
# =========================
def build_header_map(ws) -> Dict[str, int]:
    headers = with_backoff(ws.row_values, 1)
    hmap: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if key:
            hmap[key] = i
    return hmap


def col_idx(hmap: Dict[str, int], col_name: str) -> Optional[int]:
    return hmap.get((col_name or "").strip())


def get_all_values_safe(ws) -> List[List[str]]:
    try:
        return with_backoff(ws.get_all_values)
    except Exception:
        return []


def row_to_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for i, h in enumerate(headers):
        k = (h or "").strip()
        if not k:
            continue
        d[k] = (row[i] if i < len(row) else "").strip()
    return d


def find_row_by_col_value(values: List[List[str]], col_name: str, value: str) -> Optional[int]:
    if not values or len(values) < 2:
        return None
    headers = values[0]
    try:
        col = headers.index(col_name)
    except ValueError:
        return None

    target = (value or "").strip()
    for i in range(1, len(values)):
        row = values[i]
        cell = (row[col] if col < len(row) else "").strip()
        if cell == target:
            return i
    return None


def find_row_by_value(ws, col_name: str, value: str, hmap: Optional[Dict[str, int]] = None) -> Optional[int]:
    if hmap is None:
        hmap = build_header_map(ws)

    c = col_idx(hmap, col_name)
    if not c:
        return None

    values = get_all_values_safe(ws)
    if not values or len(values) < 2:
        return None

    target = (value or "").strip()
    for r_i in range(2, len(values) + 1):
        row = values[r_i - 1]
        cell = (row[c - 1] if (c - 1) < len(row) else "").strip()
        if cell == target:
            return r_i
    return None


def update_row_cells(ws, row_num: int, updates: Dict[str, Any], hmap: Optional[Dict[str, int]] = None):
    if not updates:
        return

    if hmap is None:
        hmap = build_header_map(ws)

    cells: List[Cell] = []
    for k, v in updates.items():
        c = col_idx(hmap, k)
        if not c:
            continue
        cells.append(Cell(row=row_num, col=c, value=str(v)))

    if not cells:
        return

    with_backoff(ws.update_cells, cells, value_input_option="USER_ENTERED")
