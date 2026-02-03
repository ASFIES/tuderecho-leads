# utils/sheets.py
import os
import json
import ast
import base64
import time
import random
from typing import Any, Dict, Optional, List, Tuple

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError


# =========================
# Backoff / Retry helper
# =========================
def with_backoff(fn, *args, retries: int = 6, base: float = 0.6, jitter: float = 0.25, **kwargs):
    """
    Ejecuta fn(*args, **kwargs) con reintentos exponenciales.
    Útil para errores temporales de Google Sheets / red.
    """
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep = (base * (2 ** i)) + random.uniform(0, jitter)
            time.sleep(sleep)
    raise last_err


# =========================
# Credentials loader (ENV)
# =========================
def _load_service_account_info() -> Dict[str, Any]:
    """
    Lee credenciales desde variables de entorno (Render):
      - GOOGLE_CREDENTIALS_JSON  (recomendado)
      - GOOGLE_CREDENTIALS_B64   (opcional: JSON en base64)
      - GOOGLE_CREDENTIALS_JSON_BASE64 (alias opcional)

    Soporta:
      - JSON válido con comillas dobles
      - "dict" estilo Python con comillas simples (lo convierte con ast.literal_eval)
    """
    raw = (os.environ.get("GOOGLE_CREDENTIALS_JSON") or "").strip()

    # Alternativa: base64
    b64 = (os.environ.get("GOOGLE_CREDENTIALS_B64") or os.environ.get("GOOGLE_CREDENTIALS_JSON_BASE64") or "").strip()

    if not raw and b64:
        try:
            decoded = base64.b64decode("".join(b64.split()), validate=True).decode("utf-8")
            raw = decoded.strip()
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_B64 inválido/base64 corrupto. Detalle: {e}")

    if not raw:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON (o GOOGLE_CREDENTIALS_B64) en variables de entorno.")

    # Quita comillas externas si Render lo guardó como string envuelto
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1].strip()

    # A veces Render/pegado incluye \\n literales; esto no rompe JSON en general,
    # pero si te lo pegaron como texto plano raro, ayuda a normalizar.
    # (No afecta si ya viene bien.)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    # Si NO parece JSON y NO está en dict, intentar base64 “embebido”
    # (por si alguien pegó el base64 en GOOGLE_CREDENTIALS_JSON)
    if not raw.lstrip().startswith("{"):
        try:
            decoded = base64.b64decode("".join(raw.split()), validate=True).decode("utf-8")
            raw = decoded.strip()
        except Exception:
            # Si no era base64, seguimos igual; luego fallará con mensaje claro.
            pass

    # 1) JSON normal (comillas dobles)
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # 2) Fallback: dict Python (comillas simples) => ast.literal_eval
        try:
            info = ast.literal_eval(raw)
        except Exception as e:
            raise RuntimeError(
                "GOOGLE_CREDENTIALS_JSON no es JSON válido ni dict Python válido. "
                f"Ejemplo esperado: {{\"type\":\"service_account\", ...}}. Detalle: {e}"
            )

    if not isinstance(info, dict):
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON no produjo un dict válido.")

    # Validación mínima
    if info.get("type") != "service_account":
        # No forzamos hard-fail porque hay casos raros, pero avisamos
        # (Si quieres hard-fail, cambia a raise RuntimeError)
        pass

    return info


# =========================
# gspread client (cached)
# =========================
_GC = None  # cache global

def get_gspread_client(scopes: Optional[List[str]] = None) -> gspread.Client:
    """
    Crea y cachea el cliente de gspread usando GOOGLE_CREDENTIALS_JSON.
    """
    global _GC
    if _GC is not None:
        return _GC

    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

    info = _load_service_account_info()
    try:
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        _GC = gspread.authorize(creds)
        return _GC
    except (GoogleAuthError, ValueError) as e:
        raise RuntimeError(f"No se pudo autorizar con Google Credentials. Detalle: {e}")


def open_spreadsheet(sheet_name: str):
    """
    Abre un Google Sheet por nombre.
    """
    if not sheet_name:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")
    gc = get_gspread_client()
    return with_backoff(gc.open, sheet_name)


def open_worksheet(sh, tab_name: str):
    """
    Abre una hoja dentro del spreadsheet por nombre de pestaña.
    """
    if not tab_name:
        raise RuntimeError("Nombre de pestaña vacío.")
    return with_backoff(sh.worksheet, tab_name)


# =========================
# Sheet helpers
# =========================
def get_all_values_safe(ws) -> List[List[str]]:
    """
    Obtiene todos los valores (incluye encabezados).
    Si falla, regresa [].
    """
    try:
        return with_backoff(ws.get_all_values)
    except Exception:
        return []


def build_header_map(ws) -> Dict[str, int]:
    """
    Lee la primera fila como encabezados y devuelve dict: {header: col_index(1-based)}.
    """
    values = get_all_values_safe(ws)
    if not values or not values[0]:
        return {}
    headers = [str(h).strip() for h in values[0]]
    hmap = {}
    for i, h in enumerate(headers, start=1):
        if h:
            hmap[h] = i
    return hmap


def col_idx(hmap: Dict[str, int], name: str) -> Optional[int]:
    """
    Devuelve índice 1-based de la columna por nombre.
    Hace match exacto, y si no, intenta case-insensitive.
    """
    if not hmap or not name:
        return None

    if name in hmap:
        return hmap[name]

    low = name.strip().lower()
    for k, v in hmap.items():
        if k.strip().lower() == low:
            return v
    return None


def row_to_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    """
    Convierte (headers, row) a dict.
    """
    d = {}
    for i, h in enumerate(headers):
        if not h:
            continue
        d[h] = (row[i] if i < len(row) else "")
    return d


def find_row_by_col_value(values: List[List[str]], col_name: str, target_value: str) -> Optional[int]:
    """
    Busca en 'values' (get_all_values) una fila donde col_name == target_value.
    Regresa índice de fila (0-based sobre values) o None.
    Nota: values[0] debe ser header.
    """
    if not values or len(values) < 2:
        return None

    headers = [str(x).strip() for x in values[0]]
    try:
        col = headers.index(col_name)
    except ValueError:
        # case-insensitive fallback
        col = None
        low = col_name.strip().lower()
        for i, h in enumerate(headers):
            if h.strip().lower() == low:
                col = i
                break
        if col is None:
            return None

    t = (target_value or "").strip()
    for r in range(1, len(values)):
        row = values[r]
        v = (row[col] if col < len(row) else "").strip()
        if v == t:
            return r
    return None


def find_row_by_value(ws, col_name: str, target_value: str, hmap: Optional[Dict[str, int]] = None) -> Optional[int]:
    """
    Busca número de fila (1-based en Google Sheets) donde col_name == target_value.
    """
    if hmap is None:
        hmap = build_header_map(ws)

    c = col_idx(hmap, col_name)
    if not c:
        return None

    values = get_all_values_safe(ws)
    if not values or len(values) < 2:
        return None

    t = (target_value or "").strip()
    # values[0] es header, rows empiezan en 2
    for i in range(1, len(values)):
        row = values[i]
        v = (row[c - 1] if (c - 1) < len(row) else "").strip()
        if v == t:
            return i + 1  # 1-based sheet row
    return None


def update_row_cells(ws, row_num: int, updates: Dict[str, Any], hmap: Optional[Dict[str, int]] = None):
    """
    Actualiza varias celdas en una misma fila según nombre de columna.
    Usa update_cells (batch).
    """
    if not updates:
        return

    if hmap is None:
        hmap = build_header_map(ws)

    cells: List[Cell] = []
    for k, v in updates.items():
        c = col_idx(hmap, k)
        if not c:
            # si la columna no existe, no truena el sistema (solo ignora)
            continue
        cells.append(Cell(row=row_num, col=c, value="" if v is None else str(v)))

    if not cells:
        return

    with_backoff(ws.update_cells, cells, value_input_option="USER_ENTERED")


def find_row_by_col_value_in_ws(ws, col_name: str, target_value: str) -> Optional[int]:
    """
    Atajo: busca en worksheet y regresa row_num (1-based).
    """
    values = get_all_values_safe(ws)
    idx0 = find_row_by_col_value(values, col_name, target_value)
    if idx0 is None:
        return None
    return idx0 + 1  # convierte de 0-based en values a 1-based en sheet
