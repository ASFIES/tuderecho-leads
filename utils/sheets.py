# utils/sheets.py
import os
import re
import time
import json
import base64
import ast
from typing import Any, Dict, Optional, List, Callable, Tuple

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

# Cache global (evita re-autenticar por cada request)
_GSPREAD_CLIENT: Optional[gspread.Client] = None


# =========================
# Backoff helper
# =========================
def with_backoff(fn: Callable, *args, retries: int = 6, base_sleep: float = 0.6, **kwargs):
    """
    Reintenta llamadas a Google API / gspread con backoff exponencial.
    """
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            # Backoff exponencial con jitter simple
            sleep_s = base_sleep * (2 ** i)
            time.sleep(min(sleep_s, 6.0))
    raise last_err


# =========================
# Credentials parsing
# =========================
def _looks_like_base64(s: str) -> bool:
    s = s.strip()
    if len(s) < 40:
        return False
    # “eyJ” suele ser JSON en base64 ({" ... })
    if s.startswith("eyJ"):
        return True
    # heurística: solo charset base64 y múltiplo aproximado
    if re.fullmatch(r"[A-Za-z0-9+/=\s]+", s) and ("{" not in s):
        return True
    return False


def _try_b64_decode(raw: str) -> Optional[str]:
    try:
        b = base64.b64decode(raw.strip())
        return b.decode("utf-8", errors="strict")
    except Exception:
        return None


def _unescape_if_needed(raw: str) -> str:
    """
    Si viene con secuencias escapadas tipo \" o \\n o \\' las convertimos.
    """
    if any(x in raw for x in ('\\n', '\\"', "\\'")):
        try:
            return bytes(raw, "utf-8").decode("unicode_escape")
        except Exception:
            return raw
    return raw


def _parse_service_account(raw: str) -> Dict[str, Any]:
    """
    Acepta:
    - JSON normal (con comillas dobles)
    - dict estilo Python (comillas simples)
    - JSON/dict escapado (\\", \\' , \\n)
    - Base64 de JSON
    """
    if not raw or not raw.strip():
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON está vacío.")

    raw0 = raw.strip()

    # 1) Si parece base64, intentar decode
    if _looks_like_base64(raw0):
        dec = _try_b64_decode(raw0)
        if dec:
            raw0 = dec.strip()

    # 2) Si viene envuelto en comillas externas
    if (raw0.startswith('"') and raw0.endswith('"')) or (raw0.startswith("'") and raw0.endswith("'")):
        raw0 = raw0[1:-1].strip()

    # 3) Des-escapar si tiene \", \n, \'
    raw1 = _unescape_if_needed(raw0)

    # 4) Intentar JSON directo
    try:
        data = json.loads(raw1)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # 5) Reparación común: quitar backslashes antes de comillas
    raw2 = raw1.replace("\\'", "'").replace('\\"', '"')

    # 6) Intentar JSON otra vez
    try:
        data = json.loads(raw2)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # 7) Intentar dict estilo Python
    try:
        data = ast.literal_eval(raw2)
        if isinstance(data, dict):
            return data
    except Exception as e:
        # diagnóstico útil
        snippet = raw2[:220].replace("\n", "\\n")
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON no es JSON válido ni dict Python válido.\n"
            "TIP: Pega el JSON del service account tal cual (con comillas dobles) en Render.\n"
            f"Detalle parse: {e}\n"
            f"Inicio del valor: {snippet}"
        )


def _load_service_account_info() -> Dict[str, Any]:
    """
    Lee credenciales desde env.
    Soporta varios nombres por compatibilidad.
    """
    raw = (
        os.environ.get("GOOGLE_CREDENTIALS_JSON")
        or os.environ.get("GOOGLE_CREDENTIALS")
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        or ""
    ).strip()

    info = _parse_service_account(raw)

    # Validación mínima (sin romper si falta algo no crítico)
    if not isinstance(info, dict):
        raise RuntimeError("Credenciales inválidas: no es objeto/dict.")

    # Campos típicos de service_account
    if "type" in info and str(info["type"]).strip() != "service_account":
        # no lo hacemos fatal, pero avisaría
        pass

    # Si private_key trae saltos reales de línea dentro de la cadena (mal pegado),
    # suele romper JSON antes. Aquí ya parseó, así que normalmente viene bien con \n.
    return info


# =========================
# Gspread client
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
    """
    Abre spreadsheet por:
    - Título (nombre)
    - Key (id)
    - URL completa
    """
    if not sheet_name_or_key or not sheet_name_or_key.strip():
        raise RuntimeError("Falta GOOGLE_SHEET_NAME (o el nombre/id del spreadsheet).")

    s = sheet_name_or_key.strip()

    # Si es URL, extraer key
    if "docs.google.com" in s and "/spreadsheets/d/" in s:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
        if m:
            s = m.group(1)

    gc = get_gspread_client()

    # Si parece key (id), abrir por key
    if re.fullmatch(r"[a-zA-Z0-9-_]{25,}", s):
        return with_backoff(gc.open_by_key, s)

    # Si no, asumir título
    return with_backoff(gc.open, s)


def open_worksheet(spreadsheet, tab_name: str):
    if not tab_name or not tab_name.strip():
        raise RuntimeError("Nombre de pestaña vacío.")
    return with_backoff(spreadsheet.worksheet, tab_name.strip())


# =========================
# Sheet utilities
# =========================
def build_header_map(ws) -> Dict[str, int]:
    """
    Retorna {header: col_index(1-based)}
    """
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
    """
    values: matriz completa (incluye headers en values[0])
    Retorna índice de fila (0-based) dentro de values si encuentra, si no None.
    """
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
    """
    Busca fila (1-based en Google Sheets) donde col_name == value
    """
    if hmap is None:
        hmap = build_header_map(ws)

    c = col_idx(hmap, col_name)
    if not c:
        return None

    values = get_all_values_safe(ws)
    if not values or len(values) < 2:
        return None

    target = (value or "").strip()
    for r_i in range(2, len(values) + 1):  # 1-based row, empezando en 2
        row = values[r_i - 1]
        cell = (row[c - 1] if (c - 1) < len(row) else "").strip()
        if cell == target:
            return r_i
    return None


def update_row_cells(ws, row_num: int, updates: Dict[str, Any], hmap: Optional[Dict[str, int]] = None):
    """
    Actualiza múltiples celdas de una misma fila en 1 sola llamada (update_cells).
    updates: {"ColHeader": "valor", ...}
    """
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


def find_row_by_col_value_ws(ws, col_name: str, value: str) -> Optional[int]:
    """
    Variante que opera directo con ws.
    """
    values = get_all_values_safe(ws)
    idx0 = find_row_by_col_value(values, col_name, value)
    if idx0 is None:
        return None
    return idx0 + 1  # convertir a row_num 1-based
