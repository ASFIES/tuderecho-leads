import os
import json
import time
import random
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# -------------------------
# ENV
# -------------------------
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

# Cache control
CONFIG_CACHE_TTL_SECONDS = int(os.environ.get("CONFIG_CACHE_TTL_SECONDS", "120"))  # 2 minutos

# Lazy globals
_GC = None
_SH = None


def _get_creds() -> Credentials:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if GOOGLE_CREDS_JSON:
        info = json.loads(GOOGLE_CREDS_JSON)
        return Credentials.from_service_account_info(info, scopes=scopes)

    if GOOGLE_CREDS_PATH:
        return Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=scopes)

    raise RuntimeError(
        "Falta GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS en variables de entorno."
    )


def get_gspread_client() -> gspread.Client:
    global _GC
    if _GC is None:
        creds = _get_creds()
        _GC = gspread.authorize(creds)
    return _GC


def open_spreadsheet():
    global _SH
    if _SH is None:
        if not GOOGLE_SHEET_NAME:
            raise RuntimeError("Falta GOOGLE_SHEET_NAME en variables de entorno.")
        gc = get_gspread_client()
        _SH = gc.open(GOOGLE_SHEET_NAME)
    return _SH


def open_worksheet(tab_name: str):
    sh = open_spreadsheet()
    return sh.worksheet(tab_name)


# -------------------------
# Redis cache (opcional)
# -------------------------
def _redis():
    # Import local para no romper si no está instalado en local
    try:
        from redis import Redis
    except Exception:
        return None

    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        return None
    try:
        return Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return None


def _cache_get(key: str) -> Optional[str]:
    r = _redis()
    if not r:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def _cache_set(key: str, value: str, ttl: int):
    r = _redis()
    if not r:
        return
    try:
        r.setex(key, ttl, value)
    except Exception:
        return


# -------------------------
# Backoff / Retry for 429
# -------------------------
def _with_backoff(fn, max_tries: int = 6):
    """
    Reintenta cuando Google corta por cuota (429) u otros errores temporales.
    """
    for i in range(max_tries):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            is_quota = ("429" in msg) or ("quota" in msg) or ("read requests" in msg)
            if not is_quota and i >= 1:
                # si no parece cuota y ya falló una vez, no insistas
                raise

            sleep_s = (2 ** i) + random.random()
            time.sleep(min(sleep_s, 20))
    # si llegamos aquí, re-lanzamos el último intento
    return fn()


# -------------------------
# Helpers
# -------------------------
def get_all_records_cached(tab_name: str, cache_key: str) -> List[Dict[str, Any]]:
    """
    Para Configs: cacheamos en Redis para no leer Sheets cada mensaje.
    """
    ck = f"cfg:{cache_key}:{tab_name}"
    cached = _cache_get(ck)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    ws = open_worksheet(tab_name)
    records = _with_backoff(lambda: ws.get_all_records())
    _cache_set(ck, json.dumps(records, ensure_ascii=False), CONFIG_CACHE_TTL_SECONDS)
    return records


def find_row_by_value(ws, col_name: str, value: str) -> Optional[int]:
    """
    Busca fila por valor exacto en una columna (usando cabeceras).
    """
    headers = _with_backoff(lambda: ws.row_values(1))
    if col_name not in headers:
        raise RuntimeError(f"No existe la columna '{col_name}' en la hoja {ws.title}")
    col_idx = headers.index(col_name) + 1

    def _get_col():
        return ws.col_values(col_idx)

    col_vals = _with_backoff(_get_col)
    for i, v in enumerate(col_vals[1:], start=2):  # desde fila 2
        if str(v).strip() == str(value).strip():
            return i
    return None


def update_row_dict(ws, row: int, updates: Dict[str, Any]):
    """
    Actualiza varias columnas en una fila con un solo batch (reduce lecturas/escrituras).
    """
    headers = _with_backoff(lambda: ws.row_values(1))
    cells = []
    for k, v in updates.items():
        if k not in headers:
            # si no existe la columna, ignora (para no romper producción)
            continue
        col_idx = headers.index(k) + 1
        cells.append((row, col_idx, v))

    if not cells:
        return

    def _batch():
        cell_list = ws.range(min(r for r, c, _ in cells),
                             min(c for r, c, _ in cells),
                             max(r for r, c, _ in cells),
                             max(c for r, c, _ in cells))
        # mapa para setear solo lo necesario
        for cell in cell_list:
            for rr, cc, vv in cells:
                if cell.row == rr and cell.col == cc:
                    cell.value = "" if vv is None else str(vv)
        ws.update_cells(cell_list)

    _with_backoff(_batch)
