import os
import json
import base64
import gspread
from google.oauth2.service_account import Credentials

from utils.cache import redis_client

def get_env_creds_dict():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    path = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

    if raw:
        if raw.lstrip().startswith("{"):
            return json.loads(raw)
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)

    if path:
        if not os.path.exists(path):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe.")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales Google: GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH.")

def get_gspread_client(creds_info=None):
    if creds_info is None:
        creds_info = get_env_creds_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(gc, sheet_name: str):
    if not sheet_name:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")
    return gc.open(sheet_name)

def open_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except Exception:
        raise RuntimeError(f"No existe la pestaña '{title}' en el Google Sheet.")

def build_header_map(ws):
    headers = ws.row_values(1)
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

def col_idx(headers_map: dict, name: str):
    return headers_map.get(name) or headers_map.get((name or "").lower())

def find_row_by_value(ws, col_idx_num: int, value: str):
    value = (value or "").strip()
    if not value:
        return None
    col_values = ws.col_values(col_idx_num)
    for i, v in enumerate(col_values[1:], start=2):
        if (v or "").strip() == value:
            return i
    return None

def update_cells_batch(ws, updates_a1_to_value: dict):
    payload = [{"range": a1, "values": [[val]]} for a1, val in updates_a1_to_value.items()]
    if payload:
        ws.batch_update(payload)

def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    from gspread.utils import rowcol_to_a1
    to_send = {}
    for col_name, val in (updates or {}).items():
        idx = col_idx(header_map, col_name)
        if not idx:
            continue
        a1 = rowcol_to_a1(row_idx, idx)
        to_send[a1] = val
    update_cells_batch(ws, to_send)

def safe_log(ws_logs, data: dict):
    try:
        cols = [
            "ID_Log", "Fecha_Hora", "Telefono", "ID_Lead", "Paso",
            "Mensaje_Entrante", "Mensaje_Saliente",
            "Canal", "Fuente_Lead", "Modelo_AI", "Errores"
        ]
        row = [data.get(c, "") for c in cols]
        ws_logs.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        pass

def get_all_records_cached(ws, cache_key: str, ttl: int = 180):
    """
    Cachea en Redis: evita chattering.
    """
    r = redis_client()
    if r:
        key = f"sheetcache:{cache_key}"
        cached = r.get(key)
        if cached:
            try:
                return json.loads(cached.decode("utf-8"))
            except:
                pass

    rows = ws.get_all_records()

    if r:
        try:
            r.setex(f"sheetcache:{cache_key}", ttl, json.dumps(rows, ensure_ascii=False))
        except:
            pass

    return rows

def append_row_by_headers(ws, header_map: dict, row_dict: dict):
    """
    Inserta una fila respetando headers.
    """
    headers = ws.row_values(1)
    row = [""] * len(headers)
    for k, v in (row_dict or {}).items():
        idx = col_idx(header_map, k)
        if idx and 1 <= idx <= len(row):
            row[idx - 1] = v
    ws.append_row(row, value_input_option="USER_ENTERED")
