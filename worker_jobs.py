# worker_jobs.py
import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff, get_records_cached

TZ = os.environ.get("TZ", "America/Mexico_City").strip()
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOG = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def _append_row_by_header(ws, data: dict):
    header = with_backoff(ws.row_values, 1)
    row = [""] * len(header)
    for i, h in enumerate(header):
        key = (h or "").strip()
        if key in data:
            row[i] = str(data.get(key, ""))
    with_backoff(ws.append_row, row, value_input_option="USER_ENTERED")

def _update_by_header(ws, row_num_1based: int, updates: dict):
    header = with_backoff(ws.row_values, 1)
    cell_list = []
    for k, v in updates.items():
        if k in header:
            c = header.index(k) + 1
            cell = ws.cell(row_num_1based, c)
            cell.value = str(v)
            cell_list.append(cell)
    if cell_list:
        with_backoff(ws.update_cells, cell_list)

def _money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except Exception:
        return 0.0

def _pick_abogado(ws_abog, salario_mensual: float):
    abogs = get_records_cached(ws_abog, cache_seconds=5)

    # regla: >= 50,000 => A01 si está activo
    if salario_mensual >= 50000:
        for a in abogs:
            if str(a.get("ID_Abogado", "")).strip() == "A01" and str(a.get("Activo", "")).strip().upper() in ("SI", "SÍ", "1", "TRUE"):
                return a

    # fallback: primer activo
    for a in abogs:
        if str(a.get("Activo", "")).strip().upper() in ("SI", "SÍ", "1", "TRUE"):
            return a

    return abogs[0] if abogs else {"ID_Abogado": "A01", "Nombre_Abogado": "Abogada asignada", "Telefono_Abogado": ""}

def process_lead(lead_id: str):
    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog = open_worksheet(sh, TAB_ABOG)
    ws_logs = open_worksheet(sh, TAB_LOGS)

    leads = get_records_cached(ws_leads, cache_seconds=0)
    idx = None
    lead = None
    for i, r in enumerate(leads):
        if str(r.get("ID_Lead", "")).strip() == str(lead_id).strip():
            idx = i
            lead = r
            break
    if idx is None:
        return {"ok": False, "error": f"Lead no encontrado: {lead_id}"}

    salario = _money_to_float(lead.get("Salario_Mensual", "0"))
    abogado = _pick_abogado(ws_abog, salario)

    abogado_id = str(abogado.get("ID_Abogado", "A01")).strip() or "A01"
    abogado_nombre = str(abogado.get("Nombre_Abogado", "Abogada asignada")).strip() or "Abogada asignada"

    row_num = idx + 2
    _update_by_header(ws_leads, row_num, {
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": _now_iso(),
    })

    _append_row_by_header(ws_logs, {
        "ID_Log": str(uuid.uuid4())[:8],
        "Fecha_Hora": _now_iso(),
        "Telefono": lead.get("Telefono", ""),
        "ID_Lead": lead_id,
        "Paso": "EN_PROCESO",
        "Mensaje_Entrante": "",
        "Mensaje_Saliente": f"Procesado OK. Abogado: {abogado_nombre}",
        "Canal": "SISTEMA",
        "Fuente_Lead": lead.get("Fuente_Lead", ""),
        "Modelo_AI": "",
        "Errores": "",
    })

    return {"ok": True, "lead_id": lead_id, "abogado": abogado_nombre}
