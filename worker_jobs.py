import os
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff

TZ = os.environ.get("TZ", "America/Mexico_City")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS  = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOG  = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()

def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def process_lead(lead_id: str):
    """
    Job principal: asignar abogado + generar análisis AI + preparar link reporte.
    Aquí NO spameamos lecturas para no caer en 429.
    """
    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)
    ws_logs  = open_worksheet(sh, TAB_LOGS)

    # 1) Leer fila del lead (1 sola lectura grande -> find por ID)
    records = with_backoff(ws_leads.get_all_records)
    idx = None
    for i, r in enumerate(records):
        if str(r.get("ID_Lead", "")).strip() == str(lead_id).strip():
            idx = i
            lead = r
            break
    if idx is None:
        return {"ok": False, "error": f"Lead no encontrado: {lead_id}"}

    # 2) Si ya tiene abogado asignado, no reprocesar
    if str(lead.get("ESTATUS", "")).strip() not in ("EN_PROCESO", "WAIT_RESULTADOS"):
        return {"ok": True, "msg": "No requiere procesamiento", "estatus": lead.get("ESTATUS")}

    # 3) Asignación simple: primer abogado activo (ajústalo a tu regla real)
    abogs = with_backoff(ws_abog.get_all_records)
    activo = None
    for a in abogs:
        if str(a.get("Activo", "")).strip() in ("1", "TRUE", "True", "SI", "Sí", "si"):
            activo = a
            break
    if not activo and abogs:
        activo = abogs[0]

    abogado_id = (activo or {}).get("Abogado_ID", "A01")
    abogado_nombre = (activo or {}).get("Nombre", "Abogada asignada")

    # 4) Actualizar celdas del lead (1 update)
    # Nota: ws_leads.update_cell usa índices 1-based.
    header = with_backoff(ws_leads.row_values, 1)
    def col(name): return header.index(name) + 1

    row_num = idx + 2  # header + offset
    updates = []

    # ejemplo columnas esperadas (según tus screenshots)
    if "Abogado_Asignado_ID" in header:
        updates.append((row_num, col("Abogado_Asignado_ID"), abogado_id))
    if "Abogado_Asignado" in header:
        updates.append((row_num, col("Abogado_Asignado"), abogado_nombre))
    if "ESTATUS" in header:
        updates.append((row_num, col("ESTATUS"), "CLIENTE_MENU"))
    if "Ultima_Actualizacion" in header:
        updates.append((row_num, col("Ultima_Actualizacion"), _now_iso()))

    if updates:
        cell_list = [ws_leads.cell(r, c) for (r, c, _) in updates]
        for cell, (_, _, v) in zip(cell_list, updates):
            cell.value = str(v)
        with_backoff(ws_leads.update_cells, cell_list)

    # 5) Log mínimo
    if ws_logs:
        with_backoff(ws_logs.append_row, [
            _now_iso(), "", lead_id, "EN_PROCESO",
            "", f"Procesado OK. Abogado: {abogado_nombre}",
            "SISTEMA", "", "", ""
        ])

    return {"ok": True, "lead_id": lead_id, "abogado": abogado_nombre}
