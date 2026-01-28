import os
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.sheets import (
    open_spreadsheet,
    open_worksheet,
    find_row_by_value,
    safe_update_cells,
    get_all_records_cached,
)
from utils.whatsapp import send_whatsapp_message


TZ = ZoneInfo("America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()

REPORT_BASE_URL = os.environ.get("REPORT_BASE_URL", "").strip()  # opcional


def _now_iso():
    return datetime.now(TZ).replace(microsecond=0).isoformat()


def process_lead(lead_id: str):
    """
    Job pesado:
    - Lee lead
    - Genera análisis/resultado preliminar
    - Asigna abogada
    - Guarda en Sheets
    - Notifica por WhatsApp
    """
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog = open_worksheet(sh, TAB_ABOGADOS)

    # ---------- Lee lead (1 lectura, con backoff adentro) ----------
    row_idx = find_row_by_value(ws_leads, "ID_Lead", lead_id)
    if not row_idx:
        raise RuntimeError(f"No encontré lead {lead_id} en {TAB_LEADS}.")

    # Trae headers y fila completa (evita múltiples gets)
    headers = ws_leads.row_values(1)
    row_vals = ws_leads.row_values(row_idx)
    lead = dict(zip(headers, row_vals))

    telefono = (lead.get("Telefono") or "").strip()
    nombre = (lead.get("Nombre") or "").strip() or "Hola"
    tipo_caso = (lead.get("Tipo_Caso") or "").strip()
    descripcion = (lead.get("Descripcion_Situacion") or "").strip()
    salario = (lead.get("Salario_Mensual") or "").strip()
    ini = (lead.get("Fecha_Inicio_Laboral") or "").strip()
    fin = (lead.get("Fecha_Fin_Laboral") or "").strip()

    # ---------- Asignación simple round-robin (sin matar cuota) ----------
    abogados = get_all_records_cached(ws_abog, ttl_seconds=30)
    # Espera columnas típicas: Abogado_ID, Nombre_Abogado, Activo
    activos = [a for a in abogados if str(a.get("Activo", "1")).strip() in ("1", "TRUE", "True", "SI", "Sí", "si")]
    if not activos:
        activos = abogados

    # estrategia: hash del lead para repartir estable
    pick = activos[hash(lead_id) % max(1, len(activos))]
    abogado_id = (pick.get("Abogado_ID") or pick.get("ID") or "A01").strip()
    abogado_nombre = (pick.get("Nombre_Abogado") or pick.get("Nombre") or "tu abogada").strip()

    # ---------- Resultado preliminar (placeholder) ----------
    # Aquí luego metemos cálculo real LFT.
    resultado = f"Estimación preliminar generada (tipo: {tipo_caso})."

    analisis = (
        "Con la información que compartiste, revisaremos tu caso como "
        f"“{tipo_caso}” conforme a la Ley Federal del Trabajo. "
        "De forma preliminar, se consideran prestaciones devengadas "
        "y, en su caso, indemnización. Un abogado confirmará contigo los datos "
        "para cuidar tus derechos."
    )

    token = uuid.uuid4().hex[:20]
    link_reporte = ""
    if REPORT_BASE_URL:
        link_reporte = REPORT_BASE_URL.rstrip("/") + f"/r/{token}"

    # ---------- Actualiza Sheets (1 batch) ----------
    updates = {
        "Ultima_Actualizacion": _now_iso(),
        "ESTATUS": "CLIENTE_MENU",
        "Analisis_AI": analisis,
        "Resultado_Calculo": resultado,
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Token_Reporte": token,
        "Link_Reporte_Web": link_reporte,
        "Ultimo_Error": "",
    }
    safe_update_cells(ws_leads, row_idx, updates)

    # ---------- Mensaje al cliente ----------
    msg = (
        f"{nombre}, ya tengo tu estimación preliminar ✅\n\n"
        f"Te asigné a: *{abogado_nombre}*.\n"
        "En breve te contactamos para confirmar datos y proteger tus derechos.\n"
    )
    if link_reporte:
        msg += f"\n📄 Tu reporte: {link_reporte}\n"

    if telefono:
        send_whatsapp_message(telefono, msg)

    return {"ok": True, "lead_id": lead_id}
