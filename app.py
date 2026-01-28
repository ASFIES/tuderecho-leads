import os
import re
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff

TZ = os.environ.get("TZ", "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS  = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_TEXT  = os.environ.get("TAB_TEXT", "Textos_Bot").strip()
TAB_FLOW  = os.environ.get("TAB_FLOW", "Flow").strip()

# =========================
# 🔥 REDIS (AQUÍ SOLO SE LEE DE ENV)
# =========================
# ✅ En Render > Environment:
#   REDIS_URL = redis://.... (link completo)
#   REDIS_QUEUE_NAME = ximena
REDIS_URL = os.environ.get("REDIS_URL", "redis://red-d5svi5v5r7bs73basen0:6379").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def _normalize_phone(raw: str) -> str:
    return re.sub(r"\D+", "", raw or "")

def _twiml(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def _get_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def _load_texts(ws_text):
    rows = with_backoff(ws_text.get_all_records)
    # Espera columnas: ID_Paso, Texto_Bot
    out = {}
    for r in rows:
        k = str(r.get("ID_Paso", "")).strip()
        v = str(r.get("Texto_Bot", "")).strip()
        if k:
            out[k] = v
    return out

def _load_flow(ws_flow):
    rows = with_backoff(ws_flow.get_all_records)
    # Espera columnas: ID_Paso, Tipo_Entrada, Opciones_Validas, Siguiente_Si_1, Siguiente_Si_2, Regla_Validacion, Mensaje_Error, Campo_BD_Leads_A_Actualizar
    out = {}
    for r in rows:
        k = str(r.get("ID_Paso", "")).strip()
        if k:
            out[k] = r
    return out

def _find_lead_row(ws_leads, lead_id: str):
    records = with_backoff(ws_leads.get_all_records)
    for i, r in enumerate(records):
        if str(r.get("ID_Lead", "")).strip() == str(lead_id).strip():
            return (i, r)  # idx 0-based + dict
    return (None, None)

def _find_lead_by_phone(ws_leads, phone_norm: str):
    records = with_backoff(ws_leads.get_all_records)
    for i, r in enumerate(records):
        if str(r.get("Telefono_Normalizado", "")).strip() == phone_norm:
            return (i, r)
    return (None, None)

def _ensure_lead(ws_leads, from_phone: str):
    phone_norm = _normalize_phone(from_phone)
    idx, lead = _find_lead_by_phone(ws_leads, phone_norm)
    if lead:
        return idx, lead

    # Crear lead nuevo
    lead_id = str(uuid.uuid4())[:8] + "-" + phone_norm[-6:]
    header = with_backoff(ws_leads.row_values, 1)

    def col(name): return header.index(name) + 1
    row = [""] * len(header)

    def setv(name, val):
        if name in header:
            row[header.index(name)] = str(val)

    setv("ID_Lead", lead_id)
    setv("Telefono", from_phone)
    setv("Telefono_Normalizado", phone_norm)
    setv("Fuente_Lead", "DESCONOCIDA")
    setv("Fecha_Registro", _now_iso())
    setv("Ultima_Actualizacion", _now_iso())
    setv("ESTATUS", "INICIO")

    with_backoff(ws_leads.append_row, row, value_input_option="USER_ENTERED")

    # Re-leer para devolver dict real
    idx2, lead2 = _find_lead_by_phone(ws_leads, phone_norm)
    return idx2, lead2

def _update_lead_cells(ws_leads, row_num_1based: int, updates: dict):
    header = with_backoff(ws_leads.row_values, 1)
    cell_list = []
    for k, v in updates.items():
        if k in header:
            c = header.index(k) + 1
            cell = ws_leads.cell(row_num_1based, c)
            cell.value = str(v)
            cell_list.append(cell)
    if cell_list:
        with_backoff(ws_leads.update_cells, cell_list)

def _log(ws_logs, lead_id: str, paso: str, msg_in: str, msg_out: str, err: str = ""):
    with_backoff(ws_logs.append_row, [
        _now_iso(), "", "", lead_id, paso, msg_in, msg_out,
        "WHATSAPP", "DESCONOCIDA", "gpt-4o-mini", err
    ])

@app.post("/whatsapp")
def whatsapp_webhook():
    msg_in = (request.form.get("Body") or "").strip()
    from_phone = request.form.get("From") or ""

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_text  = open_worksheet(sh, TAB_TEXT)
        ws_flow  = open_worksheet(sh, TAB_FLOW)

        texts = _load_texts(ws_text)
        flow  = _load_flow(ws_flow)

        idx, lead = _ensure_lead(ws_leads, from_phone)
        if not lead:
            return _twiml("Por el momento no pude acceder a la base de datos. Intenta nuevamente en unos minutos.")

        lead_id = lead.get("ID_Lead")
        estatus = (lead.get("ESTATUS") or "INICIO").strip()
        row_num = (idx + 2)  # 1-based real row

        # =========================
        # ✅ REGLA CLAVE: SI ES NUEVO O ESTÁ EN INICIO, PRIMERO MUESTRA INICIO
        # NO VALIDES "Hola" COMO OPCIÓN.
        # =========================
        if estatus == "INICIO":
            out = texts.get("INICIO", "Hola, soy Ximena. ¿Deseas continuar?\n1 Sí\n2 No")
            _update_lead_cells(ws_leads, row_num, {"Ultima_Actualizacion": _now_iso()})
            _log(ws_logs, lead_id, "INICIO", msg_in, out)
            return _twiml(out)

        # Flujo normal
        step = flow.get(estatus, {})
        tipo = str(step.get("Tipo_Entrada", "")).strip().upper()
        opciones = str(step.get("Opciones_Validas", "")).strip()
        err_msg = str(step.get("Mensaje_Error", "Por favor responde con una opción válida (ej. 1 o 2).")).strip()

        # Validación mínima de opciones
        if tipo == "OPCIONES":
            valid = [x.strip() for x in opciones.split(",") if x.strip()]
            if msg_in not in valid:
                _log(ws_logs, lead_id, estatus, msg_in, err_msg)
                return _twiml(err_msg)

            # calcular siguiente paso
            next_step = step.get("Siguiente_Si_1") if msg_in == "1" else step.get("Siguiente_Si_2")
            next_step = str(next_step or "").strip() or "INICIO"

            # actualizar campo si aplica
            campo = str(step.get("Campo_BD_Leads_A_Actualizar", "")).strip()
            updates = {"ESTATUS": next_step, "Ultima_Actualizacion": _now_iso()}
            if campo:
                updates[campo] = msg_in

            _update_lead_cells(ws_leads, row_num, updates)

            # responder con texto del siguiente paso
            out = texts.get(next_step, "Continuemos…")
            _log(ws_logs, lead_id, next_step, msg_in, out)
            return _twiml(out)

        # Texto libre (ej. NOMBRE, APELLIDO, DESCRIPCION, etc.)
        next_step = str(step.get("Siguiente_Si_1") or "").strip() or estatus
        campo = str(step.get("Campo_BD_Leads_A_Actualizar", "")).strip()

        updates = {"ESTATUS": next_step, "Ultima_Actualizacion": _now_iso()}
        if campo:
            updates[campo] = msg_in

        _update_lead_cells(ws_leads, row_num, updates)

        out = texts.get(next_step, "Gracias. Continuemos…")

        # Si caíste en EN_PROCESO, manda job a Redis
        if next_step == "EN_PROCESO":
            q = _get_queue()
            if q is not None:
                from worker_jobs import process_lead
                q.enqueue(process_lead, lead_id, job_timeout=120)

        _log(ws_logs, lead_id, next_step, msg_in, out)
        return _twiml(out)

    except Exception as e:
        # fallback
        return _twiml("Por el momento no pude acceder a la base de datos. Intenta de nuevo en unos minutos.")
