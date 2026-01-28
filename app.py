import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import open_worksheet, find_row_by_value, update_row_dict, get_all_records_cached
from worker_jobs import process_lead

app = Flask(__name__)

TZ = os.environ.get("TZ", "America/Mexico_City")

# Tabs
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_FLOW = os.environ.get("TAB_FLOW", "Config_XimenaAI").strip()        # tu flujo (ID_Paso -> Texto_Bot)
TAB_RULES = os.environ.get("TAB_RULES", "Config_Reglas").strip()        # reglas de validación/transición si la usas
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

# Redis / RQ
REDIS_URL = os.environ.get("REDIS_URL", "redis://red-d5svi5v5r7bs73basen0:6379").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

if not REDIS_URL:
    raise RuntimeError("Falta REDIS_URL en variables de entorno.")

redis_conn = Redis.from_url(REDIS_URL)
queue = Queue(REDIS_QUEUE_NAME, connection=redis_conn)


def now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")


def get_or_create_lead(telefono: str) -> str:
    ws = open_worksheet(TAB_LEADS)
    # busca por Telefono
    row = find_row_by_value(ws, "Telefono", telefono)
    if row:
        values = ws.row_values(row)
        headers = ws.row_values(1)
        data = {headers[i]: (values[i] if i < len(values) else "") for i in range(len(headers))}
        lead_id = str(data.get("ID_Lead", "")).strip()
        if lead_id:
            return lead_id

    # crear lead
    lead_id = str(uuid.uuid4())[:8] + "-" + telefono[-4:]
    # append básico (ideal: usar headers para armar)
    ws.append_row([lead_id, telefono, telefono, "", "", "", "DESCONOCIDA", now_iso(), now_iso(), "INICIO"])
    return lead_id


def log_event(lead_id: str, telefono: str, paso: str, msg_in: str, msg_out: str):
    try:
        ws = open_worksheet(TAB_LOGS)
        ws.append_row([now_iso(), telefono, lead_id, paso, msg_in, msg_out, "WHATSAPP", "DESCONOCIDA", "gpt-4o-mini", ""])
    except Exception:
        pass


def get_flow_text(id_paso: str) -> str:
    # cachea Config_XimenaAI en Redis para no leerlo siempre
    rows = get_all_records_cached(TAB_FLOW, cache_key="flow")
    for r in rows:
        if str(r.get("ID_Paso", "")).strip() == id_paso:
            return str(r.get("Texto_Bot", "")).strip()
    return ""


@app.post("/whatsapp")
def whatsapp():
    incoming = request.values.get("Body", "").strip()
    from_number = request.values.get("From", "").strip()  # ej: whatsapp:+52155...
    telefono = from_number

    lead_id = get_or_create_lead(telefono)

    # Lee lead para saber estatus/paso actual
    ws = open_worksheet(TAB_LEADS)
    row = find_row_by_value(ws, "ID_Lead", lead_id)
    values = ws.row_values(row)
    headers = ws.row_values(1)
    lead = {headers[i]: (values[i] if i < len(values) else "") for i in range(len(headers))}

    paso_actual = (lead.get("ESTATUS") or "INICIO").strip()

    # ---- EJEMPLO: si ya está en EN_PROCESO, no lo reproceses infinito
    if paso_actual == "EN_PROCESO":
        resp = MessagingResponse()
        msg_out = "Estoy preparando tu estimación preliminar. En un momento te envío el resultado por este medio."
        resp.message(msg_out)
        log_event(lead_id, telefono, paso_actual, incoming, msg_out)
        return str(resp)

    # ---- Aquí normalmente harías tu lógica de transición por reglas
    # Para que funcione ya: si el usuario llega a tu opción 1 después de DISCLAIMER -> EN_PROCESO
    # En tu Sheet ya existe EN_PROCESO como paso.
    # Si el usuario escribió "1" y el paso anterior era DISCLAIMER, pasamos a EN_PROCESO.
    paso_siguiente = None
    if paso_actual == "WAIT_RESULTADO" or paso_actual == "DISCLAIMER":
        if incoming == "1":
            paso_siguiente = "EN_PROCESO"

    # fallback: mantener
    if not paso_siguiente:
        # si está en CLIENTE_MENU etc, responde con el texto configurado
        paso_siguiente = paso_actual

    # Si llegamos a EN_PROCESO: encola job
    if paso_siguiente == "EN_PROCESO":
        update_row_dict(ws, row, {"ESTATUS": "EN_PROCESO", "Ultima_Actualizacion": now_iso()})

        # ✅ ENQUEUE (lo importante)
        queue.enqueue(process_lead, lead_id)

        resp = MessagingResponse()
        msg_out = "Estoy preparando tu estimación preliminar y asignando a la abogada que llevará tu caso.\nEn un momento te envío el resultado por este medio."
        resp.message(msg_out)
        log_event(lead_id, telefono, "EN_PROCESO", incoming, msg_out)
        return str(resp)

    # Respuesta normal por texto configurado
    texto = get_flow_text(paso_siguiente) or "Estoy en pruebas. Escribe 'Hola' para comenzar."
    resp = MessagingResponse()
    resp.message(texto)
    log_event(lead_id, telefono, paso_siguiente, incoming, texto)
    return str(resp)


@app.get("/health")
def health():
    return {"ok": True}
