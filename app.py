import os
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import open_spreadsheet, open_worksheet, find_row_by_value, safe_update_cells, get_all_records_cached

TZ = ZoneInfo("America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()

# ==========================================================
#  SUNTUOSAMENTE AQUÍ VAN TUS 2 VARIABLES CLAVE DE REDIS 👑
# ==========================================================
REDIS_URL = os.environ.get("REDIS_URL", "").strip()          # <-- LINK REDIS (rediss://...)
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()  # <-- NOMBRE COLA (ximena)

app = Flask(__name__)

def _now_iso():
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def _norm_phone(from_field: str) -> str:
    # Twilio trae: "whatsapp:+521..."
    return (from_field or "").strip()

def _norm_body(body: str) -> str:
    return (body or "").strip()

def _load_config_steps(ws_config):
    """
    Espera Config_XimenaAI con columnas:
    ID_Paso | Texto_Bot | Opciones_Validas (opcional)
    """
    rows = get_all_records_cached(ws_config, ttl_seconds=15)
    by_id = {}
    for r in rows:
        pid = str(r.get("ID_Paso", "")).strip()
        if pid:
            by_id[pid] = r
    return by_id

def _get_text(step_cfg, nombre=""):
    txt = str(step_cfg.get("Texto_Bot", "")).strip()
    if nombre:
        txt = txt.replace("{Nombre}", nombre)
    return txt

def _get_valid_options(step_cfg):
    opts = str(step_cfg.get("Opciones_Validas", "")).strip()
    if not opts:
        return None
    return [o.strip() for o in opts.split(",") if o.strip()]

def _next_step(current_step: str, user_msg: str) -> str:
    """
    Reglas mínimas para tu flujo actual:
    INICIO -> AVISO_PRIVACIDAD
    AVISO_PRIVACIDAD (1/2) -> CASO_TIPO o FIN_NO_ACEPTA
    CASO_TIPO -> CONFIRMACION_DATOS
    CONFIRMACION_DATOS -> NOMBRE ...
    DISCLAIMER (1 continuar) -> EN_PROCESO
    EN_PROCESO -> (se queda, worker lo pasa a CLIENTE_MENU)
    """
    m = user_msg.strip()

    if current_step == "INICIO":
        return "AVISO_PRIVACIDAD"

    if current_step == "AVISO_PRIVACIDAD":
        if m == "1":
            return "CASO_TIPO"
        if m == "2":
            return "FIN_NO_ACEPTA"
        return "AVISO_PRIVACIDAD"

    if current_step == "CASO_TIPO":
        if m in ("1", "2"):
            return "CONFIRMACION_DATOS"
        return "CASO_TIPO"

    if current_step == "CONFIRMACION_DATOS":
        if m in ("1", "2"):
            return "NOMBRE" if m == "1" else "FIN_NO_CONTINUA"
        return "CONFIRMACION_DATOS"

    # el resto lo manejas por tu tabla; aquí lo dejamos lineal:
    order = ["NOMBRE","APELLIDO","DESCRIPCION","INI_ANIO","INI_MES","INI_DIA","FIN_ANIO","FIN_MES","FIN_DIA","SALARIO","DISCLAIMER"]
    if current_step in order:
        i = order.index(current_step)
        if i < len(order)-1:
            return order[i+1]
        return "DISCLAIMER"

    if current_step == "DISCLAIMER":
        if m == "1":
            return "EN_PROCESO"
        if m == "2":
            return "FIN_NO_CONTINUA"
        return "DISCLAIMER"

    if current_step in ("EN_PROCESO","CLIENTE_MENU","FIN_NO_CONTINUA","FIN_NO_ACEPTA"):
        return current_step

    return "INICIO"


@app.post("/whatsapp")
def whatsapp_webhook():
    if not GOOGLE_SHEET_NAME:
        resp = MessagingResponse()
        resp.message("Error interno: falta GOOGLE_SHEET_NAME.")
        return str(resp)

    from_ = _norm_phone(request.form.get("From"))
    body = _norm_body(request.form.get("Body"))

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_cfg = open_worksheet(sh, TAB_CONFIG)

    cfg = _load_config_steps(ws_cfg)

    # -------- upsert lead por Telefono (o Telefono_Normalizado) --------
    lead_row = find_row_by_value(ws_leads, "Telefono", from_)
    if not lead_row:
        # crea nuevo lead: aquí lo mínimo (asumiendo headers ya existen)
        # Para simplificar sin añadir más lecturas: buscamos última fila y append
        headers = ws_leads.row_values(1)
        new = {h: "" for h in headers}
        new["ID_Lead"] = from_.replace("whatsapp:", "").replace("+", "")
        new["Telefono"] = from_
        new["Fuente_Lead"] = "DESCONOCIDA"
        new["Fecha_Registro"] = _now_iso()
        new["Ultima_Actualizacion"] = _now_iso()
        new["ESTATUS"] = "INICIO"

        ws_leads.append_row([new.get(h,"") for h in headers], value_input_option="USER_ENTERED")
        lead_row = find_row_by_value(ws_leads, "Telefono", from_)

    # Lee estatus actual
    headers = ws_leads.row_values(1)
    row_vals = ws_leads.row_values(lead_row)
    lead = dict(zip(headers, row_vals))

    current = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
    nombre = (lead.get("Nombre") or "").strip()

    # -------- valida opciones si aplica --------
    step_cfg = cfg.get(current, {})
    valid = _get_valid_options(step_cfg)
    if valid is not None:
        if body not in valid:
            r = MessagingResponse()
            r.message("Por favor responde con una opción válida (ej. 1 o 2).")
            return str(r)

    # -------- guarda dato del paso (si corresponde a una columna) --------
    # Mapeo simple; si ya lo tienes en Sheets por tabla, luego lo conectamos.
    step_to_col = {
        "NOMBRE": "Nombre",
        "APELLIDO": "Apellido",
        "DESCRIPCION": "Descripcion_Situacion",
        "SALARIO": "Salario_Mensual",
        # fecha la reconstruyes tú (aquí ejemplo rápido):
        "INI_ANIO": "Inicio_Anio",
        "INI_MES": "Inicio_Mes",
        "INI_DIA": "Inicio_Dia",
        "FIN_ANIO": "Fin_Anio",
        "FIN_MES": "Fin_Mes",
        "FIN_DIA": "Fin_Dia",
        "CASO_TIPO": "Tipo_Caso",
        "AVISO_PRIVACIDAD": "Aviso_Privacidad_Aceptado",
    }

    updates = {"Ultima_Actualizacion": _now_iso()}
    col = step_to_col.get(current)
    if col:
        updates[col] = body

    # next step
    nxt = _next_step(current, body)
    updates["ESTATUS"] = nxt

    safe_update_cells(ws_leads, lead_row, updates)

    # -------- responder al usuario --------
    resp = MessagingResponse()

    # si pasamos a EN_PROCESO, encolamos job y damos mensaje bonito
    if nxt == "EN_PROCESO":
        # cola redis
        if not REDIS_URL:
            resp.message("Error interno: falta REDIS_URL.")
            return str(resp)

        lead_id = lead.get("ID_Lead") or from_.replace("whatsapp:", "")
        redis_conn = Redis.from_url(REDIS_URL)
        q = Queue(REDIS_QUEUE_NAME, connection=redis_conn)

        from worker_jobs import process_lead
        q.enqueue(process_lead, lead_id, job_timeout=180)

        resp.message(
            "Gracias, ya tengo lo necesario ✅\n\n"
            "Estoy preparando tu estimación preliminar y asignando a la abogada que llevará tu caso.\n"
            "En un momento te envío el resultado por este medio."
        )
        return str(resp)

    # texto del siguiente paso
    next_cfg = cfg.get(nxt, {})
    msg = _get_text(next_cfg, nombre=nombre)
    if not msg:
        msg = "Perfecto. Continuemos."
    resp.message(msg)
    return str(resp)
