import os
import json
import uuid
from datetime import datetime

import gspread
from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
from google.oauth2.service_account import Credentials

# =========================
# CONFIG GENERAL
# =========================
app = Flask(__name__)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "TDLM_Sistema_Leads_v1")

# Twilio
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "whatsapp:+14155238886")  # sandbox o prod

# Credenciales Google (recomendado: JSON en variable de entorno)
# En Render crea env var: GOOGLE_CREDENTIALS_JSON (todo el JSON)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

# Nombre de pestañas (ajústalas a tus nombres reales)
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads")
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI")
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs_Mensajes")

# =========================
# GOOGLE SHEETS HELPERS
# =========================
def gs_client():
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON en variables de entorno (Render).")

    creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def ws(nombre_pestana: str):
    client = gs_client()
    sh = client.open(GOOGLE_SHEET_NAME)
    return sh.worksheet(nombre_pestana)

def header_map(worksheet):
    """Mapea encabezados (fila 1) -> índice 1-based"""
    headers = worksheet.row_values(1)
    return {h.strip(): (i + 1) for i, h in enumerate(headers) if h and h.strip()}

def get_cell_by_name(worksheet, row, col_name):
    hm = header_map(worksheet)
    col = hm.get(col_name)
    if not col:
        return ""
    return (worksheet.cell(row, col).value or "").strip()

def set_cell_by_name(worksheet, row, col_name, value):
    hm = header_map(worksheet)
    col = hm.get(col_name)
    if not col:
        return False
    worksheet.update_cell(row, col, value)
    return True

def buscar_fila_por_telefono(ws_leads, telefono):
    hm = header_map(ws_leads)
    col_tel = hm.get("Telefono")
    if not col_tel:
        return None

    col_vals = ws_leads.col_values(col_tel)
    for idx, v in enumerate(col_vals, start=1):
        if idx == 1:
            continue
        if (v or "").strip() == telefono.strip():
            return idx
    return None

def obtener_paso_actual(ws_leads, row):
    """
    Lee ESTATUS o Estatus_Chat (compatibilidad).
    Si no hay, regresa INICIO.
    """
    paso = get_cell_by_name(ws_leads, row, "ESTATUS")
    if not paso:
        paso = get_cell_by_name(ws_leads, row, "Estatus_Chat")
    return paso if paso else "INICIO"

def guardar_paso_actual(ws_leads, row, nuevo_paso):
    """
    Escribe en ESTATUS si existe; si no, en Estatus_Chat.
    """
    if set_cell_by_name(ws_leads, row, "ESTATUS", nuevo_paso):
        return True
    return set_cell_by_name(ws_leads, row, "Estatus_Chat", nuevo_paso)

def crear_lead(ws_leads, telefono, msg_inicial):
    hm = header_map(ws_leads)
    new_row = [""] * len(hm)

    def put(col, val):
        c = hm.get(col)
        if c:
            new_row[c - 1] = val

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    id_lead = str(uuid.uuid4())

    fuente = "FACEBOOK" if "facebook" in msg_inicial.lower() else "WHATSAPP"

    put("ID_Lead", id_lead)
    put("Telefono", telefono)
    put("Fuente_Lead", fuente)
    put("Fecha_Registro", now)
    put("Ultima_Actualizacion", now)
    put("Ultimo_Mensaje_Cliente", msg_inicial)

    # Paso inicial
    if "ESTATUS" in hm:
        put("ESTATUS", "INICIO")
    elif "Estatus_Chat" in hm:
        put("Estatus_Chat", "INICIO")

    ws_leads.append_row(new_row, value_input_option="RAW")
    return id_lead

def log_mensaje(ws_logs, telefono, id_lead, paso, msg_in, msg_out, canal="WHATSAPP", fuente="FACEBOOK", modelo="", error=""):
    hm = header_map(ws_logs)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    row = [""] * len(hm)

    def put(col, val):
        c = hm.get(col)
        if c:
            row[c - 1] = val

    put("ID_Log", str(uuid.uuid4()))
    put("Fecha_Hora", now)
    put("Telefono", telefono)
    put("ID_Lead", id_lead)
    put("Paso", paso)
    put("Mensaje_Entrante", msg_in)
    put("Mensaje_Saliente", msg_out)
    put("Canal", canal)
    put("Fuente_Lead", fuente)
    put("Modelo_AI", modelo)
    put("Errores", error)

    ws_logs.append_row(row, value_input_option="RAW")

# =========================
# CONFIG_XIMENAAI HELPERS
# =========================
def cfg_get_by_paso(ws_cfg, id_paso):
    """
    Busca en Config_XimenaAI por ID_Paso (col A) usando find().
    No usamos gspread.exceptions.CellNotFound para evitar incompatibilidades.
    """
    try:
        cell = ws_cfg.find(id_paso)
    except Exception:
        cell = None

    if not cell:
        return None

    r = cell.row
    # Ajustado a tu layout:
    # A ID_Paso
    # C Texto_Bot
    # D Tipo_Entrada
    # E Opciones_Validas
    # F Siguiente_Si_1
    # G Siguiente_Si_2
    # H Campo_BD_Leads_A_Actualizar
    # J Mensaje_Error
    return {
        "row": r,
        "ID_Paso": ws_cfg.cell(r, 1).value or "",
        "Texto_Bot": ws_cfg.cell(r, 3).value or "",
        "Tipo_Entrada": (ws_cfg.cell(r, 4).value or "").strip().upper(),
        "Opciones_Validas": (ws_cfg.cell(r, 5).value or "").strip(),
        "Siguiente_Si_1": (ws_cfg.cell(r, 6).value or "").strip(),
        "Siguiente_Si_2": (ws_cfg.cell(r, 7).value or "").strip(),
        "Campo_BD_Leads_A_Actualizar": (ws_cfg.cell(r, 8).value or "").strip(),
        "Mensaje_Error": (ws_cfg.cell(r, 10).value or "Responde con una opción válida.").strip(),
    }

# =========================
# RUTA WHATSAPP (TWILIO)
# =========================
@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    msg_cliente = (request.values.get("Body", "") or "").strip()
    telefono = (request.values.get("From", "") or "").strip()

    resp = MessagingResponse()

    ws_leads = ws(TAB_LEADS)
    ws_cfg = ws(TAB_CONFIG)
    ws_logs = ws(TAB_LOGS)

    # 1) Buscar o crear lead
    row = buscar_fila_por_telefono(ws_leads, telefono)
    if not row:
        id_lead = crear_lead(ws_leads, telefono, msg_cliente)
        row = buscar_fila_por_telefono(ws_leads, telefono)
    else:
        id_lead = get_cell_by_name(ws_leads, row, "ID_Lead")

    # 2) Paso actual REAL del lead
    paso_actual = obtener_paso_actual(ws_leads, row)

    # 3) Obtener config del paso actual
    cfg = cfg_get_by_paso(ws_cfg, paso_actual)
    if not cfg:
        msg_out = f"[No existe Config_XimenaAI para {paso_actual}]"
        resp.message(msg_out)
        log_mensaje(ws_logs, telefono, id_lead, paso_actual, msg_cliente, msg_out, error="CONFIG_NOT_FOUND")
        return str(resp)

    # 4) Si paso_actual es INICIO:
    #    La primera vez el cliente manda "Hola..." (no es 1/2)
    #    Respondemos con texto de INICIO y esperamos el siguiente mensaje (1 o 2)
    if paso_actual == "INICIO" and msg_cliente and msg_cliente not in ["1", "2"]:
        msg_out = cfg["Texto_Bot"]
        resp.message(msg_out)

        # actualizar timestamp / último mensaje
        set_cell_by_name(ws_leads, row, "Ultima_Actualizacion", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
        set_cell_by_name(ws_leads, row, "Ultimo_Mensaje_Cliente", msg_cliente)

        log_mensaje(ws_logs, telefono, id_lead, paso_actual, msg_cliente, msg_out)
        return str(resp)

    # 5) Procesamiento por tipo de entrada
    tipo = cfg["Tipo_Entrada"]

    if tipo == "OPCIONES":
        validas = [x.strip() for x in cfg["Opciones_Validas"].split(",") if x.strip()]
        if msg_cliente not in validas:
            resp.message(cfg["Mensaje_Error"])
            log_mensaje(ws_logs, telefono, id_lead, paso_actual, msg_cliente, cfg["Mensaje_Error"], error="OPCION_INVALIDA")
            return str(resp)

        # guardar respuesta en campo indicado (si existe)
        campo_guardar = cfg["Campo_BD_Leads_A_Actualizar"]
        if campo_guardar:
            set_cell_by_name(ws_leads, row, campo_guardar, msg_cliente)

        # avanzar a siguiente paso según 1/2
        nuevo_paso = cfg["Siguiente_Si_1"] if msg_cliente == "1" else cfg["Siguiente_Si_2"]
        if nuevo_paso:
            guardar_paso_actual(ws_leads, row, nuevo_paso)

        # actualizar timestamps
        set_cell_by_name(ws_leads, row, "Ultima_Actualizacion", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
        set_cell_by_name(ws_leads, row, "Ultimo_Mensaje_Cliente", msg_cliente)

        # responder con texto del siguiente paso
        cfg_next = cfg_get_by_paso(ws_cfg, nuevo_paso) if nuevo_paso else None
        if not cfg_next:
            msg_out = "Listo. (No hay siguiente paso configurado)"
            resp.message(msg_out)
            log_mensaje(ws_logs, telefono, id_lead, nuevo_paso or paso_actual, msg_cliente, msg_out, error="NEXT_STEP_NOT_FOUND")
            return str(resp)

        msg_out = cfg_next["Texto_Bot"]
        resp.message(msg_out)
        log_mensaje(ws_logs, telefono, id_lead, cfg_next["ID_Paso"], msg_cliente, msg_out)
        return str(resp)

    # Si no es OPCIONES, por ahora solo devolvemos el texto del paso
    msg_out = cfg["Texto_Bot"] or "OK"
    resp.message(msg_out)
    log_mensaje(ws_logs, telefono, id_lead, paso_actual, msg_cliente, msg_out, error="TIPO_NO_IMPLEMENTADO")
    return str(resp)

# =========================
# RUTA NOTIFICAR (APPSHEET)
# =========================
@app.route("/notificar", methods=["POST"])
def enviar_notificacion():
    data = request.json or {}
    numero_cliente = data.get("telefono", "")
    tipo_noticia = data.get("tipo", "")
    fecha_cita = data.get("fecha", "")
    lugar = data.get("lugar", "Oficinas del Despacho")

    client = Client(TWILIO_SID, TWILIO_TOKEN)

    cuerpo_mensaje = (
        f"⚖️ *NOTIFICACIÓN LEGAL*\n\n"
        f"Hola, te informamos de una actualización en tu proceso:\n"
        f"🔹 *Evento:* {tipo_noticia}\n"
        f"📅 *Fecha:* {fecha_cita}\n"
        f"📍 *Lugar:* {lugar}\n\n"
        f"Para más detalles o ver tu expediente, entra aquí: https://tuderecholaboral.mx/mi-caso"
    )

    try:
        message = client.messages.create(
            body=cuerpo_mensaje,
            from_=TWILIO_NUMBER,
            to=numero_cliente
        )
        return jsonify({"status": "success", "sid": message.sid}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================
# HEALTHCHECK
# =========================
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "tuderecho-leads"}), 200

if __name__ == "__main__":
    # Render usa PORT
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
