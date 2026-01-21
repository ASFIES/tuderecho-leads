import os
import json
import re
import uuid
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG (ENV)
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "TDLM_Sistema_Leads_v1")

TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads")
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI")
TAB_LOGS   = os.environ.get("TAB_LOGS", "Logs_Mensajes")
TAB_KB     = os.environ.get("TAB_KB", "Conocimiento_AI")  # opcional

# Credenciales Google: JSON completo (recomendado) o ruta de archivo.
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "credenciales.json")

# OpenAI opcional (solo si Tipo_Entrada = CHATGPT)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# =========================
# APP
# =========================
app = Flask(__name__)

# =========================
# HELPERS
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def clean_text(s: str) -> str:
    return (s or "").strip()

def detect_fuente_lead(msg_inicial: str) -> str:
    t = (msg_inicial or "").lower()
    if "facebook" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t:
        return "WEB"
    return "WHATSAPP"  # default genérico

def normalize_phone(from_value: str) -> str:
    # Twilio manda "whatsapp:+521..."
    return clean_text(from_value)

def parse_opcion(msg: str) -> str:
    # Tomamos primer dígito si existe ("1", "1.", "1 Sí", etc.)
    m = re.search(r"\b(\d+)\b", msg or "")
    return m.group(1) if m else clean_text(msg)

# =========================
# GOOGLE SHEETS (ROBUSTO)
# =========================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    if GOOGLE_CREDENTIALS_JSON:
        info = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    # fallback a archivo en repo
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

_gc = None

def gc():
    global _gc
    if _gc is None:
        _gc = get_gspread_client()
    return _gc

def sh():
    return gc().open(GOOGLE_SHEET_NAME)

def ws(nombre_hoja: str):
    """
    Nunca truena: si no existe la hoja, regresa None (y el webhook no muere).
    """
    try:
        return sh().worksheet(nombre_hoja)
    except Exception:
        return None

def get_headers_map(worksheet):
    """
    Regresa dict: { "NombreColumna": index_1based }
    """
    if worksheet is None:
        return {}
    headers = worksheet.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}

def find_row_by_value(worksheet, header_map, col_name, value):
    """
    Busca fila por valor exacto en columna col_name. Si no encuentra, regresa None.
    """
    if worksheet is None:
        return None
    col_idx = header_map.get(col_name)
    if not col_idx:
        return None
    try:
        # gspread find busca en toda la hoja, pero nosotros limitamos a la columna
        col_values = worksheet.col_values(col_idx)
        for r, v in enumerate(col_values, start=1):
            if r == 1:
                continue
            if clean_text(v) == clean_text(value):
                return r
        return None
    except Exception:
        return None

def safe_update_cell(worksheet, header_map, row, col_name, value):
    """
    Actualiza solo si existe columna.
    """
    if worksheet is None:
        return
    col_idx = header_map.get(col_name)
    if not col_idx:
        return
    worksheet.update_cell(row, col_idx, value)

def safe_append_row(worksheet, row_values):
    if worksheet is None:
        return
    worksheet.append_row(row_values, value_input_option="USER_ENTERED")

# =========================
# CONFIG FLOW
# =========================
def load_config_row(ws_config, cfg_headers, paso_id):
    """
    Busca en Config_XimenaAI la fila con ID_Paso = paso_id.
    Retorna dict con datos.
    """
    if ws_config is None:
        return None

    row = find_row_by_value(ws_config, cfg_headers, "ID_Paso", paso_id)
    if not row:
        return None

    def v(col):
        idx = cfg_headers.get(col)
        return ws_config.cell(row, idx).value.strip() if idx else ""

    return {
        "row": row,
        "ID_Paso": paso_id,
        "Texto_Bot": v("Texto_Bot"),
        "Tipo_Entrada": v("Tipo_Entrada").upper(),
        "Opciones_Validas": v("Opciones_Validas"),
        "Siguiente_Si_1": v("Siguiente_Si_1"),
        "Siguiente_Si_2": v("Siguiente_Si_2"),
        "Campo_BD_Leads_A_Actualizar": v("Campo_BD_Leads_A_Actualizar"),
        "Regla_Validacion": v("Regla_Validacion"),
        "Mensaje_Error": v("Mensaje_Error"),
    }

def opciones_validas_set(opciones_str: str):
    # "1,2" -> {"1","2"}
    parts = [p.strip() for p in (opciones_str or "").split(",") if p.strip()]
    return set(parts)

def format_bot_text(texto: str):
    # En tu Sheet tienes \n\n, aquí lo convertimos a saltos reales
    t = texto or ""
    t = t.replace("\\n", "\n")
    return t.strip()

# =========================
# AI (opcional y simple)
# =========================
def respuesta_ai_simple(descripcion: str):
    # Por ahora, seguro y neutro:
    # (Luego lo conectamos a KB + OpenAI)
    return ("Gracias. Estoy analizando tu situación. "
            "En este momento estamos en pruebas; en breve un abogado revisará tu caso. "
            "Mientras tanto, evita firmar documentos sin asesoría.")

# =========================
# LOGGING
# =========================
def log_event(ws_logs, logs_headers, payload: dict):
    """
    Inserta en Logs_Mensajes si existe.
    Columnas esperadas (si existen): ID_Log, Fecha_Hora, Telefono, ID_Lead, Paso, Mensaje_Entrante,
    Mensaje_Saliente, Canal, Fuente_Lead, Modelo_AI, Errores
    """
    if ws_logs is None:
        return

    # Si tu hoja tiene headers distintos, igual no truena: solo mete en el orden existente.
    headers = ws_logs.row_values(1)
    row = []
    for h in headers:
        key = h.strip()
        row.append(payload.get(key, ""))
    safe_append_row(ws_logs, row)

# =========================
# ENDPOINTS
# =========================
@app.route("/", methods=["GET"])
def home():
    return "OK - Tu Derecho Laboral Leads", 200

@app.route("/health", methods=["GET"])
def health():
    # Prueba de conexión a sheets sin tumbar
    _ = ws(TAB_LEADS)
    return "OK", 200

@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    """
    Webhook Twilio WhatsApp:
    - Busca/crea lead
    - Usa Estatus_Chat como paso actual
    - Lee Config_XimenaAI para validar y avanzar
    - Loggea en Logs_Mensajes
    """
    resp = MessagingResponse()

    msg_in = clean_text(request.values.get("Body", ""))
    from_phone = normalize_phone(request.values.get("From", ""))

    # Sheets
    ws_leads = ws(TAB_LEADS)
    ws_config = ws(TAB_CONFIG)
    ws_logs = ws(TAB_LOGS)

    leads_headers = get_headers_map(ws_leads)
    cfg_headers = get_headers_map(ws_config)
    logs_headers = get_headers_map(ws_logs)

    # Si falta BD_Leads o Config, respondemos sin caer
    if ws_leads is None or ws_config is None:
        resp.message("Estamos en mantenimiento. Intenta de nuevo en unos minutos.")
        return str(resp)

    # Buscar lead por Telefono
    lead_row = find_row_by_value(ws_leads, leads_headers, "Telefono", from_phone)

    # Si NO existe lead: lo creamos y mandamos INICIO
    if not lead_row:
        new_id = str(uuid.uuid4())
        fuente = detect_fuente_lead(msg_in)
        # Campos mínimos (solo si existen columnas)
        # Creamos la fila completa en el orden de headers de BD_Leads
        headers = ws_leads.row_values(1)
        data = {h: "" for h in headers}

        data["ID_Lead"] = new_id
        data["Telefono"] = from_phone
        data["Fuente_Lead"] = fuente
        data["Fecha_Registro"] = now_iso()
        data["Ultima_Actualizacion"] = now_iso()
        data["Estatus_Chat"] = "INICIO"

        # Guarda el primer mensaje del cliente si existe columna
        if "Ultimo_Mensaje_Cliente" in data:
            data["Ultimo_Mensaje_Cliente"] = msg_in

        row_to_append = [data.get(h, "") for h in headers]
        safe_append_row(ws_leads, row_to_append)

        # Respuesta INICIO desde Config
        cfg = load_config_row(ws_config, cfg_headers, "INICIO")
        if not cfg:
            text_out = "Hola, soy Ximena. Estamos en pruebas. En breve te atenderemos."
        else:
            text_out = format_bot_text(cfg["Texto_Bot"])

        resp.message(text_out)

        log_event(ws_logs, logs_headers, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
            "ID_Lead": new_id,
            "Paso": "INICIO",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": text_out,
            "Canal": "WHATSAPP",
            "Fuente_Lead": fuente,
            "Modelo_AI": "",
            "Errores": "",
        })

        return str(resp)

    # Si ya existe lead: procesar paso actual
    lead_id = ws_leads.cell(lead_row, leads_headers.get("ID_Lead")).value if leads_headers.get("ID_Lead") else ""
    paso_actual = ws_leads.cell(lead_row, leads_headers.get("Estatus_Chat")).value if leads_headers.get("Estatus_Chat") else "INICIO"
    paso_actual = (paso_actual or "INICIO").strip()

    cfg = load_config_row(ws_config, cfg_headers, paso_actual)
    if not cfg:
        # Si no hay config del paso actual, regresamos a INICIO sin caer
        cfg_inicio = load_config_row(ws_config, cfg_headers, "INICIO")
        text_out = format_bot_text(cfg_inicio["Texto_Bot"]) if cfg_inicio else "Hola, soy Ximena. En breve te atendemos."
        safe_update_cell(ws_leads, leads_headers, lead_row, "Estatus_Chat", "INICIO")
        safe_update_cell(ws_leads, leads_headers, lead_row, "Ultima_Actualizacion", now_iso())
        resp.message(text_out)
        log_event(ws_logs, logs_headers, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
            "ID_Lead": lead_id,
            "Paso": paso_actual,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": text_out,
            "Canal": "WHATSAPP",
            "Fuente_Lead": ws_leads.cell(lead_row, leads_headers.get("Fuente_Lead")).value if leads_headers.get("Fuente_Lead") else "",
            "Modelo_AI": "",
            "Errores": f"No existe Config para paso {paso_actual}",
        })
        return str(resp)

    tipo = cfg["Tipo_Entrada"]

    # Guardar último mensaje del cliente (si existe columna)
    safe_update_cell(ws_leads, leads_headers, lead_row, "Ultimo_Mensaje_Cliente", msg_in)

    # Validación y avance
    siguiente_paso = None
    error_txt = ""
    valor_a_guardar = ""

    if tipo == "OPCIONES":
        opcion = parse_opcion(msg_in)
        validas = opciones_validas_set(cfg["Opciones_Validas"])
        if validas and opcion not in validas:
            error_txt = cfg["Mensaje_Error"] or "Por favor responde con una opción válida."
        else:
            valor_a_guardar = opcion
            if opcion == "1":
                siguiente_paso = cfg["Siguiente_Si_1"]
            elif opcion == "2":
                siguiente_paso = cfg["Siguiente_Si_2"]
            else:
                # Si hay más opciones algún día
                siguiente_paso = cfg["Siguiente_Si_1"] or "INICIO"

    elif tipo in ("TEXTO", "NUMERO", "FECHA"):
        # Aquí solo validamos básico; reglas avanzadas luego.
        valor_a_guardar = msg_in

        if tipo == "NUMERO":
            # limpiar $ y comas
            raw = re.sub(r"[^\d.]", "", msg_in)
            if raw == "":
                error_txt = cfg["Mensaje_Error"] or "Indica un número válido."
            else:
                valor_a_guardar = raw

        if tipo == "FECHA":
            # acepta DD/MM/AAAA
            if not re.match(r"^\d{2}/\d{2}/\d{4}$", msg_in):
                error_txt = cfg["Mensaje_Error"] or "Usa el formato DD/MM/AAAA."

        if not error_txt:
            siguiente_paso = cfg["Siguiente_Si_1"] or "INICIO"

    elif tipo == "CHATGPT":
        valor_a_guardar = msg_in
        siguiente_paso = cfg["Siguiente_Si_1"] or "INICIO"

    elif tipo == "SISTEMA":
        # Paso informativo, no espera input; re-envía mismo texto
        siguiente_paso = cfg["Siguiente_Si_1"] or "INICIO"

    else:
        # desconocido
        error_txt = "Configuración inválida del bot. (Tipo_Entrada)"
    
    # Si hay error, repetimos el texto del paso actual + Mensaje_Error
    if error_txt:
        text_out = f"{format_bot_text(cfg['Texto_Bot'])}\n\n{error_txt}".strip()
        resp.message(text_out)
        safe_update_cell(ws_leads, leads_headers, lead_row, "Ultima_Actualizacion", now_iso())

        log_event(ws_logs, logs_headers, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
            "ID_Lead": lead_id,
            "Paso": paso_actual,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": text_out,
            "Canal": "WHATSAPP",
            "Fuente_Lead": ws_leads.cell(lead_row, leads_headers.get("Fuente_Lead")).value if leads_headers.get("Fuente_Lead") else "",
            "Modelo_AI": "",
            "Errores": error_txt,
        })
        return str(resp)

    # Guardar en BD_Leads el campo indicado por config (si existe)
    campo_update = clean_text(cfg["Campo_BD_Leads_A_Actualizar"])
    if campo_update:
        safe_update_cell(ws_leads, leads_headers, lead_row, campo_update, valor_a_guardar)

    # Si es paso CHATGPT, generamos respuesta y la guardamos en Analisis_AI (si existe)
    modelo_ai = ""
    if tipo == "CHATGPT":
        ai_text = respuesta_ai_simple(msg_in)
        modelo_ai = "AI_SIMPLE"
        safe_update_cell(ws_leads, leads_headers, lead_row, "Analisis_AI", ai_text)

    # Avanzar Estatus_Chat
    if not siguiente_paso:
        siguiente_paso = "INICIO"

    safe_update_cell(ws_leads, leads_headers, lead_row, "Estatus_Chat", siguiente_paso)
    safe_update_cell(ws_leads, leads_headers, lead_row, "Ultima_Actualizacion", now_iso())

    # Responder con el siguiente paso (texto del bot)
    cfg_next = load_config_row(ws_config, cfg_headers, siguiente_paso)
    if not cfg_next:
        text_out = "Gracias. En breve un abogado te contactará."
    else:
        text_out = format_bot_text(cfg_next["Texto_Bot"])

        # Si el siguiente paso es OPCIONES y quieres que se vean las opciones, ya vienen en Texto_Bot.
        # (Si no vienen, agrégalas en Sheet en el Texto_Bot.)

    resp.message(text_out)

    log_event(ws_logs, logs_headers, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso(),
        "Telefono": from_phone,
        "ID_Lead": lead_id,
        "Paso": paso_actual,
        "Mensaje_Entrante": msg_in,
        "Mensaje_Saliente": text_out,
        "Canal": "WHATSAPP",
        "Fuente_Lead": ws_leads.cell(lead_row, leads_headers.get("Fuente_Lead")).value if leads_headers.get("Fuente_Lead") else "",
        "Modelo_AI": modelo_ai,
        "Errores": "",
    })

    return str(resp)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
