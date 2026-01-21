import os
import re
import json
import uuid
from datetime import datetime, timezone

import gspread
from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse

# =========================
# CONFIG (Render Env Vars)
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "TDLM_Sistema_Leads_v1")

# Puedes usar UNA de estas 2:
# 1) GOOGLE_CREDS_JSON = contenido JSON completo del service account (recomendado en Render)
# 2) GOOGLE_CREDS_FILE = ruta a archivo json (local dev)
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
GOOGLE_CREDS_FILE = os.environ.get("GOOGLE_CREDS_FILE", "credenciales.json")

# Nombres de pestañas (si cambian en Sheets, cámbialos aquí o ponlos en env vars)
TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads")
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI")
TAB_LOGS   = os.environ.get("TAB_LOGS", "BD_Logs")  # ajusta si tu pestaña se llama distinto

# Default behavior
DEFAULT_ERROR_MSG = "⚠️ No pude procesar tu respuesta. Por favor intenta de nuevo."
DEFAULT_MISSING_CONFIG_MSG = "⚠️ No encuentro configuración para este paso. Un abogado te contactará."

app = Flask(__name__)

# =========================
# Helpers: Time + Text
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def normalize_phone(raw_from: str) -> str:
    # Twilio manda "whatsapp:+52..."
    return (raw_from or "").strip()

def detect_fuente(first_message: str) -> str:
    txt = (first_message or "").lower()
    if "facebook" in txt:
        return "FACEBOOK"
    if "sitio" in txt or "web" in txt or "pagina" in txt:
        return "SITIO_WEB"
    return "DESCONOCIDA"

def clean(s: str) -> str:
    return (s or "").strip()

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def is_valid_date_ddmmyyyy(s: str) -> bool:
    if not s:
        return False
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s.strip())
    if not m:
        return False
    d, mo, y = map(int, m.groups())
    try:
        datetime(y, mo, d)
        return True
    except ValueError:
        return False

# =========================
# Google Sheets Connection
# =========================
def get_gspread_client():
    if GOOGLE_CREDS_JSON:
        info = json.loads(GOOGLE_CREDS_JSON)
        return gspread.service_account_from_dict(info)
    return gspread.service_account(filename=GOOGLE_CREDS_FILE)

def open_ws(spread, title: str):
    # Evita StopIteration: regresa None si no existe
    try:
        return spread.worksheet(title)
    except Exception:
        return None

def header_map(ws):
    """
    Devuelve dict: {header_name: col_index (1-based)}
    Si no hay headers, devuelve {}.
    """
    try:
        headers = ws.row_values(1)
        m = {}
        for i, h in enumerate(headers, start=1):
            h2 = (h or "").strip()
            if h2:
                m[h2] = i
        return m
    except Exception:
        return {}

def get_first_existing_col(hmap, candidates):
    for c in candidates:
        if c in hmap:
            return hmap[c]
    return None

def safe_cell(ws, row, col):
    try:
        return clean(ws.cell(row, col).value)
    except Exception:
        return ""

def safe_update(ws, row, col, value):
    try:
        ws.update_cell(row, col, value)
        return True
    except Exception:
        return False

def safe_append(ws, values):
    try:
        ws.append_row(values, value_input_option="USER_ENTERED")
        return True
    except Exception:
        return False

# =========================
# Logging
# =========================
def safe_log(spread, data: dict):
    ws = open_ws(spread, TAB_LOGS)
    if not ws:
        # Si no existe pestaña logs, no tronar el webhook
        return

    h = header_map(ws)
    # Espera headers tipo:
    # ID_Log, Fecha_Hora, Telefono, ID_Lead, Paso, Mensaje_Entrante, Mensaje_Saliente, Canal, Fuente_Lead, Modelo_AI, Errores
    row = [""] * max(11, len(h) if h else 11)

    def put(key, val):
        if not h:
            return
        if key in h:
            idx = h[key] - 1
            if idx >= len(row):
                row.extend([""] * (idx - len(row) + 1))
            row[idx] = val

    put("ID_Log", data.get("ID_Log", str(uuid.uuid4())))
    put("Fecha_Hora", data.get("Fecha_Hora", now_iso()))
    put("Telefono", data.get("Telefono", ""))
    put("ID_Lead", data.get("ID_Lead", ""))
    put("Paso", data.get("Paso", ""))
    put("Mensaje_Entrante", data.get("Mensaje_Entrante", ""))
    put("Mensaje_Saliente", data.get("Mensaje_Saliente", ""))
    put("Canal", data.get("Canal", "WHATSAPP"))
    put("Fuente_Lead", data.get("Fuente_Lead", ""))
    put("Modelo_AI", data.get("Modelo_AI", ""))
    put("Errores", data.get("Errores", ""))

    # Si no hay headers, igual intenta append "crudo"
    if not h:
        safe_append(ws, [
            data.get("ID_Log", str(uuid.uuid4())),
            data.get("Fecha_Hora", now_iso()),
            data.get("Telefono", ""),
            data.get("ID_Lead", ""),
            data.get("Paso", ""),
            data.get("Mensaje_Entrante", ""),
            data.get("Mensaje_Saliente", ""),
            "WHATSAPP",
            data.get("Fuente_Lead", ""),
            data.get("Modelo_AI", ""),
            data.get("Errores", ""),
        ])
        return

    safe_append(ws, row)

# =========================
# Leads (BD_Leads)
# =========================
def find_lead_row_by_phone(ws_leads, phone):
    # Busca en columna "Telefono" si existe
    h = header_map(ws_leads)
    col_phone = get_first_existing_col(h, ["Telefono", "TELEFONO", "phone", "Phone"])
    if not col_phone:
        return None

    # Búsqueda manual robusta (evita cell.find que a veces se rompe con formatos)
    try:
        all_vals = ws_leads.col_values(col_phone)
        for i in range(2, len(all_vals) + 1):
            if clean(all_vals[i - 1]) == phone:
                return i
    except Exception:
        return None

    return None

def create_new_lead(ws_leads, phone, first_msg):
    h = header_map(ws_leads)
    lead_id = str(uuid.uuid4())
    fuente = detect_fuente(first_msg)

    col_id = get_first_existing_col(h, ["ID_Lead", "ID_LEAD"])
    col_phone = get_first_existing_col(h, ["Telefono", "TELEFONO"])
    col_fuente = get_first_existing_col(h, ["Fuente_Lead", "FUENTE_LEAD"])
    col_freg = get_first_existing_col(h, ["Fecha_Registro", "FECHA_REGISTRO"])
    col_update = get_first_existing_col(h, ["Ultima_Actualizacion", "ULTIMA_ACTUALIZACION"])
    col_status = get_first_existing_col(h, ["Estatus_Chat", "ESTATUS", "Status", "STATUS"])

    # Construye fila del tamaño de headers
    ncols = max(len(h), 20)
    row = [""] * ncols

    def setc(col, val):
        if col and col >= 1:
            if col - 1 >= len(row):
                row.extend([""] * (col - len(row)))
            row[col - 1] = val

    setc(col_id, lead_id)
    setc(col_phone, phone)
    setc(col_fuente, fuente)
    setc(col_freg, now_iso())
    setc(col_update, now_iso())
    setc(col_status, "INICIO")

    ok = safe_append(ws_leads, row)
    return lead_id if ok else lead_id

def get_lead_status(ws_leads, lead_row):
    h = header_map(ws_leads)
    col_status = get_first_existing_col(h, ["Estatus_Chat", "ESTATUS", "Status", "STATUS"])
    if not col_status:
        return "INICIO"
    return safe_cell(ws_leads, lead_row, col_status) or "INICIO"

def set_lead_status(ws_leads, lead_row, status):
    h = header_map(ws_leads)
    col_status = get_first_existing_col(h, ["Estatus_Chat", "ESTATUS", "Status", "STATUS"])
    if col_status:
        safe_update(ws_leads, lead_row, col_status, status)
    # también actualiza Ultima_Actualizacion si existe
    col_update = get_first_existing_col(h, ["Ultima_Actualizacion", "ULTIMA_ACTUALIZACION"])
    if col_update:
        safe_update(ws_leads, lead_row, col_update, now_iso())

def set_lead_field(ws_leads, lead_row, field_name, value):
    """
    field_name debe ser un header REAL en BD_Leads, ej: "Aviso_Privacidad_Aceptado", "Tipo_Caso", etc.
    Si no existe, no truena.
    """
    h = header_map(ws_leads)
    if field_name in h:
        safe_update(ws_leads, lead_row, h[field_name], value)
        # update timestamp
        col_update = get_first_existing_col(h, ["Ultima_Actualizacion", "ULTIMA_ACTUALIZACION"])
        if col_update:
            safe_update(ws_leads, lead_row, col_update, now_iso())
        return True
    return False

def get_lead_id(ws_leads, lead_row):
    h = header_map(ws_leads)
    col_id = get_first_existing_col(h, ["ID_Lead", "ID_LEAD"])
    return safe_cell(ws_leads, lead_row, col_id) if col_id else ""

# =========================
# Config (Config_XimenaAI)
# =========================
def load_config_row(ws_config, paso_id):
    """
    Busca la fila donde ID_Paso == paso_id.
    Devuelve dict con:
      Texto_Bot, Tipo_Entrada, Opciones_Validas, Siguiente_Si_1, Siguiente_Si_2,
      Campo_BD_Leads_A_Actualizar, Mensaje_Error
    Si no existe, devuelve None.
    """
    h = header_map(ws_config)

    # Headers esperados
    col_idpaso = get_first_existing_col(h, ["ID_Paso", "ID_PASO"])
    if not col_idpaso:
        return None

    # Buscar fila manual
    try:
        ids = ws_config.col_values(col_idpaso)
        row = None
        for i in range(2, len(ids) + 1):
            if clean(ids[i - 1]) == paso_id:
                row = i
                break
        if not row:
            return None
    except Exception:
        return None

    def v(header_name):
        idx = h.get(header_name)
        if not idx:
            return ""
        return safe_cell(ws_config, row, idx)

    return {
        "row": row,
        "ID_Paso": paso_id,
        "Texto_Bot": v("Texto_Bot"),
        "Tipo_Entrada": v("Tipo_Entrada").upper(),
        "Opciones_Validas": v("Opciones_Validas"),
        "Siguiente_Si_1": v("Siguiente_Si_1"),
        "Siguiente_Si_2": v("Siguiente_Si_2"),
        "Campo_BD_Leads_A_Actualizar": v("Campo_BD_Leads_A_Actualizar"),
        "Mensaje_Error": v("Mensaje_Error") or DEFAULT_ERROR_MSG,
    }

def build_options_text(texto_bot, opciones_validas):
    # Para WhatsApp: mandamos 1/2 en texto (evita botones por restricciones)
    # opciones_validas "1,2"
    ops = [o.strip() for o in (opciones_validas or "").split(",") if o.strip()]
    if not ops:
        return texto_bot
    out = texto_bot.strip() + "\n\n"
    # Por defecto: 1 Sí / 2 No (si tu config ya trae el texto, no dupliques)
    if "¿" in texto_bot or "?" in texto_bot:
        pass
    # mostramos las opciones genéricas
    if "1" in ops:
        out += "1) Sí\n"
    if "2" in ops:
        out += "2) No\n"
    return out.strip()

# =========================
# Validation + Next Step
# =========================
def validate_and_next(cfg, user_msg):
    tipo = cfg.get("Tipo_Entrada", "TEXTO")
    msg = clean(user_msg)

    if tipo == "OPCIONES":
        valid = [o.strip() for o in (cfg.get("Opciones_Validas") or "").split(",") if o.strip()]
        if msg not in valid:
            return False, None, cfg.get("Mensaje_Error") or "Responde con una opción válida."
        if msg == "1":
            return True, cfg.get("Siguiente_Si_1") or "", ""
        if msg == "2":
            return True, cfg.get("Siguiente_Si_2") or "", ""
        # si algún día agregas 3/4, aquí se amplía
        return True, cfg.get("Siguiente_Si_1") or "", ""

    if tipo == "FECHA":
        if not is_valid_date_ddmmyyyy(msg):
            return False, None, cfg.get("Mensaje_Error") or "Usa formato DD/MM/AAAA."
        return True, cfg.get("Siguiente_Si_1") or "", ""

    if tipo == "NUMERO":
        # acepta 10,000 o 10000.50
        m = msg.replace(",", "")
        try:
            float(m)
            return True, cfg.get("Siguiente_Si_1") or "", ""
        except Exception:
            return False, None, cfg.get("Mensaje_Error") or "Indica un número válido."

    # TEXTO, CHATGPT, SISTEMA
    return True, cfg.get("Siguiente_Si_1") or "", ""

# =========================
# Webhook: WhatsApp
# =========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": now_iso()}), 200

@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    resp = MessagingResponse()

    from_phone = normalize_phone(request.values.get("From"))
    msg_in = clean(request.values.get("Body"))

    # Conecta a Google
    try:
        gc = get_gspread_client()
        spread = gc.open(GOOGLE_SHEET_NAME)
    except Exception as e:
        out = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."
        resp.message(out)
        return str(resp)

    ws_leads = open_ws(spread, TAB_LEADS)
    ws_config = open_ws(spread, TAB_CONFIG)

    if not ws_leads:
        out = f"⚠️ No existe la pestaña {TAB_LEADS} en tu Google Sheet."
        resp.message(out)
        return str(resp)

    if not ws_config:
        out = f"⚠️ No existe la pestaña {TAB_CONFIG} en tu Google Sheet."
        resp.message(out)
        return str(resp)

    # 1) Buscar lead
    lead_row = find_lead_row_by_phone(ws_leads, from_phone)

    # 2) Si no existe lead: crear y enviar INICIO
    if not lead_row:
        lead_id = create_new_lead(ws_leads, from_phone, msg_in)
        cfg = load_config_row(ws_config, "INICIO")
        if not cfg:
            out = DEFAULT_MISSING_CONFIG_MSG + " (Falta paso INICIO en Config)"
            resp.message(out)
            safe_log(spread, {
                "Telefono": from_phone, "ID_Lead": lead_id, "Paso": "INICIO",
                "Mensaje_Entrante": msg_in, "Mensaje_Saliente": out,
                "Fuente_Lead": detect_fuente(msg_in), "Errores": "No existe config INICIO"
            })
            return str(resp)

        text_out = build_options_text(cfg["Texto_Bot"], cfg["Opciones_Validas"])
        resp.message(text_out)

        safe_log(spread, {
            "Telefono": from_phone, "ID_Lead": lead_id, "Paso": "INICIO",
            "Mensaje_Entrante": msg_in, "Mensaje_Saliente": text_out,
            "Fuente_Lead": detect_fuente(msg_in), "Errores": ""
        })
        return str(resp)

    # 3) Lead existe
    lead_id = get_lead_id(ws_leads, lead_row)
    paso_actual = get_lead_status(ws_leads, lead_row)
    cfg = load_config_row(ws_config, paso_actual)

    if not cfg:
        out = DEFAULT_MISSING_CONFIG_MSG + f" (Falta paso {paso_actual} en Config)"
        resp.message(out)
        safe_log(spread, {
            "Telefono": from_phone, "ID_Lead": lead_id, "Paso": paso_actual,
            "Mensaje_Entrante": msg_in, "Mensaje_Saliente": out,
            "Fuente_Lead": "", "Errores": f"No existe config: {paso_actual}"
        })
        return str(resp)

    # 4) Validar respuesta y determinar siguiente paso
    ok, next_step, err = validate_and_next(cfg, msg_in)
    if not ok:
        resp.message(err or DEFAULT_ERROR_MSG)
        safe_log(spread, {
            "Telefono": from_phone, "ID_Lead": lead_id, "Paso": paso_actual,
            "Mensaje_Entrante": msg_in, "Mensaje_Saliente": err or DEFAULT_ERROR_MSG,
            "Errores": f"Validación fallida en {paso_actual}"
        })
        return str(resp)

    # 5) Guardar dato en BD_Leads si corresponde
    campo = clean(cfg.get("Campo_BD_Leads_A_Actualizar", ""))
    if campo:
        # OJO: aquí se guarda en un HEADER REAL de BD_Leads (ej Aviso_Privacidad_Aceptado, Tipo_Caso, etc.)
        # Tu ajuste: AVISO_OK lo cambiaste, pero mejor es guardar en Aviso_Privacidad_Aceptado (si ya existe).
        # Este script NO truena si no existe.
        set_lead_field(ws_leads, lead_row, campo, msg_in)

    # 6) Cambiar estatus al siguiente paso
    if next_step:
        set_lead_status(ws_leads, lead_row, next_step)
    else:
        # si no hay siguiente, no tronar
        next_step = paso_actual

    # 7) Responder con el texto del siguiente paso (si existe config)
    cfg_next = load_config_row(ws_config, next_step)
    if not cfg_next:
        out = DEFAULT_MISSING_CONFIG_MSG + f" (Falta paso {next_step} en Config)"
        resp.message(out)
        safe_log(spread, {
            "Telefono": from_phone, "ID_Lead": lead_id, "Paso": next_step,
            "Mensaje_Entrante": msg_in, "Mensaje_Saliente": out,
            "Errores": f"No existe config next_step: {next_step}"
        })
        return str(resp)

    # Si el siguiente paso es SISTEMA, también puede mandar texto_bot y finalizar
    tipo_next = cfg_next.get("Tipo_Entrada", "TEXTO")
    if tipo_next == "OPCIONES":
        text_out = build_options_text(cfg_next["Texto_Bot"], cfg_next["Opciones_Validas"])
    else:
        text_out = cfg_next["Texto_Bot"] or DEFAULT_ERROR_MSG

    resp.message(text_out)

    safe_log(spread, {
        "Telefono": from_phone, "ID_Lead": lead_id, "Paso": next_step,
        "Mensaje_Entrante": msg_in, "Mensaje_Saliente": text_out,
        "Errores": ""
    })
    return str(resp)

# =========================
# MAIN (local dev)
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
