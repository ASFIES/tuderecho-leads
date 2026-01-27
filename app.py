import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

import gspread
from google.oauth2.service_account import Credentials

# =========================
# App Config
# =========================
app = Flask(__name__)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

# =========================
# Twilio & Text Utils
# =========================
def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def render_text(s: str) -> str:
    s = s or ""
    return s.replace("\\n", "\n")

def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    s = (raw or "").strip()
    return s.replace("whatsapp:", "").strip()

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    return re.sub(r"\s+", " ", s).strip()

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    return m.group(0) if m else s

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if any(x in t for x in ["facebook", "anuncio", "fb"]): return "FACEBOOK"
    if any(x in t for x in ["sitio", "web", "pagina", "página"]): return "WEB"
    return "DESCONOCIDA"

# =========================
# Google Sheets Core
# =========================
def get_env_creds_dict():
    if GOOGLE_CREDENTIALS_JSON:
        try:
            raw = GOOGLE_CREDENTIALS_JSON
            if raw.lstrip().startswith("{"): return json.loads(raw)
            return json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Error en credenciales JSON: {e}")
    if GOOGLE_CREDENTIALS_PATH and os.path.exists(GOOGLE_CREDENTIALS_PATH):
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f: return json.load(f)
    raise RuntimeError("Faltan credenciales de Google.")

def get_gspread_client():
    creds = Credentials.from_service_account_info(get_env_creds_dict(), 
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

def build_header_map(ws):
    headers = ws.row_values(1)
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1) if h.strip()}

def col_idx(headers_map: dict, name: str):
    return headers_map.get(name.lower())

def find_row_by_value(ws, col_idx_num: int, value: str):
    col_values = ws.col_values(col_idx_num)
    for i, v in enumerate(col_values[1:], start=2):
        if (v or "").strip() == value.strip(): return i
    return None

def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    payload = []
    for col_name, val in updates.items():
        idx = col_idx(header_map, col_name)
        if idx:
            payload.append({"range": gspread.utils.rowcol_to_a1(row_idx, idx), "values": [[val]]})
    if payload: ws.batch_update(payload)

# =========================
# Logic & Flow
# =========================
def get_or_create_lead(ws_leads, headers, tel_raw, tel_norm, fuente):
    idx_tel = col_idx(headers, "Telefono")
    row = find_row_by_value(ws_leads, idx_tel, tel_raw) or find_row_by_value(ws_leads, idx_tel, tel_norm)
    
    if row:
        vals = ws_leads.row_values(row)
        estatus = vals[col_idx(headers, "ESTATUS")-1] if col_idx(headers, "ESTATUS") <= len(vals) else "INICIO"
        return row, estatus, False
    
    # Crear nuevo Lead
    lead_id = str(uuid.uuid4())
    new_row = [""] * 50 # Buffer
    def set_c(name, val):
        idx = col_idx(headers, name)
        if idx: new_row[idx-1] = val

    set_c("ID_Lead", lead_id)
    set_c("Telefono", tel_raw)
    set_c("Telefono_Normalizado", tel_norm)
    set_c("Fuente_Lead", fuente)
    set_c("Fecha_Registro", now_iso_mx())
    set_c("ESTATUS", "INICIO")
    
    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")
    return find_row_by_value(ws_leads, idx_tel, tel_raw), "INICIO", True

def load_config_row(ws_config, paso_actual):
    paso_actual = paso_actual or "INICIO"
    data = ws_config.get_all_records()
    for row in data:
        if str(row.get("ID_Paso", "")).strip() == paso_actual:
            return row
    return data[0] if data else {}

def build_date_from_parts(y, m, d):
    try: return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
    except: return ""

# =========================
# Webhook Route
# =========================
@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone = phone_raw(request.form.get("From") or "")
    from_norm = phone_norm(from_phone)
    msg_in = normalize_msg(request.form.get("Body") or "")
    msg_opt = normalize_option(request.form.get("Body") or "")

    if not msg_in: return safe_reply("Hola 👋")

    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws_leads = sh.worksheet(TAB_LEADS)
        ws_config = sh.worksheet(TAB_CONFIG)
        headers = build_header_map(ws_leads)
        
        row_idx, estatus_actual, created = get_or_create_lead(ws_leads, headers, from_phone, from_norm, detect_fuente(msg_in))
        
        # Obtener configuración del paso actual
        cfg = load_config_row(ws_config, estatus_actual)
        tipo = str(cfg.get("Tipo_Entrada", "")).upper()
        
        next_paso = estatus_actual
        
        # Lógica de Navegación
        if tipo == "OPCIONES":
            opciones = [x.strip() for x in str(cfg.get("Opciones_Validas", "")).split(",")]
            if msg_opt in opciones:
                campo = cfg.get("Campo_BD_Leads_A_Actualizar")
                if campo: update_lead_batch(ws_leads, headers, row_idx, {campo: msg_opt})
                # Determinar siguiente paso
                next_paso = str(cfg.get(f"Siguiente_Si_{msg_opt}", cfg.get("Siguiente_Si_1", estatus_actual)))
            else:
                return safe_reply(render_text(f"{cfg.get('Texto_Bot')}\n\n⚠️ {cfg.get('Mensaje_Error')}"))

        elif tipo == "TEXTO":
            campo = cfg.get("Campo_BD_Leads_A_Actualizar")
            if campo: update_lead_batch(ws_leads, headers, row_idx, {campo: msg_in})
            next_paso = str(cfg.get("Siguiente_Si_1", estatus_actual))

        # Saltos especiales (No pedir correo)
        if next_paso.upper() == "CORREO": next_paso = "DESCRIPCION"

        # Manejo de Fechas (FIX)
        if next_paso in ["INI_MES", "INI_DIA", "FIN_MES", "FIN_DIA", "SALARIO"]:
            # Aquí podrías agregar validaciones de fecha si fuera necesario
            pass

        # RELEVO AL WORKER
        if next_paso == "GENERAR_RESULTADOS":
            update_lead_batch(ws_leads, headers, row_idx, {
                "ESTATUS": "PROCESANDO",
                "Procesar_AI_Status": "PENDIENTE",
                "Ultima_Actualizacion": now_iso_mx()
            })
            return safe_reply("⚖️ *Estamos analizando tu situación laboral...*\n\nNuestra inteligencia legal está revisando los datos para asignarte al mejor abogado y calcular tu estimación. Un momento, por favor.")

        # Cargar texto del siguiente paso
        cfg_next = load_config_row(ws_config, next_paso)
        update_lead_batch(ws_leads, headers, row_idx, {
            "ESTATUS": next_paso,
            "Ultima_Actualizacion": now_iso_mx(),
            "Ultimo_Mensaje_Cliente": msg_in
        })

        return safe_reply(render_text(cfg_next.get("Texto_Bot", "Continuemos...")))

    except Exception as e:
        print(f"Error: {e}")
        return safe_reply("⚠️ Lo siento, tuve un problema al conectar con mi base de datos. Intenta de nuevo.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))