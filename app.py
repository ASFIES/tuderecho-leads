import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

import gspread
from google.oauth2.service_account import Credentials
import openai

# =========================
# App
# =========================
app = Flask(__name__)

# =========================
# Env
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

# =========================
# Time + Twilio
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def render_text(s: str) -> str:
    s = s or ""
    return s.replace("\\n", "\n")

# =========================
# Normalización
# =========================
def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    s = (raw or "").strip()
    s = s.replace("whatsapp:", "").strip()
    return s

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    if m:
        return m.group(0)
    return s

# =========================
# Google creds + gspread
# =========================
def get_env_creds_dict():
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        try:
            if raw.lstrip().startswith("{"):
                return json.loads(raw)
            decoded = base64.b64decode(raw).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido. Detalle: {e}")
    if GOOGLE_CREDENTIALS_PATH:
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    raise RuntimeError("Faltan credenciales.")

def get_gspread_client():
    creds_info = get_env_creds_dict()
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    return gc.open(GOOGLE_SHEET_NAME)

def open_worksheet(sh, title: str):
    return sh.worksheet(title)

# =========================
# Headers e Indices
# =========================
def build_header_map(ws):
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if not key: continue
        m[key] = i
        m[key.lower()] = i
    return m

def col_idx(headers_map: dict, name: str):
    return headers_map.get(name) or headers_map.get((name or "").lower())

def find_row_by_value(ws, col_idx_num: int, value: str):
    col_values = ws.col_values(col_idx_num)
    for i, v in enumerate(col_values[1:], start=2):
        if (v or "").strip() == value: return i
    return None

def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    to_send = []
    for col_name, val in updates.items():
        idx = col_idx(header_map, col_name)
        if idx:
            to_send.append({"range": gspread.utils.rowcol_to_a1(row_idx, idx), "values": [[val]]})
    if to_send: ws.batch_update(to_send)

# =========================
# Lógica de Negocio
# =========================
def pick_abogado(ws_abogados, monto=0):
    if monto > 60000:
        return "A01", "Veronica Zavala", "+5215527773375"
    # Lógica secuencial por defecto
    return "A02", "Ivan Zavala", "+5215510297033"

def calcular_estimacion(tipo_caso, salario_mensual, fecha_ini, fecha_fin, params):
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        anios = (f_fin - f_ini).days / 365.0
        sdi = (salario_mensual / 30.0) * 1.0452
        total = (float(params.get("Indemnizacion", 90)) * sdi) + (float(params.get("Prima_Antiguedad", 12)) * sdi * anios)
        if tipo_caso == "1": total += (20 * sdi * anios)
        return round(total, 2)
    except: return 0.0

def run_system_step_if_needed(paso, lead_snapshot, ws_leads, leads_headers, lead_row, ws_abogados, ws_sys, ws_param):
    if paso != "GENERAR_RESULTADOS": return paso, "", ""
    params = load_parametros(ws_param)
    salario = float((lead_snapshot.get("Salario_Mensual") or "0").replace("$","").replace(",","").strip())
    monto = calcular_estimacion(lead_snapshot.get("Tipo_Caso"), salario, lead_snapshot.get("Fecha_Inicio_Laboral"), lead_snapshot.get("Fecha_Fin_Laboral"), params)
    
    # OpenAI Análisis
    resumen_ai = "Analizando..."
    if OPENAI_API_KEY:
        client_ai = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client_ai.chat.completions.create(model=OPENAI_MODEL, messages=[{"role":"system","content":"Resume en 50 palabras el caso laboral."}, {"role":"user","content":lead_snapshot.get("Descripcion_Situacion") or ""}], max_tokens=150)
        resumen_ai = response.choices[0].message.content.strip()

    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abogados, monto)
    token = uuid.uuid4().hex[:16]
    out = f"✅ *Análisis Listo*\n\n{resumen_ai}\n\nMonto: ${monto}\nAbogado: {abogado_nombre}\nInforme: tuderecholaboralmexico.com/reporte/?token={token}"

    update_lead_batch(ws_leads, leads_headers, lead_row, {"Analisis_AI": resumen_ai, "Resultado_Calculo": str(monto), "Abogado_Asignado_ID": abogado_id, "Token_Reporte": token, "ESTATUS": "CLIENTE_MENU"})
    return "CLIENTE_MENU", out, ""

# =========================
# Webhook
# =========================
@app.post("/whatsapp")
def whatsapp_webhook():
    from_raw = phone_raw(request.form.get("From") or "")
    msg_in = normalize_msg(request.form.get("Body") or "")
    
    gc = get_gspread_client()
    sh = open_spreadsheet(gc)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    leads_headers = build_header_map(ws_leads)
    
    lead_row, lead_id, estatus, created = get_or_create_lead(ws_leads, leads_headers, from_raw, phone_norm(from_raw))
    
    # Detección de Cliente Existente
    if not created and estatus == "CLIENTE_MENU":
        return safe_reply("Hola de nuevo. ¿En qué puedo ayudarte con tu caso actual?")

    # Lógica de estados XimenaAI... (Se mantiene tu lógica de load_config_row)
    return safe_reply("Procesando...")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))