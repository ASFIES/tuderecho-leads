import os, json, base64, uuid, re, unicodedata, time
from datetime import datetime, timezone
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
import gspread
from google.oauth2.service_account import Credentials
import openai

app = Flask(__name__)

# --- Variables de Entorno ---
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

# =========================
# Utilidades de Conexión
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def get_gspread_client():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    creds_info = json.loads(raw if raw.startswith("{") else base64.b64decode(raw).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    for i in range(3):
        try:
            return gspread.authorize(creds)
        except:
            time.sleep(2)
    raise RuntimeError("Error de conexión con Google Sheets.")

# =========================
# Lógica de Negocio (SDI + Regla 60k)
# =========================
def calcular_estimacion(tipo_caso, salario_mensual, fecha_ini, fecha_fin, params):
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        anios = max(0, (f_fin - f_ini).days / 365.0)
        sdi = (salario_mensual / 30.0) * 1.0452 # Factor de integración
        total = (float(params.get("Indemnizacion", 90)) * sdi) + (float(params.get("Prima_Antiguedad", 12)) * sdi * anios)
        if tipo_caso == "1": total += (20 * sdi * anios)
        return round(total, 2)
    except: return 0.0

def pick_abogado(monto=0):
    if monto > 60000: # Regla Iván: >60k siempre Veronica (A01)
        return "A01", "Veronica Zavala", "+5215527773375"
    return "A02", "Ivan de Jesus Zavala", "+5215510297033"

# =========================
# Webhook de WhatsApp
# =========================
@app.route("/whatsapp", methods=['POST'])
def whatsapp_webhook():
    from_raw = request.form.get("From", "").strip() # whatsapp:+52...
    from_norm = from_raw.replace("whatsapp:", "").strip() # +52...
    msg_in = (request.form.get("Body", "")).strip()

    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws_leads = sh.worksheet(TAB_LEADS)
        
        # Búsqueda Dual: Columna B (Raw) o C (Norm)
        col_b = ws_leads.col_values(2)
        col_c = ws_leads.col_values(3)
        row_idx = None
        if from_raw in col_b: row_idx = col_b.index(from_raw) + 1
        elif from_norm in col_c: row_idx = col_c.index(from_norm) + 1

        if not row_idx:
            # Nuevo Lead
            lead_id = str(uuid.uuid4())[:8]
            ws_leads.append_row([lead_id, from_raw, from_norm, "", "", "", "", now_iso(), "INICIO"])
            return safe_reply("¡Hola! Soy *Ximena AI*. ¿Deseas iniciar tu asesoría gratuita? Responde 'Sí'.")

        # Lógica de estados y procesamiento...
        return safe_reply("Estamos procesando tu información, por favor espera.")
    except Exception as e:
        return safe_reply(f"⚠️ Error temporal: {str(e)}")

# =========================
# Webhook para AppSheet
# =========================
@app.route("/appsheet", methods=['POST'])
def appsheet_webhook():
    data = request.json
    telefono = data.get("Telefono")
    mensaje = data.get("Mensaje")
    if TWILIO_SID and TWILIO_TOKEN:
        tw = Client(TWILIO_SID, TWILIO_TOKEN)
        tw.messages.create(from_=TWILIO_NUMBER, body=mensaje, to=f"whatsapp:{telefono}")
    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))