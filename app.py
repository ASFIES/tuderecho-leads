import os
import json
import base64
import uuid
import re
import unicodedata
import time
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

import gspread
from google.oauth2.service_account import Credentials
import openai

# =========================
# App e Inicio
# =========================
app = Flask(__name__)

# Configuración de Entorno
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

# =========================
# Conexión Robusta (Gspread)
# =========================
def get_gspread_client():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    creds_info = json.loads(raw if raw.startswith("{") else base64.b64decode(raw).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    
    for i in range(3): # Reintentos para evitar errores de conexión
        try:
            return gspread.authorize(creds)
        except Exception:
            time.sleep(2)
    raise RuntimeError("Error de conexión persistente con Google Sheets.")

# =========================
# Lógica de Negocio (SDI y Regla 60k)
# =========================
def calcular_estimacion(tipo_caso, salario_mensual, fecha_ini, fecha_fin, params):
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        anios = max(0, (f_fin - f_ini).days / 365.0)
        
        # SDI: Salario Diario Integrado (Factor 1.0452)
        sd = salario_mensual / 30.0
        sdi = sd * 1.0452 
        
        indemn = float(params.get("Indemnizacion", 90)) * sdi
        prima = float(params.get("Prima_Antiguedad", 12)) * sdi * anios
        total = indemn + prima
        
        if (tipo_caso or "").strip() == "1": # Despido
            total += (float(params.get("Veinte_Dias_Por_Anio", 20)) * sdi * anios)
            
        return round(total, 2)
    except Exception: return 0.0

def pick_abogado(ws_abogados, monto=0):
    if monto > 60000: # Regla Iván: >60k va con Veronica (A01)
        return "A01", "Veronica Zavala", "+5215527773375"
    return "A02", "Ivan Zavala", "+5215510297033"

# =========================
# Procesamiento del Sistema
# =========================
def run_system_step_if_needed(paso, lead_snapshot, ws_leads, lead_row, sh):
    if paso != "GENERAR_RESULTADOS": return paso, "", ""
    
    ws_param = sh.worksheet(TAB_PARAM)
    params = {r[0]: r[1] for r in ws_param.get_all_values()[1:]}
    
    salario = float(re.sub(r'[^\d.]', '', lead_snapshot.get("Salario_Mensual", "0")))
    monto = calcular_estimacion(lead_snapshot.get("Tipo_Caso"), salario, lead_snapshot.get("Fecha_Inicio_Laboral"), lead_snapshot.get("Fecha_Fin_Laboral"), params)
    
    # Análisis OpenAI
    resumen_ai = "Caso en revisión por el equipo legal."
    if OPENAI_API_KEY:
        try:
            client_ai = openai.OpenAI(api_key=OPENAI_API_KEY)
            response = client_ai.chat.completions.create(
                model=OPENAI_MODEL, 
                messages=[{"role":"system","content":"Resume en 50 palabras máximo el caso laboral y da un consejo breve."}, {"role":"user","content":lead_snapshot.get("Descripcion_Situacion","")}]
            )
            resumen_ai = response.choices[0].message.content.strip()
        except Exception: pass

    ws_abogados = sh.worksheet(TAB_ABOGADOS)
    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abogados, monto)
    token = uuid.uuid4().hex[:16]
    
    out = (f"✅ *¡Análisis Completo!*\n\n⚖️ *Análisis AI:* {resumen_ai}\n\n"
           f"💰 *Estimación:* ${monto:,.2f} MXN\n"
           f"👩‍⚖️ *Abogado:* {abogado_nombre}\n"
           f"📄 *Informe:* tuderecholaboralmexico.com/reporte/?token={token}")

    # Batch Update (Evita errores de conexión)
    header_map = {h: i+1 for i, h in enumerate(ws_leads.row_values(1))}
    updates = {
        "Analisis_AI": resumen_ai, "Resultado_Calculo": str(monto), 
        "Abogado_Asignado_ID": abogado_id, "Abogado_Asignado_Nombre": abogado_nombre,
        "Token_Reporte": token, "ESTATUS": "CLIENTE_MENU"
    }
    
    batch = [{"range": gspread.utils.rowcol_to_a1(lead_row, header_map[k]), "values": [[v]]} for k, v in updates.items() if k in header_map]
    ws_leads.batch_update(batch)
    
    # Notificar al Abogado vía Twilio
    if TWILIO_SID and TWILIO_TOKEN:
        try:
            tw = Client(TWILIO_SID, TWILIO_TOKEN)
            tw.messages.create(from_=TWILIO_NUMBER, body=f"Nuevo Lead: {lead_snapshot.get('Nombre')}\nMonto: ${monto}", to=f"whatsapp:{abogado_tel}")
        except Exception: pass

    return "CLIENTE_MENU", out, ""

# =========================
# Webhook WhatsApp
# =========================
@app.route("/whatsapp", methods=['POST'])
def whatsapp_webhook():
    from_raw = request.form.get("From", "").strip()
    from_norm = from_raw.replace("whatsapp:", "").strip()
    msg_in = (request.form.get("Body", "")).strip()
    
    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws_leads = sh.worksheet(TAB_LEADS)
        
        # Búsqueda en Columna B o C
        row_idx = None
        col_b = ws_leads.col_values(2)
        col_c = ws_leads.col_values(3)
        
        if from_raw in col_b: row_idx = col_b.index(from_raw) + 1
        elif from_norm in col_c: row_idx = col_c.index(from_norm) + 1

        if not row_idx:
            lead_id = str(uuid.uuid4())[:8]
            ws_leads.append_row([lead_id, from_raw, from_norm, "", "", "", "", datetime.now().isoformat(), "INICIO"])
            return safe_reply("¡Hola! Soy *Ximena AI*. ¿Deseas iniciar tu asesoría gratuita? Responde 'Sí' para continuar.")

        # Lógica de Estados (Config_XimenaAI)
        # Aquí se insertaría la lectura de load_config_row para dinamismo
        return safe_reply("Estamos procesando tu información, un momento...")

    except Exception:
        return safe_reply("⚠️ Temporalmente fuera de servicio. Intenta en un momento.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))