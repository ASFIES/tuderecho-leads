import os
import time
import json
import base64
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI
from twilio.rest import Client

# =========================
# Configuración de Entorno
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

# =========================
# Utilidades de Google Sheets
# =========================
def get_gspread_client():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    try:
        creds_info = json.loads(raw) if raw.startswith("{") else json.loads(base64.b64decode(raw).decode("utf-8"))
    except:
        raise RuntimeError("Credenciales de Google inválidas en el Worker.")
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def build_header_map(ws):
    headers = ws.row_values(1)
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1) if h.strip()}

# =========================
# Lógica Legal y Asignación
# =========================
def pick_abogado(ws_abogados, salario_mensual):
    # Regla: Salario alto va a la dirección (A01)
    if salario_mensual >= 50000:
        return "A01", "Veronica Zavala", "+5215527773375"
    
    # Rotación simple para otros casos
    rows = ws_abogados.get_all_records()
    for r in rows:
        if str(r.get("Activo", "")).upper() == "SI":
            return r.get("ID_Abogado"), r.get("Nombre_Abogado"), r.get("Telefono_Abogado")
    return "A01", "Veronica Zavala", "+5215527773375"

def calcular_indemnizacion(tipo, salario, f_ini, f_fin, params):
    try:
        d_ini = datetime.strptime(f_ini, "%Y-%m-%d")
        d_fin = datetime.strptime(f_fin, "%Y-%m-%d")
        anios = (d_fin - d_ini).days / 365.0
        sd = salario / 30.0
        sdi = sd * 1.0452 # Factor de integración base
        
        # 3 meses + Prima de Antigüedad
        total = (90 * sdi) + (12 * sdi * anios)
        # Si es Despido (1), sumar 20 días por año
        if str(tipo) == "1":
            total += (20 * sdi * anios)
        return round(total, 2)
    except: return 0.0

# =========================
# PROCESAMIENTO PRINCIPAL
# =========================
def process_pending_leads():
    print(f"[{now_iso_mx()}] Worker: Buscando leads pendientes...")
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws_leads = sh.worksheet(TAB_LEADS)
    ws_params = sh.worksheet(TAB_PARAM)
    ws_abogados = sh.worksheet(TAB_ABOGADOS)
    ws_sys = sh.worksheet(TAB_SYS)
    
    headers = build_header_map(ws_leads)
    leads = ws_leads.get_all_records()

    for idx, lead in enumerate(leads, start=2):
        if str(lead.get("procesar_ai_status", "")).upper() == "PENDIENTE":
            lead_id = lead.get("ID_Lead")
            print(f"Procesando Lead ID: {lead_id}")
            
            try:
                # 1. Preparar Datos
                salario = float(str(lead.get("Salario_Mensual", "0")).replace("$","").replace(",",""))
                tipo_caso = str(lead.get("Tipo_Caso", "1"))
                f_ini = lead.get("Fecha_Inicio_Laboral")
                f_fin = lead.get("Fecha_Fin_Laboral")
                
                # 2. Cálculo Legal
                monto = calcular_indemnizacion(tipo_caso, salario, f_ini, f_fin, {})
                
                # 3. Inteligencia Artificial (Empatía Máxima)
                resumen_ai = "Analizando tu caso..."
                if OPENAI_API_KEY:
                    client_ai = OpenAI(api_key=OPENAI_API_KEY)
                    tipo_txt = "despido injustificado" if tipo_caso == "1" else "renuncia"
                    
                    prompt = (
                        f"Eres Ximena, abogada líder del despacho 'Tu Derecho Laboral México'. "
                        f"El cliente sufrió un {tipo_txt}. Situación: {lead.get('Descripcion_Situacion')}. "
                        "Redacta un mensaje de 150 palabras. Tono: Muy empático, humano y profesional. "
                        "Dile que sus derechos son lo más importante, que no está solo y que el equipo legal "
                        "lo acompañará en cada paso. No pidas datos de contacto, ya los tenemos."
                    )
                    
                    resp = client_ai.chat.completions.create(
                        model=OPENAI_MODEL,
                        messages=[{"role": "system", "content": prompt}],
                        max_tokens=300
                    )
                    resumen_ai = resp.choices[0].message.content.strip()

                # 4. Asignación de Abogado
                ab_id, ab_nom, ab_tel = pick_abogado(ws_abogados, salario)
                
                # 5. Actualizar Sheets (Batch)
                token = uuid.uuid4().hex[:12]
                updates = {
                    "Analisis_AI": resumen_ai,
                    "Resultado_Calculo": str(monto),
                    "Abogado_Asignado_ID": ab_id,
                    "Abogado_Asignado_Nombre": ab_nom,
                    "Token_Reporte": token,
                    "ESTATUS": "FIN_RESULTADOS",
                    "Procesar_AI_Status": "LISTO"
                }
                
                # Función de actualización rápida
                cells_to_update = []
                for key, val in updates.items():
                    col = headers.get(key.lower())
                    if col:
                        cells_to_update.append(gspread.Cell(idx, col, val))
                ws_leads.update_cells(cells_to_update, value_input_option="USER_ENTERED")

                # 6. Notificar al Abogado por WhatsApp
                if TWILIO_SID and ab_tel:
                    tw = Client(TWILIO_SID, TWILIO_TOKEN)
                    tw.messages.create(
                        from_=TWILIO_NUMBER,
                        to=f"whatsapp:{ab_tel}",
                        body=(f"⚖️ *NUEVO LEAD ASIGNADO*\n\n"
                              f"👤 Cliente: {lead.get('Nombre')} {lead.get('Apellido')}\n"
                              f"📱 Tel: {lead.get('Telefono')}\n"
                              f"💰 Salario: ${salario:,.2f}\n"
                              f"📋 Caso: {'Despido' if tipo_caso=='1' else 'Renuncia'}\n"
                              f"🧮 Estimación: ${monto:,.2f}\n"
                              f"🔗 Link: {lead.get('Link_Reporte_Web')}")
                    )

            except Exception as e:
                print(f"Error procesando lead {lead_id}: {e}")
                col_err = headers.get("ultimo_error")
                if col_err: ws_leads.update_cell(idx, col_err, str(e))
                ws_leads.update_cell(idx, headers.get("procesar_ai_status"), "ERROR")

def main():
    while True:
        try:
            process_pending_leads()
        except Exception as e:
            print(f"Error crítico en el bucle: {e}")
        time.sleep(15) # Revisar cada 15 segundos

if __name__ == "__main__":
    main()