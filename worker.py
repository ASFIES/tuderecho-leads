import os
import time
import json
import base64
import uuid
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from twilio.rest import Client
from openai import OpenAI

# =========================
# Env
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()
TAB_KNOW = os.environ.get("TAB_KNOW", "Conocimiento_AI").strip()
TAB_GESTION = os.environ.get("TAB_GESTION", "Gestion_Abogados").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

# =========================
# Google Sheets
# =========================
def get_env_creds_dict():
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        if raw.lstrip().startswith("{"):
            return json.loads(raw)
        return json.loads(base64.b64decode(raw).decode("utf-8"))
    if GOOGLE_CREDENTIALS_PATH and os.path.exists(GOOGLE_CREDENTIALS_PATH):
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    raise RuntimeError("Credenciales Google no configuradas en Worker.")

def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(get_env_creds_dict(), scopes=scopes)
    return gspread.authorize(creds)

def build_header_map(ws):
    headers = ws.row_values(1)
    return { (h or "").strip().lower(): i for i, h in enumerate(headers, start=1) if (h or "").strip() }

def cell(ws, row, col):
    return ws.cell(row, col).value

def batch_update_by_map(ws, hmap, row_idx: int, updates: dict):
    cells = []
    for k, v in (updates or {}).items():
        c = hmap.get((k or "").strip().lower())
        if c:
            cells.append(gspread.Cell(row_idx, c, v))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")

def load_key_value(ws, key_col="Clave", val_col="Valor"):
    h = build_header_map(ws)
    ck = h.get(key_col.lower())
    cv = h.get(val_col.lower())
    out = {}
    if not ck or not cv:
        return out
    rows = ws.get_all_values()[1:]
    for r in rows:
        k = (r[ck-1] if ck-1 < len(r) else "").strip()
        v = (r[cv-1] if cv-1 < len(r) else "").strip()
        if k:
            out[k] = v
    return out

def load_parametros(ws_param):
    h = build_header_map(ws_param)
    c = h.get("concepto")
    v = h.get("valor")
    out = {}
    if not c or not v:
        return out
    rows = ws_param.get_all_values()[1:]
    for r in rows:
        cc = (r[c-1] if c-1 < len(r) else "").strip()
        vv = (r[v-1] if v-1 < len(r) else "").strip()
        if not cc:
            continue
        if vv.endswith("%"):
            try:
                out[cc] = float(vv.replace("%","").strip()) / 100.0
                continue
            except:
                pass
        try:
            out[cc] = float(vv)
        except:
            pass
    return out

# =========================
# Conocimiento_AI (RAG simple)
# =========================
def load_knowledge(ws_know):
    # Regresa lista de dicts con: Palabras_Clave, Contenido_Legal, Fuente
    try:
        return ws_know.get_all_records()
    except:
        return []

def pick_knowledge_snippets(knowledge_rows, user_text: str, k=2):
    text = (user_text or "").lower()
    scored = []
    for r in knowledge_rows:
        keys = str(r.get("Palabras_Clave","") or "").lower()
        contenido = str(r.get("Contenido_Legal","") or "")
        if not keys or not contenido:
            continue
        hits = 0
        for kw in [x.strip() for x in keys.split(",") if x.strip()]:
            if kw and kw in text:
                hits += 1
        if hits > 0:
            scored.append((hits, contenido))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:k]]

# =========================
# Abogados
# =========================
def pick_abogado(ws_abogados, salario_mensual: float):
    # Regla fija: >= 50k -> A01
    if salario_mensual >= 50000:
        return "A01", "Veronica Zavala", "+5215527773375"

    rows = ws_abogados.get_all_records()
    for r in rows:
        if str(r.get("Activo","")).strip().upper() == "SI":
            return str(r.get("ID_Abogado","")).strip(), str(r.get("Nombre_Abogado","")).strip(), str(r.get("Telefono_Abogado","")).strip()

    return "A01", "Veronica Zavala", "+5215527773375"

# =========================
# Cálculo (MVP)
# =========================
def calcular_indemnizacion(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: dict) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        sd = salario_mensual / 30.0
        sdi = sd * 1.0452

        indemn = float(params.get("Indemnizacion", 90))
        prima_ant = float(params.get("Prima_Antiguedad", 12))
        veinte = float(params.get("Veinte_Dias_Por_Anio", 20))

        total = (indemn * sdi) + (prima_ant * sdi * anios)
        if str(tipo_caso).strip() == "1":
            total += (veinte * sdi * anios)

        return round(total, 2)
    except:
        return 0.0

# =========================
# Mensaje final humano (WhatsApp)
# =========================
def build_whatsapp_result(nombre: str, resumen: str, monto: float, abogada: str, link_reporte: str) -> str:
    nombre = (nombre or "").strip() or "Hola"
    return (
        f"✅ *{nombre}, gracias por confiar en Tu Derecho Laboral México.*\n\n"
        f"Entiendo que esto puede sentirse pesado e injusto. No estás sola/solo: "
        f"vamos a acompañarte con calma, claridad y firmeza para proteger tus derechos.\n\n"
        f"🧾 *Resumen preliminar (informativo):*\n{resumen}\n\n"
        f"💰 *Estimación inicial aproximada:* ${monto:,.2f} MXN\n"
        f"👩‍⚖️ *Abogada asignada:* {abogada}\n\n"
        f"📄 *Informe completo (con desglose e impresión):* {link_reporte}\n\n"
        f"⚠️ *Aviso importante:* Esta información es orientativa y no constituye asesoría legal. "
        f"No existe relación abogado-cliente hasta que un abogado acepte formalmente el asunto."
    ).strip()

# =========================
# Worker main
# =========================
def process_pending_leads():
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    ws_leads = sh.worksheet(TAB_LEADS)
    ws_abogados = sh.worksheet(TAB_ABOGADOS)
    ws_sys = sh.worksheet(TAB_SYS)
    ws_param = sh.worksheet(TAB_PARAM)

    # opcionales
    try:
        ws_know = sh.worksheet(TAB_KNOW)
    except:
        ws_know = None
    try:
        ws_gestion = sh.worksheet(TAB_GESTION)
    except:
        ws_gestion = None

    hmap = build_header_map(ws_leads)
    rows = ws_leads.get_all_records()  # keys EXACTOS del header

    # configs
    sys_cfg = load_key_value(ws_sys)
    params = load_parametros(ws_param)

    base_reporte = (sys_cfg.get("RUTA_REPORTE") or "").strip().rstrip("/")
    base_url_web = (sys_cfg.get("BASE_URL_WEB") or "").strip().rstrip("/")

    knowledge_rows = load_knowledge(ws_know) if ws_know else []

    # Twilio client
    tw = None
    if TWILIO_SID and TWILIO_TOKEN:
        tw = Client(TWILIO_SID, TWILIO_TOKEN)

    ai_client = None
    if OPENAI_API_KEY:
        ai_client = OpenAI(api_key=OPENAI_API_KEY)

    for idx, lead in enumerate(rows, start=2):
        # Normalizar keys a lower para evitar el bug de mayúsculas
        lead_l = {str(k).strip().lower(): v for k, v in (lead or {}).items()}

        status = str(lead_l.get("procesar_ai_status", "") or "").strip().upper()
        estatus = str(lead_l.get("estatus", "") or "").strip().upper()

        if status != "PENDIENTE":
            continue

        # Datos
        nombre = str(lead_l.get("nombre","") or "").strip()
        apellido = str(lead_l.get("apellido","") or "").strip()
        tel = str(lead_l.get("telefono","") or "").strip()

        tipo_caso = str(lead_l.get("tipo_caso","1") or "1").strip()
        desc = str(lead_l.get("descripcion_situacion","") or "").strip()
        fecha_ini = str(lead_l.get("fecha_inicio_laboral","") or "").strip()
        fecha_fin = str(lead_l.get("fecha_fin_laboral","") or "").strip()

        sal_raw = str(lead_l.get("salario_mensual","0") or "0")
        try:
            salario = float(sal_raw.replace("$","").replace(",","").strip())
        except:
            salario = 0.0

        # Cálculo
        monto = calcular_indemnizacion(tipo_caso, salario, fecha_ini, fecha_fin, params)

        # Asignación abogado
        ab_id, ab_nom, ab_tel = pick_abogado(ws_abogados, salario)

        # Token + link
        token = uuid.uuid4().hex[:16]
        link_reporte = f"{base_reporte}/{token}" if base_reporte else (f"{base_url_web}/reporte/{token}" if base_url_web else "")

        # RAG snippets
        snippets = pick_knowledge_snippets(knowledge_rows, desc, k=2)
        contexto_legal = "\n\n".join(snippets).strip()

        # IA (mensaje largo, empático, con base LFT, SIN pedir correo)
        tipo_txt = "despido" if tipo_caso == "1" else "renuncia"
        resumen = (
            "Con la información que nos compartiste, haremos una revisión preliminar de prestaciones pendientes "
            "(salario devengado, aguinaldo proporcional, vacaciones y prima vacacional) y, si corresponde, "
            "conceptos indemnizatorios. En breve un abogado confirmará contigo los datos clave."
        )

        if ai_client:
            try:
                user_prompt = (
                    f"Contexto del caso:\n"
                    f"- Tipo: {tipo_txt}\n"
                    f"- Situación: {desc}\n"
                    f"- Fecha inicio: {fecha_ini}\n"
                    f"- Fecha fin: {fecha_fin}\n"
                    f"- Salario mensual aprox: {salario}\n\n"
                    f"Base legal y criterios internos (puede usarse para explicar con claridad):\n{contexto_legal}\n\n"
                    "Redacta un resumen de 180 a 280 palabras, en español (México), tono MUY humano, empático y profesional.\n"
                    "Incluye referencias generales a la Ley Federal del Trabajo (sin citar artículos si no estás seguro),\n"
                    "explica que es una estimación preliminar informativa, que revisará un abogado a la brevedad,\n"
                    "y que los derechos del cliente son prioridad.\n"
                    "Prohibido: pedir correo o datos de contacto."
                )
                resp = ai_client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": "Eres Ximena, recepcionista legal con alta empatía del despacho Tu Derecho Laboral México."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=420
                )
                resumen = (resp.choices[0].message.content or "").strip() or resumen
            except Exception as e:
                resumen = resumen + f"\n\n(Nota interna: no se pudo generar IA: {e})"

        # Mensaje WhatsApp final
        nombre_full = (nombre + " " + apellido).strip() or "Hola"
        msg_cliente = build_whatsapp_result(nombre_full, resumen, monto, ab_nom, link_reporte)

        # Actualizar BD_Leads
        updates = {
            "Analisis_AI": resumen,
            "Resultado_Calculo": str(monto),
            "Abogado_Asignado_ID": ab_id,
            "Abogado_Asignado_Nombre": ab_nom,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Ultima_Actualizacion": now_iso_mx(),
            "ESTATUS": "CLIENTE_MENU",
            "Procesar_AI_Status": "LISTO",
            "Ultimo_Error": "",
        }
        batch_update_by_map(ws_leads, hmap, idx, updates)

        # Registrar en Gestion_Abogados
        if ws_gestion:
            try:
                ws_gestion.append_row([
                    str(uuid.uuid4()),              # ID_Gestion
                    now_iso_mx(),                   # Fecha_Asignacion
                    str(lead_l.get("id_lead","") or ""),   # ID_Lead
                    tel,                            # Telefono_Lead
                    nombre_full,                    # Nombre_Lead
                    str(monto),                     # Monto_Estimado
                    ab_id,                          # Abogado_Asignado_ID
                    ab_nom,                         # Abogado_Asignado_Nombre
                    ab_tel,                         # Abogado_Asignado_Telefono
                    "", "", "", "", "", "", "", "", "",   # campos operativos vacíos (los llenan ustedes)
                    "NUEVO",                        # Estatus_Interno
                    ""                              # Notas_Internas
                ], value_input_option="USER_ENTERED")
            except:
                pass

        # Notificar abogado
        if tw and TWILIO_NUMBER and ab_tel:
            try:
                tw.messages.create(
                    from_=TWILIO_NUMBER,
                    to=f"whatsapp:{ab_tel}",
                    body=(
                        f"⚖️ *NUEVO LEAD ASIGNADO*\n\n"
                        f"👤 Cliente: {nombre_full}\n"
                        f"📱 Tel: {tel}\n"
                        f"📋 Caso: {'Despido' if tipo_caso=='1' else 'Renuncia'}\n"
                        f"💰 Salario: ${salario:,.2f}\n"
                        f"🧮 Estimación: ${monto:,.2f}\n"
                        f"🔗 Informe: {link_reporte}\n"
                    )
                )
            except Exception as e:
                batch_update_by_map(ws_leads, hmap, idx, {"Ultimo_Error": f"TwilioNotif: {e}"})

def main():
    print(f"[{now_iso_mx()}] Worker iniciado.")
    while True:
        try:
            process_pending_leads()
        except Exception as e:
            print(f"[{now_iso_mx()}] Error crítico Worker: {e}")
        time.sleep(12)

if __name__ == "__main__":
    main()
