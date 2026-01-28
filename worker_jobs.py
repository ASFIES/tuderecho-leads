import os
import json
import base64
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from twilio.rest import Client

# OpenAI (opcional)
from openai import OpenAI

# =========================
# ENV
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()  # whatsapp:+1415...

MX_TZ = ZoneInfo("America/Mexico_City")


def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")


# =========================
# Google helpers
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
        if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe en el filesystem.")
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales: GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH.")


def get_gspread_client():
    creds_info = get_env_creds_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(sh, title: str):
    return sh.worksheet(title)


def build_header_map(ws):
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if not key:
            continue
        m[key] = i
        m[key.lower()] = i
    return m


def col_idx(hmap: dict, name: str):
    return hmap.get(name) or hmap.get((name or "").lower())


def find_row_by_value(ws, col_idx_num: int, value: str):
    value = (value or "").strip()
    if not value:
        return None
    col_values = ws.col_values(col_idx_num)
    for i, v in enumerate(col_values[1:], start=2):
        if (v or "").strip() == value:
            return i
    return None


def update_cells_batch(ws, updates_a1_to_value: dict):
    payload = [{"range": a1, "values": [[val]]} for a1, val in updates_a1_to_value.items()]
    if payload:
        ws.batch_update(payload)


def update_row_by_headers(ws, hmap: dict, row_idx: int, updates: dict):
    to_send = {}
    for col_name, val in (updates or {}).items():
        idx = col_idx(hmap, col_name)
        if not idx:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, idx)
        to_send[a1] = val
    update_cells_batch(ws, to_send)


# =========================
# Params + cálculo
# =========================
def load_parametros(ws_param):
    h = build_header_map(ws_param)
    c = col_idx(h, "Concepto")
    v = col_idx(h, "Valor")
    out = {}
    if not c or not v:
        return out
    rows = ws_param.get_all_values()[1:]
    for r in rows:
        cc = (r[c - 1] if c - 1 < len(r) else "").strip()
        vv = (r[v - 1] if v - 1 < len(r) else "").strip()
        if not cc:
            continue
        if vv.endswith("%"):
            try:
                out[cc] = float(vv.replace("%", "").strip()) / 100.0
                continue
            except:
                pass
        try:
            out[cc] = float(vv)
        except:
            out[cc] = vv
    return out


def calcular_estimacion(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: dict) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        sd = salario_mensual / 30.0
        sdi = sd * 1.0452  # MVP

        indemn_dias = float(params.get("Indemnizacion", 90))
        prima_ant_dias = float(params.get("Prima_Antiguedad", 12))
        veinte_dias = float(params.get("Veinte_Dias_Por_Anio", 20))

        total = (indemn_dias * sdi) + (prima_ant_dias * sdi * anios)
        if (tipo_caso or "").strip() == "1":  # despido
            total += (veinte_dias * sdi * anios)

        return round(total, 2)
    except:
        return 0.0


# =========================
# Abogados
# =========================
def lookup_abogado_by_id(ws_abogados, abogado_id: str):
    h = build_header_map(ws_abogados)
    idc = col_idx(h, "ID_Abogado")
    nc = col_idx(h, "Nombre_Abogado")
    tc = col_idx(h, "Telefono_Abogado")
    row = find_row_by_value(ws_abogados, idc, abogado_id) if idc else None
    if not row:
        return abogado_id, "Abogada A01", ""
    vals = ws_abogados.row_values(row)
    nombre = (vals[nc - 1] if nc and nc - 1 < len(vals) else "").strip()
    tel = (vals[tc - 1] if tc and tc - 1 < len(vals) else "").strip()
    return abogado_id, (nombre or "Abogada A01"), (tel or "")


def pick_abogado(ws_abogados, salario_mensual: float):
    # ✅ regla pedida: >=50,000 -> ID A01
    if salario_mensual >= 50000:
        return lookup_abogado_by_id(ws_abogados, "A01")

    # fallback: primer abogado activo
    h = build_header_map(ws_abogados)
    idc = col_idx(h, "ID_Abogado")
    nc = col_idx(h, "Nombre_Abogado")
    tc = col_idx(h, "Telefono_Abogado")
    ac = col_idx(h, "Activo")

    rows = ws_abogados.get_all_values()[1:]
    for r in rows:
        activo = (r[ac - 1] if ac and ac - 1 < len(r) else "SI").strip().upper()
        if activo != "SI":
            continue
        aid = (r[idc - 1] if idc and idc - 1 < len(r) else "").strip()
        an = (r[nc - 1] if nc and nc - 1 < len(r) else "").strip()
        at = (r[tc - 1] if tc and tc - 1 < len(r) else "").strip()
        if aid:
            return aid, an, at

    return lookup_abogado_by_id(ws_abogados, "A01")


# =========================
# Config sistema (RUTA_REPORTE)
# =========================
def load_key_value(ws, key_col="Clave", val_col="Valor"):
    h = build_header_map(ws)
    k = col_idx(h, key_col)
    v = col_idx(h, val_col)
    out = {}
    if not k or not v:
        return out
    rows = ws.get_all_values()[1:]
    for r in rows:
        kk = (r[k - 1] if k - 1 < len(r) else "").strip()
        vv = (r[v - 1] if v - 1 < len(r) else "").strip()
        if kk:
            out[kk] = vv
    return out


# =========================
# OpenAI resumen (opcional)
# =========================
def generar_resumen_ai(desc: str, tipo_caso: str) -> str:
    tipo_txt = "despido" if (tipo_caso or "").strip() == "1" else "renuncia"
    fallback = (
        f"Gracias por contarnos tu situación. Con lo que compartiste, haremos una revisión preliminar como *{tipo_txt}* "
        f"para identificar prestaciones pendientes (por ejemplo: salarios devengados, aguinaldo proporcional, vacaciones y prima vacacional) "
        f"y, si aplica, conceptos indemnizatorios. Lo más importante ahora es confirmar fechas, salario y cómo ocurrió la terminación. "
        f"Una abogada revisará tu caso y te contactará lo antes posible."
    )

    if not OPENAI_API_KEY:
        return fallback

    try:
        client_ai = OpenAI(api_key=OPENAI_API_KEY)
        resp = client_ai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system",
                 "content": (
                     "Eres recepcionista legal empática de México. "
                     "Redacta un resumen humano y claro (150 a 220 palabras). "
                     "No pidas correo. No prometas resultados. "
                     "Menciona de forma general prestaciones devengadas y, si es despido, indemnización conforme a la LFT. "
                     "Cierra diciendo que una abogada revisará y contactará pronto."
                 )},
                {"role": "user",
                 "content": f"Tipo: {tipo_txt}\nSituación: {desc}"}
            ],
            max_tokens=320,
        )
        out = (resp.choices[0].message.content or "").strip()
        return out or fallback
    except:
        return fallback


# =========================
# WhatsApp send
# =========================
def send_whatsapp(to_phone: str, body: str):
    if not (TWILIO_SID and TWILIO_TOKEN and TWILIO_NUMBER):
        return
    tw = Client(TWILIO_SID, TWILIO_TOKEN)
    to_final = to_phone if to_phone.startswith("whatsapp:") else f"whatsapp:{to_phone}"
    tw.messages.create(from_=TWILIO_NUMBER, to=to_final, body=body)


# =========================
# Mensaje final
# =========================
def build_result_message(nombre: str, tipo_caso: str, resumen: str, monto: float, abogada: str, link: str) -> str:
    nombre = (nombre or "").strip() or "Hola"
    tipo_txt = "despido" if (tipo_caso or "").strip() == "1" else "renuncia"

    return (
        f"✅ *{nombre}, gracias por confiar en Tu Derecho Laboral México.*\n\n"
        f"Entiendo que vivir una situación de *{tipo_txt}* puede ser muy pesado. "
        f"De verdad: no estás sola/solo. Te vamos a acompañar con claridad y respeto.\n\n"
        f"📌 *Orientación preliminar (informativa):*\n{resumen}\n\n"
        f"💰 *Estimación inicial aproximada:* ${monto:,.2f} MXN\n"
        f"👩‍⚖️ *Abogada que acompañará tu caso:* {abogada}\n\n"
        f"📄 *Informe completo:* {link}\n\n"
        f"⏱️ *Siguiente paso:* tu abogada revisará tu información y te contactará lo antes posible.\n\n"
        f"⚠️ *Aviso importante:* Esta información es orientativa y no constituye asesoría legal. "
        f"No existe relación abogado-cliente hasta que un abogado acepte formalmente el asunto."
    ).strip()


# =========================
# JOB principal (RQ ejecuta esto)
# =========================
def process_lead(lead_id: str):
    if not lead_id:
        return

    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)

    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abogados = open_worksheet(sh, TAB_ABOGADOS)
    ws_param = open_worksheet(sh, TAB_PARAM)
    ws_sys = open_worksheet(sh, TAB_SYS)

    h_leads = build_header_map(ws_leads)
    id_col = col_idx(h_leads, "ID_Lead")
    if not id_col:
        raise RuntimeError("BD_Leads: falta columna ID_Lead")

    row = find_row_by_value(ws_leads, id_col, lead_id)
    if not row:
        return

    vals = ws_leads.row_values(row)
    headers = ws_leads.row_values(1)
    lead = {h: (vals[i] if i < len(vals) else "") for i, h in enumerate(headers)}

    estatus = (lead.get("ESTATUS") or "").strip().upper()
    if estatus != "EN_PROCESO":
        # Si no está en proceso, no hacemos nada
        return

    nombre = lead.get("Nombre") or ""
    tel = lead.get("Telefono") or ""
    tipo_caso = (lead.get("Tipo_Caso") or "").strip()
    desc = lead.get("Descripcion_Situacion") or ""

    try:
        salario = float((lead.get("Salario_Mensual") or "0").replace("$", "").replace(",", "").strip())
    except:
        salario = 0.0

    fecha_ini = (lead.get("Fecha_Inicio_Laboral") or "").strip()
    fecha_fin = (lead.get("Fecha_Fin_Laboral") or "").strip()

    params = load_parametros(ws_param)
    monto = calcular_estimacion(tipo_caso, salario, fecha_ini, fecha_fin, params)

    abog_id, abog_nombre, abog_tel = pick_abogado(ws_abogados, salario)

    sys_cfg = load_key_value(ws_sys)
    ruta = (sys_cfg.get("RUTA_REPORTE") or "").strip()
    token = uuid.uuid4().hex[:16]
    link = (ruta.rstrip("/") + "/" + token) if ruta else ""

    resumen = generar_resumen_ai(desc, tipo_caso)
    msg_final = build_result_message(nombre, tipo_caso, resumen, monto, abog_nombre, link)

    # Guardar resultados + mover a CLIENTE_MENU
    update_row_by_headers(ws_leads, h_leads, row, {
        "Analisis_AI": resumen,
        "Resultado_Calculo": str(monto),
        "Abogado_Asignado_ID": abog_id,
        "Abogado_Asignado_Nombre": abog_nombre,
        "Token_Reporte": token,
        "Link_Reporte_Web": link,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": now_iso_mx(),
    })

    # Mensaje al cliente
    if tel:
        send_whatsapp(tel, msg_final)

    # Notificación a abogada (opcional)
    if abog_tel:
        try:
            send_whatsapp(
                abog_tel,
                (
                    "⚖️ *Nuevo caso asignado*\n"
                    f"Nombre: {nombre}\n"
                    f"Tel: {tel}\n"
                    f"Tipo: {'Despido' if tipo_caso=='1' else 'Renuncia'}\n"
                    f"Salario: ${salario:,.2f}\n"
                    f"Estimación: ${monto:,.2f}\n"
                    f"Informe: {link}"
                )
            )
        except:
            pass
