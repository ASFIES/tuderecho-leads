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

# OpenAI (opcional)
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
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()  # Ej: whatsapp:+1415...

# =========================
# Time
# =========================
MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")


# =========================
# Helpers: Twilio Reply
# =========================
def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def render_text(s: str) -> str:
    """Convierte \\n literal a salto real (para que WhatsApp se vea bien)."""
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
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")  # quita control chars
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    # Busca primer dígito 0-9 en el mensaje (sirve para "1️⃣", " 1", etc.)
    m = re.search(r"\d", s)
    if m:
        return m.group(0)
    return s


# =========================
# Fuente Lead (Facebook/Web)
# =========================
def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        return "WEB"
    return "DESCONOCIDA"


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
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido (JSON/base64). Detalle: {e}")

    if GOOGLE_CREDENTIALS_PATH:
        if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe en el filesystem del servicio.")
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales: usa GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH.")

def get_gspread_client():
    creds_info = get_env_creds_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")
    return gc.open(GOOGLE_SHEET_NAME)

def open_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except Exception:
        raise RuntimeError(f"No existe la pestaña '{title}' en el Google Sheet '{GOOGLE_SHEET_NAME}'.")


# =========================
# Headers / Sheet utils
# =========================
def build_header_map(ws):
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if not key:
            continue
        if key not in m:
            m[key] = i
        low = key.lower()
        if low not in m:
            m[low] = i
    return m

def col_idx(headers_map: dict, name: str):
    return headers_map.get(name) or headers_map.get((name or "").lower())

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

def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    to_send = {}
    for col_name, val in (updates or {}).items():
        idx = col_idx(header_map, col_name)
        if not idx:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, idx)
        to_send[a1] = val
    update_cells_batch(ws, to_send)

def safe_log(ws_logs, data: dict):
    try:
        cols = [
            "ID_Log", "Fecha_Hora", "Telefono", "ID_Lead", "Paso",
            "Mensaje_Entrante", "Mensaje_Saliente",
            "Canal", "Fuente_Lead", "Modelo_AI", "Errores"
        ]
        row = [data.get(c, "") for c in cols]
        ws_logs.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        pass


# =========================
# Load Config row (soporta Siguiente_Si_3..9)
# =========================
def load_config_row(ws_config, paso_actual: str):
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso'.")

    paso_actual = (paso_actual or "").strip() or "INICIO"
    row = find_row_by_value(ws_config, idpaso_col, paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")
    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}'.")

    row_vals = ws_config.row_values(row)

    base_fields = [
        "ID_Paso", "Texto_Bot", "Tipo_Entrada", "Opciones_Validas",
        "Siguiente_Si_1", "Siguiente_Si_2",
        "Campo_BD_Leads_A_Actualizar", "Regla_Validacion", "Mensaje_Error"
    ]
    extra_siguientes = [f"Siguiente_Si_{i}" for i in range(3, 10)]

    def get_field(name):
        idx = col_idx(cfg_headers, name)
        return (row_vals[idx-1] if idx and idx-1 < len(row_vals) else "").strip()

    out = {k: get_field(k) for k in base_fields}
    for k in extra_siguientes:
        out[k] = get_field(k)
    return out


# =========================
# Config_Sistema + Parametros_Legales
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
        kk = (r[k-1] if k-1 < len(r) else "").strip()
        vv = (r[v-1] if v-1 < len(r) else "").strip()
        if kk:
            out[kk] = vv
    return out

def load_parametros(ws_param):
    h = build_header_map(ws_param)
    c = col_idx(h, "Concepto")
    v = col_idx(h, "Valor")
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
                out[cc] = float(vv.replace("%", "").strip()) / 100.0
                continue
            except:
                pass
        try:
            out[cc] = float(vv)
        except:
            pass
    return out


# =========================
# Leads: get/create
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_norm: str, fuente: str):
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono'.")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    if row:
        vals = ws_leads.row_values(row)
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        idx_fuente = col_idx(leads_headers, "Fuente_Lead")

        lead_id = (vals[idx_id - 1] or "").strip() if idx_id and idx_id - 1 < len(vals) else ""
        estatus = (vals[idx_est - 1] or "").strip() if idx_est and idx_est - 1 < len(vals) else "INICIO"
        fuente_actual = (vals[idx_fuente - 1] or "").strip() if idx_fuente and idx_fuente - 1 < len(vals) else ""

        # si está vacía, la llenamos con la detectada
        if (not fuente_actual) and fuente and fuente != "DESCONOCIDA":
            update_lead_batch(ws_leads, leads_headers, row, {"Fuente_Lead": fuente})

        return row, lead_id, estatus or "INICIO", False

    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)
    new_row = [""] * max(1, len(headers_row))

    def set_if(col_name, val):
        idx = col_idx(leads_headers, col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", tel_raw)
    set_if("Telefono_Normalizado", tel_norm)
    set_if("Fuente_Lead", fuente or "DESCONOCIDA")
    set_if("Fecha_Registro", now_iso_mx())
    set_if("Ultima_Actualizacion", now_iso_mx())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    return row, lead_id, "INICIO", True


# =========================
# Abogados
# =========================
def pick_abogado(ws_abogados, salario_mensual: float = 0.0, monto: float = 0.0):
    # Regla: si salario mensual >= 50,000 -> A01
    if salario_mensual >= 50000:
        return "A01", "Veronica Zavala", "+5215527773375"

    h = build_header_map(ws_abogados)
    idc = col_idx(h, "ID_Abogado")
    nc = col_idx(h, "Nombre_Abogado")
    tc = col_idx(h, "Telefono_Abogado")
    ac = col_idx(h, "Activo")

    rows = ws_abogados.get_all_values()[1:]
    for r in rows:
        activo = (r[ac-1] if ac and ac-1 < len(r) else "SI").strip().upper()
        if activo != "SI":
            continue
        aid = (r[idc-1] if idc and idc-1 < len(r) else "").strip()
        an = (r[nc-1] if nc and nc-1 < len(r) else "").strip()
        at = (r[tc-1] if tc and tc-1 < len(r) else "").strip()
        if aid:
            return aid, an, at

    return "A01", "Veronica Zavala", "+5215527773375"


# =========================
# Cálculo (SDI Integrado) - MVP
# =========================
def calcular_estimacion(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: dict) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        # SDI mínimo factor (MVP)
        sd = salario_mensual / 30.0
        sdi = sd * 1.0452

        indemn_dias = float(params.get("Indemnizacion", 90))
        prima_ant_dias = float(params.get("Prima_Antiguedad", 12))
        veinte_dias = float(params.get("Veinte_Dias_Por_Anio", 20))

        total = (indemn_dias * sdi) + (prima_ant_dias * sdi * anios)

        # Si despido (opción 1)
        if (tipo_caso or "").strip() == "1":
            total += (veinte_dias * sdi * anios)

        return round(total, 2)
    except:
        return 0.0


# =========================
# Resultados: Empáticos + más largos
# =========================
def build_result_message(nombre: str, resumen_ai: str, monto: float, abogado_nombre: str, link_reporte: str) -> str:
    nombre = (nombre or "").strip() or "Hola"
    # Mensaje humano + legal informativo (sin prometer)
    return (
        f"✅ *{nombre}, gracias por contarnos tu situación.*\n\n"
        f"Entiendo que este tipo de momentos pueden ser estresantes e injustos. "
        f"Quiero que sepas que *tus derechos son importantes para nosotros* y que te vamos a acompañar.\n\n"
        f"🧾 *Resumen preliminar (informativo):*\n{resumen_ai}\n\n"
        f"💰 *Estimación inicial (aproximada):* ${monto:,.2f} MXN\n\n"
        f"👩‍⚖️ *Abogada asignada para tu caso:* {abogado_nombre}\n\n"
        f"📌 *Siguiente paso:* en breve tu abogada revisará la información y te contactará. "
        f"Si necesitas agregar un detalle importante, puedes escribirlo aquí.\n\n"
        f"📄 *Informe completo:* {link_reporte}\n\n"
        f"⚠️ *Aviso importante:* Esta información es únicamente orientativa y no constituye asesoría legal. "
        f"No existe relación abogado-cliente hasta que un abogado acepte formalmente el asunto. "
        f"Los montos pueden variar según prestaciones reales, pruebas, salario integrado y criterios aplicables."
    ).strip()


# =========================
# SISTEMA (OpenAI + asignación)
# =========================
def run_system_step_if_needed(
    paso: str,
    lead_snapshot: dict,
    ws_leads,
    leads_headers,
    lead_row,
    ws_abogados,
    ws_sys,
    ws_param
) -> tuple[str, str, str]:
    errores = ""
    if paso != "GENERAR_RESULTADOS":
        return paso, "", errores

    sys_cfg = load_key_value(ws_sys)
    params = load_parametros(ws_param)

    nombre = lead_snapshot.get("Nombre") or ""

    try:
        sal_raw = (lead_snapshot.get("Salario_Mensual") or "0").replace("$", "").replace(",", "").strip()
        salario = float(sal_raw)
    except:
        salario = 0.0

    monto = calcular_estimacion(
        tipo_caso=lead_snapshot.get("Tipo_Caso") or "",
        salario_mensual=salario,
        fecha_ini=lead_snapshot.get("Fecha_Inicio_Laboral") or "",
        fecha_fin=lead_snapshot.get("Fecha_Fin_Laboral") or "",
        params=params
    )

    # --- OpenAI: resumen + contención (más humano, no 50 palabras) ---
    resumen_ai = (
        "Recibimos tu información. Con base en lo que comentas, revisaremos si hubo incumplimientos "
        "en la terminación, pagos pendientes y prestaciones proporcionales. Un abogado confirmará contigo "
        "los datos clave para proteger tus derechos."
    )

    if OPENAI_API_KEY:
        try:
            client_ai = openai.OpenAI(api_key=OPENAI_API_KEY)
            desc_user = lead_snapshot.get("Descripcion_Situacion") or "Sin detalles"
            tipo_caso = lead_snapshot.get("Tipo_Caso") or ""
            tipo_txt = "despido" if tipo_caso == "1" else "renuncia"

            response = client_ai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Eres Ximena AI, recepcionista de un despacho laboral en México. "
                            "Redacta un resumen empático y claro (120 a 220 palabras) de la situación del usuario. "
                            "Incluye: (1) validación emocional breve, (2) enfoque informativo según LFT (sin prometer resultados), "
                            "(3) qué revisará el abogado y siguientes pasos. Evita pedir correo. No des asesoría definitiva."
                        ),
                    },
                    {"role": "user", "content": f"Tipo: {tipo_txt}\nSituación: {desc_user}"},
                ],
                max_tokens=260,
            )
            resumen_ai = response.choices[0].message.content.strip()
        except Exception as e:
            errores += f"AI_Err: {e}. "

    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abogados, salario_mensual=salario, monto=monto)

    token = uuid.uuid4().hex[:16]
    ruta_reporte = (sys_cfg.get("RUTA_REPORTE") or "").strip()
    link_reporte = (ruta_reporte.rstrip("/") + "/" + token) if ruta_reporte else ""

    out = build_result_message(nombre, resumen_ai, monto, abogado_nombre, link_reporte)

    try:
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "Analisis_AI": resumen_ai,
            "Resultado_Calculo": str(monto),
            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "ESTATUS": "CLIENTE_MENU",
            "Ultima_Actualizacion": now_iso_mx(),
        })

        # Notificación proactiva al abogado (si Twilio está configurado y hay teléfono)
        if TWILIO_SID and TWILIO_TOKEN and TWILIO_NUMBER and abogado_tel:
            tw_client = Client(TWILIO_SID, TWILIO_TOKEN)
            try:
                tw_client.messages.create(
                    from_=TWILIO_NUMBER,
                    body=(
                        f"⚖️ Nuevo Lead asignado\n"
                        f"Nombre: {lead_snapshot.get('Nombre','')}\n"
                        f"Tel: {lead_snapshot.get('Telefono','')}\n"
                        f"Tipo: {'Despido' if (lead_snapshot.get('Tipo_Caso','')=='1') else 'Renuncia'}\n"
                        f"Monto estimado: ${monto:,.2f} MXN\n"
                        f"Link informe: {link_reporte}"
                    ),
                    to=f"whatsapp:{abogado_tel}"
                )
            except Exception as e:
                errores += f"TwilioNotif_Err: {e}. "

    except Exception as e:
        errores += f"UpdateResultados_Err: {e}. "

    return "CLIENTE_MENU", out, errores


# =========================
# Validation (MVP)
# =========================
def is_valid_by_rule(value: str, rule: str) -> bool:
    value = (value or "").strip()
    rule = (rule or "").strip()

    if not rule:
        return True

    if rule.startswith("REGEX:"):
        pattern = rule.replace("REGEX:", "", 1).strip()
        try:
            return re.match(pattern, value) is not None
        except:
            return False

    if rule == "EMAIL":
        # ya no pedimos correo, pero lo dejo por si un día reactivas
        return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value) is not None

    if rule == "DATE_YYYY_MM_DD":
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except:
            return False

    if rule == "MONEY":
        try:
            x = float(value.replace("$", "").replace(",", "").strip())
            return x >= 0
        except:
            return False

    return True


# =========================
# Decide next paso por opción (soporta 1..9)
# =========================
def pick_next_step_from_option(cfg: dict, msg_opt: str, default_step: str):
    # cfg tiene Siguiente_Si_1..Siguiente_Si_9 (3..9 pueden venir vacías)
    k = f"Siguiente_Si_{msg_opt}"
    if k in cfg and cfg.get(k):
        return cfg.get(k).strip()
    # fallback: si no hay ese, usa 1/2 como antes
    if msg_opt == "1" and cfg.get("Siguiente_Si_1"):
        return cfg.get("Siguiente_Si_1").strip()
    if msg_opt == "2" and cfg.get("Siguiente_Si_2"):
        return cfg.get("Siguiente_Si_2").strip()
    return default_step


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200


@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone_raw = phone_raw(request.form.get("From") or "")
    from_phone_normed = phone_norm(from_phone_raw)

    body_raw = request.form.get("Body") or ""
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)

    canal = "WHATSAPP"
    modelo_ai = OPENAI_MODEL if OPENAI_API_KEY else ""

    if not msg_in:
        return safe_reply("Hola 👋")

    # Fuente desde el primer mensaje
    fuente = detect_fuente(msg_in)

    # Conexión a Sheets
    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
        ws_abogados = open_worksheet(sh, TAB_ABOGADOS)
        ws_sys = open_worksheet(sh, TAB_SYS)
        ws_param = open_worksheet(sh, TAB_PARAM)
    except Exception:
        return safe_reply("⚠️ Error de conexión con la base de datos. Intenta de nuevo en unos minutos.")

    leads_headers = build_header_map(ws_leads)

    # Crear o cargar lead
    lead_row, lead_id, estatus_actual, created = get_or_create_lead(
        ws_leads, leads_headers, from_phone_raw, from_phone_normed, fuente
    )

    # Snapshot del lead
    row_vals = ws_leads.row_values(lead_row)
    headers_list = ws_leads.row_values(1)
    lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

    errores = ""

    # Cargar paso actual
    try:
        cfg = load_config_row(ws_config, estatus_actual)
    except Exception as e:
        errores += f"LoadCfg_Err: {e}. "
        # fallback seguro
        return safe_reply("⚠️ Tuvimos un problema interno con el flujo. Escríbenos nuevamente en unos minutos.")

    paso_actual = (cfg.get("ID_Paso") or estatus_actual or "INICIO").strip()
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = render_text(cfg.get("Texto_Bot") or "")

    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    regla = (cfg.get("Regla_Validacion") or "").strip()
    msg_error = render_text((cfg.get("Mensaje_Error") or "Respuesta inválida.").strip())

    # Si es creado o está en INICIO: manda texto y avanza a siguiente
    if created or paso_actual == "INICIO":
        # Mensaje de INICIO se responde, y estatus pasa a "AVISO_PRIVACIDAD" o al que tenga en Siguiente_Si_1
        next_paso = (cfg.get("Siguiente_Si_1") or "AVISO_PRIVACIDAD").strip()
        out = texto_bot or "Hola, soy Ximena AI 👋"
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "ESTATUS": next_paso,
            "Ultimo_Mensaje_Cliente": msg_in,
            "Ultima_Actualizacion": now_iso_mx(),
            "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
        })

        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_mx(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": next_paso,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
            "Modelo_AI": modelo_ai,
            "Errores": errores.strip(),
        })
        return safe_reply(out)

    # Procesar según tipo
    next_paso = paso_actual
    out = texto_bot

    if tipo == "OPCIONES":
        # validar opción
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            # update campo si aplica
            if campo_update:
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})

            # determinar siguiente
            next_paso = pick_next_step_from_option(cfg, msg_opt, paso_actual)

            # Cargar el siguiente paso para responder (no respondas el mismo)
            try:
                cfg2 = load_config_row(ws_config, next_paso)
            except Exception as e:
                errores += f"LoadNextCfg_Err: {e}. "
                cfg2 = None

            if cfg2 and (cfg2.get("Tipo_Entrada") or "").upper().strip() == "SISTEMA":
                # refrescar snapshot
                row_vals = ws_leads.row_values(lead_row)
                lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}
                next_paso, out_sys, err_sys = run_system_step_if_needed(
                    next_paso, lead_snapshot, ws_leads, leads_headers, lead_row,
                    ws_abogados, ws_sys, ws_param
                )
                out = out_sys or "Listo."
                errores += err_sys
            else:
                # responder con texto del siguiente paso
                if cfg2:
                    out = render_text(cfg2.get("Texto_Bot") or "")
                else:
                    out = "Gracias."

    elif tipo == "TEXTO":
        # validar
        if not is_valid_by_rule(msg_in, regla):
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            if campo_update:
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_in})

            next_paso = (cfg.get("Siguiente_Si_1") or paso_actual).strip()

            # responder con siguiente paso
            try:
                cfg2 = load_config_row(ws_config, next_paso)
                if (cfg2.get("Tipo_Entrada") or "").upper().strip() == "SISTEMA":
                    row_vals = ws_leads.row_values(lead_row)
                    lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}
                    next_paso, out_sys, err_sys = run_system_step_if_needed(
                        next_paso, lead_snapshot, ws_leads, leads_headers, lead_row,
                        ws_abogados, ws_sys, ws_param
                    )
                    out = out_sys or "Listo."
                    errores += err_sys
                else:
                    out = render_text(cfg2.get("Texto_Bot") or "Gracias.")
            except Exception as e:
                errores += f"NextCfgText_Err: {e}. "
                out = "Gracias."

    elif tipo == "SISTEMA":
        # por si llega aquí
        next_paso, out_sys, err_sys = run_system_step_if_needed(
            paso_actual, lead_snapshot, ws_leads, leads_headers, lead_row,
            ws_abogados, ws_sys, ws_param
        )
        out = out_sys or "Listo."
        errores += err_sys

    # Actualizar lead base
    update_lead_batch(ws_leads, leads_headers, lead_row, {
        "Ultima_Actualizacion": now_iso_mx(),
        "ESTATUS": next_paso,
        "Ultimo_Mensaje_Cliente": msg_in,
        "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
    })

    # Log
    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso_mx(),
        "Telefono": from_phone_raw,
        "ID_Lead": lead_id,
        "Paso": next_paso,
        "Mensaje_Entrante": msg_in,
        "Mensaje_Saliente": out,
        "Canal": canal,
        "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
        "Modelo_AI": modelo_ai,
        "Errores": errores.strip(),
    })

    return safe_reply(out)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
