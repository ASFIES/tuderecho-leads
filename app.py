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

import gspread
from google.oauth2.service_account import Credentials

# Redis / RQ
from redis import Redis
from rq import Queue

# Job module
import worker_jobs

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

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

# Redis env (ya los tienes)
REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

# =========================
# Time (MX)
# =========================
MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

# =========================
# Twilio Reply
# =========================
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
# Fuente Lead
# =========================
def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        return "WEB"
    return "DESCONOCIDA"

# =========================
# Redis queue
# =========================
def get_rq_queue():
    if not REDIS_URL:
        raise RuntimeError("Falta REDIS_URL en el servicio Webhook.")
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def enqueue_process_lead(lead_id: str):
    """
    Encola worker_jobs.process_lead(lead_id) en RQ.
    """
    q = get_rq_queue()
    # job_timeout: segundos máximos permitidos para ejecutar el cálculo
    q.enqueue(worker_jobs.process_lead, lead_id, job_timeout=180)

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
# Load Config row
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
# Leads: get/create
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_normed: str, fuente: str):
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono'.")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_normed)
    if row:
        vals = ws_leads.row_values(row)
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        idx_fuente = col_idx(leads_headers, "Fuente_Lead")

        lead_id = (vals[idx_id - 1] or "").strip() if idx_id and idx_id - 1 < len(vals) else ""
        estatus = (vals[idx_est - 1] or "").strip() if idx_est and idx_est - 1 < len(vals) else "INICIO"
        fuente_actual = (vals[idx_fuente - 1] or "").strip() if idx_fuente and idx_fuente - 1 < len(vals) else ""

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
    set_if("Telefono_Normalizado", tel_normed)
    set_if("Fuente_Lead", fuente or "DESCONOCIDA")
    set_if("Fecha_Registro", now_iso_mx())
    set_if("Ultima_Actualizacion", now_iso_mx())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")
    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_normed)
    return row, lead_id, "INICIO", True

# =========================
# Validations
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

    if rule == "MONEY":
        try:
            x = float(value.replace("$", "").replace(",", "").strip())
            return x >= 0
        except:
            return False

    return True

# =========================
# Build date from parts
# =========================
def build_date_from_parts(y: str, m: str, d: str) -> str:
    y = (y or "").strip()
    m = (m or "").strip()
    d = (d or "").strip()
    if not (y and m and d):
        return ""
    try:
        yy = int(y); mm = int(m); dd = int(d)
        dt = datetime(yy, mm, dd)
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

# =========================
# Next step helper (OPCIONES)
# =========================
def pick_next_step_from_option(cfg: dict, msg_opt: str, default_step: str):
    k = f"Siguiente_Si_{msg_opt}"
    if cfg.get(k):
        return cfg.get(k).strip()
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
    modelo_ai = ""  # aquí ya no usamos OpenAI en webhook

    if not msg_in:
        return safe_reply("Hola 👋")

    fuente = detect_fuente(msg_in)

    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception:
        return safe_reply("⚠️ Por el momento no puedo acceder a la base de datos. Intenta de nuevo en unos minutos.")

    leads_headers = build_header_map(ws_leads)

    lead_row, lead_id, estatus_actual, created = get_or_create_lead(
        ws_leads, leads_headers, from_phone_raw, from_phone_normed, fuente
    )

    headers_list = ws_leads.row_values(1)
    row_vals = ws_leads.row_values(lead_row)
    lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

    errores = ""

    # ========== NEW LEAD ==========
    if created:
        cfg_inicio = load_config_row(ws_config, "INICIO")
        out = render_text(cfg_inicio.get("Texto_Bot") or "Hola, soy Ximena AI 👋")
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "ESTATUS": "INICIO",
            "Ultimo_Mensaje_Cliente": msg_in,
            "Ultima_Actualizacion": now_iso_mx(),
            "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
        })
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_mx(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": "INICIO",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
            "Modelo_AI": modelo_ai,
            "Errores": errores.strip(),
        })
        return safe_reply(out)

    # Fail-safe: saltar CORREO si existiera por error
    if (estatus_actual or "").strip().upper() == "CORREO":
        estatus_actual = "DESCRIPCION"

    try:
        cfg = load_config_row(ws_config, estatus_actual)
    except Exception as e:
        errores += f"LoadCfg_Err: {e}. "
        return safe_reply("⚠️ Tuvimos un problema interno. Intenta de nuevo en unos minutos.")

    paso_actual = (cfg.get("ID_Paso") or estatus_actual or "INICIO").strip()
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = render_text(cfg.get("Texto_Bot") or "")

    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    regla = (cfg.get("Regla_Validacion") or "").strip()
    msg_error = render_text((cfg.get("Mensaje_Error") or "Respuesta inválida.").strip())

    next_paso = paso_actual
    out = texto_bot

    # ======================
    # OPCIONES
    # ======================
    if tipo == "OPCIONES":
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            # Guardar en BD si aplica (nunca correo)
            if campo_update and campo_update.lower() != "correo":
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})

            # Determinar siguiente paso
            next_paso = pick_next_step_from_option(cfg, msg_opt, paso_actual)

            # Salto forzado si por error apunta a CORREO
            if next_paso.upper() == "CORREO":
                next_paso = "DESCRIPCION"

            # Cargar cfg del siguiente paso
            cfg2 = load_config_row(ws_config, next_paso)

            # ✅ Caso especial: EN_PROCESO => encolar job y responder texto
            if next_paso.strip().upper() == "EN_PROCESO":
                try:
                    enqueue_process_lead(lead_id)
                except Exception as e:
                    errores += f"Enqueue_Err: {e}. "
                out = render_text(cfg2.get("Texto_Bot") or "Gracias. Estoy preparando tu estimación…")
            else:
                out = render_text(cfg2.get("Texto_Bot") or "Gracias.")

    # ======================
    # TEXTO
    # ======================
    elif tipo == "TEXTO":
        if not is_valid_by_rule(msg_in, regla):
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            # guardar campo (nunca correo)
            if campo_update and campo_update.lower() != "correo":
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_in})

            # refrescar snapshot
            row_vals = ws_leads.row_values(lead_row)
            lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

            # ----- FIX: INI_DIA debe avanzar -----
            if paso_actual.upper() == "INI_DIA":
                fecha_ini = build_date_from_parts(
                    lead_snapshot.get("Inicio_Anio"),
                    lead_snapshot.get("Inicio_Mes"),
                    lead_snapshot.get("Inicio_Dia"),
                )
                if not fecha_ini:
                    out = "Ups, esa fecha no parece válida. Por favor escribe nuevamente el *DÍA* (1 a 31)."
                    next_paso = "INI_DIA"
                else:
                    update_lead_batch(ws_leads, leads_headers, lead_row, {"Fecha_Inicio_Laboral": fecha_ini})
                    next_paso = "FIN_ANIO"  # ✅ FORZAR AVANCE

            # ----- FIX: FIN_DIA debe avanzar -----
            elif paso_actual.upper() == "FIN_DIA":
                fecha_fin = build_date_from_parts(
                    lead_snapshot.get("Fin_Anio"),
                    lead_snapshot.get("Fin_Mes"),
                    lead_snapshot.get("Fin_Dia"),
                )
                if not fecha_fin:
                    out = "Ups, esa fecha no parece válida. Por favor escribe nuevamente el *DÍA* (1 a 31)."
                    next_paso = "FIN_DIA"
                else:
                    update_lead_batch(ws_leads, leads_headers, lead_row, {"Fecha_Fin_Laboral": fecha_fin})
                    next_paso = "SALARIO"  # ✅ FORZAR AVANCE

            # flujo normal para cualquier otro TEXTO
            else:
                next_paso = (cfg.get("Siguiente_Si_1") or paso_actual).strip()
                if next_paso.upper() == "CORREO":
                    next_paso = "DESCRIPCION"

            # Responder texto del siguiente paso
            if next_paso != paso_actual:
                cfg2 = load_config_row(ws_config, next_paso)
                out = render_text(cfg2.get("Texto_Bot") or "Gracias.")

    # ======================
    # SISTEMA (solo texto)
    # ======================
    elif tipo == "SISTEMA":
        # En este webhook, "SISTEMA" solo envía el texto configurado y actualiza estatus.
        # Cálculos/resultados los hace el worker.
        out = texto_bot or "Listo."
        next_paso = (cfg.get("Siguiente_Si_1") or paso_actual).strip() or paso_actual

    # update lead base
    update_lead_batch(ws_leads, leads_headers, lead_row, {
        "Ultima_Actualizacion": now_iso_mx(),
        "ESTATUS": next_paso,
        "Ultimo_Mensaje_Cliente": msg_in,
        "Fuente_Lead": lead_snapshot.get("Fuente_Lead") or fuente,
    })

    # log
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
