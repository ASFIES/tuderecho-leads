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
    return (s or "").replace("\\n", "\n")

# =========================
# Normalización
# =========================
def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    return (raw or "").replace("whatsapp:", "").strip()

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    return m.group(0) if m else s

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if any(x in t for x in ["facebook", "anuncio", "fb"]):
        return "FACEBOOK"
    if any(x in t for x in ["sitio", "web", "pagina", "página"]):
        return "WEB"
    return "DESCONOCIDA"

# =========================
# Google Sheets
# =========================
def get_env_creds_dict():
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        try:
            if raw.lstrip().startswith("{"):
                return json.loads(raw)
            return json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido: {e}")

    if GOOGLE_CREDENTIALS_PATH:
        if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe.")
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales Google (JSON o PATH).")

def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(get_env_creds_dict(), scopes=scopes)
    return gspread.authorize(creds)

def build_header_map(ws):
    headers = ws.row_values(1)
    # map: lower(header) -> col index
    return { (h or "").strip().lower(): i for i, h in enumerate(headers, start=1) if (h or "").strip() }

def col_idx(hmap, name: str):
    return hmap.get((name or "").strip().lower())

def find_row_by_value(ws, col: int, value: str):
    value = (value or "").strip()
    if not value:
        return None
    vals = ws.col_values(col)
    for i, v in enumerate(vals[1:], start=2):
        if (v or "").strip() == value:
            return i
    return None

def update_lead_batch(ws, hmap, row_idx: int, updates: dict):
    payload = []
    for k, v in (updates or {}).items():
        c = col_idx(hmap, k)
        if not c:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, c)
        payload.append({"range": a1, "values": [[v]]})
    if payload:
        ws.batch_update(payload)

def safe_log(ws_logs, data: dict):
    try:
        cols = ["ID_Log","Fecha_Hora","Telefono","ID_Lead","Paso","Mensaje_Entrante","Mensaje_Saliente","Canal","Fuente_Lead","Modelo_AI","Errores"]
        row = [data.get(c,"") for c in cols]
        ws_logs.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        pass

# =========================
# Config_XimenaAI
# =========================
def load_config_row(ws_config, paso: str) -> dict:
    paso = (paso or "INICIO").strip()
    rows = ws_config.get_all_records()
    if not rows:
        return {}
    for r in rows:
        if str(r.get("ID_Paso","")).strip() == paso:
            return r
    # fallback
    for r in rows:
        if str(r.get("ID_Paso","")).strip() == "INICIO":
            return r
    return rows[0]

def pick_next_step(cfg: dict, opt: str, default_step: str) -> str:
    k = f"Siguiente_Si_{opt}"
    if cfg.get(k):
        return str(cfg.get(k)).strip()
    if cfg.get("Siguiente_Si_1"):
        return str(cfg.get("Siguiente_Si_1")).strip()
    return default_step

# =========================
# Lead get/create
# =========================
def get_or_create_lead(ws_leads, hmap, tel_raw, tel_normed, fuente):
    c_tel = col_idx(hmap, "Telefono")
    if not c_tel:
        raise RuntimeError("BD_Leads debe tener columna Telefono.")

    row = find_row_by_value(ws_leads, c_tel, tel_raw) or find_row_by_value(ws_leads, c_tel, tel_normed)
    if row:
        vals = ws_leads.row_values(row)
        c_est = col_idx(hmap, "ESTATUS")
        c_id = col_idx(hmap, "ID_Lead")
        estatus = (vals[c_est-1] if c_est and c_est-1 < len(vals) else "INICIO") or "INICIO"
        lead_id = (vals[c_id-1] if c_id and c_id-1 < len(vals) else "") or ""
        return row, lead_id, estatus, False

    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)
    new_row = [""] * len(headers_row)

    def setc(name, val):
        c = col_idx(hmap, name)
        if c and c-1 < len(new_row):
            new_row[c-1] = val

    setc("ID_Lead", lead_id)
    setc("Telefono", tel_raw)
    setc("Telefono_Normalizado", tel_normed)
    setc("Fuente_Lead", fuente or "DESCONOCIDA")
    setc("Fecha_Registro", now_iso_mx())
    setc("Ultima_Actualizacion", now_iso_mx())
    setc("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")
    row = find_row_by_value(ws_leads, c_tel, tel_raw) or find_row_by_value(ws_leads, c_tel, tel_normed)
    return row, lead_id, "INICIO", True

# =========================
# Validación simple
# =========================
def is_valid(value: str, rule: str) -> bool:
    value = (value or "").strip()
    rule = (rule or "").strip()
    if not rule:
        return True
    if rule.startswith("REGEX:"):
        pattern = rule.replace("REGEX:", "", 1).strip()
        try:
            return re.match(pattern, value) is not None
        except Exception:
            return False
    if rule == "MONEY":
        try:
            float(value.replace("$","").replace(",","").strip())
            return True
        except Exception:
            return False
    return True

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

    if not msg_in:
        return safe_reply("Hola 👋")

    fuente = detect_fuente(msg_in)

    try:
        gc = get_gspread_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws_leads = sh.worksheet(TAB_LEADS)
        ws_config = sh.worksheet(TAB_CONFIG)
        ws_logs = sh.worksheet(TAB_LOGS)
    except Exception:
        return safe_reply("⚠️ En este momento tenemos una falla técnica. Intenta de nuevo en unos minutos, por favor.")

    hmap = build_header_map(ws_leads)

    try:
        lead_row, lead_id, estatus_actual, created = get_or_create_lead(
            ws_leads, hmap, from_phone_raw, from_phone_normed, fuente
        )
    except Exception:
        return safe_reply("⚠️ No pudimos registrar tu información. Intenta de nuevo con 'Hola'.")

    # primer mensaje si es nuevo
    if created:
        cfg = load_config_row(ws_config, "INICIO")
        out = render_text(cfg.get("Texto_Bot") or "Hola, soy Ximena AI 👋")
        update_lead_batch(ws_leads, hmap, lead_row, {
            "ESTATUS": "INICIO",
            "Ultimo_Mensaje_Cliente": msg_in,
            "Ultima_Actualizacion": now_iso_mx(),
            "Fuente_Lead": fuente,
        })
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_mx(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": "INICIO",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": "WHATSAPP",
            "Fuente_Lead": fuente,
            "Modelo_AI": "",
            "Errores": "",
        })
        return safe_reply(out)

    # failsafe: nunca pedir correo
    if (estatus_actual or "").strip().upper() == "CORREO":
        estatus_actual = "DESCRIPCION"

    cfg = load_config_row(ws_config, estatus_actual)
    tipo = str(cfg.get("Tipo_Entrada","")).upper().strip()
    texto_bot = render_text(cfg.get("Texto_Bot",""))
    opciones_validas = [normalize_option(x) for x in str(cfg.get("Opciones_Validas","")).split(",") if x.strip()]
    campo_update = str(cfg.get("Campo_BD_Leads_A_Actualizar","") or "").strip()
    regla = str(cfg.get("Regla_Validacion","") or "").strip()
    msg_error = render_text(str(cfg.get("Mensaje_Error","Respuesta inválida.")))

    next_paso = estatus_actual
    out = texto_bot

    if tipo == "OPCIONES":
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + "⚠️ " + msg_error
            next_paso = estatus_actual
        else:
            # guardar elección (nunca correo)
            if campo_update and campo_update.lower() != "correo":
                update_lead_batch(ws_leads, hmap, lead_row, {campo_update: msg_opt})

            next_paso = pick_next_step(cfg, msg_opt, estatus_actual)

            # nunca pedir correo
            if str(next_paso).upper() == "CORREO":
                next_paso = "DESCRIPCION"

            # si toca generar resultados -> lo manda al worker
            if str(next_paso).upper() in ["GENERAR_RESULTADOS", "GENERAR_RESULTADO"]:
                update_lead_batch(ws_leads, hmap, lead_row, {
                    "ESTATUS": "PROCESANDO",
                    "Procesar_AI_Status": "PENDIENTE",
                    "Ultima_Actualizacion": now_iso_mx(),
                    "Ultimo_Mensaje_Cliente": msg_in,
                })
                out = "⚖️ *Estoy analizando tu situación con cuidado…*\n\nDame un momento, por favor. En breve te regreso una estimación preliminar y el abogado que llevará tu caso."
                safe_log(ws_logs, {
                    "ID_Log": str(uuid.uuid4()),
                    "Fecha_Hora": now_iso_mx(),
                    "Telefono": from_phone_raw,
                    "ID_Lead": lead_id,
                    "Paso": "PROCESANDO",
                    "Mensaje_Entrante": msg_in,
                    "Mensaje_Saliente": out,
                    "Canal": "WHATSAPP",
                    "Fuente_Lead": fuente,
                    "Modelo_AI": "",
                    "Errores": "",
                })
                return safe_reply(out)

            # mensaje del siguiente paso
            cfg2 = load_config_row(ws_config, next_paso)
            out = render_text(cfg2.get("Texto_Bot") or "Gracias.")

    elif tipo == "TEXTO":
        if not is_valid(msg_in, regla):
            out = (texto_bot + "\n\n" if texto_bot else "") + "⚠️ " + msg_error
            next_paso = estatus_actual
        else:
            if campo_update and campo_update.lower() != "correo":
                update_lead_batch(ws_leads, hmap, lead_row, {campo_update: msg_in})

            next_paso = str(cfg.get("Siguiente_Si_1") or estatus_actual).strip()
            if next_paso.upper() == "CORREO":
                next_paso = "DESCRIPCION"

            cfg2 = load_config_row(ws_config, next_paso)
            out = render_text(cfg2.get("Texto_Bot") or "Gracias.")

    # update estado
    update_lead_batch(ws_leads, hmap, lead_row, {
        "ESTATUS": next_paso,
        "Ultimo_Mensaje_Cliente": msg_in,
        "Ultima_Actualizacion": now_iso_mx(),
        "Fuente_Lead": fuente,
    })

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso_mx(),
        "Telefono": from_phone_raw,
        "ID_Lead": lead_id,
        "Paso": next_paso,
        "Mensaje_Entrante": msg_in,
        "Mensaje_Saliente": out,
        "Canal": "WHATSAPP",
        "Fuente_Lead": fuente,
        "Modelo_AI": "",
        "Errores": "",
    })

    return safe_reply(out)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
