import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

import gspread
from google.oauth2.service_account import Credentials

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

# =========================
# Helpers: time + twilio
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)


# =========================
# Normalización
# =========================
def phone_raw(raw: str) -> str:
    return (raw or "").strip()  # "whatsapp:+52..."


def phone_norm(raw: str) -> str:
    s = (raw or "").strip()
    return s.replace("whatsapp:", "").strip()  # "+52..."


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
        raise RuntimeError(f"No existe la pestaña '{title}' en el Google Sheet.")


# =========================
# Headers
# =========================
def norm_header(s: str) -> str:
    return (s or "").strip()


def build_header_map(ws):
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = norm_header(h)
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


# =========================
# Buscar fila (1 llamada por columna)
# =========================
def find_row_by_value(ws, col_idx_num: int, value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        col_values = ws.col_values(col_idx_num)
        for i, v in enumerate(col_values[1:], start=2):
            if (v or "").strip() == value:
                return i
        return None
    except Exception:
        return None


# =========================
# Batch update (1 request)
# =========================
def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    payload = []
    for col_name, val in (updates or {}).items():
        idx = col_idx(header_map, col_name)
        if not idx:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, idx)
        payload.append({"range": a1, "values": [[val]]})
    if payload:
        ws.batch_update(payload)


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
# Alias de campos Config -> BD_Leads
# =========================
FIELD_ALIASES = {
    "AVISO_OK": "Aviso_Privacidad_Aceptado",
}


def resolve_leads_field(leads_headers: dict, requested_field: str) -> str:
    if not requested_field:
        return ""
    if col_idx(leads_headers, requested_field):
        return requested_field
    alias = FIELD_ALIASES.get(requested_field)
    if alias and col_idx(leads_headers, alias):
        return alias
    return requested_field


# =========================
# Leads: get/create
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_norm: str, fuente: str = "FACEBOOK"):
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono'.")

    row = find_row_by_value(ws_leads, tel_col, tel_raw)
    if not row:
        row = find_row_by_value(ws_leads, tel_col, tel_norm)

    if row:
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        vals = ws_leads.row_values(row)
        lead_id = (vals[idx_id - 1] if idx_id and idx_id - 1 < len(vals) else "") or ""
        estatus = (vals[idx_est - 1] if idx_est and idx_est - 1 < len(vals) else "") or ""
        return row, lead_id.strip(), estatus.strip(), False

    lead_id = str(uuid.uuid4())
    header_row = ws_leads.row_values(1)
    new_row = [""] * max(1, len(header_row))

    def set_if(col_name, val):
        idx = col_idx(leads_headers, col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", tel_raw)
    set_if("Fuente_Lead", fuente or "FACEBOOK")
    set_if("Fecha_Registro", now_iso())
    set_if("Ultima_Actualizacion", now_iso())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    return row, lead_id, "INICIO", True


# =========================
# Config: load sin fallback al cargar SIGUIENTE
# =========================
def load_config_row(ws_config, paso: str, allow_fallback_inicio: bool = False):
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso'.")

    paso = (paso or "").strip() or "INICIO"
    row = find_row_by_value(ws_config, idpaso_col, paso)

    if not row and allow_fallback_inicio and paso != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")

    if not row:
        raise RuntimeError(f"Paso no existe en Config_XimenaAI: {paso}")

    row_vals = ws_config.row_values(row)

    def get(col_name: str) -> str:
        idx = col_idx(cfg_headers, col_name)
        if not idx or idx - 1 >= len(row_vals):
            return ""
        return (row_vals[idx - 1] or "").strip()

    return {
        "ID_Paso": get("ID_Paso"),
        "Texto_Bot": get("Texto_Bot"),
        "Tipo_Entrada": get("Tipo_Entrada"),
        "Opciones_Validas": get("Opciones_Validas"),
        "Siguiente_Si_1": get("Siguiente_Si_1"),
        "Siguiente_Si_2": get("Siguiente_Si_2"),
        "Siguiente_Si_3": get("Siguiente_Si_3"),
        "Siguiente_Si_4": get("Siguiente_Si_4"),
        "Campo_BD_Leads_A_Actualizar": get("Campo_BD_Leads_A_Actualizar"),
        "Mensaje_Error": get("Mensaje_Error"),
    }


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200


@app.post("/whatsapp")
def whatsapp_webhook():
    tel_raw = phone_raw(request.form.get("From") or "")
    tel_norm = phone_norm(tel_raw)

    body_raw = request.form.get("Body") or ""
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)

    canal = "WHATSAPP"
    fuente = "FACEBOOK"
    modelo_ai = ""

    default_error_msg = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."

    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception:
        return safe_reply(default_error_msg)

    leads_headers = build_header_map(ws_leads)

    try:
        lead_row, lead_id, estatus_actual, created = get_or_create_lead(
            ws_leads, leads_headers, tel_raw, tel_norm, fuente
        )
    except Exception as e:
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": tel_raw,
            "ID_Lead": "",
            "Paso": "",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": "⚠️ Error interno lead.",
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": str(e),
        })
        return safe_reply("⚠️ Error interno (Lead).")

    if not msg_in:
        return safe_reply("Hola 👋 ¿En qué puedo ayudarte?")

    # paso actual: aquí sí permitimos fallback a INICIO
    try:
        cfg = load_config_row(ws_config, estatus_actual, allow_fallback_inicio=True)
    except Exception:
        return safe_reply(f"⚠️ Falta configurar el paso actual: {estatus_actual} en Config_XimenaAI.")

    paso_actual = cfg.get("ID_Paso") or (estatus_actual or "INICIO")
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = cfg.get("Texto_Bot") or ""
    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]

    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    sig3 = (cfg.get("Siguiente_Si_3") or "").strip()
    sig4 = (cfg.get("Siguiente_Si_4") or "").strip()

    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = (cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.").strip()

    errores = ""
    out = "Continuemos."
    next_paso = paso_actual

    # Disparo INICIO: SOLO si lead nuevo o está en INICIO
    if created or paso_actual == "INICIO":
        next_paso = sig1 or "INICIO"
        out = texto_bot or "Hola 👋"
        try:
            update_lead_batch(ws_leads, leads_headers, lead_row, {
                "Ultima_Actualizacion": now_iso(),
                "ESTATUS": next_paso,
                "Ultimo_Mensaje_Cliente": msg_in,
            })
        except Exception as e:
            errores = f"BatchUpdate(INICIO) {e}"

        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": tel_raw,
            "ID_Lead": lead_id,
            "Paso": next_paso,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": ("DISPARO_INICIO " + errores).strip(),
        })
        return safe_reply(out)

    # Lógica por tipo
    if tipo == "OPCIONES":
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            # Guardar campo si aplica
            if campo_update:
                field_real = resolve_leads_field(leads_headers, campo_update)
                if col_idx(leads_headers, field_real):
                    try:
                        update_lead_batch(ws_leads, leads_headers, lead_row, {field_real: msg_opt})
                    except Exception as e:
                        errores += f"BatchUpdateCampo({field_real}) {e}. "
                else:
                    errores += f"Campo no existe en BD_Leads: {campo_update}. "

            # Decidir siguiente (1..4)
            if msg_opt == "1":
                next_paso = sig1 or paso_actual
            elif msg_opt == "2":
                next_paso = sig2 or paso_actual
            elif msg_opt == "3":
                next_paso = sig3 or paso_actual
            elif msg_opt == "4":
                next_paso = sig4 or paso_actual
            else:
                next_paso = paso_actual

            # Cargar siguiente: SIN fallback a INICIO
            try:
                cfg2 = load_config_row(ws_config, next_paso, allow_fallback_inicio=False)
                out = cfg2.get("Texto_Bot") or "Continuemos."
            except Exception:
                out = f"⚠️ Falta configurar el paso: {next_paso} en Config_XimenaAI."
                next_paso = paso_actual  # no avanzar

    elif tipo == "SISTEMA":
        out = texto_bot or "Listo."
        next_paso = sig1 or paso_actual

    else:
        # TEXTO libre
        if campo_update:
            field_real = resolve_leads_field(leads_headers, campo_update)
            if col_idx(leads_headers, field_real):
                try:
                    update_lead_batch(ws_leads, leads_headers, lead_row, {field_real: msg_in})
                except Exception as e:
                    errores += f"BatchUpdateCampo({field_real}) {e}. "
            else:
                errores += f"Campo no existe en BD_Leads: {campo_update}. "

        next_paso = sig1 or paso_actual
        try:
            cfg2 = load_config_row(ws_config, next_paso, allow_fallback_inicio=False)
            out = cfg2.get("Texto_Bot") or "Gracias. Continuemos."
        except Exception:
            out = f"⚠️ Falta configurar el paso: {next_paso} en Config_XimenaAI."
            next_paso = paso_actual

    # Update final
    try:
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "Ultima_Actualizacion": now_iso(),
            "ESTATUS": next_paso,
            "Ultimo_Mensaje_Cliente": msg_in,
        })
    except Exception as e:
        errores += f" BatchUpdate(FINAL) {e}."

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso(),
        "Telefono": tel_raw,
        "ID_Lead": lead_id,
        "Paso": next_paso,
        "Mensaje_Entrante": msg_in,
        "Mensaje_Saliente": out,
        "Canal": canal,
        "Fuente_Lead": fuente,
        "Modelo_AI": modelo_ai,
        "Errores": errores.strip(),
    })

    return safe_reply(out)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
