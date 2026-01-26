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
# Time + Twilio
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
    # Conserva exactamente lo que manda Twilio: "whatsapp:+52..."
    return (raw or "").strip()


def phone_norm(raw: str) -> str:
    # Canoniza a "+52..." para matching secundario
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
        raise RuntimeError("Falta GOOGLE_SHEET_NAME (nombre exacto del Google Sheet).")
    return gc.open(GOOGLE_SHEET_NAME)


def open_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except Exception:
        raise RuntimeError(
            f"No existe la pestaña '{title}' en el Google Sheet '{GOOGLE_SHEET_NAME}'. "
            f"Verifica el nombre exacto del tab."
        )


# =========================
# Headers (robusto a mayúsculas/minúsculas)
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
        # mapa exacto
        if key not in m:
            m[key] = i
        # mapa case-insensitive (lower)
        low = key.lower()
        if low not in m:
            m[low] = i
    return m


def col_idx(headers_map: dict, name: str):
    # Permite buscar por "ESTATUS" o "estatus" etc.
    return headers_map.get(name) or headers_map.get((name or "").lower())


def row_to_dict(row_vals: list, header_map: dict, canonical_headers: list):
    out = {}
    for h in canonical_headers:
        idx = col_idx(header_map, h)
        if not idx:
            out[h] = ""
            continue
        pos = idx - 1
        v = (row_vals[pos] if pos < len(row_vals) else "") or ""
        out[h] = v.strip() if isinstance(v, str) else v
    return out


def get_row_vals(ws, row_idx: int):
    return ws.row_values(row_idx)


# =========================
# Búsqueda por columna
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
# Leads: busca por Teléfono RAW y por Normalizado
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_norm: str, fuente: str = "FACEBOOK"):
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono' en el header (fila 1).")

    # 1) intenta encontrar por RAW (whatsapp:+52...)
    row = find_row_by_value(ws_leads, tel_col, tel_raw)

    # 2) si no, intenta por NORMALIZADO (+52...)
    if not row:
        row = find_row_by_value(ws_leads, tel_col, tel_norm)

    if row:
        vals = get_row_vals(ws_leads, row)
        lead_id = ""
        estatus = ""
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        if idx_id and idx_id - 1 < len(vals):
            lead_id = (vals[idx_id - 1] or "").strip()
        if idx_est and idx_est - 1 < len(vals):
            estatus = (vals[idx_est - 1] or "").strip()
        return row, lead_id, estatus, False

    # Crear lead nuevo
    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)
    new_row = [""] * max(1, len(headers_row))

    def set_if(col_name, val):
        idx = col_idx(leads_headers, col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", tel_raw)  # <-- GUARDA RAW para evitar bucles
    set_if("Telefono_Normalizado", tel_norm)  # opcional si existe esa columna
    set_if("Fuente_Lead", fuente or "FACEBOOK")
    set_if("Fecha_Registro", now_iso())
    set_if("Ultima_Actualizacion", now_iso())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    # recuperar row
    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    return row, lead_id, "INICIO", True


# =========================
# Config (fila con 1 lectura)
# =========================
def load_config_row(ws_config, paso_actual: str):
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso' en el header (fila 1).")

    paso_actual = (paso_actual or "").strip() or "INICIO"

    row = find_row_by_value(ws_config, idpaso_col, paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")

    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}' (ni para 'INICIO').")

    row_vals = ws_config.row_values(row)
    fields = [
        "ID_Paso", "Texto_Bot", "Tipo_Entrada", "Opciones_Validas",
        "Siguiente_Si_1", "Siguiente_Si_2",
        "Campo_BD_Leads_A_Actualizar", "Regla_Validacion", "Mensaje_Error"
    ]
    d = row_to_dict(row_vals, cfg_headers, fields)

    return {
        "row": row,
        **{k: (d.get(k) or "").strip() for k in fields}
    }


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200


@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone_raw = phone_raw(request.form.get("From") or "")
    from_phone_norm = phone_norm(from_phone_raw)

    body_raw = request.form.get("Body") or ""
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)

    canal = "WHATSAPP"
    fuente = "FACEBOOK"
    modelo_ai = ""

    default_error_msg = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."

    # Sheets
    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception:
        return safe_reply(default_error_msg)

    leads_headers = build_header_map(ws_leads)

    # Lead
    try:
        lead_row, lead_id, estatus_actual, created = get_or_create_lead(
            ws_leads, leads_headers, from_phone_raw, from_phone_norm, fuente
        )
    except Exception as e:
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
            "ID_Lead": "",
            "Paso": "",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": "⚠️ Error interno al crear/buscar lead.",
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": str(e),
        })
        return safe_reply("⚠️ Error interno (Lead). Revisa BD_Leads.")

    if not msg_in:
        out = "Hola 👋 ¿En qué puedo ayudarte?"
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": estatus_actual,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": "",
        })
        return safe_reply(out)

    # Config del paso actual
    try:
        cfg = load_config_row(ws_config, estatus_actual)
    except Exception as e:
        out = "⚠️ No hay configuración del bot para continuar. Revisa Config_XimenaAI."
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": estatus_actual,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": str(e),
        })
        return safe_reply(out)

    paso_actual = cfg.get("ID_Paso") or (estatus_actual or "INICIO")
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = cfg.get("Texto_Bot") or ""

    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = (cfg.get("Mensaje_Error") or "Respuesta inválida.").strip()

    errores = ""

    # ✅ Disparo INICIO sin validar lo que escribió el usuario, pero AVANZA estatus
    if created or paso_actual == "INICIO" or (estatus_actual or "").strip() == "INICIO":
        next_paso = sig1 or "INICIO"
        out = texto_bot or "Hola 👋"

        try:
            update_lead_batch(ws_leads, leads_headers, lead_row, {
                "Ultima_Actualizacion": now_iso(),
                "ESTATUS": next_paso,
                "Ultimo_Mensaje_Cliente": msg_in,
            })
        except Exception as e:
            errores = f"BatchUpdateLead(INICIO) {e}"

        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
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

    # =========================
    # Lógica por tipo
    # =========================
    next_paso = paso_actual
    out = texto_bot or "Continuemos."

    if tipo == "SISTEMA":
        out = texto_bot or "Listo."
        next_paso = sig1 or paso_actual

    elif tipo == "OPCIONES":
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + (msg_error or "Responde con una opción válida.")
            next_paso = paso_actual
        else:
            if campo_update:
                if col_idx(leads_headers, campo_update):
                    try:
                        update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})
                    except Exception as e:
                        errores += f"BatchUpdateCampo({campo_update}) {e}. "
                else:
                    errores += f"Campo no existe en BD_Leads: {campo_update}. "

            if len(opciones_validas) >= 1 and msg_opt == opciones_validas[0]:
                next_paso = sig1 or paso_actual
            else:
                next_paso = sig2 or paso_actual

            try:
                cfg2 = load_config_row(ws_config, next_paso)
                out = cfg2.get("Texto_Bot") or "Continuemos."
            except Exception as e:
                errores += f"LoadCfg2({next_paso}) {e}. "
                out = "Continuemos."

    else:
        if campo_update:
            if col_idx(leads_headers, campo_update):
                try:
                    update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_in})
                except Exception as e:
                    errores += f"BatchUpdateCampo({campo_update}) {e}. "
            else:
                errores += f"Campo no existe en BD_Leads: {campo_update}. "

        next_paso = sig1 or paso_actual
        try:
            cfg2 = load_config_row(ws_config, next_paso)
            out = cfg2.get("Texto_Bot") or "Gracias. Continuemos."
        except Exception as e:
            errores += f"LoadCfg2({next_paso}) {e}. "
            out = "Gracias. Continuemos."

    # Final update (estatus + timestamp + último mensaje)
    try:
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "Ultima_Actualizacion": now_iso(),
            "ESTATUS": next_paso,
            "Ultimo_Mensaje_Cliente": msg_in,
        })
    except Exception as e:
        errores += f" BatchUpdateLead(FINAL) {e}."

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso(),
        "Telefono": from_phone_raw,
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

