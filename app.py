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
# Normalización robusta
# =========================
def normalize_phone(raw: str) -> str:
    raw = (raw or "").strip()
    raw = raw.replace("whatsapp:", "").strip()
    return raw


def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    # Normaliza unicode (quita rarezas como variantes de dígitos)
    s = unicodedata.normalize("NFKC", s)
    # Quita caracteres de control invisibles
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    # Compacta espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_option(s: str) -> str:
    """
    Para flujos de OPCIONES:
    - " 1 " -> "1"
    - "1️⃣" -> "1" (tras NFKC, suele quedar 1, si no, extraemos dígito)
    - "1)" "1." "Opción 1" -> "1"
    """
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    if m:
        return m.group(0)
    return s


# =========================
# Google creds + gspread
# =========================
def get_env_creds_dict():
    """
    Soporta:
    - GOOGLE_CREDENTIALS_JSON = JSON literal (empieza con '{')
    - GOOGLE_CREDENTIALS_JSON = base64 del JSON
    - GOOGLE_CREDENTIALS_PATH = path a archivo JSON (local)
    """
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
# Headers + lectura en bloque por fila
# =========================
def norm_header(s: str) -> str:
    return (s or "").strip()


def build_header_map(ws):
    headers = ws.row_values(1)  # 1 llamada
    m = {}
    for i, h in enumerate(headers, start=1):
        key = norm_header(h)
        if key and key not in m:
            m[key] = i
    return m


def row_to_dict(row_vals: list, header_map: dict) -> dict:
    """
    Convierte una fila (lista) a dict por headers.
    No hace llamadas a Sheets.
    """
    out = {}
    for h, col_idx in header_map.items():
        pos = col_idx - 1
        out[h] = (row_vals[pos] if pos < len(row_vals) else "") or ""
        out[h] = out[h].strip() if isinstance(out[h], str) else out[h]
    return out


def get_row_dict(ws, header_map: dict, row_idx: int) -> dict:
    """
    Lee UNA fila completa con 1 sola llamada.
    """
    vals = ws.row_values(row_idx)  # 1 llamada
    return row_to_dict(vals, header_map)


# =========================
# Búsquedas (1 llamada por columna)
# =========================
def find_row_by_value(ws, col_idx: int, value: str):
    """
    Busca value exacto en columna col_idx.
    1 sola llamada: ws.col_values(col_idx)
    """
    value = (value or "").strip()
    if not value:
        return None
    try:
        col_values = ws.col_values(col_idx)  # 1 llamada
        for i, v in enumerate(col_values[1:], start=2):
            if (v or "").strip() == value:
                return i
        return None
    except Exception:
        return None


# =========================
# Escritura "casi atómica": batch_update (1 request)
# =========================
def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
    """
    Envía todas las actualizaciones en un solo request batch_update.
    Esto reduce cuota y minimiza inconsistencia por fallos a mitad.
    """
    payload = []
    for col, val in (updates or {}).items():
        col_idx = header_map.get(col)
        if not col_idx:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, col_idx)
        payload.append({"range": a1, "values": [[val]]})

    if payload:
        ws.batch_update(payload)


def safe_log(ws_logs, data: dict):
    """
    Inserta un log. Si falla, no rompe el bot.
    """
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
# Leads: get/create con lecturas mínimas
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, telefono: str, fuente: str = "FACEBOOK"):
    """
    Encuentra lead por teléfono; si no existe, crea uno.
    Devuelve (row_index, lead_id, estatus_actual, created_bool, lead_row_dict).
    """
    tel_col = leads_headers.get("Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono' en el header (fila 1).")

    row = find_row_by_value(ws_leads, tel_col, telefono)

    if row:
        lead_row_dict = get_row_dict(ws_leads, leads_headers, row)  # 1 llamada
        lead_id = (lead_row_dict.get("ID_Lead") or "").strip()
        estatus = (lead_row_dict.get("ESTATUS") or "").strip()
        return row, lead_id, estatus, False, lead_row_dict

    # Crear lead nuevo
    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)  # 1 llamada
    new_row = [""] * max(1, len(headers_row))

    def set_if(col_name, val):
        idx = leads_headers.get(col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", telefono)
    set_if("Fuente_Lead", fuente or "FACEBOOK")
    set_if("Fecha_Registro", now_iso())
    set_if("Ultima_Actualizacion", now_iso())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    # recuperar row recién creado (1 llamada col_values)
    row = find_row_by_value(ws_leads, tel_col, telefono)
    lead_row_dict = {"ID_Lead": lead_id, "ESTATUS": "INICIO", "Telefono": telefono}
    return row, lead_id, "INICIO", True, lead_row_dict


# =========================
# Config: carga fila con 1 sola lectura
# =========================
def load_config_row(ws_config, paso_actual: str):
    cfg_headers = build_header_map(ws_config)  # 1 llamada

    if "ID_Paso" not in cfg_headers:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso' en el header (fila 1).")

    paso_actual = (paso_actual or "").strip() or "INICIO"

    row = find_row_by_value(ws_config, cfg_headers["ID_Paso"], paso_actual)  # 1 llamada
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, cfg_headers["ID_Paso"], "INICIO")  # 1 llamada

    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}' (ni para 'INICIO').")

    # 1 sola lectura: toda la fila
    row_vals = ws_config.row_values(row)  # 1 llamada
    cfg = row_to_dict(row_vals, cfg_headers)

    # Campos esperados
    return {
        "row": row,
        "ID_Paso": (cfg.get("ID_Paso") or "").strip(),
        "Texto_Bot": (cfg.get("Texto_Bot") or "").strip(),
        "Tipo_Entrada": (cfg.get("Tipo_Entrada") or "").strip(),
        "Opciones_Validas": (cfg.get("Opciones_Validas") or "").strip(),
        "Siguiente_Si_1": (cfg.get("Siguiente_Si_1") or "").strip(),
        "Siguiente_Si_2": (cfg.get("Siguiente_Si_2") or "").strip(),
        "Campo_BD_Leads_A_Actualizar": (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip(),
        "Regla_Validacion": (cfg.get("Regla_Validacion") or "").strip(),
        "Mensaje_Error": (cfg.get("Mensaje_Error") or "").strip(),
    }


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200


@app.post("/whatsapp")
def whatsapp_webhook():
    # Entradas Twilio
    from_phone_raw = (request.form.get("From") or "").strip()
    body_raw = (request.form.get("Body") or "")

    # Normalización
    from_phone = normalize_phone(from_phone_raw)
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)  # para OPCIONES

    # Defaults
    canal = "WHATSAPP"
    fuente = "FACEBOOK"
    modelo_ai = ""

    default_error_msg = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."

    # 1) Abrir Sheets
    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception:
        return safe_reply(default_error_msg)

    # 2) Headers (1 llamada)
    leads_headers = build_header_map(ws_leads)

    # 3) Get/Create lead
    try:
        lead_row, lead_id, estatus_actual, created, lead_row_dict = get_or_create_lead(
            ws_leads, leads_headers, from_phone, fuente
        )
    except Exception as e:
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
            "ID_Lead": "",
            "Paso": "",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": "⚠️ Error interno al crear/buscar lead.",
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": str(e),
        })
        return safe_reply("⚠️ Error interno (Lead). Revisa la configuración de BD_Leads.")

    # 4) Si viene vacío
    if not msg_in:
        out = "Hola 👋 ¿En qué puedo ayudarte?"
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
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

    # 5) Cargar config del paso actual (minimiza lecturas)
    try:
        cfg = load_config_row(ws_config, estatus_actual)
    except Exception as e:
        out = "⚠️ No hay configuración del bot para continuar. Revisa Config_XimenaAI."
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
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

    opciones_validas = [x.strip() for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    # normalizamos opciones válidas por si vienen " 1, 2 "
    opciones_validas = [normalize_option(x) for x in opciones_validas]

    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = (cfg.get("Mensaje_Error") or "Respuesta inválida.").strip()

    errores = ""

    # =========================
    # REGLA: disparo INICIO sin validar lo que escriba
    # (evita loops raros al primer mensaje)
    # =========================
    if created or paso_actual == "INICIO" or (estatus_actual or "").strip() == "INICIO":
        next_paso = sig1 or "INICIO"
        out = texto_bot or "Hola 👋"

        updates = {
            "Ultima_Actualizacion": now_iso(),
            "ESTATUS": next_paso,
        }
        if "Ultimo_Mensaje_Cliente" in leads_headers:
            updates["Ultimo_Mensaje_Cliente"] = msg_in

        try:
            update_lead_batch(ws_leads, leads_headers, lead_row, updates)
        except Exception as e:
            errores = f"BatchUpdateLead(INICIO) {e}"

        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone,
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
        # Validación con opción normalizada (robusto)
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + (msg_error or "Responde con una opción válida.")
            next_paso = paso_actual
        else:
            # Guardar valor en campo si existe
            if campo_update:
                if campo_update in leads_headers:
                    try:
                        update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})
                    except Exception as e:
                        errores += f"BatchUpdateCampo({campo_update}) {e}. "
                else:
                    errores += f"Campo no existe en BD_Leads: {campo_update}. "

            # Determinar siguiente
            if len(opciones_validas) >= 1 and msg_opt == opciones_validas[0]:
                next_paso = sig1 or paso_actual
            else:
                next_paso = sig2 or paso_actual

            # Responder con texto del siguiente paso
            try:
                cfg2 = load_config_row(ws_config, next_paso)
                out = cfg2.get("Texto_Bot") or "Continuemos."
            except Exception as e:
                errores += f"LoadCfg2({next_paso}) {e}. "
                out = "Continuemos."

    else:
        # TEXTO libre (default)
        if campo_update:
            if campo_update in leads_headers:
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

    # =========================
    # Batch update final (estatus + timestamp + último mensaje)
    # =========================
    final_updates = {
        "Ultima_Actualizacion": now_iso(),
        "ESTATUS": next_paso,
    }
    if "Ultimo_Mensaje_Cliente" in leads_headers:
        final_updates["Ultimo_Mensaje_Cliente"] = msg_in

    try:
        update_lead_batch(ws_leads, leads_headers, lead_row, final_updates)
    except Exception as e:
        errores += f" BatchUpdateLead(FINAL) {e}."

    # =========================
    # Log
    # =========================
    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso(),
        "Telefono": from_phone,
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
