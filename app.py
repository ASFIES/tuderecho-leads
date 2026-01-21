import os
import json
import base64
import uuid
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

# Opcionales (si quieres cambiar nombres de tabs sin tocar código)
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

# Credenciales: JSON directo o path
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

# =========================
# Helpers
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def norm_header(s: str) -> str:
    """Normaliza headers para mapear columnas aunque tengan espacios."""
    return (s or "").strip()


def build_header_map(ws):
    """Devuelve dict header->col_index (1-based)."""
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = norm_header(h)
        if key and key not in m:
            m[key] = i
    return m


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
            # si no empieza con "{", intentamos base64
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
    """
    Abre worksheet por título de forma segura.
    Si no existe, lanza error controlado con mensaje claro.
    """
    try:
        return sh.worksheet(title)
    except Exception:
        # gspread a veces lanza WorksheetNotFound o StopIteration interno
        raise RuntimeError(
            f"No existe la pestaña '{title}' en el Google Sheet '{GOOGLE_SHEET_NAME}'. "
            f"Verifica el nombre exacto del tab."
        )


def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)


def safe_log(ws_logs, data: dict):
    """
    Inserta un log en ws_logs si existen headers; si no, no truena.
    """
    try:
        headers = build_header_map(ws_logs)
        # Orden recomendado
        cols = [
            "ID_Log", "Fecha_Hora", "Telefono", "ID_Lead", "Paso",
            "Mensaje_Entrante", "Mensaje_Saliente",
            "Canal", "Fuente_Lead", "Modelo_AI", "Errores"
        ]
        row = []
        for c in cols:
            row.append(data.get(c, ""))

        # Si la sheet no tiene headers como esperamos, igual append
        ws_logs.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        # Nunca romper el flujo por logs
        pass


def find_row_by_value(ws, col_idx: int, value: str):
    """
    Busca value exacto en columna col_idx.
    Devuelve row_index o None.
    No depende de CellNotFound.
    """
    value = (value or "").strip()
    if not value:
        return None
    try:
        col_values = ws.col_values(col_idx)
        # col_values[0] es header
        for i, v in enumerate(col_values[1:], start=2):
            if (v or "").strip() == value:
                return i
        return None
    except Exception:
        return None


def get_or_create_lead(ws_leads, headers, telefono: str, fuente: str = "FACEBOOK"):
    """
    Encuentra lead por teléfono; si no existe, crea uno.
    Devuelve (row_index, lead_id, estatus_actual).
    """
    tel_col = headers.get("Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono' en el header (fila 1).")

    row = find_row_by_value(ws_leads, tel_col, telefono)

    if row:
        lead_id = ws_leads.cell(row, headers.get("ID_Lead")).value if headers.get("ID_Lead") else ""
        estatus = ws_leads.cell(row, headers.get("ESTATUS")).value if headers.get("ESTATUS") else ""
        return row, (lead_id or ""), (estatus or "").strip()

    # Crear lead nuevo
    lead_id = str(uuid.uuid4())
    new_row = [""] * max(1, len(ws_leads.row_values(1)))

    def set_if(col_name, val):
        idx = headers.get(col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", telefono)
    set_if("Fuente_Lead", fuente or "FACEBOOK")
    set_if("Fecha_Registro", now_iso())
    set_if("Ultima_Actualizacion", now_iso())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    # recuperar row recién creado
    row = find_row_by_value(ws_leads, tel_col, telefono)
    return row, lead_id, "INICIO"


def load_config_row(ws_config, paso_actual: str):
    """
    Carga la fila del paso en Config_XimenaAI.
    Si no existe, intenta INICIO.
    Devuelve dict con campos de config.
    """
    cfg_headers = build_header_map(ws_config)
    if "ID_Paso" not in cfg_headers:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso' en el header (fila 1).")

    paso_actual = (paso_actual or "").strip() or "INICIO"

    row = find_row_by_value(ws_config, cfg_headers["ID_Paso"], paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, cfg_headers["ID_Paso"], "INICIO")

    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}' (ni para 'INICIO').")

    def v(col_name):
        idx = cfg_headers.get(col_name)
        if not idx:
            return ""
        try:
            return (ws_config.cell(row, idx).value or "").strip()
        except Exception:
            return ""

    return {
        "row": row,
        "ID_Paso": v("ID_Paso"),
        "Texto_Bot": v("Texto_Bot"),
        "Tipo_Entrada": v("Tipo_Entrada"),
        "Opciones_Validas": v("Opciones_Validas"),
        "Siguiente_Si_1": v("Siguiente_Si_1"),
        "Siguiente_Si_2": v("Siguiente_Si_2"),
        "Campo_BD_Leads_A_Actualizar": v("Campo_BD_Leads_A_Actualizar"),
        "Regla_Validacion": v("Regla_Validacion"),
        "Mensaje_Error": v("Mensaje_Error"),
    }


def update_lead(ws_leads, leads_headers, lead_row: int, updates: dict):
    """
    updates: { "COLNAME": "value", ... }
    Solo actualiza columnas que existan; si no existen, las ignora.
    """
    for col, val in updates.items():
        idx = leads_headers.get(col)
        if not idx:
            continue
        try:
            ws_leads.update_cell(lead_row, idx, val)
        except Exception:
            pass


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200


@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone = (request.form.get("From") or "").strip()  # "whatsapp:+52..."
    msg_in = (request.form.get("Body") or "").strip()
    # Canal y fuente (por ahora fijo)
    canal = "WHATSAPP"
    fuente = "FACEBOOK"
    modelo_ai = ""

    # Respuesta por defecto (si algo falla)
    default_error_msg = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."

    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception as e:
        # Si ni siquiera abre Sheets: responde y listo
        return safe_reply(default_error_msg)

    # Headers
    leads_headers = build_header_map(ws_leads)

    try:
        lead_row, lead_id, estatus_actual = get_or_create_lead(ws_leads, leads_headers, from_phone, fuente)
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

    # Si no hay mensaje (Twilio a veces manda vacíos)
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

    # Load config
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
    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = (cfg.get("Mensaje_Error") or "Respuesta inválida.").strip()

    # Si estamos en INICIO: normalmente solo enviamos el texto inicial y pasamos al siguiente paso
    # (Si tu Config INICIO es OPCIONES, entonces se valida como OPCIONES)
    # Regla general:
    # - SISTEMA: solo responde Texto_Bot y NO guarda entrada.
    # - OPCIONES: valida 1/2, guarda si hay campo_update, mueve al siguiente.
    # - TEXTO (o vacío): guarda msg_in y pasa a Siguiente_Si_1
    errores = ""

    # =========================
    # Ejecutar lógica por tipo
    # =========================
    if tipo == "SISTEMA":
        out = texto_bot or "Listo."
        # No cambia paso si no hay sig1
        next_paso = sig1 or paso_actual

    elif tipo == "OPCIONES":
        # Validar respuesta
        if opciones_validas and msg_in not in opciones_validas:
            out = msg_error or "Por favor responde con una opción válida."
            next_paso = paso_actual  # se queda
        else:
            # Guardar valor en campo si existe
            if campo_update:
                if campo_update in leads_headers:
                    update_lead(ws_leads, leads_headers, lead_row, {campo_update: msg_in})
                else:
                    errores += f"Campo no existe en BD_Leads: {campo_update}. "
            # Determinar siguiente
            if len(opciones_validas) >= 1 and msg_in == opciones_validas[0]:
                next_paso = sig1 or paso_actual
            else:
                next_paso = sig2 or paso_actual

            # Responder con el texto del siguiente paso (para que el chat avance en el mismo mensaje)
            try:
                cfg2 = load_config_row(ws_config, next_paso)
                out = cfg2.get("Texto_Bot") or "Continuemos."
                # set paso_actual al siguiente para logs
            except Exception:
                out = "Continuemos."
            # actualizar estatus
        # Si el texto_bot del paso actual es el que quieres enviar antes de validar, cámbialo.
        # En tu caso: tú ya envías INICIO y luego respondes 1/2 para avanzar.
        # Aquí, cuando validas 1, te manda el texto del siguiente paso.

    else:
        # TEXTO libre (default)
        if campo_update:
            if campo_update in leads_headers:
                update_lead(ws_leads, leads_headers, lead_row, {campo_update: msg_in})
            else:
                errores += f"Campo no existe en BD_Leads: {campo_update}. "
        next_paso = sig1 or paso_actual
        try:
            cfg2 = load_config_row(ws_config, next_paso)
            out = cfg2.get("Texto_Bot") or "Gracias. Continuemos."
        except Exception:
            out = "Gracias. Continuemos."

    # =========================
    # Actualizar BD_Leads (estatus + timestamps)
    # =========================
    updates = {
        "Ultima_Actualizacion": now_iso(),
        "ESTATUS": next_paso,
    }
    # Si tu BD_Leads también tiene esta columna, la actualizamos (opcional)
    if "Ultimo_Mensaje_Cliente" in leads_headers:
        updates["Ultimo_Mensaje_Cliente"] = msg_in

    update_lead(ws_leads, leads_headers, lead_row, updates)

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
