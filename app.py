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

from redis import Redis
from rq import Queue

from utils.sheets import (
    get_gspread_client,
    open_spreadsheet,
    open_worksheet,
    build_header_map,
    col_idx,
    find_row_by_value,
    update_lead_batch,
    safe_log,
)
from utils.flow import load_config_row, pick_next_step_from_option
from utils.text import (
    render_text,
    phone_raw,
    phone_norm,
    normalize_msg,
    normalize_option,
    detect_fuente,
    is_valid_by_rule,
    build_date_from_parts,
)

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
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()  # (lo usa worker)
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()  # (lo usa worker)
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()  # (lo usa worker)
TAB_CONOC = os.environ.get("TAB_CONOC", "Conocimiento_AI").strip()  # (lo usa worker)

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

# Redis / RQ
REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

MX_TZ = ZoneInfo("America/Mexico_City")

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def get_queue():
    if not REDIS_URL:
        raise RuntimeError("Falta REDIS_URL en variables de entorno.")
    r = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=r, default_timeout=180)

# =========================
# Google creds
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

# =========================
# Lead: get/create
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
    modelo_ai = OPENAI_MODEL  # solo para log

    if not msg_in:
        return safe_reply("Hola 👋")

    fuente = detect_fuente(msg_in)

    try:
        gc = get_gspread_client(get_env_creds_dict())
        sh = open_spreadsheet(gc, GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except Exception:
        return safe_reply("⚠️ Por el momento no puedo acceder a la base de datos. Intenta de nuevo en unos minutos.")

    leads_headers = build_header_map(ws_leads)

    lead_row, lead_id, estatus_actual, created = get_or_create_lead(
        ws_leads, leads_headers, from_phone_raw, from_phone_normed, fuente
    )

    row_vals = ws_leads.row_values(lead_row)
    headers_list = ws_leads.row_values(1)
    lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

    errores = ""

    # Si es nuevo, saluda con INICIO y ya
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

    # Fail-safe: saltar CORREO si existiera
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
            # Nunca guardar correo
            if campo_update and campo_update.lower() != "correo":
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})

            next_paso = pick_next_step_from_option(cfg, msg_opt, paso_actual)
            if next_paso.upper() == "CORREO":
                next_paso = "DESCRIPCION"

            # Si el siguiente paso es GENERAR_RESULTADOS, encolamos y respondemos rápido
            if next_paso.upper() == "GENERAR_RESULTADOS":
                try:
                    # Marcamos que estamos esperando resultados
                    update_lead_batch(ws_leads, leads_headers, lead_row, {
                        "ESTATUS": "WAIT_RESULTADOS",
                        "Ultima_Actualizacion": now_iso_mx(),
                    })
                    q = get_queue()
                    q.enqueue(
                        "worker_jobs.process_resultados",
                        {
                            "telefono_raw": from_phone_raw,
                            "telefono_norm": from_phone_normed,
                            "lead_id": lead_id,
                        }
                    )
                    out = (
                        "Gracias, ya tengo lo necesario ✅\n\n"
                        "Estoy preparando tu *estimación preliminar* y asignando a la abogada que llevará tu caso.\n"
                        "En un momento te envío el resultado por este mismo medio."
                    )
                    next_paso = "WAIT_RESULTADOS"
                except Exception as e:
                    errores += f"Enqueue_Err: {e}. "
                    out = "⚠️ Por el momento no pude generar tus resultados. Intenta de nuevo en unos minutos."
                    next_paso = paso_actual
            else:
                cfg2 = load_config_row(ws_config, next_paso)
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

            # refrescar snapshot para fechas
            row_vals = ws_leads.row_values(lead_row)
            lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

            # INI_DIA: construir fecha inicio y avanzar
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
                    next_paso = "FIN_ANIO"

            # FIN_DIA: construir fecha fin y avanzar
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
                    next_paso = "SALARIO"
            else:
                next_paso = (cfg.get("Siguiente_Si_1") or paso_actual).strip()
                if next_paso.upper() == "CORREO":
                    next_paso = "DESCRIPCION"

            if next_paso != paso_actual:
                cfg2 = load_config_row(ws_config, next_paso)
                out = render_text(cfg2.get("Texto_Bot") or "Gracias.")

    # ======================
    # SISTEMA (en main no ejecutamos procesos pesados)
    # ======================
    else:
        # Mantener robustez: si cae aquí, devolvemos texto del paso actual
        out = texto_bot or "Gracias."

    # update base lead
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
