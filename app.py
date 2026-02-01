import os
import re
import uuid
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import (
    open_spreadsheet,
    open_worksheet,
    with_backoff,
    get_all_values_safe,
    header_map,
    row_to_dict,
    find_row_by_col_value,
    update_row_cells,
)
from utils.text import (
    render_text,
    normalize_msg,
    normalize_option,
    detect_fuente,
    is_valid_by_rule,
    template_fill,
)

TZ = os.environ.get("TZ", "America/Mexico_City").strip()

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS  = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()

# Redis / RQ
REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

MX_TZ = ZoneInfo(TZ)
app = Flask(__name__)

# Cache corto para Config (evita pegarle a Sheets cada msg)
_CFG_CACHE = {"ts": 0.0, "data": {}}
_CFG_TTL = int(os.environ.get("CFG_CACHE_SECONDS", "20"))


def now_iso() -> str:
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")


def twiml(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)


def redis_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)


def load_config_table(ws_config) -> dict:
    """Carga Config_XimenaAI -> dict por ID_Paso, con cache TTL."""
    now = time.time()
    if _CFG_CACHE["data"] and (now - _CFG_CACHE["ts"] <= _CFG_TTL):
        return _CFG_CACHE["data"]

    values = get_all_values_safe(ws_config)
    if not values or len(values) < 2:
        return {}

    out = {}
    for row in values[1:]:
        d = row_to_dict(values[0], row)
        step_id = (d.get("ID_Paso") or "").strip()
        if step_id:
            out[step_id] = d

    _CFG_CACHE["data"] = out
    _CFG_CACHE["ts"] = now
    return out


def pick_next_step(cfg: dict, opt: str, default_step: str) -> str:
    """Soporta Siguiente_Si_1..Siguiente_Si_9."""
    opt = (opt or "").strip()
    if opt:
        k = f"Siguiente_Si_{opt}"
        v = (cfg.get(k) or "").strip()
        if v:
            return v
    if opt == "1" and (cfg.get("Siguiente_Si_1") or "").strip():
        return (cfg.get("Siguiente_Si_1") or "").strip()
    if opt == "2" and (cfg.get("Siguiente_Si_2") or "").strip():
        return (cfg.get("Siguiente_Si_2") or "").strip()
    return default_step


def ensure_lead(ws_leads, from_phone: str, first_msg: str) -> tuple[int, dict]:
    """Busca por Telefono_Normalizado; si no existe, crea fila nueva. Regresa (idx_values, lead_dict)."""
    values = get_all_values_safe(ws_leads)
    if not values or not values[0]:
        raise RuntimeError("BD_Leads no tiene encabezados.")

    hdr = values[0]
    hmap = header_map(hdr)

    phone_raw = (from_phone or "").strip()
    phone_norm = phone_raw.replace("whatsapp:", "").strip()
    phone_norm = re.sub(r"\D+", "", phone_norm)

    idx = find_row_by_col_value(values, "Telefono_Normalizado", phone_norm)
    if idx is not None:
        lead = row_to_dict(hdr, values[idx])
        return idx, lead

    lead_id = f"{str(uuid.uuid4())[:8]}-{phone_norm[-6:] if len(phone_norm) >= 6 else phone_norm}"
    fuente = detect_fuente(first_msg)

    new_row = [""] * len(hdr)

    def setv(col, val):
        if col in hmap:
            new_row[hmap[col] - 1] = str(val)

    setv("ID_Lead", lead_id)
    setv("Telefono", phone_raw)
    setv("Telefono_Normalizado", phone_norm)
    setv("Fuente_Lead", fuente or "DESCONOCIDA")
    setv("Fecha_Registro", now_iso())
    setv("Ultima_Actualizacion", now_iso())
    setv("ESTATUS", "INICIO")
    setv("Paso_Anterior", "")
    setv("Ultimo_Mensaje_Cliente", normalize_msg(first_msg))

    with_backoff(ws_leads.append_row, new_row, value_input_option="USER_ENTERED")

    values2 = get_all_values_safe(ws_leads)
    idx2 = find_row_by_col_value(values2, "Telefono_Normalizado", phone_norm)
    if idx2 is None:
        raise RuntimeError("No se pudo crear el lead en BD_Leads.")
    lead2 = row_to_dict(values2[0], values2[idx2])
    return idx2, lead2


def log_row(ws_logs, data: dict):
    values = get_all_values_safe(ws_logs)
    if not values or not values[0]:
        return
    hdr = values[0]
    hmap = header_map(hdr)

    row = [""] * len(hdr)

    def setv(col, val):
        if col in hmap:
            row[hmap[col] - 1] = str(val)

    setv("ID_Log", str(uuid.uuid4())[:8])
    setv("Fecha_Hora", now_iso())
    setv("Telefono", data.get("Telefono", ""))
    setv("ID_Lead", data.get("ID_Lead", ""))
    setv("Paso", data.get("Paso", ""))
    setv("Mensaje_Entrante", data.get("Mensaje_Entrante", ""))
    setv("Mensaje_Saliente", data.get("Mensaje_Saliente", ""))
    setv("Canal", "WHATSAPP")
    setv("Fuente_Lead", data.get("Fuente_Lead", ""))
    setv("Modelo_AI", data.get("Modelo_AI", ""))
    setv("Errores", data.get("Errores", ""))

    with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")


@app.get("/")
def health():
    return {"ok": True, "service": "tuderecho-leads", "ts": now_iso()}


@app.post("/whatsapp")
def whatsapp_webhook():
    msg_in_raw = (request.form.get("Body") or "").strip()
    from_phone = (request.form.get("From") or "").strip()

    msg_in = normalize_msg(msg_in_raw)
    msg_opt = normalize_option(msg_in)

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_cfg   = open_worksheet(sh, TAB_CONFIG)

        cfg_table = load_config_table(ws_cfg)

        idx, lead = ensure_lead(ws_leads, from_phone, msg_in)
        lead_id = (lead.get("ID_Lead") or "").strip()

        paso = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        cfg = cfg_table.get(paso, {})

        # Si está en proceso, responde humano (evita silencio)
        if paso in ("EN_PROCESO", "GENERAR_RESULTADOS"):
            out = "Ya estoy preparando tu estimación preliminar y asignando a la abogada que llevará tu caso. 🙏\n\nEn breve te compartiré el resultado por este medio."
            out = template_fill(out, lead)
            log_row(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": paso,
                "Mensaje_Entrante": msg_in_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": lead.get("Fuente_Lead", ""),
            })
            return twiml(out)

        tipo = (cfg.get("Tipo_Entrada") or "").strip().upper()
        opciones_validas = (cfg.get("Opciones_Validas") or "").strip()
        regla = (cfg.get("Regla_Validacion") or "").strip()
        campo = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
        msg_error = (cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.").strip()

        # ===== OPCIONES =====
        if tipo == "OPCIONES":
            valid = [x.strip() for x in opciones_validas.split(",") if x.strip()]

            # INICIO: si escribe "hola" (no opción), solo muéstrale INICIO sin error
            if paso == "INICIO" and msg_opt not in valid:
                inicio_cfg = cfg_table.get("INICIO", cfg)
                inicio_txt = render_text((inicio_cfg.get("Texto_Bot") or "Hola.\n1) Sí\n2) No"))
                inicio_txt = template_fill(inicio_txt, lead)

                update_row_cells(ws_leads, idx, {
                    "Ultima_Actualizacion": now_iso(),
                    "Ultimo_Mensaje_Cliente": msg_in,
                })
                log_row(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "INICIO",
                    "Mensaje_Entrante": msg_in_raw,
                    "Mensaje_Saliente": inicio_txt,
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                })
                return twiml(inicio_txt)

            if msg_opt not in valid:
                log_row(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": paso,
                    "Mensaje_Entrante": msg_in_raw,
                    "Mensaje_Saliente": msg_error,
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                    "Errores": "OPCION_INVALIDA",
                })
                return twiml(msg_error)

            next_step = pick_next_step(cfg, msg_opt, paso)

            updates = {
                "Paso_Anterior": paso,
                "ESTATUS": next_step,
                "Ultima_Actualizacion": now_iso(),
                "Ultimo_Mensaje_Cliente": msg_in,
            }

            if (lead.get("Fuente_Lead") or "").strip().upper() in ("", "DESCONOCIDA"):
                updates["Fuente_Lead"] = detect_fuente(msg_in)

            if campo:
                updates[campo] = msg_opt

            update_row_cells(ws_leads, idx, updates)

            # EN_PROCESO => encolar worker + mensaje de “procesando”
            if next_step in ("EN_PROCESO", "GENERAR_RESULTADOS"):
                proc_txt = render_text(
                    (cfg_table.get("EN_PROCESO", {}).get("Texto_Bot") or
                     cfg_table.get("GENERAR_RESULTADOS", {}).get("Texto_Bot") or
                     "Gracias. Estoy generando tu estimación preliminar y asignando a la abogada que llevará tu caso…")
                )

                lead2_values = get_all_values_safe(ws_leads)
                lead2 = row_to_dict(lead2_values[0], lead2_values[idx])
                proc_txt = template_fill(proc_txt, lead2)

                q = redis_queue()
                if q is not None:
                    from worker_jobs import process_lead
                    q.enqueue(process_lead, lead_id, job_timeout=180)

                log_row(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": next_step,
                    "Mensaje_Entrante": msg_in_raw,
                    "Mensaje_Saliente": proc_txt,
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                })
                return twiml(proc_txt)

            # Respuesta normal
            lead2_values = get_all_values_safe(ws_leads)
            lead2 = row_to_dict(lead2_values[0], lead2_values[idx])

            out = render_text((cfg_table.get(next_step, {}).get("Texto_Bot") or "Continuemos…"))
            out = template_fill(out, lead2)

            # Fallback menú si CLIENTE_MENU quedó con texto de “procesando”
            if next_step == "CLIENTE_MENU":
                if ("preparando" in out.lower() or "estimación" in out.lower()) and (lead2.get("Resultado_Calculo") or "").strip():
                    out = "Listo ✅ Ya tengo tu estimación preliminar.\n\n¿Qué deseas hacer?\n1) Ver resumen\n2) Agendar llamada\n3) Hablar con un abogado\n\nResponde con 1, 2 o 3."
                    out = template_fill(out, lead2)

            log_row(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": next_step,
                "Mensaje_Entrante": msg_in_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": lead.get("Fuente_Lead", ""),
            })
            return twiml(out)

        # ===== TEXTO =====
        if tipo in ("TEXTO", "", None):
            if not is_valid_by_rule(msg_in, regla):
                log_row(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": paso,
                    "Mensaje_Entrante": msg_in_raw,
                    "Mensaje_Saliente": msg_error,
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                    "Errores": "VALIDACION_FALLA",
                })
                return twiml(msg_error)

            next_step = pick_next_step(cfg, "1", paso)

            updates = {
                "Paso_Anterior": paso,
                "ESTATUS": next_step,
                "Ultima_Actualizacion": now_iso(),
                "Ultimo_Mensaje_Cliente": msg_in,
            }
            if campo:
                updates[campo] = msg_in

            if (lead.get("Fuente_Lead") or "").strip().upper() in ("", "DESCONOCIDA"):
                updates["Fuente_Lead"] = detect_fuente(msg_in)

            update_row_cells(ws_leads, idx, updates)

            lead2_values = get_all_values_safe(ws_leads)
            lead2 = row_to_dict(lead2_values[0], lead2_values[idx])

            out = render_text((cfg_table.get(next_step, {}).get("Texto_Bot") or "Gracias. Continuemos…"))
            out = template_fill(out, lead2)

            log_row(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": next_step,
                "Mensaje_Entrante": msg_in_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": lead.get("Fuente_Lead", ""),
            })
            return twiml(out)

        # ===== SISTEMA =====
        out = render_text((cfg.get("Texto_Bot") or "Entendido."))
        out = template_fill(out, lead)
        log_row(ws_logs, {
            "Telefono": from_phone,
            "ID_Lead": lead_id,
            "Paso": paso,
            "Mensaje_Entrante": msg_in_raw,
            "Mensaje_Saliente": out,
            "Fuente_Lead": lead.get("Fuente_Lead", ""),
        })
        return twiml(out)

    except Exception:
        return twiml("Perdón, tuve un problema técnico 🙏\nIntenta de nuevo en un momento.")
