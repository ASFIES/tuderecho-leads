# app.py
import os
import re
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff, get_records_cached

TZ = os.environ.get("TZ", "America/Mexico_City").strip()
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CFG = os.environ.get("TAB_CFG", "Config_XimenaAI").strip()

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def _twiml(text: str):
    resp = MessagingResponse()
    resp.message(text or "")
    return str(resp)

def _normalize_phone(raw: str) -> str:
    raw = (raw or "").strip()
    raw = raw.replace("whatsapp:", "").strip()
    return re.sub(r"\D+", "", raw)

def _normalize_option(msg: str) -> str:
    s = (msg or "").strip()
    m = re.search(r"\d", s)
    return m.group(0) if m else s

def _is_valid_by_rule(value: str, rule: str) -> bool:
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
            float(value.replace("$", "").replace(",", "").strip())
            return True
        except Exception:
            return False
    return True

def _append_row_by_header(ws, data: dict):
    header = with_backoff(ws.row_values, 1)
    row = [""] * len(header)
    for i, h in enumerate(header):
        key = (h or "").strip()
        if key in data:
            row[i] = str(data.get(key, ""))
    with_backoff(ws.append_row, row, value_input_option="USER_ENTERED")

def _update_by_header(ws, row_num_1based: int, updates: dict):
    header = with_backoff(ws.row_values, 1)
    cell_list = []
    for k, v in updates.items():
        if k in header:
            c = header.index(k) + 1  # OJO: si hay duplicados, toma la primera ocurrencia (por eso conviene arreglar headers)
            cell = ws.cell(row_num_1based, c)
            cell.value = str(v)
            cell_list.append(cell)
    if cell_list:
        with_backoff(ws.update_cells, cell_list)

def _load_cfg(ws_cfg):
    rows = get_records_cached(ws_cfg, cache_seconds=10)
    out = {}
    for r in rows:
        pid = str(r.get("ID_Paso", "")).strip()
        if pid:
            out[pid] = r
    return out

def _get_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def _find_lead_by_phone(records, phone_norm: str):
    for i, r in enumerate(records):
        if str(r.get("Telefono_Normalizado", "")).strip() == phone_norm:
            return i, r
    return None, None

def _ensure_lead(ws_leads, from_phone: str, msg_in: str):
    phone_norm = _normalize_phone(from_phone)
    records = get_records_cached(ws_leads, cache_seconds=3)
    idx, lead = _find_lead_by_phone(records, phone_norm)
    if lead:
        return idx, lead

    lead_id = str(uuid.uuid4())[:8] + "-" + (phone_norm[-6:] if len(phone_norm) >= 6 else phone_norm)
    fuente = "DESCONOCIDA"
    t = (msg_in or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        fuente = "FACEBOOK"
    elif "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        fuente = "WEB"

    _append_row_by_header(ws_leads, {
        "ID_Lead": lead_id,
        "Telefono": from_phone,
        "Telefono_Normalizado": phone_norm,
        "Fuente_Lead": fuente,
        "Fecha_Registro": _now_iso(),
        "Ultima_Actualizacion": _now_iso(),
        "ESTATUS": "INICIO",
        "Paso_Anterior": "",
        "Ultimo_Mensaje_Cliente": (msg_in or "").strip(),
        "Ultimo_Error": "",
    })

    # re-lee
    records = get_records_cached(ws_leads, cache_seconds=0)
    idx, lead = _find_lead_by_phone(records, phone_norm)
    return idx, lead

def _log(ws_logs, payload: dict):
    # Si falla el log, NO tumbes el bot
    try:
        _append_row_by_header(ws_logs, payload)
    except Exception:
        pass

@app.post("/whatsapp")
def whatsapp_webhook():
    msg_in = (request.form.get("Body") or "").strip()
    from_phone = request.form.get("From") or ""

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs = open_worksheet(sh, TAB_LOGS)
        ws_cfg = open_worksheet(sh, TAB_CFG)

        cfg = _load_cfg(ws_cfg)

        idx, lead = _ensure_lead(ws_leads, from_phone, msg_in)
        if not lead:
            return _twiml("Perdón, tuve un problema técnico 🙏 Intenta de nuevo en un momento.")

        lead_id = str(lead.get("ID_Lead", "")).strip()
        estatus = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        row_num = idx + 2  # 1 = header

        # Caso “Hola” inicial: si está en INICIO y no mandó 1/2, solo mostramos INICIO
        if estatus == "INICIO":
            opt_try = _normalize_option(msg_in)
            if opt_try not in ("1", "2"):
                texto = (cfg.get("INICIO", {}).get("Texto_Bot") or
                         "Hola, soy Ximena 👋\n\n¿Deseas continuar?\n1 Sí\n2 No")
                _update_by_header(ws_leads, row_num, {
                    "Ultima_Actualizacion": _now_iso(),
                    "Ultimo_Mensaje_Cliente": msg_in,
                    "Ultimo_Error": "",
                })
                _log(ws_logs, {
                    "ID_Log": str(uuid.uuid4())[:8],
                    "Fecha_Hora": _now_iso(),
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "INICIO",
                    "Mensaje_Entrante": msg_in,
                    "Mensaje_Saliente": texto,
                    "Canal": "WHATSAPP",
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                    "Modelo_AI": "",
                    "Errores": "",
                })
                return _twiml(texto)

        step = cfg.get(estatus) or cfg.get("INICIO") or {}
        tipo = str(step.get("Tipo_Entrada", "")).strip().upper()
        opciones = str(step.get("Opciones_Validas", "")).strip()
        campo = str(step.get("Campo_BD_Leads_A_Actualizar", "")).strip()
        regla = str(step.get("Regla_Validacion", "")).strip()
        err_msg = str(step.get("Mensaje_Error", "Por favor responde con una opción válida.")).strip()

        # =========================
        # OPCIONES
        # =========================
        if tipo == "OPCIONES":
            opt = _normalize_option(msg_in)
            valid = [x.strip() for x in opciones.split(",") if x.strip()]
            if opt not in valid:
                _log(ws_logs, {
                    "ID_Log": str(uuid.uuid4())[:8],
                    "Fecha_Hora": _now_iso(),
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": estatus,
                    "Mensaje_Entrante": msg_in,
                    "Mensaje_Saliente": err_msg,
                    "Canal": "WHATSAPP",
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                    "Modelo_AI": "",
                    "Errores": "OPCION_INVALIDA",
                })
                return _twiml(err_msg)

            # Siguiente_Si_{n}
            next_key = f"Siguiente_Si_{opt}"
            next_step = str(step.get(next_key) or "").strip()
            if not next_step:
                next_step = str(step.get("Siguiente_Si_1") if opt == "1" else step.get("Siguiente_Si_2") or "").strip()
            next_step = next_step or "INICIO"

            updates = {
                "Paso_Anterior": estatus,
                "ESTATUS": next_step,
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Mensaje_Cliente": msg_in,
                "Ultimo_Error": "",
            }
            if campo:
                updates[campo] = opt

            _update_by_header(ws_leads, row_num, updates)

            out = str((cfg.get(next_step) or {}).get("Texto_Bot") or "Continuemos…").strip()

            # Si llegamos a GENERAR_RESULTADOS, encolamos job y ponemos EN_PROCESO
            if next_step == "GENERAR_RESULTADOS":
                _update_by_header(ws_leads, row_num, {"ESTATUS": "EN_PROCESO"})
                q = _get_queue()
                if q is not None:
                    from worker_jobs import process_lead
                    q.enqueue(process_lead, lead_id, job_timeout=180)

            _log(ws_logs, {
                "ID_Log": str(uuid.uuid4())[:8],
                "Fecha_Hora": _now_iso(),
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": next_step,
                "Mensaje_Entrante": msg_in,
                "Mensaje_Saliente": out,
                "Canal": "WHATSAPP",
                "Fuente_Lead": lead.get("Fuente_Lead", ""),
                "Modelo_AI": "",
                "Errores": "",
            })
            return _twiml(out)

        # =========================
        # TEXTO
        # =========================
        if tipo == "TEXTO":
            val = (msg_in or "").strip()
            if not _is_valid_by_rule(val, regla):
                _log(ws_logs, {
                    "ID_Log": str(uuid.uuid4())[:8],
                    "Fecha_Hora": _now_iso(),
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": estatus,
                    "Mensaje_Entrante": msg_in,
                    "Mensaje_Saliente": err_msg,
                    "Canal": "WHATSAPP",
                    "Fuente_Lead": lead.get("Fuente_Lead", ""),
                    "Modelo_AI": "",
                    "Errores": "VALIDACION_FALLA",
                })
                return _twiml(err_msg)

            next_step = str(step.get("Siguiente_Si_1") or "").strip() or estatus

            updates = {
                "Paso_Anterior": estatus,
                "ESTATUS": next_step,
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Mensaje_Cliente": msg_in,
                "Ultimo_Error": "",
            }
            if campo:
                updates[campo] = val

            _update_by_header(ws_leads, row_num, updates)
            out = str((cfg.get(next_step) or {}).get("Texto_Bot") or "Gracias. Continuemos…").strip()

            if next_step == "GENERAR_RESULTADOS":
                _update_by_header(ws_leads, row_num, {"ESTATUS": "EN_PROCESO"})
                q = _get_queue()
                if q is not None:
                    from worker_jobs import process_lead
                    q.enqueue(process_lead, lead_id, job_timeout=180)

            _log(ws_logs, {
                "ID_Log": str(uuid.uuid4())[:8],
                "Fecha_Hora": _now_iso(),
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": next_step,
                "Mensaje_Entrante": msg_in,
                "Mensaje_Saliente": out,
                "Canal": "WHATSAPP",
                "Fuente_Lead": lead.get("Fuente_Lead", ""),
                "Modelo_AI": "",
                "Errores": "",
            })
            return _twiml(out)

        # =========================
        # SISTEMA (fallback)
        # =========================
        out = str(step.get("Texto_Bot") or "Perfecto. Continuemos…").strip()
        return _twiml(out)

    except Exception as e:
        # Si falla, guarda el error en Ultimo_Error si puedes (sin tirar todo)
        try:
            sh = open_spreadsheet(GOOGLE_SHEET_NAME)
            ws_leads = open_worksheet(sh, TAB_LEADS)
            phone_norm = _normalize_phone(from_phone)
            records = get_records_cached(ws_leads, cache_seconds=0)
            idx, lead = _find_lead_by_phone(records, phone_norm)
            if lead is not None:
                _update_by_header(ws_leads, idx + 2, {"Ultimo_Error": str(e)[:250], "Ultima_Actualizacion": _now_iso()})
        except Exception:
            pass

        return _twiml("Perdón, tuve un problema técnico 🙏 Intenta de nuevo en un momento.")
