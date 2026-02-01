import os
import re
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import (
    open_spreadsheet, open_worksheet, with_backoff,
    build_header_map, col_idx, find_row_by_value, update_row_cells
)

MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City"))

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS   = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()  # 👈 ÚNICA config
TAB_SYS    = os.environ.get("TAB_SYS", "Config_Sistema").strip()      # 👈 Clave/Valor (opcional)

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _twiml(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def normalize_option(msg: str) -> str:
    s = (msg or "").strip()
    # tomar el primer dígito que aparezca (soporta "1️⃣", " 1 ", "opción 1", etc.)
    m = re.search(r"\d", s)
    return m.group(0) if m else s

def render_text(s: str) -> str:
    s = (s or "").strip()
    # si vienen comillas envolventes
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1]
    # si alguien dejó \n literal
    s = s.replace("\\n", "\n").replace("\\r\\n", "\n")
    return s

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "página" in t or "pagina" in t:
        return "WEB"
    return "DESCONOCIDA"

def load_config(ws_config):
    rows = with_backoff(ws_config.get_all_records)
    cfg = {}
    for r in rows:
        pid = (r.get("ID_Paso") or "").strip()
        if pid:
            cfg[pid] = r
    return cfg

def get_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def log(ws_logs, lead_id, paso, msg_in, msg_out, telefono="", err=""):
    try:
        row = [
            uuid.uuid4().hex[:8],
            now_iso(),
            telefono,
            lead_id,
            paso,
            msg_in,
            msg_out,
            "WHATSAPP",
            "",
            "",
            err
        ]
        with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")
    except Exception:
        pass

def ensure_lead(ws_leads, from_phone: str):
    phone_norm = re.sub(r"\D+", "", (from_phone or "").replace("whatsapp:", ""))
    h = build_header_map(ws_leads)

    # buscar por telefono_normalizado
    row = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm)
    if row:
        # leer fila completa en 1 llamada
        vals = with_backoff(ws_leads.row_values, row)
        def get(name):
            c = col_idx(h, name)
            return (vals[c-1] if c and c-1 < len(vals) else "").strip()
        lead_id = get("ID_Lead") or ""
        if not lead_id:
            lead_id = uuid.uuid4().hex[:12]
            update_row_cells(ws_leads, row, {"ID_Lead": lead_id})
        return row, lead_id, phone_norm

    # crear nuevo
    lead_id = uuid.uuid4().hex[:12]
    new_row = [""] * len(h)
    def setv(name, val):
        c = col_idx(h, name)
        if c:
            new_row[c-1] = str(val)

    setv("ID_Lead", lead_id)
    setv("Telefono", from_phone)
    setv("Telefono_Normalizado", phone_norm)
    setv("Fuente_Lead", "DESCONOCIDA")
    setv("Fecha_Registro", now_iso())
    setv("Ultima_Actualizacion", now_iso())
    setv("ESTATUS", "INICIO")

    with_backoff(ws_leads.append_row, new_row, value_input_option="USER_ENTERED")
    # Re-buscar
    row2 = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm)
    return row2, lead_id, phone_norm

def step_type(cfg_row) -> str:
    return (cfg_row.get("Tipo_Entrada") or "").strip().upper()

def next_for_option(cfg_row, opt: str) -> str:
    if opt == "1":
        return (cfg_row.get("Siguiente_Si_1") or "").strip()
    if opt == "2":
        return (cfg_row.get("Siguiente_Si_2") or "").strip()
    # si algún día agregas 3,4,5... puedes extender aquí
    return ""

def get_text(cfg_row) -> str:
    return render_text(cfg_row.get("Texto_Bot") or "")

def auto_run_system(ws_leads, ws_logs, cfg, lead_row, lead_id, telefono, current_step: str):
    """
    Ejecuta 1-2 pasos SISTEMA sin esperar mensaje del usuario.
    - Si cae en GENERAR_RESULTADOS o EN_PROCESO: encola y deja estatus EN_PROCESO.
    """
    safety = 0
    step = current_step

    while safety < 3:
        safety += 1
        row_cfg = cfg.get(step)
        if not row_cfg:
            break

        t = step_type(row_cfg)
        if t != "SISTEMA":
            break

        out = get_text(row_cfg) or "..."
        # caso especial: disparar worker
        if step in ("GENERAR_RESULTADOS", "EN_PROCESO"):
            update_row_cells(ws_leads, lead_row, {
                "Paso_Anterior": step,
                "ESTATUS": "EN_PROCESO",
                "Ultima_Actualizacion": now_iso(),
                "Procesar_AI_Status": "ENQUEUED",
                "Ultimo_Error": ""
            })
            q = get_queue()
            if q is not None:
                from worker_jobs import process_lead
                q.enqueue(process_lead, lead_id, job_timeout=180)
            log(ws_logs, lead_id, step, "", out, telefono=telefono, err="")
            return out, "EN_PROCESO"

        # avanzar al siguiente paso (usa Siguiente_Si_1 como "siguiente" para SISTEMA)
        nxt = (row_cfg.get("Siguiente_Si_1") or "").strip()
        if not nxt or nxt == step:
            # si no hay siguiente, nos quedamos
            log(ws_logs, lead_id, step, "", out, telefono=telefono, err="")
            return out, step

        # actualizar estatus al siguiente
        update_row_cells(ws_leads, lead_row, {
            "Paso_Anterior": step,
            "ESTATUS": nxt,
            "Ultima_Actualizacion": now_iso()
        })
        log(ws_logs, lead_id, step, "", out, telefono=telefono, err="")
        step = nxt

    # Si sale del loop, responde con el texto del paso actual (si existe)
    row_cfg = cfg.get(step) or {}
    out = get_text(row_cfg) or "..."
    return out, step

@app.post("/whatsapp")
def whatsapp_webhook():
    msg_in_raw = (request.form.get("Body") or "").strip()
    from_phone = (request.form.get("From") or "").strip()

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_cfg   = open_worksheet(sh, TAB_CONFIG)

        cfg = load_config(ws_cfg)

        lead_row, lead_id, phone_norm = ensure_lead(ws_leads, from_phone)

        # leer estatus y nombre en 1 llamada
        h = build_header_map(ws_leads)
        vals = with_backoff(ws_leads.row_values, lead_row)
        def get(name):
            c = col_idx(h, name)
            return (vals[c-1] if c and c-1 < len(vals) else "").strip()

        estatus = get("ESTATUS") or "INICIO"
        nombre  = get("Nombre") or ""

        # guardar último mensaje y detectar fuente si es desconocida
        fuente_actual = get("Fuente_Lead") or "DESCONOCIDA"
        if fuente_actual == "DESCONOCIDA":
            fuente_actual = detect_fuente(msg_in_raw)

        update_row_cells(ws_leads, lead_row, {
            "Ultimo_Mensaje_Cliente": msg_in_raw,
            "Fuente_Lead": fuente_actual,
            "Ultima_Actualizacion": now_iso()
        })

        # Si el paso actual es SISTEMA, lo ejecutamos sin esperar
        if estatus in cfg and step_type(cfg[estatus]) == "SISTEMA":
            out, _ = auto_run_system(ws_leads, ws_logs, cfg, lead_row, lead_id, from_phone, estatus)
            return _twiml(out)

        # Si estamos en INICIO y el usuario no mandó 1/2, solo mostramos INICIO sin validar
        msg_opt = normalize_option(msg_in_raw)
        if estatus == "INICIO" and msg_opt not in ("1", "2"):
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n1 Sí\n2 No"
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        row_cfg = cfg.get(estatus)
        if not row_cfg:
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n1 Sí\n2 No"
            update_row_cells(ws_leads, lead_row, {"ESTATUS": "INICIO"})
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="missing_step")
            return _twiml(out)

        t = step_type(row_cfg)
        msg_err = render_text(row_cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.")

        # OPCIONES
        if t == "OPCIONES":
            valid = [x.strip() for x in (row_cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
            if msg_opt not in valid:
                log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_option")
                return _twiml(msg_err)

            nxt = next_for_option(row_cfg, msg_opt) or "INICIO"

            campo = (row_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
            upd = {"Paso_Anterior": estatus, "ESTATUS": nxt, "Ultima_Actualizacion": now_iso()}
            if campo:
                upd[campo] = msg_opt
            if estatus == "INICIO":
                # guardamos aceptación de "comenzar" como opcional si quieres (no obligatorio)
                pass

            update_row_cells(ws_leads, lead_row, upd)

            # Si el siguiente es SISTEMA, ejecútalo en caliente
            if nxt in cfg and step_type(cfg[nxt]) == "SISTEMA":
                out, _ = auto_run_system(ws_leads, ws_logs, cfg, lead_row, lead_id, from_phone, nxt)
                return _twiml(out)

            out = get_text(cfg.get(nxt, {})) or "Continuemos…"
            out = out.replace("{Nombre}", nombre or "")
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        # TEXTO
        if t == "TEXTO":
            regla = (row_cfg.get("Regla_Validacion") or "").strip()
            if regla.startswith("REGEX:"):
                pattern = regla.replace("REGEX:", "", 1).strip()
                try:
                    if not re.fullmatch(pattern, msg_in_raw.strip()):
                        log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_regex")
                        return _twiml(msg_err)
                except re.error:
                    pass
            elif regla.upper() == "MONEY":
                if not re.fullmatch(r"\d{1,9}", msg_in_raw.strip()):
                    log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_money")
                    return _twiml(msg_err)

            campo = (row_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
            nxt = (row_cfg.get("Siguiente_Si_1") or "").strip() or estatus

            upd = {"Paso_Anterior": estatus, "ESTATUS": nxt, "Ultima_Actualizacion": now_iso()}
            if campo:
                upd[campo] = msg_in_raw.strip()
            update_row_cells(ws_leads, lead_row, upd)

            # Si el siguiente es SISTEMA, ejecútalo en caliente
            if nxt in cfg and step_type(cfg[nxt]) == "SISTEMA":
                out, _ = auto_run_system(ws_leads, ws_logs, cfg, lead_row, lead_id, from_phone, nxt)
                return _twiml(out)

            out = get_text(cfg.get(nxt, {})) or "Gracias. Continuemos…"
            out = out.replace("{Nombre}", nombre or "")
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        # fallback
        out = "Gracias. Continuemos…"
        log(ws_logs, lead_id, estatus, msg_in_raw, out, telefono=from_phone, err="unknown_tipo")
        return _twiml(out)

    except Exception:
        return _twiml("Perdón, tuve un problema técnico 🙏\nIntenta de nuevo en un momento.")


