# app.py
import os
import re
import uuid
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue
from gspread.utils import rowcol_to_a1

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff

TZ = os.environ.get("TZ", "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS  = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CFG   = os.environ.get("TAB_CFG", "Config_XimenaAI").strip()
TAB_GEST  = os.environ.get("TAB_GEST", "Gestion_Abogados").strip()

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

# =========================
# Helpers (self-contained)
# =========================
def now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def twiml(text: str):
    resp = MessagingResponse()
    resp.message(text or "")
    return str(resp)

def normalize_phone(raw: str) -> str:
    return re.sub(r"\D+", "", raw or "")

def normalize_option(s: str) -> str:
    s = (s or "").strip()
    # quita caracteres raros y emojis numerados (1️⃣ etc.)
    m = re.search(r"\d", s)
    return m.group(0) if m else s

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        return "WEB"
    return "DESCONOCIDA"

def safe_name(nombre: str) -> str:
    n = (nombre or "").strip()
    if not n:
        return "Hola"
    return n[:1].upper() + n[1:]

def render_text(s: str) -> str:
    return (s or "").replace("\\n", "\n").strip()

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

def get_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def batch_update(ws, a1_to_value: dict):
    """Batch update 1 celda por A1; best effort."""
    if not a1_to_value:
        return
    data = [{"range": a1, "values": [[str(v)]]} for a1, v in a1_to_value.items()]
    with_backoff(ws.batch_update, data, value_input_option="USER_ENTERED")

def update_row_fields(ws, row_num: int, updates: dict):
    """Update por header -> A1. Si falla, no debe tumbar el flujo."""
    if not updates:
        return
    header = with_backoff(ws.row_values, 1)
    if not header:
        return
    a1 = {}
    for k, v in updates.items():
        if k in header:
            c = header.index(k) + 1
            a1[rowcol_to_a1(row_num, c)] = v
    if a1:
        batch_update(ws, a1)

def load_cfg(ws_cfg):
    rows = with_backoff(ws_cfg.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("ID_Paso", "")).strip()
        if k:
            out[k] = r
    return out

def format_template(template: str, lead: dict) -> str:
    t = render_text(template)
    t = t.replace("{Nombre}", safe_name(lead.get("Nombre", "")))
    t = t.replace("{Resultado_Calculo}", str(lead.get("Resultado_Calculo", "") or "").strip())
    t = t.replace("{Abogado_Asignado_Nombre}", str(lead.get("Abogado_Asignado_Nombre", "") or "").strip())
    t = t.replace("{Link_Reporte_Web}", str(lead.get("Link_Reporte_Web", "") or "").strip())
    return t.strip()

def safe_append_log(ws_logs, payload: dict):
    """
    BEST-EFFORT LOG: si Logs tiene headers raros o falla, NO tumba el bot.
    """
    try:
        header = with_backoff(ws_logs.row_values, 1)
        if not header:
            return
        row = [""] * len(header)
        def setv(col, val):
            if col in header:
                row[header.index(col)] = str(val)

        setv("ID_Log", payload.get("ID_Log", str(uuid.uuid4())[:10]))
        setv("Fecha_Hora", payload.get("Fecha_Hora", now_iso()))
        setv("Telefono", payload.get("Telefono", ""))
        setv("ID_Lead", payload.get("ID_Lead", ""))
        setv("Paso", payload.get("Paso", ""))
        setv("Mensaje_Entrante", payload.get("Mensaje_Entrante", ""))
        setv("Mensaje_Saliente", payload.get("Mensaje_Saliente", ""))
        setv("Canal", payload.get("Canal", "WHATSAPP"))
        setv("Fuente_Lead", payload.get("Fuente_Lead", "DESCONOCIDA"))
        setv("Modelo_AI", payload.get("Modelo_AI", "gpt-4o-mini"))
        setv("Errores", payload.get("Errores", ""))

        with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")
    except:
        # Nunca romper el flujo por logging
        return

def find_lead_by_phone(records: list[dict], phone_norm: str):
    for i, r in enumerate(records):
        if str(r.get("Telefono_Normalizado", "")).strip() == phone_norm:
            return i, r
    return None, None

def ensure_lead(ws_leads, leads_records: list[dict], from_phone: str, msg_in: str):
    phone_norm = normalize_phone(from_phone)
    idx, lead = find_lead_by_phone(leads_records, phone_norm)
    if lead:
        return idx, lead, leads_records

    header = with_backoff(ws_leads.row_values, 1)
    if not header:
        return None, None, leads_records

    row = [""] * len(header)
    def setv(name, val):
        if name in header:
            row[header.index(name)] = str(val)

    lead_id = str(uuid.uuid4())[:8] + "-" + phone_norm[-6:]
    fuente = detect_fuente(msg_in)

    setv("ID_Lead", lead_id)
    setv("Telefono", from_phone)
    setv("Telefono_Normalizado", phone_norm)
    setv("Fuente_Lead", fuente)
    setv("Fecha_Registro", now_iso())
    setv("Ultima_Actualizacion", now_iso())
    setv("ESTATUS", "INICIO")
    setv("Paso_Anterior", "")

    with_backoff(ws_leads.append_row, row, value_input_option="USER_ENTERED")

    # re-leer bulk una vez
    leads_records = with_backoff(ws_leads.get_all_records)
    idx2, lead2 = find_lead_by_phone(leads_records, phone_norm)
    return idx2, lead2, leads_records

def pick_next_step(step_cfg: dict, opt: str, default_step: str):
    k = f"Siguiente_Si_{opt}"
    if step_cfg.get(k):
        return str(step_cfg.get(k)).strip()
    if opt == "1" and step_cfg.get("Siguiente_Si_1"):
        return str(step_cfg.get("Siguiente_Si_1")).strip()
    if opt == "2" and step_cfg.get("Siguiente_Si_2"):
        return str(step_cfg.get("Siguiente_Si_2")).strip()
    return default_step

# =========================
# Webhook
# =========================
@app.post("/whatsapp")
def whatsapp_webhook():
    req_id = str(uuid.uuid4())[:8]
    msg_raw = (request.form.get("Body") or "").strip()
    from_phone = (request.form.get("From") or "").strip()

    # log mínimo en Render
    app.logger.info(f"[{req_id}] IN msg='{msg_raw[:60]}' from='{from_phone}'")

    try:
        if not GOOGLE_SHEET_NAME:
            raise RuntimeError("GOOGLE_SHEET_NAME vacío")

        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_cfg   = open_worksheet(sh, TAB_CFG)
        # Logs puede fallar, NO debe tumbar flujo
        ws_logs = None
        try:
            ws_logs = open_worksheet(sh, TAB_LOGS)
        except:
            ws_logs = None

        cfg_map = load_cfg(ws_cfg)
        leads_records = with_backoff(ws_leads.get_all_records)

        idx, lead, leads_records = ensure_lead(ws_leads, leads_records, from_phone, msg_raw)
        if not lead:
            return twiml("Perdón, por el momento no pude acceder a la base de datos 🙏 Intenta nuevamente en unos minutos.")

        row_num = idx + 2
        lead_id = (lead.get("ID_Lead") or "").strip()
        estatus = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        paso_anterior = (lead.get("Paso_Anterior") or "").strip()
        fuente = (lead.get("Fuente_Lead") or detect_fuente(msg_raw)).strip() or "DESCONOCIDA"

        # siempre guardar último mensaje (best effort)
        try:
            update_row_fields(ws_leads, row_num, {"Ultimo_Mensaje_Cliente": msg_raw, "Ultima_Actualizacion": now_iso()})
        except:
            pass

        # 1) INICIO se envía una sola vez
        if estatus == "INICIO" and paso_anterior != "INICIO_ENVIADO":
            cfg = cfg_map.get("INICIO", {})
            out = format_template(cfg.get("Texto_Bot", ""), lead) or "Hola, soy Ximena 👋\n\n¿Deseas continuar?\n1 Sí\n2 No"

            # marcar que ya enviamos INICIO (para que el siguiente mensaje sea validado)
            try:
                update_row_fields(ws_leads, row_num, {
                    "Paso_Anterior": "INICIO_ENVIADO",
                    "Fuente_Lead": fuente,
                    "Ultimo_Error": "",
                })
            except:
                pass

            if ws_logs:
                safe_append_log(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "INICIO",
                    "Mensaje_Entrante": msg_raw,
                    "Mensaje_Saliente": out,
                    "Fuente_Lead": fuente,
                })

            return twiml(out)

        # 2) validar existencia del paso en Config
        step_cfg = cfg_map.get(estatus)
        if not step_cfg:
            # reset suave a INICIO
            cfg = cfg_map.get("INICIO", {})
            out = format_template(cfg.get("Texto_Bot", ""), lead) or "Hola, soy Ximena 👋\n\n¿Deseas continuar?\n1 Sí\n2 No"

            try:
                update_row_fields(ws_leads, row_num, {
                    "ESTATUS": "INICIO",
                    "Paso_Anterior": "INICIO_ENVIADO",
                    "Ultimo_Error": f"[{req_id}] Paso no existe en Config: {estatus}",
                })
            except:
                pass

            if ws_logs:
                safe_append_log(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "RESET_INICIO",
                    "Mensaje_Entrante": msg_raw,
                    "Mensaje_Saliente": out,
                    "Fuente_Lead": fuente,
                    "Errores": f"Paso no existe: {estatus}",
                })

            return twiml(out)

        tipo = (step_cfg.get("Tipo_Entrada") or "").strip().upper()
        opciones = (step_cfg.get("Opciones_Validas") or "").strip()
        regla = (step_cfg.get("Regla_Validacion") or "").strip()
        campo = (step_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
        err_msg = (step_cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.").strip()

        msg_opt = normalize_option(msg_raw)

        # 3) pasos tipo SISTEMA: solo mostrar texto
        if tipo == "SISTEMA":
            out = format_template(step_cfg.get("Texto_Bot", ""), lead) or "Entendido."
            if ws_logs:
                safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": estatus, "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": out, "Fuente_Lead": fuente})
            return twiml(out)

        # 4) Opciones
        if tipo == "OPCIONES":
            valid = [x.strip() for x in opciones.split(",") if x.strip()]
            if msg_opt not in valid:
                if ws_logs:
                    safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": estatus, "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": err_msg, "Fuente_Lead": fuente})
                return twiml(err_msg)

            next_step = pick_next_step(step_cfg, msg_opt, estatus) or estatus

            updates = {
                "Paso_Anterior": estatus,
                "Ultima_Actualizacion": now_iso(),
                "Ultimo_Error": "",
                "Fuente_Lead": fuente,
            }
            if campo:
                updates[campo] = msg_opt

            # bloqueos
            if next_step == "FIN_NO_ACEPTA":
                updates["Bloqueado_Por_No_Aceptar"] = "SI"
            if next_step == "FIN_NO_CONTINUA":
                updates["Bloqueado_Por_No_Aceptar"] = "NO"

            # generación de resultados: EN_PROCESO + cola
            if next_step == "GENERAR_RESULTADOS":
                updates["ESTATUS"] = "EN_PROCESO"
                updates["Paso_Anterior"] = "GENERAR_RESULTADOS"
                try:
                    update_row_fields(ws_leads, row_num, updates)
                except:
                    pass

                out = format_template((cfg_map.get("GENERAR_RESULTADOS", {}) or {}).get("Texto_Bot", ""), lead) or "Gracias. Estoy generando tu estimación preliminar y asignándote una abogada…"

                # encolar worker (si Redis)
                try:
                    q = get_queue()
                    if q is not None:
                        from worker_jobs import process_lead
                        q.enqueue(process_lead, lead_id, job_timeout=180)
                except Exception as e:
                    # no tumbar; solo registrar error
                    try:
                        update_row_fields(ws_leads, row_num, {"Ultimo_Error": f"[{req_id}] Cola/Worker: {str(e)[:180]}"})
                    except:
                        pass

                if ws_logs:
                    safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": "GENERAR_RESULTADOS", "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": out, "Fuente_Lead": fuente})
                return twiml(out)

            # normal: avanzar
            updates["ESTATUS"] = next_step
            try:
                update_row_fields(ws_leads, row_num, updates)
            except:
                pass

            out = format_template((cfg_map.get(next_step, {}) or {}).get("Texto_Bot", ""), lead) or "Continuemos…"
            if ws_logs:
                safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": next_step, "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": out, "Fuente_Lead": fuente})
            return twiml(out)

        # 5) Texto libre
        if not is_valid_by_rule(msg_raw, regla):
            if ws_logs:
                safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": estatus, "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": err_msg, "Fuente_Lead": fuente})
            return twiml(err_msg)

        next_step = str(step_cfg.get("Siguiente_Si_1") or "").strip() or estatus
        updates = {
            "Paso_Anterior": estatus,
            "ESTATUS": next_step,
            "Ultima_Actualizacion": now_iso(),
            "Ultimo_Error": "",
            "Fuente_Lead": fuente,
        }
        if campo:
            updates[campo] = msg_raw

        # construir fechas cuando se completa día
        if estatus == "INI_DIA":
            inicio = build_date_from_parts(lead.get("Inicio_Anio"), lead.get("Inicio_Mes"), msg_raw)
            if inicio:
                updates["Fecha_Inicio_Laboral"] = inicio
        if estatus == "FIN_DIA":
            fin = build_date_from_parts(lead.get("Fin_Anio"), lead.get("Fin_Mes"), msg_raw)
            if fin:
                updates["Fecha_Fin_Laboral"] = fin

        try:
            update_row_fields(ws_leads, row_num, updates)
        except:
            pass

        out = format_template((cfg_map.get(next_step, {}) or {}).get("Texto_Bot", ""), lead) or "Gracias. Continuemos…"
        if ws_logs:
            safe_append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": next_step, "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": out, "Fuente_Lead": fuente})

        return twiml(out)

    except Exception as e:
        # ======= ERROR REAL (Render log) =======
        err_short = f"[{req_id}] {type(e).__name__}: {str(e)[:200]}"
        app.logger.error(err_short)
        app.logger.error(traceback.format_exc())

        # ======= best effort: registrar en BD_Leads.Ultimo_Error =======
        try:
            sh = open_spreadsheet(GOOGLE_SHEET_NAME)
            ws_leads = open_worksheet(sh, TAB_LEADS)
            leads_records = with_backoff(ws_leads.get_all_records)
            phone_norm = normalize_phone(from_phone)
            idx, lead = find_lead_by_phone(leads_records, phone_norm)
            if lead is not None:
                row_num = idx + 2
                update_row_fields(ws_leads, row_num, {"Ultimo_Error": err_short, "Ultima_Actualizacion": now_iso()})
        except:
            pass

        # Respuesta humana (sin revelar internos)
        return twiml("Perdón, tuve un problema técnico 🙏\nIntenta de nuevo en un momento.")

