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

from gspread.utils import rowcol_to_a1

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff
from utils.text import (
    render_text,
    normalize_option,
    detect_fuente,
    is_valid_by_rule,
    build_date_from_parts,
    safe_name,
)

TZ = os.environ.get("TZ", "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS  = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CFG   = os.environ.get("TAB_CFG", "Config_XimenaAI").strip()
TAB_GEST  = os.environ.get("TAB_GEST", "Gestion_Abogados").strip()  # opcional, solo para menú

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

app = Flask(__name__)

# -------------------------
# Helpers
# -------------------------
def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def _normalize_phone(raw: str) -> str:
    return re.sub(r"\D+", "", raw or "")

def _twiml(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def _get_queue():
    if not REDIS_URL:
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(REDIS_QUEUE_NAME, connection=conn)

def _load_cfg(ws_cfg):
    rows = with_backoff(ws_cfg.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("ID_Paso", "")).strip()
        if k:
            out[k] = r
    return out

def _batch_update(ws, updates_a1: dict):
    data = [{"range": a1, "values": [[str(val)]]} for a1, val in updates_a1.items()]
    if data:
        with_backoff(ws.batch_update, data, value_input_option="USER_ENTERED")

def _update_row_fields(ws, row_num: int, updates: dict):
    header = with_backoff(ws.row_values, 1)
    a1_updates = {}
    for k, v in (updates or {}).items():
        if k in header:
            col = header.index(k) + 1
            a1_updates[rowcol_to_a1(row_num, col)] = v
    _batch_update(ws, a1_updates)

def _append_log(ws_logs, payload: dict):
    """
    Logs robusto por header (no depende del orden).
    Esperado (según tu sheet):
    ID_Log, Fecha_Hora, Telefono, ID_Lead, Paso, Mensaje_Entrante, Mensaje_Saliente, Canal, Fuente_Lead, Modelo_AI, Errores
    """
    header = with_backoff(ws_logs.row_values, 1)
    row = [""] * len(header)

    def setv(col_name, val):
        if col_name in header:
            row[header.index(col_name)] = str(val)

    setv("ID_Log", payload.get("ID_Log", str(uuid.uuid4())[:10]))
    setv("Fecha_Hora", payload.get("Fecha_Hora", _now_iso()))
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

def _find_lead_by_phone(records: list[dict], phone_norm: str):
    for i, r in enumerate(records):
        if str(r.get("Telefono_Normalizado", "")).strip() == phone_norm:
            return i, r
    return None, None

def _ensure_lead(ws_leads, leads_records: list[dict], from_phone: str, msg_in: str):
    phone_norm = _normalize_phone(from_phone)
    idx, lead = _find_lead_by_phone(leads_records, phone_norm)
    if lead:
        return idx, lead, leads_records

    # crear lead nuevo con header real
    header = with_backoff(ws_leads.row_values, 1)
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
    setv("Fecha_Registro", _now_iso())
    setv("Ultima_Actualizacion", _now_iso())
    setv("ESTATUS", "INICIO")
    setv("Paso_Anterior", "")

    with_backoff(ws_leads.append_row, row, value_input_option="USER_ENTERED")

    # re-leer una vez (bulk)
    leads_records = with_backoff(ws_leads.get_all_records)
    idx2, lead2 = _find_lead_by_phone(leads_records, phone_norm)
    return idx2, lead2, leads_records

def _format(template: str, lead: dict) -> str:
    t = render_text(template or "")
    t = t.replace("{Nombre}", safe_name(lead.get("Nombre", "")))
    t = t.replace("{Abogado_Asignado_Nombre}", str(lead.get("Abogado_Asignado_Nombre", "") or "").strip())
    t = t.replace("{Resultado_Calculo}", str(lead.get("Resultado_Calculo", "") or "").strip())
    t = t.replace("{Link_Reporte_Web}", str(lead.get("Link_Reporte_Web", "") or "").strip())
    return t.strip()

def _pick_next_step(step_cfg: dict, opt: str, default_step: str):
    k = f"Siguiente_Si_{opt}"
    if step_cfg.get(k):
        return str(step_cfg.get(k)).strip()
    if opt == "1" and step_cfg.get("Siguiente_Si_1"):
        return str(step_cfg.get("Siguiente_Si_1")).strip()
    if opt == "2" and step_cfg.get("Siguiente_Si_2"):
        return str(step_cfg.get("Siguiente_Si_2")).strip()
    return default_step

# -------------------------
# Webhook
# -------------------------
@app.post("/whatsapp")
def whatsapp_webhook():
    msg_raw = (request.form.get("Body") or "").strip()
    from_phone = (request.form.get("From") or "").strip()

    try:
        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_cfg   = open_worksheet(sh, TAB_CFG)

        # bulk reads (1 por pestaña)
        cfg_map = _load_cfg(ws_cfg)
        leads_records = with_backoff(ws_leads.get_all_records)

        idx, lead, leads_records = _ensure_lead(ws_leads, leads_records, from_phone, msg_raw)
        if not lead:
            return _twiml("Perdón, por el momento no pude acceder a la base de datos 🙏 Intenta nuevamente en unos minutos.")

        row_num = idx + 2
        lead_id = (lead.get("ID_Lead") or "").strip()
        estatus = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        paso_anterior = (lead.get("Paso_Anterior") or "").strip()
        fuente = (lead.get("Fuente_Lead") or detect_fuente(msg_raw)).strip() or "DESCONOCIDA"

        # mantener fuente si estaba vacía
        if (lead.get("Fuente_Lead") or "").strip() in ("", "DESCONOCIDA"):
            _update_row_fields(ws_leads, row_num, {"Fuente_Lead": fuente})

        # -------------------------
        # INICIO: se envía 1 vez, luego ya se valida normalmente
        # -------------------------
        if estatus == "INICIO" and paso_anterior != "INICIO_ENVIADO":
            inicio_cfg = cfg_map.get("INICIO", {})
            out = _format(inicio_cfg.get("Texto_Bot", ""), lead) or "Hola, soy Ximena 👋\n\n¿Deseas continuar?\n1 Sí\n2 No"

            _update_row_fields(ws_leads, row_num, {
                "Paso_Anterior": "INICIO_ENVIADO",
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Mensaje_Cliente": msg_raw,
            })

            _append_log(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": "INICIO",
                "Mensaje_Entrante": msg_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": fuente,
            })
            return _twiml(out)

        # -------------------------
        # Paso inválido -> reset elegante
        # -------------------------
        step_cfg = cfg_map.get(estatus)
        if not step_cfg:
            inicio_cfg = cfg_map.get("INICIO", {})
            out = _format(inicio_cfg.get("Texto_Bot", ""), lead) or "Hola, soy Ximena 👋\n\n¿Deseas continuar?\n1 Sí\n2 No"

            _update_row_fields(ws_leads, row_num, {
                "ESTATUS": "INICIO",
                "Paso_Anterior": "INICIO_ENVIADO",
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Error": f"Estatus no existe en Config_XimenaAI: {estatus}",
                "Ultimo_Mensaje_Cliente": msg_raw,
            })

            _append_log(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": "RESET_INICIO",
                "Mensaje_Entrante": msg_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": fuente,
                "Errores": f"Estatus no existe: {estatus}",
            })
            return _twiml(out)

        tipo = (step_cfg.get("Tipo_Entrada") or "").strip().upper()
        opciones = (step_cfg.get("Opciones_Validas") or "").strip()
        regla = (step_cfg.get("Regla_Validacion") or "").strip()
        campo = (step_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
        err_msg = (step_cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.").strip()

        msg_opt = normalize_option(msg_raw)

        # -------------------------
        # SISTEMA: solo manda texto (útil para FIN_NO_ACEPTA / FIN_NO_CONTINUA)
        # -------------------------
        if tipo == "SISTEMA":
            out = _format(step_cfg.get("Texto_Bot", ""), lead) or "Entendido."
            _update_row_fields(ws_leads, row_num, {
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Mensaje_Cliente": msg_raw,
            })
            _append_log(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": estatus,
                "Mensaje_Entrante": msg_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": fuente,
            })
            return _twiml(out)

        # -------------------------
        # Menú Cliente (dinámico)
        # -------------------------
        if estatus == "CLIENTE_MENU":
            if msg_opt not in ("1", "2", "3"):
                out = _format(step_cfg.get("Texto_Bot", ""), lead) or err_msg
                _append_log(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "CLIENTE_MENU_ERR",
                    "Mensaje_Entrante": msg_raw,
                    "Mensaje_Saliente": out,
                    "Fuente_Lead": fuente,
                })
                return _twiml(out)

            # Opción 1: próximas fechas (si existe Gestion_Abogados)
            if msg_opt == "1":
                texto = "Por ahora no tengo una fecha agendada registrada. Si tu abogada programa una cita, aquí te aparecerá."
                try:
                    ws_gest = open_worksheet(sh, TAB_GEST)
                    gest = with_backoff(ws_gest.get_all_records)
                    # busca por ID_Lead
                    row = next((g for g in gest if str(g.get("ID_Lead","")).strip() == lead_id), None)
                    if row:
                        f = (row.get("Proximo_Evento_Fecha") or "").strip()
                        h = (row.get("Proximo_Evento_Hora") or "").strip()
                        t = (row.get("Proximo_Evento_Texto") or "").strip()
                        if f or h or t:
                            texto = "📅 Próximo evento agendado:\n"
                            if f: texto += f"• Fecha: {f}\n"
                            if h: texto += f"• Hora: {h}\n"
                            if t: texto += f"• Detalle: {t}\n"
                            texto += "\nSi necesitas algo antes, responde 3 para contactar a tu abogada."
                except:
                    pass

                _update_row_fields(ws_leads, row_num, {"Ultima_Actualizacion": _now_iso(), "Ultimo_Mensaje_Cliente": msg_raw})
                _append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": "CLIENTE_MENU_1", "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": texto, "Fuente_Lead": fuente})
                return _twiml(texto)

            # Opción 2: resumen del caso hasta hoy
            if msg_opt == "2":
                tipo_caso = "Despido" if str(lead.get("Tipo_Caso","")).strip() == "1" else "Renuncia"
                monto = (lead.get("Resultado_Calculo") or "").strip()
                abogado = (lead.get("Abogado_Asignado_Nombre") or "").strip()
                link = (lead.get("Link_Reporte_Web") or "").strip()
                resumen = (lead.get("Analisis_AI") or "").strip()
                # mensaje breve pero humano
                texto = (
                    f"Hola {safe_name(lead.get('Nombre',''))} 👋\n\n"
                    f"📌 Tipo de caso: {tipo_caso}\n"
                    f"📆 Inicio: {lead.get('Fecha_Inicio_Laboral','')}\n"
                    f"📆 Término: {lead.get('Fecha_Fin_Laboral','')}\n"
                    f"💰 Estimación preliminar: {monto or 'en proceso'}\n"
                    f"👩‍⚖️ Abogada asignada: {abogado or 'en proceso'}\n\n"
                    "Si quieres ver el desglose completo, aquí está tu reporte:\n"
                    f"{link}\n\n"
                    "Si deseas, responde 3 para contactar a tu abogada."
                )
                # no saturar whatsapp
                if resumen and len(texto) < 1200:
                    texto += "\n\n" + resumen[:450].strip()

                _update_row_fields(ws_leads, row_num, {"Ultima_Actualizacion": _now_iso(), "Ultimo_Mensaje_Cliente": msg_raw})
                _append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": "CLIENTE_MENU_2", "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": texto, "Fuente_Lead": fuente})
                return _twiml(texto)

            # Opción 3: contactar abogado (solo instrucción; el push al abogado lo hace worker o AppSheet)
            if msg_opt == "3":
                abogado = (lead.get("Abogado_Asignado_Nombre") or "").strip() or "tu abogada"
                texto = (
                    f"Perfecto, {safe_name(lead.get('Nombre',''))} 🙌\n\n"
                    f"Voy a dejar registrado que deseas contacto con {abogado}. "
                    "En cuanto esté disponible, te buscará por este mismo medio.\n\n"
                    "Si quieres agregar un dato importante (por ejemplo: salario real, bonos o si te hicieron firmar algo), escríbelo aquí."
                )
                _update_row_fields(ws_leads, row_num, {"Ultima_Actualizacion": _now_iso(), "Ultimo_Mensaje_Cliente": msg_raw})
                _append_log(ws_logs, {"Telefono": from_phone, "ID_Lead": lead_id, "Paso": "CLIENTE_MENU_3", "Mensaje_Entrante": msg_raw, "Mensaje_Saliente": texto, "Fuente_Lead": fuente})
                return _twiml(texto)

        # -------------------------
        # OPCIONES (flujo general)
        # -------------------------
        if tipo == "OPCIONES":
            valid = [x.strip() for x in opciones.split(",") if x.strip()]
            if msg_opt not in valid:
                _append_log(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": estatus,
                    "Mensaje_Entrante": msg_raw,
                    "Mensaje_Saliente": err_msg,
                    "Fuente_Lead": fuente,
                })
                return _twiml(err_msg)

            next_step = _pick_next_step(step_cfg, msg_opt, estatus) or estatus

            updates = {
                "Paso_Anterior": estatus,
                "Ultima_Actualizacion": _now_iso(),
                "Ultimo_Mensaje_Cliente": msg_raw,
                "Ultimo_Error": "",
                "Fuente_Lead": fuente,
            }

            # guardar campo asociado
            if campo:
                updates[campo] = msg_opt

            # banderas finales
            if next_step == "FIN_NO_ACEPTA":
                updates["Bloqueado_Por_No_Aceptar"] = "SI"
            if next_step == "FIN_NO_CONTINUA":
                updates["Bloqueado_Por_No_Aceptar"] = "NO"

            # si toca resultados: cambia a EN_PROCESO y encola worker
            if next_step == "GENERAR_RESULTADOS":
                updates["ESTATUS"] = "EN_PROCESO"
                updates["Paso_Anterior"] = "GENERAR_RESULTADOS"
                _update_row_fields(ws_leads, row_num, updates)

                gen_cfg = cfg_map.get("GENERAR_RESULTADOS", {})
                out = _format(gen_cfg.get("Texto_Bot", ""), {**lead, **updates}) or "Estoy generando tu estimación preliminar…"

                q = _get_queue()
                if q is not None:
                    from worker_jobs import process_lead
                    q.enqueue(process_lead, lead_id, job_timeout=180)

                _append_log(ws_logs, {
                    "Telefono": from_phone,
                    "ID_Lead": lead_id,
                    "Paso": "GENERAR_RESULTADOS",
                    "Mensaje_Entrante": msg_raw,
                    "Mensaje_Saliente": out,
                    "Fuente_Lead": fuente,
                })
                return _twiml(out)

            # normal: set estatus al siguiente
            updates["ESTATUS"] = next_step
            _update_row_fields(ws_leads, row_num, updates)

            out = _format((cfg_map.get(next_step, {}) or {}).get("Texto_Bot", ""), {**lead, **updates}) or "Continuemos…"

            _append_log(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": next_step,
                "Mensaje_Entrante": msg_raw,
                "Mensaje_Saliente": out,
                "Fuente_Lead": fuente,
            })
            return _twiml(out)

        # -------------------------
        # TEXTO (flujo general)
        # -------------------------
        if not is_valid_by_rule(msg_raw, regla):
            _append_log(ws_logs, {
                "Telefono": from_phone,
                "ID_Lead": lead_id,
                "Paso": estatus,
                "Mensaje_Entrante": msg_raw,
                "Mensaje_Saliente": err_msg,
                "Fuente_Lead": fuente,
            })
            return _twiml(err_msg)

        next_step = str(step_cfg.get("Siguiente_Si_1") or "").strip() or estatus

        updates = {
            "Paso_Anterior": estatus,
            "ESTATUS": next_step,
            "Ultima_Actualizacion": _now_iso(),
            "Ultimo_Mensaje_Cliente": msg_raw,
            "Ultimo_Error": "",
            "Fuente_Lead": fuente,
        }
        if campo:
            updates[campo] = msg_raw

        # fecha inicio y fin cuando se completan piezas
        if estatus == "INI_DIA":
            inicio = build_date_from_parts(lead.get("Inicio_Anio"), lead.get("Inicio_Mes"), msg_raw)
            if inicio:
                updates["Fecha_Inicio_Laboral"] = inicio

        if estatus == "FIN_DIA":
            fin = build_date_from_parts(lead.get("Fin_Anio"), lead.get("Fin_Mes"), msg_raw)
            if fin:
                updates["Fecha_Fin_Laboral"] = fin

        _update_row_fields(ws_leads, row_num, updates)

        out = _format((cfg_map.get(next_step, {}) or {}).get("Texto_Bot", ""), {**lead, **updates}) or "Gracias. Continuemos…"

        _append_log(ws_logs, {
            "Telefono": from_phone,
            "ID_Lead": lead_id,
            "Paso": next_step,
            "Mensaje_Entrante": msg_raw,
            "Mensaje_Saliente": out,
            "Fuente_Lead": fuente,
        })
        return _twiml(out)

    except Exception as e:
        return _twiml("Perdón, tuve un problema técnico 🙏 Intenta de nuevo en un momento.")
