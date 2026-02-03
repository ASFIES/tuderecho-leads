# app.py
import os
import re
import uuid
import html
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request, Response
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

from utils.sheets import (
    open_spreadsheet, open_worksheet, with_backoff,
    build_header_map, col_idx, find_row_by_value, update_row_cells,
    get_all_values_safe, row_to_dict, find_row_by_col_value
)
from utils.text import normalize_option, render_text, template_fill, detect_fuente

# =========================
# TIMEZONE / ENV
# =========================
MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City").strip() or "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS   = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_CONFIG = (os.environ.get("TAB_CONFIG") or os.environ.get("TAB_FLOW") or "Config_XimenaAI").strip()
TAB_SYS    = os.environ.get("TAB_SYS", "Config_Sistema").strip()

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

# =========================
# FLASK APP
# =========================
app = Flask(__name__)
app.url_map.strict_slashes = False  # ✅ evita problema /whatsapp vs /whatsapp/

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _twiml(text: str):
    """✅ Twilio responde mejor si devolvemos XML explícito."""
    resp = MessagingResponse()
    resp.message(text)
    return Response(str(resp), mimetype="application/xml")

def get_queue():
    """Devuelve cola si REDIS_URL existe; si no, None."""
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

def load_config(ws_config):
    rows = with_backoff(ws_config.get_all_records)
    cfg = {}
    for r in rows:
        pid = (r.get("ID_Paso") or "").strip()
        if pid:
            cfg[pid] = r
    return cfg

def step_type(cfg_row) -> str:
    return (cfg_row.get("Tipo_Entrada") or "").strip().upper()

def get_text(cfg_row) -> str:
    return render_text(cfg_row.get("Texto_Bot") or "")

def next_for_option(cfg_row, opt: str) -> str:
    # Soporta Siguiente_Si_1 ... Siguiente_Si_9 si existen en Sheets
    k = f"Siguiente_Si_{opt}"
    if cfg_row.get(k):
        return (cfg_row.get(k) or "").strip()
    # fallback legacy
    if opt == "1":
        return (cfg_row.get("Siguiente_Si_1") or "").strip()
    if opt == "2":
        return (cfg_row.get("Siguiente_Si_2") or "").strip()
    return ""

def ensure_lead(ws_leads, from_phone: str):
    phone_norm = re.sub(r"\D+", "", (from_phone or "").replace("whatsapp:", ""))
    h = build_header_map(ws_leads)

    row = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm, hmap=h)
    if row:
        vals = with_backoff(ws_leads.row_values, row)

        def get(name):
            c = col_idx(h, name)
            return (vals[c-1] if c and c-1 < len(vals) else "").strip()

        lead_id = get("ID_Lead") or ""
        if not lead_id:
            lead_id = uuid.uuid4().hex[:12]
            update_row_cells(ws_leads, row, {"ID_Lead": lead_id}, hmap=h)
        return row, lead_id, phone_norm, h

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
    row2 = find_row_by_value(ws_leads, "Telefono_Normalizado", phone_norm, hmap=h)
    return row2, lead_id, phone_norm, h

def read_lead_row(ws_leads, row_num: int, hmap):
    vals = with_backoff(ws_leads.row_values, row_num)
    d = {}
    for k, idx in hmap.items():
        d[k] = (vals[idx-1] if idx-1 < len(vals) else "").strip()
    return d

# =========================
# ROUTES
# =========================
@app.get("/")
def home():
    return {"ok": True, "service": "ximena-web", "ts": now_iso()}

@app.route("/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    # ✅ GET para probar desde navegador que el endpoint existe
    if request.method == "GET":
        return {"ok": True, "hint": "Twilio debe hacer POST aqui", "ts": now_iso()}, 200

    # ✅ DEBUG: esto debe verse en Render logs SI Twilio pega al endpoint
    try:
        msg_in_raw = (request.form.get("Body") or "").strip()
        from_phone = (request.form.get("From") or "").strip()

        print(f"[HIT /whatsapp] ts={now_iso()} from={from_phone} body={msg_in_raw}")
        # Si quieres más: print("form_keys=", list(request.form.keys()))

        sh = open_spreadsheet(GOOGLE_SHEET_NAME)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_logs  = open_worksheet(sh, TAB_LOGS)
        ws_cfg   = open_worksheet(sh, TAB_CONFIG)

        cfg = load_config(ws_cfg)

        lead_row, lead_id, phone_norm, h = ensure_lead(ws_leads, from_phone)
        lead = read_lead_row(ws_leads, lead_row, h)

        estatus = (lead.get("ESTATUS") or "INICIO").strip() or "INICIO"
        nombre  = (lead.get("Nombre") or "").strip()

        # Si está bloqueado por no aceptar aviso, no lo dejes avanzar
        if (lead.get("Bloqueado_Por_No_Aceptar") or "").strip():
            out = get_text(cfg.get("FIN_NO_ACEPTA", {})) or "Sin aviso de privacidad no podemos continuar."
            log(ws_logs, lead_id, "FIN_NO_ACEPTA", msg_in_raw, out, telefono=from_phone, err="blocked")
            return _twiml(out)

        # Detectar fuente solo si no se había identificado
        fuente_actual = (lead.get("Fuente_Lead") or "DESCONOCIDA").strip()
        if fuente_actual == "DESCONOCIDA":
            fuente_actual = detect_fuente(msg_in_raw)

        update_row_cells(ws_leads, lead_row, {
            "Ultimo_Mensaje_Cliente": msg_in_raw,
            "Fuente_Lead": fuente_actual,
            "Ultima_Actualizacion": now_iso()
        }, hmap=h)

        msg_opt = normalize_option(msg_in_raw)

        # ====== MENÚ CLIENTE (YA REGISTRADO) ======
        if estatus == "CLIENTE_MENU":
            menu_txt = get_text(cfg.get("CLIENTE_MENU", {})) or (
                f"Hola {nombre or ''} 👋 ¿Qué opción deseas?\n\n"
                "1️⃣ Próximas fechas agendadas\n"
                "2️⃣ Resumen de mi caso hasta hoy\n"
                "3️⃣ Contactar a mi abogada"
            )
            if msg_opt not in ("1", "2", "3"):
                out = menu_txt.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "CLIENTE_MENU", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

            # 1) Fechas
            if msg_opt == "1":
                out = get_text(cfg.get("MENU_FECHAS", {})) or (
                    f"{nombre or 'Hola'}, por ahora no tengo una fecha agendada aquí.\n\n"
                    "📌 Tu abogada te contactará lo antes posible para coordinar el siguiente paso."
                )
                out = out.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "MENU_FECHAS", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

            # 2) Resumen
            if msg_opt == "2":
                resumen = (lead.get("Analisis_AI") or "").strip()
                calc = (lead.get("Resultado_Calculo") or "").strip()
                out = get_text(cfg.get("MENU_RESUMEN", {})) or (
                    "📌 *Resumen de tu caso hasta hoy*\n\n"
                    "{Analisis_AI}\n\n"
                    "{Resultado_Calculo}\n\n"
                    "Si deseas, responde 3 para contactar a tu abogada."
                )
                out = out.replace("{Nombre}", nombre or "")
                out = out.replace("{Analisis_AI}", resumen or "Aún estamos integrando tu información.")
                out = out.replace("{Resultado_Calculo}", calc or "Aún no hay cálculo registrado.")
                log(ws_logs, lead_id, "MENU_RESUMEN", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

            # 3) Contactar abogada
            if msg_opt == "3":
                abog = (lead.get("Abogado_Asignado_Nombre") or "Tu abogada").strip()
                link = (lead.get("Link_WhatsApp") or "").strip()
                out = get_text(cfg.get("MENU_CONTACTO", {})) or (
                    f"👩‍⚖️ La abogada que acompaña tu caso es: {abog}.\n\n"
                    f"{('📲 Puedes escribirle aquí: ' + link) if link else '📲 En breve te compartimos el medio de contacto.'}\n\n"
                    "Si quieres volver al menú, escribe: *menu*"
                )
                log(ws_logs, lead_id, "MENU_CONTACTO", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

        # Atajo: si el usuario escribe "menu" y ya es cliente (DONE)
        if msg_in_raw.strip().lower() in ("menu", "menú"):
            if (lead.get("Procesar_AI_Status") or "").strip().upper() == "DONE":
                update_row_cells(ws_leads, lead_row, {"ESTATUS": "CLIENTE_MENU"}, hmap=h)
                out = get_text(cfg.get("CLIENTE_MENU", {})) or "Menú:\n1 Fechas\n2 Resumen\n3 Abogada"
                out = out.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "CLIENTE_MENU", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

        # Si estamos en INICIO y todavía no manda 1/2, solo mostramos INICIO
        if estatus == "INICIO" and msg_opt not in ("1", "2"):
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n1 Sí\n2 No"
            out = out.replace("{Nombre}", nombre or "")
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        row_cfg = cfg.get(estatus)
        if not row_cfg:
            update_row_cells(ws_leads, lead_row, {"ESTATUS": "INICIO"}, hmap=h)
            out = get_text(cfg.get("INICIO", {})) or "Hola, soy Ximena.\n1 Sí\n2 No"
            log(ws_logs, lead_id, "INICIO", msg_in_raw, out, telefono=from_phone, err="missing_step")
            return _twiml(out)

        t = step_type(row_cfg)
        msg_err = render_text(row_cfg.get("Mensaje_Error") or "Por favor responde con una opción válida.")

        # ====== OPCIONES ======
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

            # Si rechazó aviso, bloquea
            if estatus == "AVISO_PRIVACIDAD" and msg_opt == "2":
                upd["Bloqueado_Por_No_Aceptar"] = "SI"

            update_row_cells(ws_leads, lead_row, upd, hmap=h)

            # si el siguiente es EN_PROCESO, encola y responde EN_PROCESO
            if nxt == "EN_PROCESO":
                out = get_text(cfg.get("EN_PROCESO", {})) or "Estoy preparando tu estimación…"

                # encolar una sola vez (si hay redis + worker_jobs existe)
                q = get_queue()
                if q is not None:
                    try:
                        from worker_jobs import process_lead
                        update_row_cells(ws_leads, lead_row, {"Procesar_AI_Status": "ENQUEUED"}, hmap=h)
                        q.enqueue(process_lead, lead_id, job_timeout=180)
                    except Exception as e:
                        # ✅ no rompas el chat si worker no existe
                        update_row_cells(ws_leads, lead_row, {"Procesar_AI_Status": "ERROR_WORKER_IMPORT"}, hmap=h)
                        log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err=f"worker_import:{e}")

                out = out.replace("{Nombre}", nombre or "")
                log(ws_logs, lead_id, "EN_PROCESO", msg_in_raw, out, telefono=from_phone, err="")
                return _twiml(out)

            out = get_text(cfg.get(nxt, {})) or "Continuemos…"
            lead2 = read_lead_row(ws_leads, lead_row, h)
            out = out.replace("{Nombre}", (lead2.get("Nombre") or "").strip())
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        # ====== TEXTO ======
        if t == "TEXTO":
            regla = (row_cfg.get("Regla_Validacion") or "").strip()
            ok = True
            if regla.upper() == "MONEY":
                ok = bool(re.fullmatch(r"\d{1,12}", msg_in_raw.strip()))
            elif regla.upper().startswith("REGEX:"):
                pattern = regla.split(":", 1)[1].strip()
                try:
                    ok = bool(re.fullmatch(pattern, msg_in_raw.strip()))
                except re.error:
                    ok = True

            if not ok:
                log(ws_logs, lead_id, estatus, msg_in_raw, msg_err, telefono=from_phone, err="invalid_text")
                return _twiml(msg_err)

            campo = (row_cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
            nxt = (row_cfg.get("Siguiente_Si_1") or "").strip() or estatus

            upd = {"Paso_Anterior": estatus, "ESTATUS": nxt, "Ultima_Actualizacion": now_iso()}
            if campo:
                upd[campo] = msg_in_raw.strip()

            update_row_cells(ws_leads, lead_row, upd, hmap=h)

            out = get_text(cfg.get(nxt, {})) or "Gracias. Continuemos…"
            lead2 = read_lead_row(ws_leads, lead_row, h)
            out = out.replace("{Nombre}", (lead2.get("Nombre") or "").strip())
            log(ws_logs, lead_id, nxt, msg_in_raw, out, telefono=from_phone, err="")
            return _twiml(out)

        # ====== SISTEMA (FIN, etc.) ======
        out = get_text(row_cfg) or "Gracias."
        out = out.replace("{Nombre}", nombre or "")
        log(ws_logs, lead_id, estatus, msg_in_raw, out, telefono=from_phone, err="system_step")
        return _twiml(out)

    except Exception as e:
        print("[ERROR /whatsapp]", repr(e))
        print(traceback.format_exc())
        return _twiml("Perdón, tuve un problema técnico 🙏\nIntenta de nuevo en un momento.")

# ====== REPORTE WEB ======
@app.get("/reporte")
def reporte():
    token = (request.args.get("token") or "").strip()
    lead_id = (request.args.get("lead") or "").strip()

    if not token and not lead_id:
        return ("Falta token o lead.", 400)

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    values = get_all_values_safe(ws_leads)

    idx = None
    if token:
        idx = find_row_by_col_value(values, "Token_Reporte", token)
    if idx is None and lead_id:
        idx = find_row_by_col_value(values, "ID_Lead", lead_id)

    if idx is None:
        return ("Reporte no encontrado.", 404)

    lead = row_to_dict(values[0], values[idx])

    nombre = html.escape((lead.get("Nombre") or "").strip())
    apellido = html.escape((lead.get("Apellido") or "").strip())
    tipo = html.escape((lead.get("Tipo_Caso") or "").strip())
    desc = html.escape((lead.get("Descripcion_Situacion") or "").strip())
    res = html.escape((lead.get("Resultado_Calculo") or "").strip())
    ai = html.escape((lead.get("Analisis_AI") or "").strip())

    tipo_h = "Despido" if tipo == "1" else ("Renuncia" if tipo == "2" else "Caso laboral")

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Reporte preliminar</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b0f14; color:#f2f4f7; margin:0; }}
    .wrap {{ max-width:980px; margin:0 auto; padding:22px; }}
    .card {{ background:#111827; border:1px solid #1f2937; border-radius:16px; padding:18px; margin-bottom:14px; }}
    h1 {{ margin:0 0 8px 0; font-size:22px; }}
    h2 {{ margin:0 0 8px 0; font-size:16px; color:#93c5fd; }}
    p {{ margin:0; line-height:1.45; white-space:pre-wrap; }}
    .muted {{ color:#9ca3af; font-size:12px; }}
    .btn {{ display:inline-block; margin-top:10px; background:#2563eb; color:white; padding:10px 14px; border-radius:10px; text-decoration:none; }}
    .btn2 {{ display:inline-block; margin-top:10px; background:#111827; border:1px solid #374151; color:white; padding:10px 14px; border-radius:10px; text-decoration:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Reporte preliminar</h1>
      <p class="muted">Generado: {now_iso()} · Este reporte es informativo y no constituye asesoría legal.</p>
      <a class="btn" href="#" onclick="window.print();return false;">Imprimir</a>
      <a class="btn2" href="/">Volver</a>
    </div>

    <div class="card">
      <h2>Datos del caso</h2>
      <p><b>Nombre:</b> {nombre} {apellido}</p>
      <p><b>Tipo:</b> {tipo_h}</p>
      <p><b>Descripción:</b> {desc}</p>
    </div>

    <div class="card">
      <h2>Estimación preliminar</h2>
      <p>{res}</p>
    </div>

    <div class="card">
      <h2>Orientación (informativa)</h2>
      <p>{ai}</p>
    </div>
  </div>
</body>
</html>
"""
