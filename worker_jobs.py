import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from twilio.rest import Client
from openai import OpenAI

from utils.sheets import (
    get_gspread_client,
    open_spreadsheet,
    open_worksheet,
    build_header_map,
    col_idx,
    find_row_by_value,
    update_lead_batch,
    append_row_by_headers,
    get_all_records_cached,
)
from utils.cache import redis_client
from utils.calc import calcular_estimacion
from utils.abogados import pick_abogado_from_sheet
from utils.ai import generar_resumen_legal_empatico
from utils.text import money_to_float, safe_name

MX_TZ = ZoneInfo("America/Mexico_City")

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()
TAB_CONOC = os.environ.get("TAB_CONOC", "Conocimiento_AI").strip()
TAB_GESTION = os.environ.get("TAB_GESTION", "Gestion_Abogados").strip()

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()  # whatsapp:+...

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

def process_resultados(payload: dict):
    """
    Job RQ: genera resultados pesados y envía mensajes por Twilio REST
    """
    telefono_raw = (payload or {}).get("telefono_raw", "")
    telefono_norm = (payload or {}).get("telefono_norm", "")
    lead_id = (payload or {}).get("lead_id", "")

    if not telefono_raw:
        return

    # Twilio client (para enviar fuera del webhook)
    tw = None
    if TWILIO_SID and TWILIO_TOKEN:
        tw = Client(TWILIO_SID, TWILIO_TOKEN)

    # OpenAI client
    ai = None
    if OPENAI_API_KEY:
        ai = OpenAI(api_key=OPENAI_API_KEY)

    # Sheets
    gc = get_gspread_client()
    sh = open_spreadsheet(gc, GOOGLE_SHEET_NAME)

    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abogados = open_worksheet(sh, TAB_ABOGADOS)
    ws_sys = open_worksheet(sh, TAB_SYS)
    ws_param = open_worksheet(sh, TAB_PARAM)
    ws_conoc = open_worksheet(sh, TAB_CONOC)
    ws_gestion = open_worksheet(sh, TAB_GESTION)

    leads_headers = build_header_map(ws_leads)

    # Encontrar lead por teléfono
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("BD_Leads sin columna Telefono")

    row = find_row_by_value(ws_leads, tel_col, telefono_raw) or find_row_by_value(ws_leads, tel_col, telefono_norm)
    if not row:
        return

    # Snapshot (solo esa fila)
    headers_list = ws_leads.row_values(1)
    row_vals = ws_leads.row_values(row)
    lead = {h: (row_vals[i] if i < len(row_vals) else "") for i, h in enumerate(headers_list)}

    tipo_caso = (lead.get("Tipo_Caso") or "").strip()
    desc = (lead.get("Descripcion_Situacion") or "").strip()
    nombre = safe_name(lead.get("Nombre") or "")

    salario = money_to_float(lead.get("Salario_Mensual") or "0")
    fecha_ini = (lead.get("Fecha_Inicio_Laboral") or "").strip()
    fecha_fin = (lead.get("Fecha_Fin_Laboral") or "").strip()

    # Config sistema (cacheado)
    sys_cfg = get_all_records_cached(ws_sys, cache_key="sys_cfg", ttl=180)
    sys_dict = {}
    # sys_cfg es lista de dicts con Clave/Valor
    for r in sys_cfg:
        k = (r.get("Clave") or "").strip()
        v = (r.get("Valor") or "").strip()
        if k:
            sys_dict[k] = v

    base_url = (sys_dict.get("BASE_URL_WEB") or "").strip()
    ruta_reporte = (sys_dict.get("RUTA_REPORTE") or "").strip()

    # Parametros legales (cacheado)
    params_rows = get_all_records_cached(ws_param, cache_key="param_legal", ttl=180)
    params = {}
    # Parametros_Legales: Concepto/Valor
    for r in params_rows:
        c = (r.get("Concepto") or "").strip()
        v = (r.get("Valor") or "").strip()
        if not c:
            continue
        if v.endswith("%"):
            try:
                params[c] = float(v.replace("%", "").strip()) / 100.0
                continue
            except:
                pass
        try:
            params[c] = float(v)
        except:
            pass

    # Cálculo (MVP)
    monto = calcular_estimacion(tipo_caso, salario, fecha_ini, fecha_fin, params)

    # Asignación abogado (>=50k => A01 desde sheet)
    abogado_id, abogado_nombre, abogado_tel = pick_abogado_from_sheet(ws_abogados, salario)

    # Conocimiento AI (cacheado)
    conoc_rows = get_all_records_cached(ws_conoc, cache_key="conocimiento_ai", ttl=300)

    # Resumen largo legal + empático
    tipo_txt = "despido" if tipo_caso == "1" else "renuncia"
    resumen = generar_resumen_legal_empatico(
        ai_client=ai,
        model=OPENAI_MODEL,
        tipo_txt=tipo_txt,
        descripcion_usuario=desc,
        conocimiento_rows=conoc_rows,
        max_words=220,   # más largo (tu querías más largo)
    )

    # Token + link
    token = uuid.uuid4().hex[:16]
    link_reporte = ""
    if ruta_reporte:
        link_reporte = ruta_reporte.rstrip("/") + "/" + token
    elif base_url:
        link_reporte = base_url.rstrip("/") + "/reporte/" + token

    # Mensaje final humano
    msg_final = (
        f"✅ *{nombre}, gracias por confiar en Tu Derecho Laboral México.*\n\n"
        f"Entiendo que vivir una situación laboral así puede sentirse pesado e incierto. "
        f"Quiero que sepas algo: *no estás sola/solo*. Vamos paso a paso y cuidaremos tu caso con seriedad.\n\n"
        f"📌 *Resumen preliminar (informativo):*\n{resumen}\n\n"
        f"💰 *Estimación inicial aproximada:* ${monto:,.2f} MXN\n"
        f"👩‍⚖️ *Abogada que acompañará tu caso:* {abogado_nombre}\n\n"
        f"📄 *Informe completo:* {link_reporte}\n\n"
        f"⚠️ *Aviso importante:* Esta información es únicamente orientativa y no constituye asesoría legal. "
        f"No existe relación abogado-cliente hasta que una abogada revise tu caso, confirme viabilidad y acepte formalmente el asunto."
    ).strip()

    # Actualizar BD_Leads (batch)
    update_lead_batch(ws_leads, leads_headers, row, {
        "Analisis_AI": resumen,
        "Resultado_Calculo": str(monto),
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Token_Reporte": token,
        "Link_Reporte_Web": link_reporte,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": now_iso_mx(),
        "Ultimo_Error": "",
    })

    # Insertar en Gestion_Abogados (operativo)
    gestion_headers = build_header_map(ws_gestion)
    append_row_by_headers(ws_gestion, gestion_headers, {
        "ID_Gestion": str(uuid.uuid4()),
        "Fecha_Asignacion": now_iso_mx(),
        "ID_Lead": lead_id or lead.get("ID_Lead") or "",
        "Telefono_Lead": telefono_raw,
        "Nombre_Lead": f"{lead.get('Nombre','')} {lead.get('Apellido','')}".strip(),
        "Monto_Estimado": str(monto),
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Abogado_Asignado_Telefono": abogado_tel,
        "Estatus_Interno": "NUEVO",
    })

    # Enviar WhatsApp al cliente
    if tw and TWILIO_NUMBER and telefono_raw:
        tw.messages.create(
            from_=TWILIO_NUMBER,
            to=f"whatsapp:{telefono_raw.replace('whatsapp:','')}",
            body=msg_final
        )

    # Notificar abogado (si tiene teléfono)
    if tw and TWILIO_NUMBER and abogado_tel:
        tw.messages.create(
            from_=TWILIO_NUMBER,
            to=f"whatsapp:{abogado_tel.replace('whatsapp:','')}",
            body=(
                f"⚖️ *Nuevo Lead asignado*\n"
                f"Nombre: {lead.get('Nombre','')} {lead.get('Apellido','')}\n"
                f"Tel: {telefono_raw.replace('whatsapp:','')}\n"
                f"Tipo: {'Despido' if tipo_caso=='1' else 'Renuncia'}\n"
                f"Salario: ${salario:,.2f}\n"
                f"Monto estimado: ${monto:,.2f}\n"
                f"Informe: {link_reporte}"
            )
        )

    return {
        "ok": True,
        "telefono": telefono_raw,
        "lead_id": lead_id,
        "abogado": abogado_id,
        "monto": monto,
    }
