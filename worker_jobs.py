# worker_jobs.py
import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from twilio.rest import Client
from openai import OpenAI

from utils.sheets import (
    get_gspread_client,
    open_spreadsheet,
    build_header_map,
    col_idx,
    read_row_range,
    batch_update_row,
    with_backoff,
)

MX_TZ = ZoneInfo("America/Mexico_City")

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()  # whatsapp:+1415...

def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")


def load_key_value(ws, key_col="Clave", val_col="Valor"):
    def _do_headers():
        return ws.row_values(1)
    headers = with_backoff(_do_headers)
    hm = build_header_map(headers)
    kc = col_idx(hm, key_col)
    vc = col_idx(hm, val_col)
    if not kc or not vc:
        return {}

    def _do_all():
        return ws.get_all_values()[1:]
    rows = with_backoff(_do_all)

    out = {}
    for r in rows:
        k = (r[kc-1] if kc-1 < len(r) else "").strip()
        v = (r[vc-1] if vc-1 < len(r) else "").strip()
        if k:
            out[k] = v
    return out


def load_parametros(ws_param):
    headers = with_backoff(lambda: ws_param.row_values(1))
    hm = build_header_map(headers)
    c = col_idx(hm, "Concepto")
    v = col_idx(hm, "Valor")
    if not c or not v:
        return {}

    rows = with_backoff(lambda: ws_param.get_all_values()[1:])
    out = {}
    for r in rows:
        cc = (r[c-1] if c-1 < len(r) else "").strip()
        vv = (r[v-1] if v-1 < len(r) else "").strip()
        if not cc:
            continue
        if vv.endswith("%"):
            try:
                out[cc] = float(vv.replace("%", "").strip()) / 100.0
                continue
            except:
                pass
        try:
            out[cc] = float(vv)
        except:
            out[cc] = vv
    return out


def calcular_estimacion(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: dict) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        sd = salario_mensual / 30.0
        sdi = sd * 1.0452  # MVP

        indemn_dias = float(params.get("Indemnizacion", 90))
        prima_ant_dias = float(params.get("Prima_Antiguedad", 12))
        veinte_dias = float(params.get("Veinte_Dias_Por_Anio", 20))

        total = (indemn_dias * sdi) + (prima_ant_dias * sdi * anios)
        if (tipo_caso or "").strip() == "1":  # despido
            total += (veinte_dias * sdi * anios)

        return round(total, 2)
    except:
        return 0.0


def pick_abogado(ws_abogados, salario_mensual: float):
    """
    REGLA TUYA:
    *50 mil de salario va al id de abogado que tenga el ID A01*
    """
    if salario_mensual >= 50000:
        return "A01", "Veronica Zavala", "+5215527773375"

    headers = with_backoff(lambda: ws_abogados.row_values(1))
    hm = build_header_map(headers)

    idc = col_idx(hm, "ID_Abogado")
    nc = col_idx(hm, "Nombre_Abogado")
    tc = col_idx(hm, "Telefono_Abogado")
    ac = col_idx(hm, "Activo")

    rows = with_backoff(lambda: ws_abogados.get_all_values()[1:])
    for r in rows:
        activo = (r[ac-1] if ac and ac-1 < len(r) else "SI").strip().upper()
        if activo != "SI":
            continue
        aid = (r[idc-1] if idc and idc-1 < len(r) else "").strip()
        an = (r[nc-1] if nc and nc-1 < len(r) else "").strip()
        at = (r[tc-1] if tc and tc-1 < len(r) else "").strip()
        if aid:
            return aid, an, at

    return "A01", "Veronica Zavala", "+5215527773375"


def build_result_message(nombre: str, resumen_ai: str, monto: float, abogado_nombre: str, link_reporte: str) -> str:
    nombre = (nombre or "").strip() or "Hola"
    return (
        f"✅ *{nombre}, gracias por confiar en Tu Derecho Laboral México.*\n\n"
        f"Entiendo que vivir esto puede ser desgastante. De verdad: *no estás solo(a)*. "
        f"Nuestro trabajo es ayudarte a recuperar claridad y defender tus derechos con seriedad y respeto.\n\n"
        f"📌 *Resumen preliminar (informativo):*\n{resumen_ai}\n\n"
        f"💰 *Estimación inicial aproximada:* ${monto:,.2f} MXN\n"
        f"👩‍⚖️ *Abogada que llevará tu caso:* {abogado_nombre}\n\n"
        f"🕒 En cuanto recibimos tu información, iniciamos la revisión. "
        f"Tu abogada te contactará lo antes posible para confirmar datos y explicarte opciones.\n\n"
        f"📄 *Informe completo:* {link_reporte}\n\n"
        f"⚠️ *Aviso importante:* Esta información es orientativa y no constituye asesoría legal. "
        f"No existe relación abogado-cliente hasta que un abogado acepte formalmente el asunto."
    ).strip()


def process_lead(lead_row: int, lead_id: str):
    """
    Job principal: corre en worker.
    - 1 lectura fuerte de la fila del lead (rango)
    - 1 batch update final
    """
    gc = get_gspread_client()
    sh = open_spreadsheet(gc)

    ws_leads = sh.worksheet(TAB_LEADS)
    ws_abogados = sh.worksheet(TAB_ABOGADOS)
    ws_sys = sh.worksheet(TAB_SYS)
    ws_param = sh.worksheet(TAB_PARAM)

    # Headers (1 lectura)
    headers = with_backoff(lambda: ws_leads.row_values(1))
    hm = build_header_map(headers)

    # Leemos solo la fila completa del lead en un rango amplio (1 request)
    max_col = len(headers) if headers else 40
    row_vals = read_row_range(ws_leads, lead_row, 1, max_col)

    def get(colname, default=""):
        idx = col_idx(hm, colname)
        if not idx:
            return default
        return (row_vals[idx-1] if idx-1 < len(row_vals) else "") or default

    nombre = get("Nombre", "")
    tel = get("Telefono", "")
    tipo_caso = (get("Tipo_Caso", "") or "").strip()
    desc = get("Descripcion_Situacion", "Sin detalles").strip() or "Sin detalles"

    fecha_ini = get("Fecha_Inicio_Laboral", "")
    fecha_fin = get("Fecha_Fin_Laboral", "")

    try:
        salario = float((get("Salario_Mensual", "0") or "0").replace("$", "").replace(",", "").strip())
    except:
        salario = 0.0

    sys_cfg = load_key_value(ws_sys)
    params = load_parametros(ws_param)

    monto = calcular_estimacion(tipo_caso, salario, fecha_ini, fecha_fin, params)

    tipo_txt = "despido" if tipo_caso == "1" else "renuncia"

    # Resumen por default (si no hay OpenAI)
    resumen_ai = (
        f"Con la información que compartiste, revisaremos tu caso como *{tipo_txt}* conforme a la Ley Federal del Trabajo. "
        f"De forma preliminar, se consideran prestaciones devengadas (salario pendiente, proporcionales, vacaciones y prima vacacional, aguinaldo) "
        f"y, si aplica, indemnización constitucional, prima de antigüedad y otros conceptos. "
        f"Un abogado confirmará contigo los datos clave para cuidar tus derechos."
    )

    # OpenAI (si está)
    if OPENAI_API_KEY:
        try:
            client_ai = OpenAI(api_key=OPENAI_API_KEY)
            resp = client_ai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system",
                     "content": (
                         "Eres un abogado laboral en México. Redacta un resumen empático y claro (150 a 220 palabras). "
                         "Incluye que se revisa conforme a LFT, que se confirmarán datos y que se contactará pronto. "
                         "NO pidas correo, NO pidas documentos, NO prometas resultados. "
                         "Tono humano, respetuoso y tranquilizador."
                     )},
                    {"role": "user", "content": f"Tipo: {tipo_txt}\nSituación: {desc}"}
                ],
                max_tokens=320,
            )
            resumen_ai = (resp.choices[0].message.content or "").strip() or resumen_ai
        except Exception:
            pass

    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abogados, salario_mensual=salario)

    token = uuid.uuid4().hex[:16]
    ruta_reporte = (sys_cfg.get("RUTA_REPORTE") or "").strip()
    link_reporte = (ruta_reporte.rstrip("/") + "/" + token) if ruta_reporte else ""

    msg_out = build_result_message(nombre, resumen_ai, monto, abogado_nombre, link_reporte)

    # Batch update FINAL (1 request)
    updates = {
        col_idx(hm, "Analisis_AI"): resumen_ai,
        col_idx(hm, "Resultado_Calculo"): str(monto),
        col_idx(hm, "Abogado_Asignado_ID"): abogado_id,
        col_idx(hm, "Abogado_Asignado_Nombre"): abogado_nombre,
        col_idx(hm, "Token_Reporte"): token,
        col_idx(hm, "Link_Reporte_Web"): link_reporte,
        col_idx(hm, "ESTATUS"): "CLIENTE_MENU",
        col_idx(hm, "Ultima_Actualizacion"): now_iso_mx(),
        col_idx(hm, "Ultimo_Error"): "",
    }

    # Limpia Nones
    updates = {k: v for k, v in updates.items() if k}

    batch_update_row(ws_leads, lead_row, updates)

    # Notificar abogado (opcional)
    if TWILIO_SID and TWILIO_TOKEN and TWILIO_NUMBER and abogado_tel:
        try:
            tw = Client(TWILIO_SID, TWILIO_TOKEN)
            tw.messages.create(
                from_=TWILIO_NUMBER,
                to=f"whatsapp:{abogado_tel}",
                body=(
                    f"⚖️ Nuevo Lead asignado\n"
                    f"Nombre: {nombre}\n"
                    f"Tel: {tel}\n"
                    f"Tipo: {'Despido' if tipo_caso=='1' else 'Renuncia'}\n"
                    f"Salario: ${salario:,.2f}\n"
                    f"Monto estimado: ${monto:,.2f}\n"
                    f"Informe: {link_reporte}\n"
                    f"ID Lead: {lead_id}"
                )
            )
        except:
            pass

    # IMPORTANTE: regresamos el texto para que el webhook lo mande al cliente
    return msg_out
