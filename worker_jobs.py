# worker_jobs.py
import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from gspread.utils import rowcol_to_a1

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff

# Import flexible para no fallar por nombre de archivo
_send = None
try:
    from whatsapp import send_whatsapp_message as _send
except:
    try:
        from whastapp import send_whatsapp_message as _send
    except:
        _send = None

try:
    from openai import OpenAI
except:
    OpenAI = None

from ai import generar_resumen_legal_empatico

TZ = os.environ.get("TZ", "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS  = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_LOGS   = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOG   = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_KNOW   = os.environ.get("TAB_KNOW", "Conocimiento_AI").strip()
TAB_PARAM  = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()
TAB_SYS    = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_GEST   = os.environ.get("TAB_GEST", "Gestion_Abogados").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

def now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

def money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except:
        return 0.0

def safe_name(nombre: str) -> str:
    n = (nombre or "").strip()
    return (n[:1].upper() + n[1:]) if n else "Hola"

def batch_update(ws, a1_to_value: dict):
    if not a1_to_value:
        return
    data = [{"range": a1, "values": [[str(v)]]} for a1, v in a1_to_value.items()]
    with_backoff(ws.batch_update, data, value_input_option="USER_ENTERED")

def update_row_fields(ws, row_num: int, updates: dict):
    header = with_backoff(ws.row_values, 1)
    if not header:
        return
    a1 = {}
    for k, v in updates.items():
        if k in header:
            col = header.index(k) + 1
            a1[rowcol_to_a1(row_num, col)] = v
    batch_update(ws, a1)

def safe_append_log(ws_logs, payload: dict):
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
        setv("Canal", payload.get("Canal", "SISTEMA"))
        setv("Fuente_Lead", payload.get("Fuente_Lead", ""))
        setv("Modelo_AI", payload.get("Modelo_AI", OPENAI_MODEL))
        setv("Errores", payload.get("Errores", ""))
        with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")
    except:
        return

def load_params(ws_param):
    rows = with_backoff(ws_param.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("Concepto", "")).strip()
        v = str(r.get("Valor", "")).strip()
        if k:
            out[k] = v
    return out

def load_sys(ws_sys):
    rows = with_backoff(ws_sys.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("Clave", "")).strip()
        v = str(r.get("Valor", "")).strip()
        if k:
            out[k] = v
    return out

def is_active(a: dict) -> bool:
    return str(a.get("Activo", "")).strip().upper() in ("SI", "SÍ", "1", "TRUE")

def acepta_tipo(a: dict, tipo_caso: str) -> bool:
    # tipo_caso: "1" despido, "2" renuncia
    if tipo_caso == "1":
        return str(a.get("Acepta_Casos_Despido", "SI")).strip().upper() in ("SI", "SÍ", "1", "TRUE", "")
    if tipo_caso == "2":
        return str(a.get("Acepta_Casos_Renuncia", "SI")).strip().upper() in ("SI", "SÍ", "1", "TRUE", "")
    return True

def leads_hoy(a: dict) -> int:
    try:
        return int(str(a.get("Leads_Asignados_Hoy", "0")).strip() or "0")
    except:
        return 0

def pick_abogado(rows_abog: list[dict], tipo_caso: str, salario: float) -> dict:
    # Regla: salario >= 50k => A01 (si activo)
    if salario >= 50000:
        for a in rows_abog:
            if str(a.get("ID_Abogado", "")).strip() == "A01" and is_active(a) and acepta_tipo(a, tipo_caso):
                return a

    # fallback: activo con menos leads hoy
    candidatos = [a for a in rows_abog if is_active(a) and acepta_tipo(a, tipo_caso)]
    if candidatos:
        candidatos.sort(key=leads_hoy)
        return candidatos[0]

    return rows_abog[0] if rows_abog else {}

def calc_estimacion_preliminar(tipo_caso: str, salario_mensual: float, params: dict):
    daily = salario_mensual / 30.0 if salario_mensual > 0 else 0.0

    indemn_dias = float(str(params.get("Indemnizacion", "90")).replace("%","") or 90)
    veinte = float(str(params.get("Veinte_Dias_Por_Anio", "20")).replace("%","") or 20)
    prima_ant = float(str(params.get("Prima_Antiguedad", "12")).replace("%","") or 12)
    agui = float(str(params.get("Aguinaldo", "15")).replace("%","") or 15)

    prestaciones = daily * (agui * 0.5)  # conservador

    if tipo_caso == "1":  # despido
        indemn = daily * indemn_dias
        extra = daily * (veinte * 1.0) + daily * (prima_ant * 1.0)
        total = max(0.0, indemn + extra + prestaciones)
        desglose = (
            f"• Indemnización (aprox.): ${indemn:,.0f} MXN\n"
            f"• 20 días/año + prima antigüedad (aprox.): ${extra:,.0f} MXN\n"
            f"• Prestaciones proporcionales (aprox.): ${prestaciones:,.0f} MXN\n"
            "Nota: estimación preliminar informativa; el cálculo final depende de SDI, antigüedad real y prestaciones acreditadas."
        )
        return total, desglose

    total = max(0.0, prestaciones)
    desglose = (
        f"• Prestaciones proporcionales (aprox.): ${prestaciones:,.0f} MXN\n"
        "Nota: estimación preliminar; puede variar por vacaciones, aguinaldo real, comisiones u otros adeudos."
    )
    return total, desglose

def append_gestion(ws_gest, lead: dict, monto_txt: str, abogado: dict):
    header = with_backoff(ws_gest.row_values, 1)
    if not header:
        return
    row = [""] * len(header)
    def setv(col, val):
        if col in header:
            row[header.index(col)] = str(val)

    setv("ID_Gestion", str(uuid.uuid4())[:10])
    setv("Fecha_Asignacion", now_iso())
    setv("ID_Lead", lead.get("ID_Lead", ""))
    setv("Telefono_Lead", lead.get("Telefono", ""))
    setv("Nombre_Lead", safe_name(lead.get("Nombre", "")))
    setv("Monto_Estimado", monto_txt)
    setv("Abogado_Asignado_ID", abogado.get("ID_Abogado", "A01"))
    setv("Abogado_Asignado_Nombre", abogado.get("Nombre_Abogado", "Abogada asignada"))
    setv("Abogado_Asignado_Telefono", abogado.get("Telefono_Abogado", ""))
    setv("Estatus_Interno", "NUEVO")

    with_backoff(ws_gest.append_row, row, value_input_option="USER_ENTERED")

def process_lead(lead_id: str):
    sh = open_spreadsheet(GOOGLE_SHEET_NAME)

    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)

    ws_logs = None
    try:
        ws_logs = open_worksheet(sh, TAB_LOGS)
    except:
        ws_logs = None

    ws_know = ws_param = ws_sys = ws_gest = None
    try: ws_know = open_worksheet(sh, TAB_KNOW)
    except: pass
    try: ws_param = open_worksheet(sh, TAB_PARAM)
    except: pass
    try: ws_sys = open_worksheet(sh, TAB_SYS)
    except: pass
    try: ws_gest = open_worksheet(sh, TAB_GEST)
    except: pass

    leads = with_backoff(ws_leads.get_all_records)
    idx = None
    lead = None
    for i, r in enumerate(leads):
        if str(r.get("ID_Lead", "")).strip() == str(lead_id).strip():
            idx = i
            lead = r
            break
    if idx is None:
        return {"ok": False, "error": f"Lead no encontrado: {lead_id}"}

    row_num = idx + 2
    estatus = str(lead.get("ESTATUS","")).strip()
    if estatus != "EN_PROCESO":
        return {"ok": True, "msg": "No requiere procesamiento", "estatus": estatus}

    telefono = (lead.get("Telefono") or "").strip()
    nombre = safe_name(lead.get("Nombre") or "")
    tipo_caso = str(lead.get("Tipo_Caso","")).strip()  # "1" despido, "2" renuncia
    salario = money_to_float(lead.get("Salario_Mensual","0"))
    descripcion = (lead.get("Descripcion_Situacion") or "").strip()

    conocimiento = with_backoff(ws_know.get_all_records) if ws_know else []
    params = load_params(ws_param) if ws_param else {}
    syskv = load_sys(ws_sys) if ws_sys else {}

    # token + link reporte
    token = (lead.get("Token_Reporte") or "").strip() or str(uuid.uuid4())[:18]
    ruta_reporte = (syskv.get("RUTA_REPORTE") or "").strip()
    base_url = (syskv.get("BASE_URL_WEB") or "").strip()
    if not ruta_reporte and base_url:
        ruta_reporte = base_url.rstrip("/") + "/reporte"
    link_reporte = f"{ruta_reporte}?token={token}" if ruta_reporte else ""

    # abogado
    abogs = with_backoff(ws_abog.get_all_records)
    elegido = pick_abogado(abogs, tipo_caso, salario) if abogs else {}
    abogado_id = (elegido.get("ID_Abogado") or "A01").strip() or "A01"
    abogado_nombre = (elegido.get("Nombre_Abogado") or "Abogada asignada").strip() or "Abogada asignada"

    # cálculo
    monto, desglose = calc_estimacion_preliminar(tipo_caso, salario, params)
    monto_txt = f"${monto:,.0f} MXN"

    # AI resumen empático largo
    ai_client = None
    if OPENAI_API_KEY and OpenAI:
        ai_client = OpenAI(api_key=OPENAI_API_KEY)

    tipo_txt = "despido" if tipo_caso == "1" else "renuncia"
    resumen = generar_resumen_legal_empatico(
        ai_client=ai_client,
        model=OPENAI_MODEL,
        tipo_txt=tipo_txt,
        descripcion_usuario=descripcion,
        conocimiento_rows=conocimiento,
        max_words=290,
    )

    # update leads
    updates = {
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Resultado_Calculo": monto_txt,
        "Analisis_AI": resumen,
        "Token_Reporte": token,
        "Link_Reporte_Web": link_reporte,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": now_iso(),
        "Ultimo_Error": "",
        "Es_Cliente": "SI",
    }
    update_row_fields(ws_leads, row_num, updates)

    # Gestion_Abogados (si existe)
    try:
        if ws_gest:
            append_gestion(ws_gest, {**lead, **updates}, monto_txt, elegido)
    except:
        pass

    # mensaje final (humano y largo, sin pedir correo)
    msg_out = (
        f"Hola {nombre} 👋\n\n"
        "Gracias por escribirnos. Entiendo que estos momentos pueden sentirse difíciles y hasta injustos; "
        "de verdad queremos que sepas que aquí te vamos a acompañar con calma, respeto y estrategia.\n\n"
        f"📌 Estimación preliminar (informativa): *{monto_txt}*\n"
        f"👩‍⚖️ La abogada que acompañará tu caso será: *{abogado_nombre}*\n\n"
        "Con base en lo que nos compartiste, revisaremos prestaciones devengadas y, si aplica, la indemnización correspondiente. "
        "En términos generales, en despido se analiza lo relativo a causales y consecuencias previstas en la LFT (por ejemplo, arts. 47 y 48, según el caso); "
        "en renuncia se revisa finiquito y pagos proporcionales.\n\n"
        f"📄 Tu reporte con desglose: {link_reporte}\n\n"
        "Si quieres, responde con una opción:\n"
        "1️⃣ Próximas fechas agendadas\n"
        "2️⃣ Resumen de mi caso hasta hoy\n"
        "3️⃣ Contactar a mi abogado\n\n"
        f"{desglose}"
    )

    # enviar por whatsapp si disponible
    if _send and telefono:
        try:
            _send(telefono, msg_out[:1500])
        except Exception as e:
            if ws_logs:
                safe_append_log(ws_logs, {"Telefono": telefono, "ID_Lead": lead_id, "Paso": "ENVIO_RESULTADOS_ERROR", "Mensaje_Saliente": "No pude enviar WhatsApp desde worker.", "Errores": str(e)})

    if ws_logs:
        safe_append_log(ws_logs, {"Telefono": telefono, "ID_Lead": lead_id, "Paso": "PROCESADO_OK", "Mensaje_Saliente": f"Asignado {abogado_id} {abogado_nombre} | {monto_txt}"})

    return {"ok": True, "lead_id": lead_id, "abogado": abogado_nombre, "monto": monto_txt}
