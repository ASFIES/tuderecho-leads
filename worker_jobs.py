# worker_jobs.py
import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from gspread.utils import rowcol_to_a1
from openai import OpenAI

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff
from utils.text import money_to_float, safe_name
from whatsapp import send_whatsapp_message
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

def _now_iso():
    return datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

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
    setv("Canal", payload.get("Canal", "SISTEMA"))
    setv("Fuente_Lead", payload.get("Fuente_Lead", ""))
    setv("Modelo_AI", payload.get("Modelo_AI", OPENAI_MODEL))
    setv("Errores", payload.get("Errores", ""))

    with_backoff(ws_logs.append_row, row, value_input_option="USER_ENTERED")

def _load_params(ws_param):
    rows = with_backoff(ws_param.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("Concepto", "")).strip()
        v = str(r.get("Valor", "")).strip()
        if k:
            out[k] = v
    return out

def _load_sys(ws_sys):
    rows = with_backoff(ws_sys.get_all_records)
    out = {}
    for r in rows:
        k = str(r.get("Clave", "")).strip()
        v = str(r.get("Valor", "")).strip()
        if k:
            out[k] = v
    return out

def _is_active(a: dict) -> bool:
    return str(a.get("Activo", "")).strip().upper() in ("SI", "SÍ", "1", "TRUE")

def _acepta_tipo(a: dict, tipo_caso: str) -> bool:
    # tipo_caso: "1" despido, "2" renuncia
    if tipo_caso == "1":
        k = str(a.get("Acepta_Casos_Despido", "")).strip().upper()
        return k in ("", "SI", "SÍ", "1", "TRUE")
    if tipo_caso == "2":
        k = str(a.get("Acepta_Casos_Renuncia", "")).strip().upper()
        return k in ("", "SI", "SÍ", "1", "TRUE")
    return True

def _leads_hoy(a: dict) -> int:
    try:
        return int(str(a.get("Leads_Asignados_Hoy", "0")).strip() or "0")
    except:
        return 0

def _pick_abogado(rows_abog: list[dict], tipo_caso: str, salario: float) -> dict:
    # Regla principal: salario >= 50,000 -> A01 si activo
    if salario >= 50000:
        for a in rows_abog:
            if str(a.get("ID_Abogado", "")).strip() == "A01" and _is_active(a) and _acepta_tipo(a, tipo_caso):
                return a

    # fallback: activo con menos leads hoy
    candidatos = [a for a in rows_abog if _is_active(a) and _acepta_tipo(a, tipo_caso)]
    if candidatos:
        candidatos.sort(key=_leads_hoy)
        return candidatos[0]

    return rows_abog[0] if rows_abog else {}

def _calc_estimacion_preliminar(tipo_caso: str, salario_mensual: float, params: dict):
    """
    Estimación preliminar informativa.
    Basado en Parametros_Legales si existen:
    - Indemnizacion (días, default 90)
    - Veinte_Dias_Por_Anio (default 20)
    - Prima_Antiguedad (default 12)
    - Aguinaldo (default 15)
    Nota: cálculo final depende de SDI, antigüedad real, prestaciones y pruebas.
    """
    daily = salario_mensual / 30.0 if salario_mensual > 0 else 0.0

    indemn_dias = float(str(params.get("Indemnizacion", "90")).replace("%","") or 90)
    veinte = float(str(params.get("Veinte_Dias_Por_Anio", "20")).replace("%","") or 20)
    prima_ant = float(str(params.get("Prima_Antiguedad", "12")).replace("%","") or 12)
    agui = float(str(params.get("Aguinaldo", "15")).replace("%","") or 15)

    # prestaciones mínimas conservadoras (aguinaldo proporcional aproximado)
    prestaciones = daily * (agui * 0.5)

    if tipo_caso == "1":  # despido
        indemn = daily * indemn_dias
        # aproximación 1 año para 20 días + prima antigüedad si no tenemos años precisos
        extra = daily * (veinte * 1.0) + daily * (prima_ant * 1.0)
        total = max(0.0, indemn + extra + prestaciones)
        desglose = (
            f"• Indemnización constitucional (aprox.): ${indemn:,.0f} MXN\n"
            f"• 20 días por año + prima antigüedad (aprox.): ${extra:,.0f} MXN\n"
            f"• Prestaciones proporcionales (aprox.): ${prestaciones:,.0f} MXN\n"
            "Nota: es una estimación preliminar informativa; el cálculo final depende de salario diario integrado, antigüedad real y prestaciones acreditadas."
        )
        return total, desglose

    # renuncia (finiquito)
    total = max(0.0, prestaciones)
    desglose = (
        f"• Prestaciones proporcionales (aprox.): ${prestaciones:,.0f} MXN\n"
        "Nota: es una estimación preliminar; puede variar según vacaciones, aguinaldo real, comisiones u otros adeudos."
    )
    return total, desglose

def _append_gestion(ws_gest, lead: dict, monto_txt: str, abogado: dict):
    """
    Inserta en Gestion_Abogados por header, sin depender del orden exacto.
    """
    header = with_backoff(ws_gest.row_values, 1)
    row = [""] * len(header)

    def setv(col, val):
        if col in header:
            row[header.index(col)] = str(val)

    setv("ID_Gestion", str(uuid.uuid4())[:10])
    setv("Fecha_Asignacion", _now_iso())
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
    ws_logs  = open_worksheet(sh, TAB_LOGS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)

    # opcionales
    ws_know = None
    ws_param = None
    ws_sys = None
    ws_gest = None

    try:
        ws_know = open_worksheet(sh, TAB_KNOW)
    except:
        pass
    try:
        ws_param = open_worksheet(sh, TAB_PARAM)
    except:
        pass
    try:
        ws_sys = open_worksheet(sh, TAB_SYS)
    except:
        pass
    try:
        ws_gest = open_worksheet(sh, TAB_GEST)
    except:
        pass

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
    telefono = (lead.get("Telefono") or "").strip()
    nombre = safe_name(lead.get("Nombre") or "")
    fuente = (lead.get("Fuente_Lead") or "").strip()

    # solo procesar EN_PROCESO
    if str(lead.get("ESTATUS", "")).strip() not in ("EN_PROCESO",):
        return {"ok": True, "msg": "No requiere procesamiento", "estatus": lead.get("ESTATUS")}

    tipo_caso = str(lead.get("Tipo_Caso", "")).strip()  # "1" despido, "2" renuncia
    salario = money_to_float(lead.get("Salario_Mensual", "0"))

    # carga sheets auxiliares
    conocimiento = with_backoff(ws_know.get_all_records) if ws_know else []
    params = _load_params(ws_param) if ws_param else {}
    syskv = _load_sys(ws_sys) if ws_sys else {}

    # ruta reporte por token
    ruta_reporte = (syskv.get("RUTA_REPORTE") or "").strip()
    base_url = (syskv.get("BASE_URL_WEB") or "").strip()
    if not ruta_reporte and base_url:
        ruta_reporte = base_url.rstrip("/") + "/reporte/"
    if ruta_reporte and not ruta_reporte.endswith("/"):
        ruta_reporte += "/"

    token = (lead.get("Token_Reporte") or "").strip() or str(uuid.uuid4())[:18]
    link_reporte = (ruta_reporte + f"?token={token}") if ruta_reporte else ""

    # escoger abogado
    abogs = with_backoff(ws_abog.get_all_records)
    elegido = _pick_abogado(abogs, tipo_caso, salario) if abogs else {}

    abogado_id = (elegido.get("ID_Abogado") or "A01").strip() or "A01"
    abogado_nombre = (elegido.get("Nombre_Abogado") or "Abogada asignada").strip() or "Abogada asignada"
    abogado_tel = (elegido.get("Telefono_Abogado") or "").strip()

    # cálculo preliminar
    monto, desglose = _calc_estimacion_preliminar(tipo_caso, salario, params)
    monto_txt = f"${monto:,.0f} MXN"

    # resumen humano (más largo) + base legal general
    ai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
    tipo_txt = "despido" if tipo_caso == "1" else "renuncia"
    descripcion = (lead.get("Descripcion_Situacion") or "").strip()

    resumen = generar_resumen_legal_empatico(
        ai_client=ai_client,
        model=OPENAI_MODEL,
        tipo_txt=tipo_txt,
        descripcion_usuario=descripcion,
        conocimiento_rows=conocimiento,
        max_words=280,
    )

    # guardar en BD_Leads
    updates = {
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Resultado_Calculo": monto_txt,
        "Analisis_AI": resumen,
        "Token_Reporte": token,
        "Link_Reporte_Web": link_reporte,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": _now_iso(),
        "Ultimo_Error": "",
        "Es_Cliente": "SI",
    }
    _update_row_fields(ws_leads, row_num, updates)

    # sumar lead al abogado (si existe columna)
    try:
        header_ab = with_backoff(ws_abog.row_values, 1)
        if "ID_Abogado" in header_ab and "Leads_Asignados_Hoy" in header_ab:
            ab_rows = with_backoff(ws_abog.get_all_records)
            ab_idx = None
            for i, a in enumerate(ab_rows):
                if str(a.get("ID_Abogado","")).strip() == abogado_id:
                    ab_idx = i
                    break
            if ab_idx is not None:
                rnum = ab_idx + 2
                cur = _leads_hoy(ab_rows[ab_idx])
                col = header_ab.index("Leads_Asignados_Hoy") + 1
                _batch_update(ws_abog, {rowcol_to_a1(rnum, col): str(cur + 1)})
    except:
        pass

    # insertar en Gestion_Abogados (si existe)
    try:
        if ws_gest:
            _append_gestion(ws_gest, {**lead, **updates}, monto_txt, {
                "ID_Abogado": abogado_id,
                "Nombre_Abogado": abogado_nombre,
                "Telefono_Abogado": abogado_tel
            })
    except:
        pass

    # Mensaje final al cliente (tono humano + base general)
    msg_out = (
        f"Hola {nombre} 👋\n\n"
        "Gracias por confiar en nosotros. Entiendo que esta situación puede sentirse pesada y confusa, "
        "pero no estás solo/a: vamos a acompañarte con seriedad y respeto.\n\n"
        f"📌 Estimación preliminar (informativa): *{monto_txt}*\n"
        f"👩‍⚖️ La abogada que llevará tu caso será: *{abogado_nombre}*\n\n"
        "De forma general, en materia laboral se revisan prestaciones devengadas y, si aplica, "
        "la indemnización conforme a la Ley Federal del Trabajo (por ejemplo, en despido se consideran criterios relacionados con los arts. 47 y 48, dependiendo del caso).\n\n"
        f"📄 Tu reporte con desglose: {link_reporte}\n\n"
        "Si quieres, responde aquí con:\n"
        "1) Próximas fechas agendadas\n"
        "2) Resumen de mi caso hasta hoy\n"
        "3) Contactar a mi abogado\n\n"
        f"{desglose}"
    )

    # OJO: envío por sesión (si ya estás dentro de ventana de conversación)
    try:
        if telefono:
            send_whatsapp_message(telefono, msg_out[:1500])
    except Exception as e:
        _append_log(ws_logs, {
            "Telefono": telefono,
            "ID_Lead": lead_id,
            "Paso": "ENVIO_RESULTADOS_ERROR",
            "Mensaje_Saliente": "No pude enviar WhatsApp al cliente desde worker.",
            "Fuente_Lead": fuente,
            "Errores": str(e),
        })

    _append_log(ws_logs, {
        "Telefono": telefono,
        "ID_Lead": lead_id,
        "Paso": "PROCESADO_OK",
        "Mensaje_Saliente": f"Asignado {abogado_id} {abogado_nombre} | {monto_txt}",
        "Fuente_Lead": fuente,
    })

    return {"ok": True, "lead_id": lead_id, "abogado": abogado_nombre, "monto": monto_txt}
