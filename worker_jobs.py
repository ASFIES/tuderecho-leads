import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

from twilio.rest import Client

from utils.sheets import open_spreadsheet, open_worksheet, with_backoff, build_header_map, col_idx, find_row_by_value, update_row_cells

MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City"))

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOG  = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
WHATSAPP_NUMBER    = os.environ.get("WHATSAPP_NUMBER", "").strip()  # whatsapp:+1415...

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _wa_to(to_raw: str) -> str:
    t = (to_raw or "").strip()
    return t if t.startswith("whatsapp:") else "whatsapp:" + t

def send_whatsapp(to_number: str, body: str):
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and WHATSAPP_NUMBER):
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / WHATSAPP_NUMBER.")
    c = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    c.messages.create(from_=WHATSAPP_NUMBER, to=_wa_to(to_number), body=body)

def money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except:
        return 0.0

def safe_int(s: str) -> int:
    try:
        return int(str(s).strip())
    except:
        return 0

def pick_abogado(ws_abog, salario_mensual: float):
    """
    Regla:
      salario >= 50000 => ID_Abogado A01 (si está activo).
      si no, primer activo.
    """
    h = build_header_map(ws_abog)
    rows = with_backoff(ws_abog.get_all_values)
    if not rows or len(rows) < 2:
        return ("A01", "Abogada asignada", "")

    def get_cell(r, name):
        c = col_idx(h, name)
        return (r[c-1] if c and c-1 < len(r) else "").strip()

    # 1) A01 si salario >= 50k
    if salario_mensual >= 50000:
        for r in rows[1:]:
            if get_cell(r, "ID_Abogado") == "A01":
                activo = get_cell(r, "Activo").upper()
                if activo in ("SI", "SÍ", "TRUE", "1"):
                    return ("A01", get_cell(r, "Nombre_Abogado") or "Abogada A01", get_cell(r, "Telefono_Abogado"))

    # 2) primer activo
    for r in rows[1:]:
        aid = get_cell(r, "ID_Abogado")
        if not aid:
            continue
        activo = get_cell(r, "Activo").upper() or "SI"
        if activo in ("SI", "SÍ", "TRUE", "1"):
            return (aid, get_cell(r, "Nombre_Abogado") or f"Abogada {aid}", get_cell(r, "Telefono_Abogado"))

    return ("A01", "Abogada asignada", "")

def build_resumen(tipo_caso: str, nombre: str) -> str:
    # más humano + base legal general (sin prometer)
    if str(tipo_caso).strip() == "1":
        return (
            f"{nombre}, lamento mucho que estés pasando por esto. No estás solo/a: te acompañaremos paso a paso.\n\n"
            "En casos de despido, la Ley Federal del Trabajo prevé que el patrón debe acreditar una causa (art. 47) "
            "y, si no lo hace, puede proceder indemnización o reinstalación (art. 48), además de prestaciones pendientes.\n\n"
            "Esta es una estimación preliminar e informativa. Una abogada revisará tu caso y te orientará con precisión."
        )
    return (
        f"{nombre}, gracias por confiar en nosotros. Entiendo que este tipo de cierre laboral puede ser pesado.\n\n"
        "En renuncia normalmente corresponde finiquito (proporcionales de aguinaldo, vacaciones, prima vacacional y pagos pendientes). "
        "Esta es información preliminar; una abogada revisará tu situación para darte claridad y acompañarte."
    )

def calc_estimacion_simple(tipo_caso: str, salario_mensual: float, ini: date, fin: date) -> str:
    daily = salario_mensual / 30.0 if salario_mensual else 0.0
    days = max((fin - ini).days, 0) if ini and fin else 0
    years = days / 365.0 if days else 0.0

    if str(tipo_caso).strip() == "1":
        ind_3m = daily * 90
        ind_20 = daily * 20 * years
        total = ind_3m + ind_20
        return (
            "📌 Estimación preliminar (informativa)\n"
            f"• Antigüedad estimada: {years:.2f} años\n"
            f"• 3 meses (90 días): ${ind_3m:,.2f}\n"
            f"• 20 días por año: ${ind_20:,.2f}\n"
            f"✅ Total estimado: ${total:,.2f}\n\n"
            "Nota: puede variar por salario integrado, prima de antigüedad, salarios caídos y prestaciones."
        )

    return (
        "📌 Estimación preliminar (informativa)\n"
        "En renuncia normalmente se calcula finiquito con proporcionales (aguinaldo, vacaciones, prima vacacional, etc.).\n"
        "Un abogado lo calculará con precisión según tus recibos y condiciones reales."
    )

def process_lead(lead_id: str):
    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)

    row = find_row_by_value(ws_leads, "ID_Lead", lead_id)
    if not row:
        raise RuntimeError(f"Lead no encontrado: {lead_id}")

    h = build_header_map(ws_leads)
    vals = with_backoff(ws_leads.row_values, row)

    def get(name):
        c = col_idx(h, name)
        return (vals[c-1] if c and c-1 < len(vals) else "").strip()

    telefono = get("Telefono")
    nombre = get("Nombre") or "Hola"
    tipo_caso = get("Tipo_Caso")  # "1" despido, "2" renuncia

    ini = date(safe_int(get("Inicio_Anio")), safe_int(get("Inicio_Mes")), safe_int(get("Inicio_Dia")))
    fin = date(safe_int(get("Fin_Anio")), safe_int(get("Fin_Mes")), safe_int(get("Fin_Dia")))
    salario = money_to_float(get("Salario_Mensual"))

    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abog, salario)

    resumen = build_resumen(tipo_caso, nombre)
    estimacion = calc_estimacion_simple(tipo_caso, salario, ini, fin)

    mensaje_final = (
        f"✅ {nombre}, ya tengo tu estimación preliminar.\n\n"
        f"{resumen}\n\n"
        f"{estimacion}\n\n"
        f"👩‍⚖️ La abogada que acompañará tu caso será: {abogado_nombre}.\n"
        "Si quieres, puedo ayudarte a dar el siguiente paso desde aquí."
    )

    # guardar en leads
    update_row_cells(ws_leads, row, {
        "Resultado_Calculo": estimacion,
        "Analisis_AI": resumen,
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Procesar_AI_Status": "DONE",
        "ESTATUS": "CLIENTE_MENU",
        "Ultimo_Error": "",
        "Ultima_Actualizacion": now_iso(),
    })

    # enviar WhatsApp
    send_whatsapp(telefono, mensaje_final)

    # menú final
    menu = (
        f"Hola {nombre} 👋 ¿Qué opción deseas?\n\n"
        "1️⃣ Próximas fechas agendadas\n"
        "2️⃣ Resumen de mi caso hasta hoy\n"
        "3️⃣ Contactar a mi abogado"
    )
    send_whatsapp(telefono, menu)

    return {"ok": True, "lead_id": lead_id}
