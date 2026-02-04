# worker_jobs.py
import os
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo

from twilio.rest import Client

from utils.sheets import (
    open_spreadsheet, open_worksheet, with_backoff,
    build_header_map, col_idx, find_row_by_value, update_row_cells,
    get_all_values_safe, row_to_dict
)

MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City").strip() or "America/Mexico_City")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOG  = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS   = os.environ.get("TAB_SYS", "Config_Sistema").strip()

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "").strip()  # whatsapp:+1415...

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _wa_to(to_raw: str) -> str:
    t = (to_raw or "").strip()
    return t if t.startswith("whatsapp:") else "whatsapp:" + t

def send_whatsapp(to_number: str, body: str):
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_NUMBER):
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_WHATSAPP_NUMBER.")
    c = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    c.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=_wa_to(to_number), body=body)

def money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except Exception:
        return 0.0

def safe_int(s: str) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return 0

def read_sys_config(ws_sys) -> dict:
    values = get_all_values_safe(ws_sys)
    if not values or len(values) < 2:
        return {}
    hdr = values[0]
    out = {}
    for r in values[1:]:
        d = row_to_dict(hdr, r)
        k = (d.get("Clave") or "").strip()
        v = (d.get("Valor") or "").strip()
        if k:
            out[k] = v
    return out

def pick_abogado(ws_abog, salario_mensual: float):
    h = build_header_map(ws_abog)
    rows = with_backoff(ws_abog.get_all_values)
    if not rows or len(rows) < 2:
        return ("A01", "Abogada asignada", "")

    def cell(r, name):
        c = col_idx(h, name)
        return (r[c-1] if c and c-1 < len(r) else "").strip()

    def is_active(r):
        v = cell(r, "Activo").upper()
        return v in ("SI", "SÍ", "TRUE", "1")

    # regla VIP por salario
    if salario_mensual >= 50000:
        for r in rows[1:]:
            if cell(r, "ID_Abogado") == "A01" and is_active(r):
                return ("A01", cell(r, "Nombre_Abogado") or "Abogada A01", cell(r, "Telefono_Abogado"))

    candidates = []
    for r in rows[1:]:
        aid = cell(r, "ID_Abogado")
        if not aid or not is_active(r):
            continue
        load_raw = cell(r, "Leads_Asignados_Hoy")
        try:
            load = int(float(load_raw or "0"))
        except Exception:
            load = 0
        candidates.append((load, r))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        r = candidates[0][1]
        aid = cell(r, "ID_Abogado")
        return (aid, cell(r, "Nombre_Abogado") or f"Abogada {aid}", cell(r, "Telefono_Abogado"))

    # fallback A01 aunque no esté Activo
    for r in rows[1:]:
        if cell(r, "ID_Abogado") == "A01":
            return ("A01", cell(r, "Nombre_Abogado") or "Abogada A01", cell(r, "Telefono_Abogado"))

    return ("A01", "Abogada asignada", "")

def years_of_service(ini: date, fin: date) -> float:
    days = max((fin - ini).days, 0)
    return days / 365.0 if days else 0.0

def vacation_days_by_years(y: int) -> int:
    if y <= 0:
        return 0
    if y == 1: return 12
    if y == 2: return 14
    if y == 3: return 16
    if y == 4: return 18
    if y == 5: return 20
    extra_blocks = (y - 6) // 5 + 1
    return 20 + 2 * extra_blocks

def calc_estimacion(tipo_caso: str, salario_mensual: float, ini: date, fin: date) -> str:
    sd = salario_mensual / 30.0 if salario_mensual else 0.0
    y = years_of_service(ini, fin)
    y_int = max(int(y), 1) if y > 0 else 0

    start_year = date(fin.year, 1, 1)
    from_dt = max(start_year, ini)
    days_in_year = max((fin - from_dt).days, 0)

    aguinaldo_days = 15
    prima_vac = 0.25

    aguinaldo_prop = sd * aguinaldo_days * (days_in_year / 365.0) if sd else 0.0
    vac_days = vacation_days_by_years(y_int)
    vacaciones_prop = sd * vac_days * (days_in_year / 365.0) if sd else 0.0
    prima_vac_prop = vacaciones_prop * prima_vac

    if str(tipo_caso).strip() == "1":
        ind_3m = sd * 90
        ind_20 = sd * 20 * y
        total = ind_3m + ind_20 + aguinaldo_prop + vacaciones_prop + prima_vac_prop

        return (
            "📌 *Estimación preliminar (informativa)*\n"
            f"• Antigüedad estimada: {y:.2f} años\n"
            f"• 3 meses (90 días): ${ind_3m:,.2f}\n"
            f"• 20 días por año: ${ind_20:,.2f}\n"
            f"• Aguinaldo proporcional: ${aguinaldo_prop:,.2f}\n"
            f"• Vacaciones proporcionales: ${vacaciones_prop:,.2f}\n"
            f"• Prima vacacional proporcional: ${prima_vac_prop:,.2f}\n"
            f"✅ *Total estimado:* ${total:,.2f}\n\n"
            "Nota: puede variar por salario integrado, prima de antigüedad (topada), salarios caídos y otras prestaciones."
        )

    total = aguinaldo_prop + vacaciones_prop + prima_vac_prop
    return (
        "📌 *Estimación preliminar (informativa)*\n"
        "En renuncia normalmente procede *finiquito*: aguinaldo proporcional, "
        "vacaciones proporcionales/no gozadas y prima vacacional (más pagos pendientes si existieran).\n\n"
        f"• Aguinaldo proporcional (aprox): ${aguinaldo_prop:,.2f}\n"
        f"• Vacaciones proporcionales (aprox): ${vacaciones_prop:,.2f}\n"
        f"• Prima vacacional (aprox): ${prima_vac_prop:,.2f}\n"
        f"✅ *Subtotal estimado:* ${total:,.2f}\n\n"
        "Nota: puede variar según recibos, prestaciones reales y pagos pendientes."
    )

def build_resumen_largo(tipo_caso: str, nombre: str) -> str:
    if str(tipo_caso).strip() == "1":
        return (
            f"{nombre}, lamento mucho lo que estás viviendo. Gracias por contarnos tu situación.\n\n"
            "📌 *Lo más importante:* tus derechos laborales importan y vamos a acompañarte paso a paso.\n\n"
            "En términos generales, ante un despido el patrón debe acreditar causa legal y cumplir formalidades. "
            "Cuando no se acredita, normalmente se reclama indemnización o reinstalación, además de prestaciones pendientes.\n\n"
            "⚖️ Esta orientación es *informativa* (no es asesoría legal). Una abogada revisará tu caso con detalle."
        )
    return (
        f"{nombre}, gracias por confiar en nosotros.\n\n"
        "📌 *Lo más importante:* aunque sea renuncia, conservas derechos. Usualmente corresponde finiquito "
        "(proporcionales de aguinaldo, vacaciones y prima vacacional, además de pagos pendientes si existieran).\n\n"
        "⚖️ Esta orientación es *informativa* (no es asesoría legal). Una abogada revisará tu caso."
    )

def _parse_date_parts(h, vals, prefix: str) -> date:
    def get(name):
        c = col_idx(h, name)
        return (vals[c-1] if c and c-1 < len(vals) else "").strip()

    y = safe_int(get(f"{prefix}_Anio"))
    m = safe_int(get(f"{prefix}_Mes"))
    d = safe_int(get(f"{prefix}_Dia"))

    if y < 1900 or y > 2100:
        raise ValueError(f"{prefix}: año inválido ({y})")
    if m < 1 or m > 12:
        raise ValueError(f"{prefix}: mes inválido ({m})")
    if d < 1 or d > 31:
        raise ValueError(f"{prefix}: día inválido ({d})")
    return date(y, m, d)

def process_lead(lead_id: str):
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    ws_abog  = open_worksheet(sh, TAB_ABOG)
    ws_sys   = open_worksheet(sh, TAB_SYS)

    row = find_row_by_value(ws_leads, "ID_Lead", lead_id)
    if not row:
        raise RuntimeError(f"Lead no encontrado: {lead_id}")

    h = build_header_map(ws_leads)
    vals = with_backoff(ws_leads.row_values, row)

    def get(name):
        c = col_idx(h, name)
        return (vals[c-1] if c and c-1 < len(vals) else "").strip()

    update_row_cells(ws_leads, row, {
        "Procesar_AI_Status": "RUNNING",
        "Ultimo_Error": "",
        "Ultima_Actualizacion": now_iso(),
    }, hmap=h)

    syscfg = read_sys_config(ws_sys)

    try:
        telefono = get("Telefono")
        nombre = get("Nombre") or "Hola"
        tipo_caso = get("Tipo_Caso")
        salario = money_to_float(get("Salario_Mensual"))

        ini = _parse_date_parts(h, vals, "Inicio")
        fin = _parse_date_parts(h, vals, "Fin")
        if fin < ini:
            raise ValueError("Fecha fin es menor a fecha inicio.")

        abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abog, salario)

        resumen = build_resumen_largo(tipo_caso, nombre)
        estimacion = calc_estimacion(tipo_caso, salario, ini, fin)

        token = uuid.uuid4().hex[:18]
        base_url = (syscfg.get("RUTA_REPORTE") or syscfg.get("BASE_URL_WEB") or "").strip()
        if base_url and not base_url.endswith("/"):
            base_url += "/"
        link_reporte = f"{base_url}?token={token}" if base_url else ""

        link_abog = ""
        if abogado_tel:
            tnorm = "".join([c for c in abogado_tel if c.isdigit() or c == "+"])
            if tnorm:
                link_abog = f"https://wa.me/{tnorm.replace('+','')}"
        # Busca esta línea (aprox. 262) y reemplázala:
    mensaje_final = (
        f"✅ {nombre}, ya tengo una *estimación preliminar*.\n\n"
        f"{resumen}\n\n"
        f"{estimacion}\n\n"
        f"👩‍⚖️ La abogada que acompañará tu caso será: *{abogado_nombre}*.\n"
        "Te contactaremos lo antes posible para revisar detalles.\n\n"
       + (f"📄 Reporte en web: {link_reporte}\n\n" if link_reporte else "")
       + "Si quieres, escribe *menu* para ver opciones."
        )
      

        update_row_cells(ws_leads, row, {
            "Resultado_Calculo": estimacion,
            "Analisis_AI": resumen,
            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Procesar_AI_Status": "DONE",
            "ESTATUS": "CLIENTE_MENU",
            "Ultimo_Error": "",
            "Ultima_Actualizacion": now_iso(),
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Link_WhatsApp": link_abog,
        }, hmap=h)

        send_whatsapp(telefono, mensaje_final)

        menu = (
            f"Hola {nombre} 👋 Estoy contigo.\n\n"
            "¿Qué opción deseas?\n"
            "1️⃣ Próximas fechas agendadas\n"
            "2️⃣ Resumen de mi caso hasta hoy\n"
            "3️⃣ Contactar a mi abogada\n\n"
            "Tip: también puedes escribir *menu* en cualquier momento."
        )
        send_whatsapp(telefono, menu)

        return {"ok": True, "lead_id": lead_id}

    except Exception as e:
        update_row_cells(ws_leads, row, {
            "Procesar_AI_Status": "FAILED",
            "Ultimo_Error": f"{type(e).__name__}: {e}",
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)
        raise
