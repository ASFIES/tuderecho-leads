# worker_jobs.py
import os
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

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
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "").strip()  # whatsapp:+....

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _wa_addr(raw: str) -> str:
    """
    Normaliza dirección WhatsApp para Twilio.
    Acepta:
      - "whatsapp:+521..." (ok)
      - "+521..."         -> "whatsapp:+521..."
      - "521..."          -> "whatsapp:521..." (preferible que venga con +)
    """
    t = (raw or "").strip()
    if not t:
        return ""
    return t if t.startswith("whatsapp:") else "whatsapp:" + t

def _get_twilio_client() -> Client:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN.")
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def send_whatsapp_safe(to_number: str, body: str):
    """
    Envía WhatsApp y NO truena el job: regresa (ok, detail).
    """
    try:
        if not TWILIO_WHATSAPP_NUMBER:
            return (False, "Falta TWILIO_WHATSAPP_NUMBER.")

        client = _get_twilio_client()

        from_num = _wa_addr(TWILIO_WHATSAPP_NUMBER)  # 🔥 normaliza FROM
        to_num = _wa_addr(to_number)                  # 🔥 normaliza TO

        msg = client.messages.create(from_=from_num, to=to_num, body=body)
        return (True, f"SID={getattr(msg, 'sid', '')}")
    except TwilioRestException as e:
        # incluye code si existe
        code = getattr(e, "code", "")
        return (False, f"TwilioRestException {code}: {str(e)}")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")

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

def safe_float(s: str) -> float:
    try:
        return float(str(s).strip())
    except Exception:
        return 0.0

def clip_words(text: str, max_words: int) -> str:
    if not text:
        return ""
    w = text.split()
    if max_words and len(w) > max_words:
        return " ".join(w[:max_words]).rstrip() + "…"
    return text

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
    """
    - salario >= 50,000 => intenta A01 (si Activo)
    - si no, activo con menor Leads_Asignados_Hoy
    - si no hay activos, fallback A01
    """
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

def calc_estimacion(
    tipo_caso: str,
    salario_mensual: float,
    ini: date,
    fin: date,
    salario_min_diario: float = 0.0
):
    """
    Implementa tu estructura:
    Despido:
      (SDI*90) + (SDI*20*Años) + (SD_top*12*Años) + proporcionales
    Renuncia:
      proporcionales + (prima antigüedad si >=15 años)
    NOTA: SDI aquí se aproxima con SD (no tenemos integración real).
    """
    sd = salario_mensual / 30.0 if salario_mensual else 0.0
    sdi = sd  # aproximación

    y = years_of_service(ini, fin)
    y_int = int(y) if y > 0 else 0

    # Proporcionales del año de terminación (aprox)
    start_year = date(fin.year, 1, 1)
    from_dt = max(start_year, ini)
    days_in_year = max((fin - from_dt).days, 0)

    aguinaldo_days = 15
    prima_vac = 0.25

    aguinaldo_prop = sd * aguinaldo_days * (days_in_year / 365.0) if sd else 0.0

    vac_days = vacation_days_by_years(max(y_int, 1) if y > 0 else 0)
    vacaciones_prop = sd * vac_days * (days_in_year / 365.0) if sd else 0.0
    prima_vac_prop = vacaciones_prop * prima_vac

    # Prima antigüedad (topable a 2x salario mínimo diario si nos dan el mínimo)
    sd_top = sd
    if salario_min_diario and salario_min_diario > 0:
        sd_top = min(sd, 2.0 * salario_min_diario)

    prima_ant = sd_top * 12.0 * y if (sd_top and y > 0) else 0.0

    if str(tipo_caso).strip() == "1":
        ind_3m = sdi * 90.0
        ind_20 = sdi * 20.0 * y

        total = ind_3m + ind_20 + prima_ant + aguinaldo_prop + vacaciones_prop + prima_vac_prop

        texto = (
            "📌 *Estimación preliminar (informativa)*\n"
            f"• Antigüedad estimada: {y:.2f} años\n"
            f"• 3 meses (90 días): ${ind_3m:,.2f}\n"
            f"• 20 días por año: ${ind_20:,.2f}\n"
            f"• Prima de antigüedad (12 días/año): ${prima_ant:,.2f}\n"
            f"• Aguinaldo proporcional: ${aguinaldo_prop:,.2f}\n"
            f"• Vacaciones proporcionales: ${vacaciones_prop:,.2f}\n"
            f"• Prima vacacional proporcional: ${prima_vac_prop:,.2f}\n"
            f"✅ *Total estimado:* ${total:,.2f}\n\n"
            "Nota: puede variar por salario integrado real, salarios caídos y otras prestaciones."
        )
        return (texto, total)

    # Renuncia
    total = aguinaldo_prop + vacaciones_prop + prima_vac_prop
    prima_ant_ren = 0.0
    if y >= 15:
        prima_ant_ren = prima_ant
        total += prima_ant_ren

    texto = (
        "📌 *Estimación preliminar (informativa)*\n"
        "En renuncia normalmente procede *finiquito*: aguinaldo proporcional, "
        "vacaciones proporcionales/no gozadas y prima vacacional (más pagos pendientes si existieran).\n\n"
        f"• Aguinaldo proporcional (aprox): ${aguinaldo_prop:,.2f}\n"
        f"• Vacaciones proporcionales (aprox): ${vacaciones_prop:,.2f}\n"
        f"• Prima vacacional (aprox): ${prima_vac_prop:,.2f}\n"
        + (f"• Prima de antigüedad (≥15 años): ${prima_ant_ren:,.2f}\n" if prima_ant_ren else "")
        + f"✅ *Total estimado:* ${total:,.2f}\n\n"
        "Nota: puede variar según recibos, prestaciones reales y pagos pendientes."
    )
    return (texto, total)

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

    # RUNNING
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

        resumen_largo = build_resumen_largo(tipo_caso, nombre)

        # Resumen corto WhatsApp según Config_Sistema.MAX_PALABRAS_RESUMEN
        max_words = safe_int(syscfg.get("MAX_PALABRAS_RESUMEN") or "50")
        resumen_corto = clip_words(resumen_largo, max_words if max_words > 0 else 50)

        salario_min_diario = safe_float(syscfg.get("SALARIO_MIN_DIARIO") or "0")

        estimacion_txt, total_estimado = calc_estimacion(
            tipo_caso=tipo_caso,
            salario_mensual=salario,
            ini=ini,
            fin=fin,
            salario_min_diario=salario_min_diario
        )

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

        # Mensaje final (WhatsApp)
        mensaje_final = (
            f"✅ {nombre}, ya tengo una *estimación preliminar*.\n\n"
            f"{resumen_corto}\n\n"
            f"{estimacion_txt}\n\n"
            f"👩‍⚖️ Abogada asignada: *{abogado_nombre}*.\n"
        )
        if link_reporte:
            mensaje_final += f"\n📄 Reporte en web: {link_reporte}\n"
        mensaje_final += "\nSi quieres, escribe *menu* para ver opciones."

        menu = (
            f"Hola {nombre} 👋 Estoy contigo.\n\n"
            "¿Qué opción deseas?\n"
            "1️⃣ Próximas fechas agendadas\n"
            "2️⃣ Resumen de mi caso hasta hoy\n"
            "3️⃣ Contactar a mi abogada\n\n"
            "Tip: también puedes escribir *menu* en cualquier momento."
        )

        # 1) Guarda SIEMPRE el cálculo en Sheets
        update_row_cells(ws_leads, row, {
            "Resultado_Calculo": estimacion_txt,
            "Analisis_AI": resumen_largo,               # largo para el /reporte
            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Link_WhatsApp": link_abog,
            "Ultimo_Error": "",
            "Ultima_Actualizacion": now_iso(),
            # opcional si creas esta columna:
            "Total_Estimado": f"{total_estimado:.2f}",
        }, hmap=h)

        # 2) Envía WhatsApp (y si falla, no truena todo)
        ok1, det1 = send_whatsapp_safe(telefono, mensaje_final)
        ok2, det2 = send_whatsapp_safe(telefono, menu) if ok1 else (False, "skip_menu_por_error_previo")

        if ok1 and ok2:
            update_row_cells(ws_leads, row, {
                "Procesar_AI_Status": "DONE",
                "ESTATUS": "CLIENTE_MENU",
                "Ultimo_Error": "",
                "Ultima_Actualizacion": now_iso(),
            }, hmap=h)
        else:
            # Importante: si NO se pudo enviar, no lo pases a CLIENTE_MENU para no “romper” el chat
            err = f"send1={ok1}({det1}) send2={ok2}({det2})"
            update_row_cells(ws_leads, row, {
                "Procesar_AI_Status": "DONE_SEND_ERROR",
                "ESTATUS": "EN_PROCESO",
                "Ultimo_Error": err[:450],
                "Ultima_Actualizacion": now_iso(),
            }, hmap=h)

        return {"ok": True, "lead_id": lead_id, "send1": ok1, "send2": ok2}

    except Exception as e:
        update_row_cells(ws_leads, row, {
            "Procesar_AI_Status": "FAILED",
            "Ultimo_Error": f"{type(e).__name__}: {e}",
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)
        raise
