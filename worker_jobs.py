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
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "").strip()

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _wa_addr(raw: str) -> str:
    """
    Normaliza dirección WhatsApp para Twilio.
    Acepta:
      - "whatsapp:+521..." (ok)
      - "+521..."         -> "whatsapp:+521..."
      - "521..."          -> "whatsapp:+521..."  (evita fallas comunes)
    """
    t = (raw or "").strip()
    if not t:
        return ""
    if t.startswith("whatsapp:"):
        num = t.split(":", 1)[1].strip()
    else:
        num = t
    if num and num[0].isdigit() and not num.startswith("+"):
        num = "+" + num
    return "whatsapp:" + num

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
        msg = client.messages.create(
            from_=_wa_addr(TWILIO_WHATSAPP_NUMBER),
            to=_wa_addr(to_number),
            body=body
        )
        return (True, f"SID={getattr(msg, 'sid', '')}")
    except TwilioRestException as e:
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

def _filter_to_existing_columns(data: dict, hmap: dict) -> dict:
    """
    Evita errores si intentas guardar columnas que aún no existen en Sheets.
    (cambio mínimo, no toca utils)
    """
    if not isinstance(hmap, dict) or not hmap:
        return data
    return {k: v for k, v in data.items() if k in hmap}

def pick_abogado(ws_abog, salario_mensual: float):
    """
    Mantiene tu lógica original: VIP a A01 si salario>=50k, si no por carga.
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
    # Ley vigente (aprox) 2023+: 12,14,16,18,20...
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

def _safe_anniversary(base: date, year: int) -> date:
    """
    Devuelve aniversario (mismo mes/día) en 'year' manejando 29/02.
    """
    try:
        return date(year, base.month, base.day)
    except Exception:
        # fallback 28 feb si es 29 feb
        if base.month == 2 and base.day == 29:
            return date(year, 2, 28)
        # fallback general
        return date(year, base.month, min(base.day, 28))

def calc_estimacion(tipo_caso: str, salario_mensual: float, ini: date, fin: date, salario_min_diario: float):
    """
    Devuelve:
      - detalle_txt (para web)
      - total (float)
      - breakdown (dict para guardar campos opcionales)
    """
    sd = salario_mensual / 30.0 if salario_mensual else 0.0
    sdi = sd  # aproximación SDI

    y = years_of_service(ini, fin)
    y_int = int(y) if y > 0 else 0
    vac_days = vacation_days_by_years(max(y_int, 1) if y > 0 else 0)

    # Aguinaldo proporcional: por año calendario (desde 1 enero o desde inicio si fue este año)
    start_year = date(fin.year, 1, 1)
    from_agui = max(start_year, ini)
    days_agui = max((fin - from_agui).days, 0)
    aguinaldo_days = 15
    aguinaldo_prop = sd * aguinaldo_days * (days_agui / 365.0) if sd else 0.0

    # Vacaciones proporcionales: desde último aniversario (más “realista” en la práctica)
    ann_this = _safe_anniversary(ini, fin.year)
    last_ann = ann_this if fin >= ann_this else _safe_anniversary(ini, fin.year - 1)
    days_vac = max((fin - last_ann).days, 0)
    vacaciones_prop = sd * vac_days * (days_vac / 365.0) if sd else 0.0
    prima_vac_prop = vacaciones_prop * 0.25

    # Prima de antigüedad topada a 2 SM diarios
    sd_top = sd
    if salario_min_diario and salario_min_diario > 0:
        sd_top = min(sd, 2.0 * salario_min_diario)
    prima_ant = sd_top * 12.0 * y if (sd_top and y > 0) else 0.0

    breakdown = {
        "SD": sd,
        "SD_TOP": sd_top,
        "ANIOS": y,
        "VAC_DIAS": vac_days,
        "DIAS_AGUINALDO_PROP": days_agui,
        "DIAS_VAC_PROP": days_vac,
        "AGUINALDO_PROP": aguinaldo_prop,
        "VACACIONES_PROP": vacaciones_prop,
        "PRIMA_VAC_PROP": prima_vac_prop,
        "PRIMA_ANT": prima_ant,
    }

    if str(tipo_caso).strip() == "1":
        ind_90 = sdi * 90.0
        ind_20 = sdi * 20.0 * y
        total = ind_90 + ind_20 + prima_ant + aguinaldo_prop + vacaciones_prop + prima_vac_prop

        breakdown.update({
            "IND_90": ind_90,
            "IND_20": ind_20,
            "TOTAL": total
        })

        detalle_txt = (
            "DESGLOSE DETALLADO (REFERENCIAL)\n"
            f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
            f"- Salario diario (SD aprox): ${sd:,.2f}\n"
            f"- Antigüedad estimada: {y:.2f} años\n\n"
            "INDEMNIZACIÓN (DESPIDO)\n"
            f"- 3 meses (90 días): ${ind_90:,.2f}\n"
            f"- 20 días por año: ${ind_20:,.2f}\n"
            f"- Prima de antigüedad (12 días/año, topada): ${prima_ant:,.2f}\n\n"
            "PRESTACIONES PROPORCIONALES\n"
            f"- Aguinaldo proporcional (desde {from_agui}): ${aguinaldo_prop:,.2f}\n"
            f"- Vacaciones proporcionales (desde {last_ann} / {vac_days} días/año): ${vacaciones_prop:,.2f}\n"
            f"- Prima vacacional (25%): ${prima_vac_prop:,.2f}\n\n"
            f"TOTAL ESTIMADO: ${total:,.2f}\n\n"
            "Nota: el monto puede variar por salario integrado real, prestaciones adicionales, "
            "salarios caídos, prima de antigüedad conforme topes vigentes y documentación del caso."
        )
        return (detalle_txt, total, breakdown)

    # Renuncia / finiquito
    total = aguinaldo_prop + vacaciones_prop + prima_vac_prop
    prima_ant_ren = 0.0
    if y >= 15:
        prima_ant_ren = prima_ant
        total += prima_ant_ren

    breakdown.update({
        "IND_90": 0.0,
        "IND_20": 0.0,
        "PRIMA_ANT_REN": prima_ant_ren,
        "TOTAL": total
    })

    detalle_txt = (
        "DESGLOSE DETALLADO (REFERENCIAL)\n"
        f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
        f"- Salario diario (SD aprox): ${sd:,.2f}\n"
        f"- Antigüedad estimada: {y:.2f} años\n\n"
        "FINIQUITO (RENUNCIA)\n"
        f"- Aguinaldo proporcional (desde {from_agui}): ${aguinaldo_prop:,.2f}\n"
        f"- Vacaciones proporcionales (desde {last_ann} / {vac_days} días/año): ${vacaciones_prop:,.2f}\n"
        f"- Prima vacacional (25%): ${prima_vac_prop:,.2f}\n"
        + (f"- Prima de antigüedad (≥15 años): ${prima_ant_ren:,.2f}\n" if prima_ant_ren else "")
        + f"\nTOTAL ESTIMADO: ${total:,.2f}\n\n"
        "Nota: puede variar según recibos, prestaciones reales, pagos pendientes y documentación."
    )
    return (detalle_txt, total, breakdown)

def build_resumen_largo(tipo_caso: str, nombre: str) -> str:
    if str(tipo_caso).strip() == "1":
        return (
            f"{nombre}, lamento mucho lo que estás viviendo.\n\n"
            "En un despido, lo clave es revisar: (1) causa y forma del despido, (2) salario real/integrado, "
            "(3) antigüedad y (4) prestaciones efectivamente pagadas.\n\n"
            "Con tu información generamos una estimación preliminar para darte una idea clara. "
            "Una abogada revisará tu caso y te indicará el mejor camino (negociación, demanda, reinstalación o indemnización).\n\n"
            "⚖️ Orientación informativa (no constituye asesoría legal)."
        )
    return (
        f"{nombre}, gracias por confiar en nosotros.\n\n"
        "En renuncia voluntaria normalmente corresponde finiquito: aguinaldo proporcional, vacaciones proporcionales/no gozadas "
        "y prima vacacional, además de pagos pendientes (si existieran).\n\n"
        "Una abogada revisará recibos y condiciones reales para confirmar el monto y el siguiente paso.\n\n"
        "⚖️ Orientación informativa (no constituye asesoría legal)."
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

    update_row_cells(ws_leads, row, _filter_to_existing_columns({
        "Procesar_AI_Status": "RUNNING",
        "Ultimo_Error": "",
        "Ultima_Actualizacion": now_iso(),
    }, h), hmap=h)

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

        max_words = safe_int(syscfg.get("MAX_PALABRAS_RESUMEN") or "50")
        resumen_corto = clip_words(resumen_largo, max_words if max_words > 0 else 50)

        # Default mínimo si no está en Config_Sistema
        salario_min_diario = safe_float(syscfg.get("SALARIO_MIN_DIARIO") or "248.93")

        detalle_web, total_estimado, b = calc_estimacion(
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

        # WhatsApp: SOLO TOTAL + texto humano + link web
        mensaje_final = (
            f"✅ {nombre}, ya tengo tu *estimación preliminar*.\n\n"
            f"💰 *Total estimado:* ${total_estimado:,.2f}\n\n"
            f"{resumen_corto}\n\n"
            f"👩‍⚖️ La abogada asignada es *{abogado_nombre}* y se comunicará contigo muy pronto.\n"
        )
        if link_reporte:
            mensaje_final += f"\n📄 *Detalle en web:* {link_reporte}\n"
        mensaje_final += "\nSi deseas opciones, escribe *menu*."

        # Guardado en Sheets (web detallado)
        payload = {
            "Resultado_Calculo": detalle_web,          # ✅ detalle para web
            "Analisis_AI": resumen_largo,              # ✅ texto más completo
            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Link_WhatsApp": link_abog,
            "Ultimo_Error": "",
            "Ultima_Actualizacion": now_iso(),
            "Total_Estimado": f"{total_estimado:.2f}",

            # Campos opcionales (solo si existen en BD_Leads)
            "Indemnizacion_90": f"{b.get('IND_90', 0.0):.2f}",
            "Indemnizacion_20": f"{b.get('IND_20', 0.0):.2f}",
            "Prima_Antiguedad": f"{b.get('PRIMA_ANT', 0.0):.2f}",
            "Aguinaldo_Prop": f"{b.get('AGUINALDO_PROP', 0.0):.2f}",
            "Vacaciones_Prop": f"{b.get('VACACIONES_PROP', 0.0):.2f}",
            "Prima_Vac_Prop": f"{b.get('PRIMA_VAC_PROP', 0.0):.2f}",
            "Vac_Dias_Base": str(b.get("VAC_DIAS", "")),
        }

        update_row_cells(ws_leads, row, _filter_to_existing_columns(payload, h), hmap=h)

        ok1, det1 = send_whatsapp_safe(telefono, mensaje_final)

        if ok1:
            update_row_cells(ws_leads, row, _filter_to_existing_columns({
                "Procesar_AI_Status": "DONE",
                "ESTATUS": "CLIENTE_MENU",
                "Ultimo_Error": "",
                "Ultima_Actualizacion": now_iso(),
            }, h), hmap=h)
        else:
            update_row_cells(ws_leads, row, _filter_to_existing_columns({
                "Procesar_AI_Status": "DONE_SEND_ERROR",
                "ESTATUS": "EN_PROCESO",
                "Ultimo_Error": f"send1={ok1}({det1})"[:450],
                "Ultima_Actualizacion": now_iso(),
            }, h), hmap=h)

        return {"ok": True, "lead_id": lead_id, "send1": ok1, "det1": det1}

    except Exception as e:
        update_row_cells(ws_leads, row, _filter_to_existing_columns({
            "Procesar_AI_Status": "FAILED",
            "Ultimo_Error": f"{type(e).__name__}: {e}",
            "Ultima_Actualizacion": now_iso(),
        }, h), hmap=h)
        raise
