# worker_jobs.py
import os
import re
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
TAB_ABOG_ADMIN = os.environ.get("TAB_ABOGADOS_ADMIN", "Abogados_Admin").strip()
TAB_CONOCIMIENTO_AI = os.environ.get("TAB_CONOCIMIENTO_AI", "Conocimiento_AI").strip()

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "").strip()

# OpenAI (opcional)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# --------------------
# Helpers
# --------------------
def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _wa_addr(raw: str) -> str:
    t = (raw or "").strip()
    if not t:
        return ""
    return t if t.startswith("whatsapp:") else "whatsapp:" + t

def _get_twilio_client() -> Client:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        raise RuntimeError("Faltan credenciales de Twilio (TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN).")
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def send_whatsapp_safe(to_number: str, body: str):
    """Envía WhatsApp y NO truena el job: regresa (ok, detail)."""
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

def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-záéíóúüñ0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clip_chars(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= max_chars else s[:max_chars].rstrip() + "…"

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

def set_sys_value(ws_sys, key: str, value: str):
    """
    Escribe/actualiza en Config_Sistema (Clave/Valor).
    Si no existe la clave, la agrega.
    """
    key = (key or "").strip()
    if not key:
        return

    values = get_all_values_safe(ws_sys)
    if not values:
        # hoja vacía: crea header mínimo
        with_backoff(ws_sys.append_row, ["Clave", "Valor"], value_input_option="RAW")
        values = get_all_values_safe(ws_sys)

    hdr = values[0]
    # si header no trae Clave/Valor, no rompemos
    if "Clave" not in hdr or "Valor" not in hdr:
        return

    # buscar fila por Clave
    for i in range(1, len(values)):
        row = values[i]
        d = row_to_dict(hdr, row)
        if (d.get("Clave") or "").strip() == key:
            # actualizar Valor
            hmap = build_header_map(ws_sys)
            row_num = i + 1  # sheet is 1-indexed
            update_row_cells(ws_sys, row_num, {"Valor": str(value)}, hmap=hmap)
            return

    # si no existe, append
    with_backoff(ws_sys.append_row, [key, str(value)], value_input_option="RAW")

def list_active_abogados(ws_abog):
    """
    Regresa lista ordenada: [(ID, Nombre, Telefono), ...] solo activos.
    Columnas esperadas: ID_Abogado, Nombre_Abogado, Telefono_Abogado, Activo
    """
    h = build_header_map(ws_abog)
    rows = with_backoff(ws_abog.get_all_values)
    if not rows or len(rows) < 2:
        return []

    def cell(r, name):
        c = col_idx(h, name)
        return (r[c-1] if c and c-1 < len(r) else "").strip()

    def is_active(r):
        v = cell(r, "Activo").upper()
        return v in ("SI", "SÍ", "TRUE", "1")

    out = []
    for r in rows[1:]:
        aid = cell(r, "ID_Abogado")
        if not aid or not is_active(r):
            continue
        out.append((aid, cell(r, "Nombre_Abogado") or f"Abogada {aid}", cell(r, "Telefono_Abogado")))
    out.sort(key=lambda x: x[0])
    return out

def pick_abogado_secuencial(ws_abog, ws_sys, salario_mensual: float, syscfg: dict):
    """
    ✅ Regla:
    - salario >= 50,000 => A01 (si está activo; si no, igualmente A01 si existe; si no, fallback primer activo)
    - si no, round-robin entre activos usando Config_Sistema.Clave = ABOGADO_ULTIMO_ID
    """
    activos = list_active_abogados(ws_abog)
    if not activos:
        return ("A01", "Abogada asignada", "")

    # helper: obtener por id
    def by_id(aid: str):
        for x in activos:
            if x[0] == aid:
                return x
        return None

    # VIP
    if salario_mensual >= 50000:
        a01 = by_id("A01")
        if a01:
            # nota: NO movemos el puntero de rotación por VIP (opcional). Aquí lo dejo SIN mover.
            return a01
        # si no está activo A01 pero existe otro activo, se asigna el primero
        return activos[0]

    # Round-robin (incluye A01 si está activo)
    last_id = (syscfg.get("ABOGADO_ULTIMO_ID") or "").strip()
    ids = [a[0] for a in activos]

    if last_id in ids:
        idx = ids.index(last_id)
        nxt = activos[(idx + 1) % len(activos)]
    else:
        nxt = activos[0]

    # guardar puntero
    try:
        set_sys_value(ws_sys, "ABOGADO_ULTIMO_ID", nxt[0])
    except Exception:
        pass

    return nxt

def years_of_service(ini: date, fin: date) -> float:
    days = max((fin - ini).days, 0)
    return days / 365.0 if days else 0.0

def vacation_days_by_years(y: int) -> int:
    # Reforma: 1=12, 2=14, 3=16, 4=18, 5=20; luego +2 cada 5 años.
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

def _last_anniversary(ini: date, fin: date) -> date:
    """Último aniversario de ingreso antes (o igual) a la fecha fin."""
    try:
        ann = date(fin.year, ini.month, ini.day)
    except ValueError:
        ann = date(fin.year, ini.month, min(ini.day, 28))
    if ann > fin:
        try:
            ann = date(fin.year - 1, ini.month, ini.day)
        except ValueError:
            ann = date(fin.year - 1, ini.month, min(ini.day, 28))
    return max(ann, ini)

def calc_estimacion_detallada(tipo_caso: str, salario_mensual: float, ini: date, fin: date, salario_min_diario: float = 0.0):
    """
    ✅ Cambiado: YA NO calcula indemnización de 20 días.
    Devuelve:
      - desglose_texto (para web / Sheets)
      - total_estimado (float)
      - componentes (dict) para columnas:
        Indemnizacion_90, Indemnizacion_20(=0), Prima_Antiguedad,
        Aguinaldo_Prop, Vacaciones_Prop, Prima_Vac_Prop, Vac_Dias_Base
    Nota: aproximación con SD (no SDI real).
    """
    sd = salario_mensual / 30.0 if salario_mensual else 0.0
    y = years_of_service(ini, fin)
    y_int = int(y) if y > 0 else 0

    # Aguinaldo: proporcional por calendario (desde 1-ene del año de terminación)
    start_year = date(fin.year, 1, 1)
    days_agu = max((fin - start_year).days + 1, 0)
    aguinaldo_prop = sd * 15 * (days_agu / 365.0) if sd else 0.0

    # Vacaciones: proporcional por aniversario (desde el último aniversario)
    vac_from = _last_anniversary(ini, fin)
    days_vac_period = max((fin - vac_from).days + 1, 0)
    vac_days_base = vacation_days_by_years(max(y_int, 1) if y > 0 else 0)
    vacaciones_prop = sd * vac_days_base * (days_vac_period / 365.0) if sd else 0.0
    prima_vac_prop = vacaciones_prop * 0.25

    # Prima de antigüedad (topable 2x mínimo si existe)
    sd_top = sd
    if salario_min_diario and salario_min_diario > 0:
        sd_top = min(sd, 2.0 * salario_min_diario)
    prima_ant = sd_top * 12.0 * y if (sd_top and y > 0) else 0.0

    ind_90 = 0.0
    ind_20 = 0.0  # ✅ siempre 0 (compatibilidad)

    if str(tipo_caso).strip() == "1":  # Despido
        ind_90 = sd * 90.0
        total = ind_90 + prima_ant + aguinaldo_prop + vacaciones_prop + prima_vac_prop

        desglose = (
            "DESGLOSE DETALLADO (REFERENCIAL)\n"
            f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
            f"- Salario diario (SD aprox): ${sd:,.2f}\n"
            f"- Antigüedad estimada: {y:.2f} años\n\n"
            "INDEMNIZACIÓN (DESPIDO)\n"
            f"- 3 meses (90 días): ${ind_90:,.2f}\n"
            f"- Prima de antigüedad (12 días/año, topada si aplica): ${prima_ant:,.2f}\n\n"
            "PRESTACIONES PROPORCIONALES\n"
            f"- Aguinaldo proporcional (desde {start_year.isoformat()}): ${aguinaldo_prop:,.2f}\n"
            f"- Vacaciones proporcionales (desde {vac_from.isoformat()} / {vac_days_base} días/año): ${vacaciones_prop:,.2f}\n"
            f"- Prima vacacional proporcional (25%): ${prima_vac_prop:,.2f}\n\n"
            f"TOTAL ESTIMADO: ${total:,.2f}\n\n"
            "Nota: el monto puede variar por salario integrado real, prestaciones adicionales, salarios caídos, topes vigentes y documentación."
        )
    else:  # Renuncia
        total = aguinaldo_prop + vacaciones_prop + prima_vac_prop
        prima_ant_ren = 0.0
        if y >= 15:
            prima_ant_ren = prima_ant
            total += prima_ant_ren

        desglose = (
            "DESGLOSE DETALLADO (REFERENCIAL)\n"
            f"- Salario mensual considerado: ${salario_mensual:,.2f}\n"
            f"- Salario diario (SD aprox): ${sd:,.2f}\n"
            f"- Antigüedad estimada: {y:.2f} años\n\n"
            "FINIQUITO (RENUNCIA)\n"
            f"- Aguinaldo proporcional (desde {start_year.isoformat()}): ${aguinaldo_prop:,.2f}\n"
            f"- Vacaciones proporcionales (desde {vac_from.isoformat()} / {vac_days_base} días/año): ${vacaciones_prop:,.2f}\n"
            f"- Prima vacacional proporcional (25%): ${prima_vac_prop:,.2f}\n"
            + (f"- Prima de antigüedad (si ≥15 años): ${prima_ant_ren:,.2f}\n" if prima_ant_ren else "")
            + f"\nTOTAL ESTIMADO: ${total:,.2f}\n\n"
            "Nota: el monto puede variar según recibos, prestaciones reales y pagos pendientes."
        )

    componentes = {
        "Indemnizacion_90": ind_90,
        "Indemnizacion_20": ind_20,  # ✅ 0
        "Prima_Antiguedad": prima_ant,
        "Aguinaldo_Prop": aguinaldo_prop,
        "Vacaciones_Prop": vacaciones_prop,
        "Prima_Vac_Prop": prima_vac_prop,
        "Vac_Dias_Base": vac_days_base,
    }
    return desglose, total, componentes

def build_resumen_whatsapp(tipo_caso: str, nombre: str) -> str:
    # WhatsApp: corto y humano
    if str(tipo_caso).strip() == "1":
        return f"{nombre}, lamento lo ocurrido. Este total es una referencia preliminar; lo afinamos con documentos."
    return f"{nombre}, gracias por contarnos tu caso. Este total es una referencia preliminar; lo afinamos con documentos."

def load_conocimiento(ws_con):
    """
    Regresa lista de dicts con columnas:
    ID_Tema, Titulo_Visible, Contenido_Legal, Palabras_Clave, Fuente
    """
    values = get_all_values_safe(ws_con)
    if not values or len(values) < 2:
        return []
    hdr = values[0]
    out = []
    for r in values[1:]:
        d = row_to_dict(hdr, r)
        out.append({
            "ID_Tema": (d.get("ID_Tema") or "").strip(),
            "Titulo_Visible": (d.get("Titulo_Visible") or "").strip(),
            "Contenido_Legal": (d.get("Contenido_Legal") or "").strip(),
            "Palabras_Clave": (d.get("Palabras_Clave") or "").strip(),
            "Fuente": (d.get("Fuente") or "").strip(),
        })
    return out

def select_conocimiento(con_rows, descripcion: str, tipo_caso: str, k=3):
    """
    Selecciona hasta k temas por coincidencia simple de palabras clave.
    """
    desc_n = _normalize_text(descripcion)
    tokens = set([t for t in desc_n.split() if len(t) >= 4])

    # boost por tipo
    if str(tipo_caso).strip() == "1":
        tokens |= {"despido", "indemnizacion", "indemnización", "finiquito", "rescision", "rescisión"}
    elif str(tipo_caso).strip() == "2":
        tokens |= {"renuncia", "finiquito", "prestaciones", "vacaciones", "aguinaldo"}

    scored = []
    for row in con_rows:
        keys = _normalize_text(row.get("Palabras_Clave", ""))
        key_list = [x.strip() for x in re.split(r"[;,]", keys) if x.strip()]
        score = 0
        for kw in key_list:
            # match por palabra o frase
            if kw and (kw in desc_n or kw in tokens):
                score += 2
        # también revisa el título
        title = _normalize_text(row.get("Titulo_Visible", ""))
        for t in tokens:
            if t in title:
                score += 1
        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    pick = [r for _, r in scored[:k]]
    return pick

def build_analisis_web_gpt(nombre: str, tipo_caso: str, descripcion: str, salario_mensual: float, ini: date, fin: date, temas: list):
    """
    Genera análisis consultivo (web) usando OpenAI + Conocimiento_AI.
    Fallback: plantilla sensible si no hay OpenAI.
    """
    tipo_h = "Despido" if str(tipo_caso).strip() == "1" else ("Renuncia" if str(tipo_caso).strip() == "2" else "Caso laboral")
    desc = (descripcion or "").strip()

    # ---- fallback (si no hay openai) ----
    def fallback():
        bullets = []
        for t in temas[:3]:
            title = t.get("Titulo_Visible") or "Punto legal relevante"
            bullets.append(f"• {title}")
        bullets_txt = "\n".join(bullets) if bullets else "• Revisión de causa/forma del evento y prestaciones.\n• Validación de salario real/integrado.\n• Confirmación de pagos pendientes y documentos."

        return (
            f"{nombre}, lamento la situación y gracias por confiar en nosotros.\n\n"
            "ANÁLISIS CONSULTIVO (REFERENCIAL)\n"
            f"Tipo de caso: {tipo_h}\n"
            f"Lo que nos compartiste: {desc if desc else '—'}\n\n"
            "PUNTOS CLAVE A REVISAR\n"
            f"{bullets_txt}\n\n"
            "DOCUMENTOS QUE AYUDAN\n"
            "• Recibos de nómina / transferencias\n"
            "• Contrato o condiciones (si existen)\n"
            "• Alta/baja IMSS, semanas cotizadas\n"
            "• Mensajes o evidencia del motivo (si aplica)\n\n"
            "PRÓXIMOS PASOS RECOMENDADOS\n"
            "1) Reunir documentos y confirmar salario integrado.\n"
            "2) Identificar prestaciones pendientes y pagos realizados.\n"
            "3) Definir estrategia (negociación/demanda) según evidencia.\n\n"
            "⚖️ Orientación informativa; no constituye asesoría legal."
        )

    if not (OPENAI_API_KEY and OpenAI):
        return fallback()

    # ---- construir contexto con Conocimiento_AI ----
    temas_txt = []
    for t in temas[:3]:
        temas_txt.append(
            f"- {t.get('Titulo_Visible','Tema')}\n"
            f"  Contenido: {_clip_chars(t.get('Contenido_Legal',''), 900)}\n"
            f"  Fuente: {_clip_chars(t.get('Fuente',''), 180)}"
        )
    contexto = "\n".join(temas_txt).strip()

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)

        messages = [
            {
                "role": "system",
                "content": (
                    "Eres un asistente legal experto y amigable en derecho laboral mexicano. "
                    "Tu objetivo es explicar la situación al usuario de forma clara, sencilla y empática, sin usar tecnicismos complejos. "
                    "Debes guiarlo paso a paso en lo que debe hacer, explicando el 'por qué' de cada acción de manera práctica. "
                    "Utiliza la información de la base de conocimiento proporcionada para dar consejos precisos y fundamentados. "
                    "Habla siempre en primera persona del plural ('te recomendamos', 'sugerimos') o como un aliado experto."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Analiza el siguiente caso laboral y genera una respuesta personalizada para {nombre}.\n\n"
                    f"**Datos del Usuario:**\n"
                    f"- Nombre: {nombre}\n"
                    f"- Tipo de Caso: {tipo_h}\n"
                    f"- Descripción: {desc}\n"
                    f"- Salario Mensual: ${salario_mensual:,.2f}\n"
                    f"- Antigüedad: {ini.isoformat()} a {fin.isoformat()}\n\n"
                    f"**Información Legal Relevante (Base de Conocimiento):**\n"
                    f"{contexto if contexto else '(No se encontró información específica en la base de conocimiento, usa tu conocimiento general de la LFT)'}\n\n"
                    "**Instrucciones para la Respuesta:**\n"
                    "1.  **Empatía Inicial**: Empieza reconociendo su situación con un tono humano y comprensivo.\n"
                    "2.  **Explicación del Caso**: Explica brevemente qué derechos generales aplican para un {tipo_h} con su antigüedad y salario. Usa lenguaje sencillo (evita jerga como 'laudo', 'litis' a menos que lo expliques).\n"
                    "3.  **Guía Paso a Paso (Plan de Acción)**: Detalla una lista numerada de acciones concretas que debe realizar el usuario (ej. documentación a reunir, revisar contrato, no firmar hojas en blanco, etc.). Para cada paso, explica brevemente **por qué** es importante.\n"
                    "4.  **Uso de Conocimiento**: Si hay temas en la 'Información Legal Relevante', incorpóralos naturalmente en la explicación.\n"
                    "5.  **Personalización**: Menciona alguno de sus datos (ej. 'Al tener X años de antigüedad...', 'Considerando tu salario...') para que sienta que la respuesta es única para él.\n"
                    "6.  **Cierre**: Termina con una frase de apoyo e invita a seguir el proceso.\n"
                    "7.  **Sello Legal**: Incluye al final, separado, la leyenda: 'Orientación informativa; no constituye asesoría legal definitiva.'\n"
                )
            }
        ]

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.4,
            max_tokens=850
        )
        txt = (resp.choices[0].message.content or "").strip()
        return txt if txt else fallback()

    except Exception:
        return fallback()

def upsert_abogados_admin(sh, lead_id: str, abogado_id: str):
    """
    Crea (si no existe) registro en Abogados_Admin con:
      ID_Lead, ID_Abogado, Estatus, Acepto_Asesoria, Enviar_Cuestionario, Proxima_Fecha_Evento, Notas
    """
    try:
        ws = open_worksheet(sh, TAB_ABOG_ADMIN)
    except Exception:
        return  # si no existe, no rompe

    # si ya existe, solo actualiza ID_Abogado / Estatus
    try:
        existing = find_row_by_value(ws, "ID_Lead", lead_id)
        if existing:
            h = build_header_map(ws)
            update_row_cells(ws, existing, {"ID_Abogado": abogado_id, "Estatus": "ASIGNADO"}, hmap=h)
            return
    except Exception:
        pass

    try:
        header = with_backoff(ws.row_values, 1)
        h = build_header_map(ws)

        row_out = [""] * len(header)

        def set_cell(col: str, val: str):
            c = col_idx(h, col)
            if c and 1 <= c <= len(row_out):
                row_out[c - 1] = val

        set_cell("ID_Lead", lead_id)
        set_cell("ID_Abogado", abogado_id)
        set_cell("Estatus", "ASIGNADO")
        set_cell("Acepto_Asesoria", "")
        set_cell("Enviar_Cuestionario", "")
        set_cell("Proxima_Fecha_Evento", "")
        set_cell("Notas", "")

        with_backoff(ws.append_row, row_out, value_input_option="RAW")
    except Exception:
        return


# --------------------
# Main job
# --------------------
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
        descripcion = get("Descripcion_Situacion")

        ini = _parse_date_parts(h, vals, "Inicio")
        fin = _parse_date_parts(h, vals, "Fin")
        if fin < ini:
            raise ValueError("Fecha fin es menor a fecha inicio.")

        # ✅ abogado secuencial + VIP
        abogado_id, abogado_nombre, abogado_tel = pick_abogado_secuencial(ws_abog, ws_sys, salario, syscfg)

        salario_min_diario = safe_float(syscfg.get("SALARIO_MIN_DIARIO") or "0")

        # ✅ cálculo sin 20 días
        desglose_txt, total_estimado, comp = calc_estimacion_detallada(
            tipo_caso=tipo_caso,
            salario_mensual=salario,
            ini=ini,
            fin=fin,
            salario_min_diario=salario_min_diario
        )

        # ✅ análisis web consultivo (GPT + Conocimiento_AI)
        con_rows = []
        try:
            ws_con = open_worksheet(sh, TAB_CONOCIMIENTO_AI)
            con_rows = load_conocimiento(ws_con)
        except Exception:
            con_rows = []

        temas = select_conocimiento(con_rows, descripcion, tipo_caso, k=3)
        analisis_web = build_analisis_web_gpt(
            nombre=nombre,
            tipo_caso=tipo_caso,
            descripcion=descripcion,
            salario_mensual=salario,
            ini=ini,
            fin=fin,
            temas=temas
        )

        resumen_wa = build_resumen_whatsapp(tipo_caso, nombre)

        token = uuid.uuid4().hex[:18]
        base_url = (syscfg.get("RUTA_REPORTE") or syscfg.get("BASE_URL_WEB") or "").strip()
        if base_url and not base_url.endswith("/") and "?" not in base_url:
            base_url += "/"
        link_reporte = f"{base_url}?token={token}" if base_url else ""

        link_abog = ""
        if abogado_tel:
            tnorm = "".join([c for c in abogado_tel if c.isdigit() or c == "+"])
            if tnorm:
                link_abog = f"https://wa.me/{tnorm.replace('+','')}"

        # WhatsApp: SOLO TOTAL + link web
        mensaje_final = (
            f"✅ {nombre}, ya tengo tu *estimación preliminar*.\n\n"
            f"💰 *Total estimado:* ${total_estimado:,.2f}\n\n"
            f"{resumen_wa}\n\n"
            f"👩⚖️ Tu abogada asignada es *{abogado_nombre}* y se comunicará contigo muy pronto.\n"
        )
        if link_reporte:
            mensaje_final += f"\n📄 Ver desglose en web: {link_reporte}\n"
        mensaje_final += "\n(Orientación informativa; no constituye asesoría legal.)"

        # Guardar en Sheets (Web toma esto)
        update_row_cells(ws_leads, row, {
            "Analisis_AI": analisis_web,          # ✅ ahora es análisis consultivo (web)
            "Resultado_Calculo": desglose_txt,
            "Total_Estimado": f"{total_estimado:.2f}",

            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Link_WhatsApp": link_abog,

            "Fecha_Inicio_Laboral": ini.isoformat(),
            "Fecha_Fin_Laboral": fin.isoformat(),
            "Es_Cliente": "1",

            # componentes
            "Indemnizacion_90": f"{comp['Indemnizacion_90']:.2f}",
            "Indemnizacion_20": f"{comp['Indemnizacion_20']:.2f}",  # ✅ 0
            "Prima_Antiguedad": f"{comp['Prima_Antiguedad']:.2f}",
            "Aguinaldo_Prop": f"{comp['Aguinaldo_Prop']:.2f}",
            "Vacaciones_Prop": f"{comp['Vacaciones_Prop']:.2f}",
            "Prima_Vac_Prop": f"{comp['Prima_Vac_Prop']:.2f}",
            "Vac_Dias_Base": str(comp["Vac_Dias_Base"]),

            "Ultimo_Error": "",
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)

        # Crear registro en Abogados_Admin (si existe)
        upsert_abogados_admin(sh, lead_id, abogado_id)

        # Enviar WhatsApp
        ok1, det1 = send_whatsapp_safe(telefono, mensaje_final)

        if ok1:
            update_row_cells(ws_leads, row, {
                "Procesar_AI_Status": "DONE",
                "ESTATUS": "CLIENTE_MENU",
                "Ultimo_Error": "",
                "Ultima_Actualizacion": now_iso(),
            }, hmap=h)
        else:
            update_row_cells(ws_leads, row, {
                "Procesar_AI_Status": "DONE_SEND_ERROR",
                "ESTATUS": "EN_PROCESO",
                "Ultimo_Error": f"send1={ok1}({det1})"[:450],
                "Ultima_Actualizacion": now_iso(),
            }, hmap=h)

        return {"ok": True, "lead_id": lead_id, "send1": ok1}

    except Exception as e:
        update_row_cells(ws_leads, row, {
            "Procesar_AI_Status": "FAILED",
            "Ultimo_Error": f"{type(e).__name__}: {e}",
            "Ultima_Actualizacion": now_iso(),
        }, hmap=h)
        raise
