import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime
from typing import Dict, Optional, Tuple, List

from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client as TwilioClient

import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# APP
# =========================================================
app = Flask(__name__)

# =========================================================
# ENV
# =========================================================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()
TAB_KNOW = os.environ.get("TAB_KNOW", "Conocimiento_AI").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_PHONE = os.environ.get("TWILIO_PHONE", "").strip()  # whatsapp:+1415...
twilio_client = TwilioClient(TWILIO_SID, TWILIO_TOKEN) if (TWILIO_SID and TWILIO_TOKEN) else None

# =========================================================
# TIME
# =========================================================
def now_iso_utc() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# =========================================================
# TWILIO INBOUND REPLY
# =========================================================
def safe_reply(text: str) -> str:
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

# =========================================================
# TWILIO OUTBOUND (NORMAL or TEMPLATE)
# =========================================================
def send_whatsapp_message(to_whatsapp: str, body: Optional[str] = None,
                          content_sid: Optional[str] = None,
                          content_variables: Optional[dict] = None) -> Tuple[bool, str]:
    """
    Sends either:
    - normal WhatsApp message (body)
    - template message using Twilio Content API (content_sid + content_variables)
    """
    if not twilio_client or not TWILIO_PHONE:
        return False, "Twilio no configurado (faltan credenciales o TWILIO_PHONE)."

    try:
        kwargs = {"from_": TWILIO_PHONE, "to": to_whatsapp}

        if content_sid:
            kwargs["content_sid"] = content_sid
            if content_variables:
                kwargs["content_variables"] = json.dumps(content_variables, ensure_ascii=False)
            # body no es requerido con content_sid
        else:
            kwargs["body"] = body or ""

        twilio_client.messages.create(**kwargs)
        return True, ""
    except Exception as e:
        return False, str(e)

# =========================================================
# NORMALIZACIÓN
# =========================================================
def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    s = (raw or "").strip().replace("whatsapp:", "").strip()
    return s  # +52...

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    return m.group(0) if m else s

# =========================================================
# GOOGLE CREDS
# =========================================================
def get_env_creds_dict() -> dict:
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        if raw.lstrip().startswith("{"):
            return json.loads(raw)
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)

    if GOOGLE_CREDENTIALS_PATH:
        if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe en el filesystem del servicio.")
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales: GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH.")

def get_gspread_client():
    creds_info = get_env_creds_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    if not GOOGLE_SHEET_NAME:
        raise RuntimeError("Falta GOOGLE_SHEET_NAME (nombre exacto del Google Sheet).")
    return gc.open(GOOGLE_SHEET_NAME)

# =========================================================
# HEADERS MAP + HELPERS
# =========================================================
def build_header_map(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    m = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if not key:
            continue
        if key not in m:
            m[key] = i
        low = key.lower()
        if low not in m:
            m[low] = i
    return m

def col_idx(headers_map: dict, name: str) -> Optional[int]:
    return headers_map.get(name) or headers_map.get((name or "").lower())

def find_row_by_value(ws, col_idx_num: int, value: str) -> Optional[int]:
    value = (value or "").strip()
    if not value:
        return None
    col_values = ws.col_values(col_idx_num)
    for i, v in enumerate(col_values[1:], start=2):
        if (v or "").strip() == value:
            return i
    return None

def update_cells_batch(ws, updates_a1_to_value: dict):
    payload = [{"range": a1, "values": [[val]]} for a1, val in updates_a1_to_value.items()]
    if payload:
        ws.batch_update(payload)

def update_row_by_headers(ws, header_map: dict, row_idx: int, updates: dict):
    to_send = {}
    for col_name, val in (updates or {}).items():
        idx = col_idx(header_map, col_name)
        if not idx:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, idx)
        to_send[a1] = val
    update_cells_batch(ws, to_send)

def safe_log(ws_logs, data: dict):
    try:
        cols = [
            "ID_Log", "Fecha_Hora", "Telefono", "ID_Lead", "Paso",
            "Mensaje_Entrante", "Mensaje_Saliente",
            "Canal", "Fuente_Lead", "Modelo_AI", "Errores"
        ]
        row = [data.get(c, "") for c in cols]
        ws_logs.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        pass

# =========================================================
# CONFIG_SISTEMA / PARAMS
# =========================================================
def load_key_value_sheet(ws, key_col="Clave", val_col="Valor") -> Dict[str, str]:
    headers = build_header_map(ws)
    k_idx = col_idx(headers, key_col)
    v_idx = col_idx(headers, val_col)
    out = {}
    if not k_idx or not v_idx:
        return out
    rows = ws.get_all_values()[1:]
    for r in rows:
        k = (r[k_idx-1] if k_idx-1 < len(r) else "").strip()
        v = (r[v_idx-1] if v_idx-1 < len(r) else "").strip()
        if k:
            out[k] = v
    return out

def load_parametros(ws_param) -> Dict[str, float]:
    headers = build_header_map(ws_param)
    c_idx = col_idx(headers, "Concepto")
    v_idx = col_idx(headers, "Valor")
    out = {}
    if not c_idx or not v_idx:
        return out
    rows = ws_param.get_all_values()[1:]
    for r in rows:
        c = (r[c_idx-1] if c_idx-1 < len(r) else "").strip()
        v = (r[v_idx-1] if v_idx-1 < len(r) else "").strip()
        if not c:
            continue
        try:
            out[c] = float(v)
        except Exception:
            try:
                out[c] = float(v.replace("%", "").strip()) / 100.0
            except Exception:
                pass
    return out

# =========================================================
# FUENTE LEAD
# =========================================================
def infer_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        return "WEB"
    return "DESCONOCIDA"

# =========================================================
# LEAD: buscar/crear
# =========================================================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_normed: str, fuente_guess: str) -> Tuple[int, str, str, bool]:
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono'.")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_normed)
    if row:
        vals = ws_leads.row_values(row)
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        lead_id = (vals[idx_id-1] if idx_id and idx_id-1 < len(vals) else "").strip()
        estatus = (vals[idx_est-1] if idx_est and idx_est-1 < len(vals) else "").strip() or "INICIO"
        return row, lead_id, estatus, False

    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)
    new_row = [""] * len(headers_row)

    def set_if(col_name, val):
        idx = col_idx(leads_headers, col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", tel_raw)
    set_if("Telefono_Normalizado", tel_normed)
    set_if("Fuente_Lead", fuente_guess or "DESCONOCIDA")
    set_if("Fecha_Registro", now_iso_utc())
    set_if("Ultima_Actualizacion", now_iso_utc())
    set_if("ESTATUS", "INICIO")
    set_if("Es_Cliente", "NO")
    set_if("Cuestionario_Enviado", "NO")
    set_if("Evento_Enviado", "NO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_normed)
    if not row:
        raise RuntimeError("No pude recuperar la fila recién creada en BD_Leads.")
    return row, lead_id, "INICIO", True

# =========================================================
# CONFIG paso
# =========================================================
def load_config_row(ws_config, paso_actual: str) -> Dict[str, str]:
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso'.")

    paso_actual = (paso_actual or "").strip() or "INICIO"
    row = find_row_by_value(ws_config, idpaso_col, paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")
    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}' (ni para 'INICIO').")

    vals = ws_config.row_values(row)

    def get_field(name):
        idx = col_idx(cfg_headers, name)
        if not idx:
            return ""
        return (vals[idx-1] if idx-1 < len(vals) else "").strip()

    return {
        "ID_Paso": get_field("ID_Paso"),
        "Texto_Bot": get_field("Texto_Bot"),
        "Tipo_Entrada": get_field("Tipo_Entrada"),
        "Opciones_Validas": get_field("Opciones_Validas"),
        "Siguiente_Si_1": get_field("Siguiente_Si_1"),
        "Siguiente_Si_2": get_field("Siguiente_Si_2"),
        "Campo_BD_Leads_A_Actualizar": get_field("Campo_BD_Leads_A_Actualizar"),
        "Regla_Validacion": get_field("Regla_Validacion"),
        "Mensaje_Error": get_field("Mensaje_Error"),
    }

# =========================================================
# ABOGADOS: pick + read
# =========================================================
def find_abogado_row(ws_abogados, abogado_id: str) -> Optional[int]:
    headers = build_header_map(ws_abogados)
    idc = col_idx(headers, "ID_Abogado")
    if not idc:
        return None
    return find_row_by_value(ws_abogados, idc, abogado_id)

def read_abogado(ws_abogados, abogado_id: str) -> Dict[str, str]:
    headers = build_header_map(ws_abogados)
    row = find_abogado_row(ws_abogados, abogado_id)
    if not row:
        return {}
    vals = ws_abogados.row_values(row)

    def g(col):
        idx = col_idx(headers, col)
        return (vals[idx-1] if idx and idx-1 < len(vals) else "").strip()

    return {
        "_row": str(row),
        "ID_Abogado": g("ID_Abogado"),
        "Nombre_Abogado": g("Nombre_Abogado"),
        "Telefono_Abogado": g("Telefono_Abogado"),
        "Activo": g("Activo"),
        "Orden_Rotacion": g("Orden_Rotacion"),
        "Acepto_Asesoria": g("Acepto_Asesoria"),
        "Enviar_Cuestionario_SID": g("Enviar_Cuestionario_SID"),
        "Enviar_Cuestionario_Vars_JSON": g("Enviar_Cuestionario_Vars_JSON"),
        "Proximo_Evento_Fecha": g("Proximo_Evento_Fecha"),
        "Proximo_Evento_SID": g("Proximo_Evento_SID"),
        "Proximo_Evento_Vars_JSON": g("Proximo_Evento_Vars_JSON"),
    }

def pick_abogado_rotacion(ws_abogados) -> Tuple[str, str, str]:
    headers = build_header_map(ws_abogados)
    idc = col_idx(headers, "ID_Abogado")
    nc = col_idx(headers, "Nombre_Abogado")
    tc = col_idx(headers, "Telefono_Abogado")
    ac = col_idx(headers, "Activo")
    oc = col_idx(headers, "Orden_Rotacion")

    rows = ws_abogados.get_all_values()[1:]
    pool = []
    for r in rows:
        if not idc or idc-1 >= len(r):
            continue
        activo = (r[ac-1] if ac and ac-1 < len(r) else "SI").strip().upper()
        if activo != "SI":
            continue
        orden = 999999
        try:
            orden = int((r[oc-1] if oc and oc-1 < len(r) else "999999").strip())
        except Exception:
            pass
        pool.append((orden, r))

    if not pool:
        return "A01", "Abogado asignado", ""

    pool.sort(key=lambda x: x[0])
    r = pool[0][1]
    aid = (r[idc-1] if idc-1 < len(r) else "").strip() or "A01"
    anom = (r[nc-1] if nc and nc-1 < len(r) else "").strip() or "Abogado"
    atel = (r[tc-1] if tc and tc-1 < len(r) else "").strip()
    return aid, anom, atel

# =========================================================
# CÁLCULO PRELIMINAR
# =========================================================
def calcular_estimacion_mx(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: Dict[str, float]) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        sd = salario_mensual / 30.0
        indemn_90 = params.get("Indemnizacion_Dias", 90.0) * sd
        prima_ant = params.get("Prima_Antiguedad_Dias_Por_Anio", 12.0) * sd * anios

        total = indemn_90 + prima_ant

        # 20 días por año: estimación típica para despido
        if str(tipo_caso).strip() == "1":
            veinte = params.get("Veinte_Dias_Por_Anio", 20.0) * sd * anios
            total += veinte

        return round(float(total), 2)
    except Exception:
        return 0.0

# =========================================================
# OPENAI (opcional)
# =========================================================
def generar_resumen_simple(descripcion: str, max_words: int = 50) -> str:
    base = re.sub(r"\s+", " ", (descripcion or "").strip())
    words = base.split(" ")
    return " ".join(words[:max_words]) if len(words) > max_words else base

def generar_resumen_ai(descripcion: str, max_words: int = 50) -> str:
    if not descripcion:
        return "Caso recibido. Un abogado revisará tu información para orientarte con precisión."
    if not OPENAI_API_KEY:
        return generar_resumen_simple(descripcion, max_words)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        sys = f"Eres un abogado laboral mexicano. Resume en máximo {max_words} palabras y da 1 recomendación práctica."
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys},
                {"role": "user", "content": descripcion}
            ],
            max_tokens=180
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return "Análisis preliminar en proceso. Un abogado revisará tu caso."

# =========================================================
# ROUTES
# =========================================================
@app.get("/")
def health():
    return "ok", 200

# ---------------------------------------------------------
# 1) WHATSAPP BOT
# ---------------------------------------------------------
@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone_raw = phone_raw(request.form.get("From") or request.values.get("From") or "")
    from_phone_normed = phone_norm(from_phone_raw)

    body_raw = request.form.get("Body") or request.values.get("Body") or ""
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)

    canal = "WHATSAPP"
    fuente_guess = infer_fuente(msg_in)
    modelo_ai = OPENAI_MODEL if OPENAI_API_KEY else ""

    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = sh.worksheet(TAB_LEADS)
        ws_config = sh.worksheet(TAB_CONFIG)
        ws_logs = sh.worksheet(TAB_LOGS)
        ws_abogados = sh.worksheet(TAB_ABOGADOS)
        ws_sys = sh.worksheet(TAB_SYS)
        ws_param = sh.worksheet(TAB_PARAM)
    except Exception:
        return safe_reply("⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales.")

    leads_headers = build_header_map(ws_leads)

    # Lead
    try:
        lead_row, lead_id, estatus_actual, created = get_or_create_lead(
            ws_leads, leads_headers, from_phone_raw, from_phone_normed, fuente_guess
        )
    except Exception as e:
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_utc(),
            "Telefono": from_phone_raw,
            "ID_Lead": "",
            "Paso": "",
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": "⚠️ Error interno al crear/buscar lead.",
            "Canal": canal,
            "Fuente_Lead": fuente_guess,
            "Modelo_AI": modelo_ai,
            "Errores": str(e),
        })
        return safe_reply("⚠️ Error interno (Lead). Revisa BD_Leads.")

    # read lead dict (por header)
    row_vals = ws_leads.row_values(lead_row)

    def lead_get(col):
        idx = col_idx(leads_headers, col)
        return (row_vals[idx-1] if idx and idx-1 < len(row_vals) else "").strip()

    lead_data = {h: lead_get(h) for h in ws_leads.row_values(1) if h}
    fuente = lead_data.get("Fuente_Lead") or fuente_guess
    bloqueado = (lead_data.get("Bloqueado_Por_No_Aceptar") or "").strip()
    es_cliente = (lead_data.get("Es_Cliente") or "NO").strip().upper()

    if bloqueado:
        out = "Entendido. Sin aceptación del aviso de privacidad no podemos continuar. Si cambias de opinión, escríbenos nuevamente."
        return safe_reply(out)

    # Menú cliente registrado
    if es_cliente == "SI" and estatus_actual != "CLIENTE_MENU":
        estatus_actual = "CLIENTE_MENU"

    # Config del paso
    try:
        cfg = load_config_row(ws_config, estatus_actual)
    except Exception:
        return safe_reply("⚠️ No hay configuración del bot para continuar. Revisa Config_XimenaAI.")

    paso_actual = (cfg.get("ID_Paso") or estatus_actual or "INICIO").strip()
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = cfg.get("Texto_Bot") or ""
    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = (cfg.get("Mensaje_Error") or "Respuesta inválida.").strip()

    errores = ""
    next_paso = paso_actual
    out = ""

    # Disparo INICIO para leads nuevos
    if created and paso_actual == "INICIO":
        next_paso = sig1 or "AVISO_PRIVACIDAD"
        out = texto_bot or "Hola 👋"
        update_row_by_headers(ws_leads, leads_headers, lead_row, {
            "Ultima_Actualizacion": now_iso_utc(),
            "Paso_Anterior": "INICIO",
            "ESTATUS": next_paso,
            "Ultimo_Mensaje_Cliente": msg_in,
            "Fuente_Lead": fuente_guess,
            "Ultimo_Error": "",
        })
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_utc(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": next_paso,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente_guess,
            "Modelo_AI": modelo_ai,
            "Errores": "",
        })
        return safe_reply(out)

    # CLIENTE_MENU (simple)
    if paso_actual == "CLIENTE_MENU":
        if msg_opt not in ("1", "2", "3"):
            out = texto_bot + "\n\n" + msg_error
        else:
            if msg_opt == "1":
                out = "📅 Próximas fechas: (en construcción). Si deseas, pide *3* para contactar a tu abogado."
            elif msg_opt == "2":
                out = "📌 Resumen: (en construcción). Si deseas, pide *3* para contactar a tu abogado."
            else:
                lw = lead_data.get("Link_WhatsApp") or ""
                abn = lead_data.get("Abogado_Asignado_Nombre") or "tu abogado"
                out = f"Puedes contactar a {abn} aquí:\n{lw}" if lw else f"En breve {abn} te contactará."
        next_paso = "CLIENTE_MENU"
    else:
        # OPCIONES / TEXTO / SISTEMA (mínimo)
        if tipo == "OPCIONES":
            if opciones_validas and msg_opt not in opciones_validas:
                out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
                next_paso = paso_actual
            else:
                if campo_update and col_idx(leads_headers, campo_update):
                    update_row_by_headers(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})
                elif campo_update:
                    errores += f"Campo no existe: {campo_update}. "

                next_paso = sig1 if (opciones_validas and msg_opt == opciones_validas[0]) else (sig2 or paso_actual)
                # responde con el texto del siguiente paso
                try:
                    cfg2 = load_config_row(ws_config, next_paso)
                    out = cfg2.get("Texto_Bot") or "Continuemos."
                except Exception:
                    out = "Continuemos."

        elif tipo == "TEXTO":
            if campo_update and col_idx(leads_headers, campo_update):
                update_row_by_headers(ws_leads, leads_headers, lead_row, {campo_update: msg_in})
            elif campo_update:
                errores += f"Campo no existe: {campo_update}. "

            next_paso = sig1 or paso_actual
            try:
                cfg2 = load_config_row(ws_config, next_paso)
                out = cfg2.get("Texto_Bot") or "Gracias. Continuemos."
            except Exception:
                out = "Gracias. Continuemos."

        else:
            # SISTEMA: GENERAR_RESULTADOS (aquí sí cerramos)
            if paso_actual == "GENERAR_RESULTADOS":
                sys_cfg = load_key_value_sheet(ws_sys)
                params = load_parametros(ws_param)

                # refresh lead
                row_vals2 = ws_leads.row_values(lead_row)
                lead_data = {h: ((row_vals2[col_idx(leads_headers, h)-1] if col_idx(leads_headers, h) else "") or "").strip()
                             for h in ws_leads.row_values(1) if h}

                # cálculo + resumen
                try:
                    salario = float((lead_data.get("Salario_Mensual") or "0").strip())
                except Exception:
                    salario = 0.0

                monto = calcular_estimacion_mx(
                    tipo_caso=lead_data.get("Tipo_Caso") or "",
                    salario_mensual=salario,
                    fecha_ini=lead_data.get("Fecha_Inicio_Laboral") or "",
                    fecha_fin=lead_data.get("Fecha_Fin_Laboral") or "",
                    params=params
                )

                max_words = int(float(sys_cfg.get("MAX_PALABRAS_RESUMEN", "50") or "50"))
                resumen = generar_resumen_ai(lead_data.get("Descripcion_Situacion") or "", max_words=max_words)

                # asignar abogado
                abogado_id, abogado_nombre, abogado_tel = pick_abogado_rotacion(ws_abogados)
                token = uuid.uuid4().hex[:16]
                base_url = (sys_cfg.get("BASE_URL_WEB") or "").strip()
                ruta_reporte = (sys_cfg.get("RUTA_REPORTE") or "").strip()
                link_reporte = (ruta_reporte.rstrip("/") + "/" + token) if ruta_reporte else (base_url.rstrip("/") + "/reporte/" + token if base_url else "")

                # Mensaje al cliente (normal)
                out = (
                    f"✅ *Estimación preliminar*\n\n"
                    f"🧾 {resumen}\n\n"
                    f"💰 Monto estimado: *${monto:,.2f} MXN*\n"
                    f"👩‍⚖️ Abogado asignado: *{abogado_nombre}* ({abogado_id})\n\n"
                    f"📄 Informe: {link_reporte}\n"
                    f"ℹ️ {base_url}"
                ).strip()

                # persistencia
                update_row_by_headers(ws_leads, leads_headers, lead_row, {
                    "Analisis_AI": resumen,
                    "Resultado_Calculo": str(monto),
                    "Abogado_Asignado_ID": abogado_id,
                    "Abogado_Asignado_Nombre": abogado_nombre,
                    "Token_Reporte": token,
                    "Link_Reporte_Web": link_reporte,
                    "Es_Cliente": "SI",
                    "Cuestionario_Enviado": lead_data.get("Cuestionario_Enviado") or "NO",
                    "Evento_Enviado": lead_data.get("Evento_Enviado") or "NO",
                })

                # Mensaje al abogado (normal, inmediato)
                if abogado_tel:
                    ok, err = send_whatsapp_message(
                        to_whatsapp=f"whatsapp:{abogado_tel}",
                        body=(
                            f"📩 NUEVO LEAD TDLM\n"
                            f"Nombre: {lead_data.get('Nombre','')}\n"
                            f"Tel: {from_phone_normed}\n"
                            f"Monto est.: ${monto:,.2f}\n"
                            f"Reporte: {link_reporte}\n"
                            f"ID Lead: {lead_id}"
                        )
                    )
                    if not ok:
                        errores += f" TwilioAbogado {err}. "

                next_paso = "CLIENTE_MENU"

            else:
                out = texto_bot or "Listo."
                next_paso = sig1 or paso_actual

    # Final update + log
    update_row_by_headers(ws_leads, leads_headers, lead_row, {
        "Ultima_Actualizacion": now_iso_utc(),
        "Paso_Anterior": paso_actual,
        "ESTATUS": next_paso,
        "Ultimo_Mensaje_Cliente": msg_in,
        "Ultimo_Error": errores.strip(),
    })

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso_utc(),
        "Telefono": from_phone_raw,
        "ID_Lead": lead_id,
        "Paso": next_paso,
        "Mensaje_Entrante": msg_in,
        "Mensaje_Saliente": out,
        "Canal": canal,
        "Fuente_Lead": fuente,
        "Modelo_AI": modelo_ai,
        "Errores": errores.strip(),
    })

    return safe_reply(out)

# ---------------------------------------------------------
# 2) TRIGGER (APPSHEET): send template outside 24h
# ---------------------------------------------------------
@app.post("/trigger")
def trigger():
    """
    AppSheet POST JSON:
      {"action":"send_cuestionario","abogado_id":"A01"}
      {"action":"send_evento","abogado_id":"A01"}
    Optional:
      {"action":"send_cuestionario","abogado_id":"A01","lead_id":"..."}
    """
    payload = request.get_json(silent=True) or {}
    action = (payload.get("action") or "").strip()
    abogado_id = (payload.get("abogado_id") or "").strip()
    lead_id = (payload.get("lead_id") or "").strip()

    if action not in ("send_cuestionario", "send_evento"):
        return jsonify({"ok": False, "error": "action inválida"}), 400
    if not abogado_id:
        return jsonify({"ok": False, "error": "abogado_id requerido"}), 400

    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = sh.worksheet(TAB_LEADS)
        ws_abogados = sh.worksheet(TAB_ABOGADOS)
        ws_logs = sh.worksheet(TAB_LOGS)
    except Exception as e:
        return jsonify({"ok": False, "error": f"No pude abrir Sheets: {e}"}), 500

    leads_headers = build_header_map(ws_leads)

    # localizar lead objetivo:
    target_row = None
    target_lead_id = None

    if lead_id:
        id_col = col_idx(leads_headers, "ID_Lead")
        if not id_col:
            return jsonify({"ok": False, "error": "BD_Leads sin ID_Lead"}), 500
        target_row = find_row_by_value(ws_leads, id_col, lead_id)
        target_lead_id = lead_id
    else:
        # fallback: último lead asignado a ese abogado con bandera NO enviada
        ab_col = col_idx(leads_headers, "Abogado_Asignado_ID")
        if not ab_col:
            return jsonify({"ok": False, "error": "BD_Leads sin Abogado_Asignado_ID"}), 500
        ab_vals = ws_leads.col_values(ab_col)[1:]  # sin header
        rows = []
        for i, v in enumerate(ab_vals, start=2):
            if (v or "").strip() == abogado_id:
                rows.append(i)
        rows = rows[::-1]  # más reciente primero

        for r in rows:
            vals = ws_leads.row_values(r)
            def g(col):
                idx = col_idx(leads_headers, col)
                return (vals[idx-1] if idx and idx-1 < len(vals) else "").strip()

            if action == "send_cuestionario":
                if (g("Cuestionario_Enviado") or "NO").upper() != "SI":
                    target_row = r
                    target_lead_id = g("ID_Lead")
                    break
            else:
                if (g("Evento_Enviado") or "NO").upper() != "SI":
                    target_row = r
                    target_lead_id = g("ID_Lead")
                    break

    if not target_row:
        return jsonify({"ok": False, "error": "No encontré lead pendiente para ese abogado."}), 404

    # leer lead
    vals = ws_leads.row_values(target_row)
    def lead_g(col):
        idx = col_idx(leads_headers, col)
        return (vals[idx-1] if idx and idx-1 < len(vals) else "").strip()

    tel_raw = lead_g("Telefono")  # puede venir whatsapp:+52...
    tel_normed = phone_norm(tel_raw)
    to_client = tel_raw if tel_raw.startswith("whatsapp:") else f"whatsapp:{tel_normed}"

    abogado = read_abogado(ws_abogados, abogado_id)
    if not abogado:
        return jsonify({"ok": False, "error": "No encontré abogado en Cat_Abogados."}), 404

    # Template data
    if action == "send_cuestionario":
        sid = (abogado.get("Enviar_Cuestionario_SID") or "").strip()
        vars_json = (abogado.get("Enviar_Cuestionario_Vars_JSON") or "").strip()
        flag_col = "Cuestionario_Enviado"
    else:
        sid = (abogado.get("Proximo_Evento_SID") or "").strip()
        vars_json = (abogado.get("Proximo_Evento_Vars_JSON") or "").strip()
        flag_col = "Evento_Enviado"

    vars_obj = None
    if vars_json:
        try:
            vars_obj = json.loads(vars_json)
        except Exception:
            vars_obj = None

    ok, err = send_whatsapp_message(
        to_whatsapp=to_client,
        body=None if sid else "Hola, te compartimos información de seguimiento de tu caso.",
        content_sid=sid or None,
        content_variables=vars_obj or None
    )

    if ok:
        update_row_by_headers(ws_leads, leads_headers, target_row, {
            flag_col: "SI",
            "Ultima_Actualizacion": now_iso_utc(),
        })

        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso_utc(),
            "Telefono": tel_raw,
            "ID_Lead": target_lead_id or "",
            "Paso": f"TRIGGER_{action}",
            "Mensaje_Entrante": json.dumps(payload, ensure_ascii=False),
            "Mensaje_Saliente": f"TEMPLATE_SENT sid={sid}",
            "Canal": "SYSTEM",
            "Fuente_Lead": lead_g("Fuente_Lead"),
            "Modelo_AI": "",
            "Errores": "",
        })
        return jsonify({"ok": True, "lead_id": target_lead_id, "sent": action}), 200

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso_utc(),
        "Telefono": tel_raw,
        "ID_Lead": target_lead_id or "",
        "Paso": f"TRIGGER_{action}",
        "Mensaje_Entrante": json.dumps(payload, ensure_ascii=False),
        "Mensaje_Saliente": "FAILED",
        "Canal": "SYSTEM",
        "Fuente_Lead": lead_g("Fuente_Lead"),
        "Modelo_AI": "",
        "Errores": err,
    })
    return jsonify({"ok": False, "error": err}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
