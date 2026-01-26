import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

import gspread
from google.oauth2.service_account import Credentials

# =========================
# App
# =========================
app = Flask(__name__)

# =========================
# Env
# =========================
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

# =========================
# Time + Twilio
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_reply(text: str):
    resp = MessagingResponse()
    resp.message(text)
    return str(resp)

def render_text(s: str) -> str:
    """Convierte \\n literal a salto real (para que WhatsApp se vea bien)."""
    s = s or ""
    return s.replace("\\n", "\n")

# =========================
# Normalización
# =========================
def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    s = (raw or "").strip()
    s = s.replace("whatsapp:", "").strip()
    return s

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    if m:
        return m.group(0)
    return s

# =========================
# Google creds + gspread
# =========================
def get_env_creds_dict():
    if GOOGLE_CREDENTIALS_JSON:
        raw = GOOGLE_CREDENTIALS_JSON
        try:
            if raw.lstrip().startswith("{"):
                return json.loads(raw)
            decoded = base64.b64decode(raw).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON inválido (JSON/base64). Detalle: {e}")

    if GOOGLE_CREDENTIALS_PATH:
        if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
            raise RuntimeError("GOOGLE_CREDENTIALS_PATH no existe en el filesystem del servicio.")
        with open(GOOGLE_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("Faltan credenciales: usa GOOGLE_CREDENTIALS_JSON o GOOGLE_CREDENTIALS_PATH.")

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
        raise RuntimeError("Falta GOOGLE_SHEET_NAME.")
    return gc.open(GOOGLE_SHEET_NAME)

def open_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except Exception:
        raise RuntimeError(f"No existe la pestaña '{title}' en el Google Sheet '{GOOGLE_SHEET_NAME}'.")

# =========================
# Headers
# =========================
def build_header_map(ws):
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

def col_idx(headers_map: dict, name: str):
    return headers_map.get(name) or headers_map.get((name or "").lower())

def find_row_by_value(ws, col_idx_num: int, value: str):
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

def update_lead_batch(ws, header_map: dict, row_idx: int, updates: dict):
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

# =========================
# Leads: get/create
# =========================
def get_or_create_lead(ws_leads, leads_headers: dict, tel_raw: str, tel_norm: str, fuente: str = "DESCONOCIDA"):
    tel_col = col_idx(leads_headers, "Telefono")
    if not tel_col:
        raise RuntimeError("En BD_Leads falta la columna 'Telefono'.")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    if row:
        vals = ws_leads.row_values(row)
        idx_id = col_idx(leads_headers, "ID_Lead")
        idx_est = col_idx(leads_headers, "ESTATUS")
        lead_id = (vals[idx_id - 1] or "").strip() if idx_id and idx_id - 1 < len(vals) else ""
        estatus = (vals[idx_est - 1] or "").strip() if idx_est and idx_est - 1 < len(vals) else "INICIO"
        return row, lead_id, estatus or "INICIO", False

    lead_id = str(uuid.uuid4())
    headers_row = ws_leads.row_values(1)
    new_row = [""] * max(1, len(headers_row))

    def set_if(col_name, val):
        idx = col_idx(leads_headers, col_name)
        if idx and idx <= len(new_row):
            new_row[idx - 1] = val

    set_if("ID_Lead", lead_id)
    set_if("Telefono", tel_raw)
    set_if("Telefono_Normalizado", tel_norm)
    set_if("Fuente_Lead", fuente)
    set_if("Fecha_Registro", now_iso())
    set_if("Ultima_Actualizacion", now_iso())
    set_if("ESTATUS", "INICIO")

    ws_leads.append_row(new_row, value_input_option="USER_ENTERED")

    row = find_row_by_value(ws_leads, tel_col, tel_raw) or find_row_by_value(ws_leads, tel_col, tel_norm)
    return row, lead_id, "INICIO", True

# =========================
# Config row
# =========================
def load_config_row(ws_config, paso_actual: str):
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso'.")

    paso_actual = (paso_actual or "").strip() or "INICIO"
    row = find_row_by_value(ws_config, idpaso_col, paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")
    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}'.")

    row_vals = ws_config.row_values(row)

    fields = [
        "ID_Paso", "Texto_Bot", "Tipo_Entrada", "Opciones_Validas",
        "Siguiente_Si_1", "Siguiente_Si_2",
        "Campo_BD_Leads_A_Actualizar", "Regla_Validacion", "Mensaje_Error"
    ]

    def get_field(name):
        idx = col_idx(cfg_headers, name)
        return (row_vals[idx-1] if idx and idx-1 < len(row_vals) else "").strip()

    return {k: get_field(k) for k in fields}

# =========================
# Config_Sistema + Parametros_Legales
# =========================
def load_key_value(ws, key_col="Clave", val_col="Valor"):
    h = build_header_map(ws)
    k = col_idx(h, key_col)
    v = col_idx(h, val_col)
    out = {}
    if not k or not v:
        return out
    rows = ws.get_all_values()[1:]
    for r in rows:
        kk = (r[k-1] if k-1 < len(r) else "").strip()
        vv = (r[v-1] if v-1 < len(r) else "").strip()
        if kk:
            out[kk] = vv
    return out

def load_parametros(ws_param):
    h = build_header_map(ws_param)
    c = col_idx(h, "Concepto")
    v = col_idx(h, "Valor")
    out = {}
    if not c or not v:
        return out
    rows = ws_param.get_all_values()[1:]
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
            pass
    return out

# =========================
# Abogados
# =========================
def pick_abogado(ws_abogados):
    h = build_header_map(ws_abogados)
    idc = col_idx(h, "ID_Abogado")
    nc = col_idx(h, "Nombre_Abogado")
    tc = col_idx(h, "Telefono_Abogado")
    ac = col_idx(h, "Activo")

    rows = ws_abogados.get_all_values()[1:]
    for r in rows:
        activo = (r[ac-1] if ac and ac-1 < len(r) else "SI").strip().upper()
        if activo != "SI":
            continue
        aid = (r[idc-1] if idc and idc-1 < len(r) else "").strip()
        an = (r[nc-1] if nc and nc-1 < len(r) else "").strip()
        at = (r[tc-1] if tc and tc-1 < len(r) else "").strip()
        if aid:
            return aid, an, at
    return "A01", "Abogado", ""

# =========================
# Cálculo (compat con tu Parametros_Legales)
# =========================
def calcular_estimacion(tipo_caso: str, salario_mensual: float, fecha_ini: str, fecha_fin: str, params: dict) -> float:
    try:
        f_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")
        f_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        dias = max(0, (f_fin - f_ini).days)
        anios = dias / 365.0

        sd = salario_mensual / 30.0

        indemn_dias = float(params.get("Indemnizacion", 90))
        prima_ant_dias = float(params.get("Prima_Antiguedad", 12))
        veinte_dias = float(params.get("Veinte_Dias_Por_Anio", 20))

        total = (indemn_dias * sd) + (prima_ant_dias * sd * anios)

        # si fue "Me despidieron" (1) sumamos 20 días por año como estimación
        if (tipo_caso or "").strip() == "1":
            total += (veinte_dias * sd * anios)

        return round(total, 2)
    except:
        return 0.0

# =========================
# AUTO EJECUCIÓN SISTEMA (CLAVE)
# =========================
def run_system_step_if_needed(paso: str, lead_snapshot: dict, ws_leads, leads_headers, lead_row,
                              ws_abogados, ws_sys, ws_param) -> tuple[str, str, str]:
    """
    Si el paso es GENERAR_RESULTADOS, calcula + asigna + arma respuesta final.
    Retorna: (next_paso, out_text, errores)
    """
    errores = ""

    if paso != "GENERAR_RESULTADOS":
        return paso, "", errores

    sys_cfg = load_key_value(ws_sys)
    params = load_parametros(ws_param)

    # leer datos necesarios
    try:
        salario = float((lead_snapshot.get("Salario_Mensual") or "0").strip())
    except:
        salario = 0.0

    monto = calcular_estimacion(
        tipo_caso=lead_snapshot.get("Tipo_Caso") or "",
        salario_mensual=salario,
        fecha_ini=lead_snapshot.get("Fecha_Inicio_Laboral") or "",
        fecha_fin=lead_snapshot.get("Fecha_Fin_Laboral") or "",
        params=params
    )

    # resumen simple (por ahora sin GPT)
    desc = (lead_snapshot.get("Descripcion_Situacion") or "").strip()
    resumen = desc[:250] if desc else "Caso recibido. Un abogado revisará tu información."

    abogado_id, abogado_nombre, abogado_tel = pick_abogado(ws_abogados)

    token = uuid.uuid4().hex[:16]
    base_url = (sys_cfg.get("BASE_URL_WEB") or "").strip()
    ruta_reporte = (sys_cfg.get("RUTA_REPORTE") or "").strip()
    link_reporte = (ruta_reporte.rstrip("/") + "/" + token) if ruta_reporte else ""

    out = (
        f"✅ *Estimación preliminar*\n\n"
        f"🧾 {resumen}\n\n"
        f"💰 Monto estimado: *${monto:,.2f} MXN*\n"
        f"👩‍⚖️ Abogado asignado: *{abogado_nombre}* ({abogado_id})\n\n"
        f"📄 Informe: {link_reporte}\n"
        f"ℹ️ {base_url}"
    ).strip()

    # guardar en BD_Leads
    try:
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "Analisis_AI": resumen,
            "Resultado_Calculo": str(monto),
            "Abogado_Asignado_ID": abogado_id,
            "Abogado_Asignado_Nombre": abogado_nombre,
            "Token_Reporte": token,
            "Link_Reporte_Web": link_reporte,
            "Es_Cliente": "SI",
        })
    except Exception as e:
        errores += f"UpdateResultados {e}. "

    # siguiente paso del sistema principal
    return "CLIENTE_MENU", out, errores

# =========================
# Routes
# =========================
@app.get("/")
def health():
    return "ok", 200

@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone_raw = phone_raw(request.form.get("From") or "")
    from_phone_normed = phone_norm(from_phone_raw)

    body_raw = request.form.get("Body") or ""
    msg_in = normalize_msg(body_raw)
    msg_opt = normalize_option(body_raw)

    canal = "WHATSAPP"
    fuente = "DESCONOCIDA"
    modelo_ai = OPENAI_MODEL if OPENAI_API_KEY else ""

    default_error_msg = "⚠️ Servicio activo, pero no puedo abrir Google Sheets. Revisa credenciales."

    # Sheets
    try:
        gc = get_gspread_client()
        sh = open_spreadsheet(gc)
        ws_leads = open_worksheet(sh, TAB_LEADS)
        ws_config = open_worksheet(sh, TAB_CONFIG)
        ws_logs = open_worksheet(sh, TAB_LOGS)
        ws_abogados = open_worksheet(sh, TAB_ABOGADOS)
        ws_sys = open_worksheet(sh, TAB_SYS)
        ws_param = open_worksheet(sh, TAB_PARAM)
    except Exception:
        return safe_reply(default_error_msg)

    leads_headers = build_header_map(ws_leads)

    # Lead
    lead_row, lead_id, estatus_actual, created = get_or_create_lead(
        ws_leads, leads_headers, from_phone_raw, from_phone_normed, fuente
    )

    # leer snapshot del lead
    row_vals = ws_leads.row_values(lead_row)
    headers_list = ws_leads.row_values(1)
    lead_snapshot = {}
    for i, h in enumerate(headers_list):
        lead_snapshot[h] = (row_vals[i] if i < len(row_vals) else "") or ""

    if not msg_in:
        out = "Hola 👋 ¿En qué puedo ayudarte?"
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": estatus_actual,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": "",
        })
        return safe_reply(out)

    # Config paso actual
    cfg = load_config_row(ws_config, estatus_actual)
    paso_actual = (cfg.get("ID_Paso") or estatus_actual or "INICIO").strip()
    tipo = (cfg.get("Tipo_Entrada") or "").upper().strip()
    texto_bot = render_text(cfg.get("Texto_Bot") or "")

    opciones_validas = [normalize_option(x) for x in (cfg.get("Opciones_Validas") or "").split(",") if x.strip()]
    sig1 = (cfg.get("Siguiente_Si_1") or "").strip()
    sig2 = (cfg.get("Siguiente_Si_2") or "").strip()
    campo_update = (cfg.get("Campo_BD_Leads_A_Actualizar") or "").strip()
    msg_error = render_text((cfg.get("Mensaje_Error") or "Respuesta inválida.").strip())

    errores = ""

    # Disparo INICIO (primer mensaje)
    if created or paso_actual == "INICIO":
        next_paso = sig1 or "AVISO_PRIVACIDAD"
        out = texto_bot or "Hola 👋"
        update_lead_batch(ws_leads, leads_headers, lead_row, {
            "Ultima_Actualizacion": now_iso(),
            "Paso_Anterior": "INICIO",
            "ESTATUS": next_paso,
            "Ultimo_Mensaje_Cliente": msg_in,
        })
        safe_log(ws_logs, {
            "ID_Log": str(uuid.uuid4()),
            "Fecha_Hora": now_iso(),
            "Telefono": from_phone_raw,
            "ID_Lead": lead_id,
            "Paso": next_paso,
            "Mensaje_Entrante": msg_in,
            "Mensaje_Saliente": out,
            "Canal": canal,
            "Fuente_Lead": fuente,
            "Modelo_AI": modelo_ai,
            "Errores": "",
        })
        return safe_reply(out)

    # Lógica por tipo
    next_paso = paso_actual
    out = texto_bot or "Continuemos."

    if tipo == "OPCIONES":
        if opciones_validas and msg_opt not in opciones_validas:
            out = (texto_bot + "\n\n" if texto_bot else "") + msg_error
            next_paso = paso_actual
        else:
            if campo_update and col_idx(leads_headers, campo_update):
                update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_opt})

            next_paso = sig1 if (opciones_validas and msg_opt == opciones_validas[0]) else (sig2 or paso_actual)

            # ✅ AQUÍ VIENE LA CLAVE: si el siguiente paso es SISTEMA, lo ejecutamos en la MISMA interacción
            cfg2 = load_config_row(ws_config, next_paso)
            tipo2 = (cfg2.get("Tipo_Entrada") or "").upper().strip()

            # refrescar snapshot antes de sistema
            row_vals = ws_leads.row_values(lead_row)
            headers_list = ws_leads.row_values(1)
            lead_snapshot = {h: (row_vals[i] if i < len(row_vals) else "") or "" for i, h in enumerate(headers_list)}

            if tipo2 == "SISTEMA":
                next_paso2, out_sys, err_sys = run_system_step_if_needed(
                    next_paso, lead_snapshot, ws_leads, leads_headers, lead_row,
                    ws_abogados, ws_sys, ws_param
                )
                if out_sys:
                    out = out_sys
                    next_paso = next_paso2
                else:
                    out = render_text(cfg2.get("Texto_Bot") or out)
                errores += err_sys
            else:
                out = render_text(cfg2.get("Texto_Bot") or out)

    elif tipo == "TEXTO":
        if campo_update and col_idx(leads_headers, campo_update):
            update_lead_batch(ws_leads, leads_headers, lead_row, {campo_update: msg_in})
        next_paso = sig1 or paso_actual
        cfg2 = load_config_row(ws_config, next_paso)
        out = render_text(cfg2.get("Texto_Bot") or "Gracias. Continuemos.")

    else:
        # SISTEMA normal (si alguien cae aquí)
        out = texto_bot or "Listo."
        next_paso = sig1 or paso_actual

    # Final update
    update_lead_batch(ws_leads, leads_headers, lead_row, {
        "Ultima_Actualizacion": now_iso(),
        "Paso_Anterior": paso_actual,
        "ESTATUS": next_paso,
        "Ultimo_Mensaje_Cliente": msg_in,
    })

    safe_log(ws_logs, {
        "ID_Log": str(uuid.uuid4()),
        "Fecha_Hora": now_iso(),
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
