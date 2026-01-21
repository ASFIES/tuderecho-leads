import os
import json
import uuid
from datetime import datetime

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client as TwilioClient


# -----------------------------
# APP
# -----------------------------
app = Flask(__name__)

# -----------------------------
# ENV
# -----------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # (lo usaremos después)
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "TDLM_Sistema_Leads_v1")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")


# -----------------------------
# HELPERS: TIME
# -----------------------------
def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# -----------------------------
# HELPERS: GSPREAD
# -----------------------------
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_gspread_book = None


def _get_book():
    global _gspread_book
    if _gspread_book is not None:
        return _gspread_book

    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON en variables de entorno (Render).")

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
    client = gspread.authorize(creds)
    _gspread_book = client.open(GOOGLE_SHEET_NAME)
    return _gspread_book


def ws(nombre):
    return _get_book().worksheet(nombre)


def normalize_text(s: str) -> str:
    """Convierte '\\n' en saltos reales para WhatsApp."""
    if not s:
        return ""
    return s.replace("\\n", "\n").strip()


# -----------------------------
# BD_Leads columns
# -----------------------------
COL = {
    "ID_LEAD": 1,
    "TELEFONO": 2,
    "FUENTE": 3,
    "FECHA_REG": 4,
    "ULT_ACT": 5,
    "ESTATUS": 6,
    "AVISO_OK": 7,
    "TIPO_CASO": 8,
    "NOMBRE": 9,
    "APELLIDO": 10,
    "CORREO": 11,
    "SITUACION": 12,
    "ANALISIS_AI": 13,
    "F_INICIO": 14,
    "F_FIN": 15,
    "SALARIO": 16,
    "RESULTADO": 17,
    "ABOGADO_ID": 18,
    "ABOGADO_NOMBRE": 19,
    "LINK_WA": 20,
    "LINK_WEB": 21,
    "TOKEN_WEB": 22,
    "NOTAS_ABOG": 23,
    "BLOQUEADO": 24,
    "ULT_MSG": 25,
}


def buscar_fila_por_telefono(ws_leads, telefono: str):
    """Busca el teléfono (columna B) y devuelve la fila o None."""
    try:
        col_values = ws_leads.col_values(COL["TELEFONO"])
        try:
            idx = col_values.index(telefono)
            return idx + 1
        except ValueError:
            return None
    except Exception:
        try:
            cell = ws_leads.find(telefono)
            if cell is None:
                return None
            return cell.row
        except Exception:
            return None


def detectar_fuente(msg: str):
    m = (msg or "").lower()
    if "facebook" in m or "face" in m:
        return "FACEBOOK"
    if "sitio" in m or "web" in m or "pagina" in m or "página" in m:
        return "WEB"
    return "DESCONOCIDA"


def crear_lead(ws_leads, telefono: str, msg_inicial: str):
    lead_id = str(uuid.uuid4())
    fuente = detectar_fuente(msg_inicial)
    token = str(uuid.uuid4()).replace("-", "")
    link_web = ""

    fila = [""] * 25
    fila[COL["ID_LEAD"] - 1] = lead_id
    fila[COL["TELEFONO"] - 1] = telefono
    fila[COL["FUENTE"] - 1] = fuente
    fila[COL["FECHA_REG"] - 1] = now_iso()
    fila[COL["ULT_ACT"] - 1] = now_iso()
    fila[COL["ESTATUS"] - 1] = "INICIO"
    fila[COL["TOKEN_WEB"] - 1] = token
    fila[COL["LINK_WEB"] - 1] = link_web
    fila[COL["ULT_MSG"] - 1] = msg_inicial

    ws_leads.append_row(fila, value_input_option="RAW")
    return lead_id, fuente


def obtener(ws_leads, row: int, col_key: str):
    try:
        return ws_leads.cell(row, COL[col_key]).value
    except Exception:
        return ""


def actualizar(ws_leads, row: int, col_key: str, value: str) -> bool:
    """Actualiza solo si col_key existe en COL. Si no, regresa False (sin tronar)."""
    if col_key not in COL:
        return False
    try:
        ws_leads.update_cell(row, COL[col_key], value)
        return True
    except Exception:
        return False


# -----------------------------
# Config_XimenaAI helpers
# -----------------------------
def get_config_row(paso_id: str):
    """
    Devuelve un dict con la configuración del paso.
    Columnas esperadas:
    A ID_Paso | B Orden | C Texto_Bot | D Tipo_Entrada | E Opciones_Validas
    F Siguiente_Si_1 | G Siguiente_Si_2 | H Campo_BD_Leads_A_Actualizar
    I Regla_Validacion | J Mensaje_Error
    """
    cfg = ws("Config_XimenaAI")
    c = cfg.find(paso_id)
    if c is None:
        return None

    r = c.row
    return {
        "ID_PASO": cfg.cell(r, 1).value or "",
        "ORDEN": cfg.cell(r, 2).value or "",
        "TEXTO_BOT": cfg.cell(r, 3).value or "",
        "TIPO_ENTRADA": (cfg.cell(r, 4).value or "").strip().upper(),
        "OPCIONES_VALIDAS": (cfg.cell(r, 5).value or "").strip(),
        "SIGUIENTE_SI_1": (cfg.cell(r, 6).value or "").strip(),
        "SIGUIENTE_SI_2": (cfg.cell(r, 7).value or "").strip(),
        "CAMPO_ACTUALIZAR": (cfg.cell(r, 8).value or "").strip(),
        "REGLA_VALIDACION": (cfg.cell(r, 9).value or "").strip(),
        "MENSAJE_ERROR": (cfg.cell(r, 10).value or "").strip(),
    }


def get_text_for_step(paso_id: str) -> str:
    conf = get_config_row(paso_id)
    if not conf:
        return f"[No existe Config_XimenaAI para {paso_id}]"
    return normalize_text(conf["TEXTO_BOT"]) or f"[Config vacía en {paso_id}]"


def parse_opciones_validas(s: str):
    # "1,2" -> {"1","2"}
    if not s:
        return set()
    return {x.strip() for x in s.split(",") if x.strip()}


# -----------------------------
# Chat_Log
# -----------------------------
def log_chat(telefono: str, lead_id: str, paso: str, msg_in: str, msg_out: str, canal="WHATSAPP", fuente="", errores=""):
    try:
        wl = ws("Chat_Log")
        fila = [
            str(uuid.uuid4()),
            now_iso(),
            telefono,
            lead_id,
            paso,
            msg_in or "",
            msg_out or "",
            canal,
            fuente or "",
            "",          # Modelo_AI
            errores or ""  # Errores
        ]
        wl.append_row(fila, value_input_option="RAW")
    except Exception:
        pass


# -----------------------------
# FLOW ENGINE (OPCIONES)
# -----------------------------
def procesar_paso(ws_leads, row: int, telefono: str, msg_cliente: str):
    """
    Motor de flujo:
    - Lee ESTATUS actual
    - Lee config de ese paso
    - Si OPCIONES: valida y avanza a siguiente
    - Devuelve (texto_respuesta, paso_log, errores)
    """
    lead_id = obtener(ws_leads, row, "ID_LEAD")
    fuente = obtener(ws_leads, row, "FUENTE") or ""

    estatus = (obtener(ws_leads, row, "ESTATUS") or "INICIO").strip()

    conf = get_config_row(estatus)
    if not conf:
        texto = f"[No existe Config_XimenaAI para {estatus}]"
        return texto, estatus, fuente, ""

    tipo = conf["TIPO_ENTRADA"]

    # PASOS "SISTEMA": solo muestra texto y no espera validación
    if tipo == "SISTEMA":
        texto = normalize_text(conf["TEXTO_BOT"])
        return texto, estatus, fuente, ""

    # PASOS "OPCIONES": valida 1/2, decide siguiente y actualiza
    if tipo == "OPCIONES":
        opciones = parse_opciones_validas(conf["OPCIONES_VALIDAS"])
        r = (msg_cliente or "").strip()

        if r not in opciones:
            msg_err = normalize_text(conf["MENSAJE_ERROR"]) or "Por favor responde con una opción válida."
            return msg_err, estatus, fuente, ""

        # Guardar respuesta en BD_Leads si corresponde
        errores = ""
        campo = conf["CAMPO_ACTUALIZAR"]
        if campo:
            ok = actualizar(ws_leads, row, campo, r)
            if not ok:
                errores = f"Campo_BD_Leads_A_Actualizar inválido o no existe en COL: {campo}"

        # Siguiente paso
        if r == "1":
            siguiente = conf["SIGUIENTE_SI_1"] or ""
        else:
            siguiente = conf["SIGUIENTE_SI_2"] or ""

        if not siguiente:
            # Si no hay siguiente definido, nos quedamos en el mismo estatus
            texto = normalize_text(conf["TEXTO_BOT"])
            return texto, estatus, fuente, errores

        # Actualiza ESTATUS al siguiente paso
        actualizar(ws_leads, row, "ESTATUS", siguiente)

        # Respuesta = texto del siguiente paso
        texto_siguiente = get_text_for_step(siguiente)
        return texto_siguiente, siguiente, fuente, errores

    # Si no reconocemos tipo, devolvemos el texto del paso
    texto = normalize_text(conf["TEXTO_BOT"])
    return texto, estatus, fuente, f"Tipo_Entrada no soportado: {tipo}"


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET", "POST"])
def root():
    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "tuderecho-leads", "time": now_iso()})


@app.route("/health/sheets", methods=["GET"])
def health_sheets():
    ws_leads = ws("BD_Leads")
    a1 = ws_leads.acell("A1").value
    return jsonify({"ok": True, "sheet": GOOGLE_SHEET_NAME, "BD_Leads_A1": a1})


@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    try:
        msg_cliente = (request.values.get("Body", "") or "").strip()
        telefono = (request.values.get("From", "") or "").strip()
        resp = MessagingResponse()

        ws_leads = ws("BD_Leads")
        row = buscar_fila_por_telefono(ws_leads, telefono)

        # NUEVO LEAD: crea y manda INICIO
        if row is None:
            lead_id, fuente = crear_lead(ws_leads, telefono, msg_cliente)
            texto = get_text_for_step("INICIO")
            resp.message(texto)
            log_chat(telefono, lead_id, "INICIO", msg_cliente, texto, fuente=fuente)
            return str(resp)

        # EXISTENTE: procesa paso actual (motor)
        lead_id = obtener(ws_leads, row, "ID_LEAD")
        fuente = obtener(ws_leads, row, "FUENTE") or ""

        # Auditoría
        actualizar(ws_leads, row, "ULT_ACT", now_iso())
        actualizar(ws_leads, row, "ULT_MSG", msg_cliente)

        texto_out, paso_log, fuente_log, errores = procesar_paso(ws_leads, row, telefono, msg_cliente)

        resp.message(texto_out)
        log_chat(telefono, lead_id, paso_log, msg_cliente, texto_out, fuente=fuente_log or fuente, errores=errores)
        return str(resp)

    except Exception:
        resp = MessagingResponse()
        resp.message("Estamos teniendo un detalle técnico. Por favor intenta nuevamente en 1 minuto.")
        return str(resp)


@app.route("/notificar", methods=["POST"])
def notificar():
    data = request.get_json(force=True, silent=True) or {}

    numero_cliente = (data.get("telefono") or "").strip()
    mensaje = (data.get("mensaje") or "").strip()

    if not numero_cliente or not mensaje:
        return jsonify({"status": "error", "message": "Faltan campos: telefono y mensaje"}), 400

    if not TWILIO_SID or not TWILIO_TOKEN:
        return jsonify({"status": "error", "message": "Twilio no configurado (SID/TOKEN)"}), 500

    client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
    try:
        m = client.messages.create(
            body=mensaje,
            from_=TWILIO_WHATSAPP_NUMBER,
            to=numero_cliente
        )
        return jsonify({"status": "success", "sid": m.sid}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
