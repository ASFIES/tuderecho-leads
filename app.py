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
# HELPERS: GSPREAD (ROBUST)
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


# -----------------------------
# HELPERS: BD_Leads
# -----------------------------
# Ajustado a tu estructura final (PASO 1)
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
    """
    Devuelve el número de fila (int) donde está el teléfono, o None si no existe.
    Versión ultra-robusta: evita depender de gspread.exceptions.CellNotFound
    y evita el caso cell=None.
    """
    try:
        # Busca en columna B (Telefono) para que sea rápido y determinista
        col_values = ws_leads.col_values(COL["TELEFONO"])  # incluye encabezado
        try:
            idx = col_values.index(telefono)
            return idx + 1  # Sheets es 1-based
        except ValueError:
            return None
    except Exception:
        # Si por alguna razón col_values falla, fallback a find() sin romper
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

    # Link reporte: lo afinamos después; por ahora placeholder
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


def actualizar(ws_leads, row: int, col_key: str, value: str):
    try:
        ws_leads.update_cell(row, COL[col_key], value)
        return True
    except Exception:
        return False


# -----------------------------
# HELPERS: CONFIG_XimenaAI
# -----------------------------
def get_config_text(id_paso: str):
    """
    Lee Config_XimenaAI: busca ID_Paso en col A y toma Texto_Bot en col C (col 3).
    Si no existe o está vacío, devuelve un placeholder para que nunca truene.
    """
    try:
        cfg = ws("Config_XimenaAI")
        c = cfg.find(id_paso)
        if c is None:
            return f"[No existe Config_XimenaAI para {id_paso}]"
        texto = cfg.cell(c.row, 3).value
        return texto if texto else f"[Config vacía en {id_paso}]"
    except Exception:
        return f"[Error leyendo Config_XimenaAI: {id_paso}]"


# -----------------------------
# HELPERS: CHAT_LOG
# -----------------------------
def log_chat(telefono: str, lead_id: str, paso: str, msg_in: str, msg_out: str, canal="WHATSAPP", fuente=""):
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
            "",  # Modelo_AI (luego)
            "",  # Errores
        ]
        wl.append_row(fila, value_input_option="RAW")
    except Exception:
        # No rompemos el flujo si la bitácora falla.
        pass


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET", "POST"])
def root():
    # Evita ruido de 404 si algún callback pega a /
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
    """
    Webhook de Twilio: recibe mensaje entrante y responde.
    Por ahora:
    - Si no existe lead: crea y manda INICIO
    - Si existe y COMPLETADO: manda menú cliente
    - Si existe y no completado: manda INICIO (en el PASO 5 hacemos flujo completo)
    """
    try:
        msg_cliente = (request.values.get("Body", "") or "").strip()
        telefono = (request.values.get("From", "") or "").strip()  # "whatsapp:+521..."
        resp = MessagingResponse()

        ws_leads = ws("BD_Leads")
        row = buscar_fila_por_telefono(ws_leads, telefono)

        # NUEVO LEAD
        if row is None:
            lead_id, fuente = crear_lead(ws_leads, telefono, msg_cliente)
            texto = get_config_text("INICIO")
            resp.message(texto)
            log_chat(telefono, lead_id, "INICIO", msg_cliente, texto, fuente=fuente)
            return str(resp)

        # LEAD EXISTENTE
        lead_id = obtener(ws_leads, row, "ID_LEAD")
        estatus = obtener(ws_leads, row, "ESTATUS") or "INICIO"
        fuente = obtener(ws_leads, row, "FUENTE") or ""

        # Actualizar auditoría mínima
        actualizar(ws_leads, row, "ULT_ACT", now_iso())
        actualizar(ws_leads, row, "ULT_MSG", msg_cliente)

        if estatus == "COMPLETADO":
            nombre = obtener(ws_leads, row, "NOMBRE") or ""
            texto = (
                f"Hola {nombre}".strip() + " esperamos te encuentres bien. ¿Qué opción deseas?\n\n"
                "1) Próximas fechas agendadas\n"
                "2) Resumen de mi caso hasta hoy\n"
                "3) Contactar a mi abogado"
            )
            resp.message(texto)
            log_chat(telefono, lead_id, "MENU_CLIENTE", msg_cliente, texto, fuente=fuente)
            return str(resp)

        # En proceso: por ahora reenvía INICIO hasta que activemos el flujo completo
        texto = get_config_text("INICIO")
        resp.message(texto)
        log_chat(telefono, lead_id, "INICIO", msg_cliente, texto, fuente=fuente)
        return str(resp)

    except Exception as e:
        # Respuesta segura para que Twilio no reintente indefinidamente
        resp = MessagingResponse()
        resp.message("Estamos teniendo un detalle técnico. Por favor intenta nuevamente en 1 minuto.")
        return str(resp)


@app.route("/notificar", methods=["POST"])
def notificar():
    """
    Webhook para AppSheet: envía mensaje proactivo por WhatsApp.
    Body JSON esperado:
      { "telefono": "whatsapp:+521...", "mensaje": "texto..." }
    """
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
