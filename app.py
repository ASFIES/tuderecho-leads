# app.py
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

from redis import Redis
from rq import Queue

import gspread
from utils.sheets import get_gspread_client, open_spreadsheet, build_header_map, col_idx, with_backoff

MX_TZ = ZoneInfo("America/Mexico_City")

# ===== ENV =====
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()

REDIS_URL = os.environ.get("REDIS_URL", "redis://red-d5svi5v5r7bs73basen0:6379").strip()  # 🔥🔥🔥 URL de Render Redis
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()  # 🔥🔥🔥 nombre de cola: "ximena"

app = Flask(__name__)

def now_iso():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

def normalize_phone(p: str) -> str:
    p = (p or "").strip()
    p = p.replace("whatsapp:", "")
    return re.sub(r"\D+", "", p)

def enqueue_process_lead(lead_row: int, lead_id: str) -> bool:
    """
    🔥🔥🔥 Encola SOLO 1 job por lead usando lock Redis.
    """
    if not REDIS_URL:
        return False

    r = Redis.from_url(REDIS_URL)
    lock_key = f"lock:lead:{lead_id}"

    # NX = solo si NO existe; EX=180s evita duplicados por reintentos
    ok = r.set(lock_key, "1", nx=True, ex=180)
    if not ok:
        return False

    q = Queue(REDIS_QUEUE_NAME, connection=r)
    q.enqueue("worker_jobs.process_lead", lead_row, lead_id, job_timeout=120)
    return True

def get_or_create_lead(ws, telefono_norm: str):
    """
    Busca lead por Telefono_Normalizado. Si no existe, crea fila nueva.
    Retorna: (row_idx, lead_id, headers_map)
    """
    headers = with_backoff(lambda: ws.row_values(1))
    hm = build_header_map(headers)

    c_tel = col_idx(hm, "Telefono_Normalizado") or col_idx(hm, "Telefono")
    c_id = col_idx(hm, "ID_Lead")
    c_est = col_idx(hm, "ESTATUS")
    c_freg = col_idx(hm, "Fecha_Registro")
    c_uact = col_idx(hm, "Ultima_Actualizacion")
    c_tel_raw = col_idx(hm, "Telefono")

    if not (c_tel and c_id and c_est):
        raise RuntimeError("BD_Leads debe tener columnas: ID_Lead, Telefono_Normalizado, ESTATUS")

    # Lee toda la columna de teléfonos normalizados (1 request grande; MVP)
    col_vals = with_backoff(lambda: ws.col_values(c_tel))
    # col_vals incluye header en index 0
    for i in range(2, len(col_vals) + 1):
        if (col_vals[i-1] or "").strip() == telefono_norm:
            lead_id = (ws.cell(i, c_id).value or "").strip()
            return i, lead_id, hm

    # Crear nuevo lead
    lead_id = f"{telefono_norm}-{os.urandom(3).hex()}"
    row = [""] * len(headers)

    def setv(col, val):
        if col and 1 <= col <= len(row):
            row[col-1] = val

    setv(c_id, lead_id)
    setv(c_tel, telefono_norm)
    setv(c_tel_raw, f"whatsapp:+{telefono_norm}")
    setv(c_est, "INICIO")
    setv(c_freg, now_iso())
    setv(c_uact, now_iso())

    with_backoff(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
    # nueva fila al final
    new_row_idx = len(with_backoff(lambda: ws.col_values(1)))
    return new_row_idx, lead_id, hm

@app.post("/whatsapp")
def whatsapp_webhook():
    incoming = (request.form.get("Body") or "").strip()
    from_ = (request.form.get("From") or "").strip()

    phone_norm = normalize_phone(from_)

    gc = get_gspread_client()
    sh = open_spreadsheet(gc)
    ws = sh.worksheet(TAB_LEADS)

    lead_row, lead_id, hm = get_or_create_lead(ws, phone_norm)

    # Obtén estatus actual
    c_est = col_idx(hm, "ESTATUS")
    estatus = (ws.cell(lead_row, c_est).value or "").strip() if c_est else "INICIO"

    resp = MessagingResponse()

    # Flujo MVP: si llega "hola", inicia
    if estatus == "INICIO":
        resp.message(
            "Hola, soy Ximena, asistente virtual de *Tu Derecho Laboral México*.\n\n"
            "Te acompañaré durante todo el proceso hasta asignarte un abogado.\n\n"
            "¿Deseas continuar?\n"
            "1) Sí\n"
            "2) No"
        )
        # Cambia a AVISO_PRIVACIDAD
        ws.update_cell(lead_row, c_est, "AVISO_PRIVACIDAD")
        return str(resp)

    if estatus == "AVISO_PRIVACIDAD":
        if incoming not in ("1", "2"):
            resp.message("Por favor responde con 1 o 2.")
            return str(resp)
        if incoming == "2":
            resp.message("Entendido. Si deseas retomar tu caso, escríbenos cuando gustes.")
            ws.update_cell(lead_row, c_est, "FIN_NO_ACEPTA")
            return str(resp)

        resp.message("Perfecto. Continuemos:\n1) Me acaban de despedir\n2) Tuve que renunciar")
        ws.update_cell(lead_row, c_est, "CASO_TIPO")
        return str(resp)

    if estatus == "CASO_TIPO":
        if incoming not in ("1", "2"):
            resp.message("Responde con 1 o 2.")
            return str(resp)

        # Guarda Tipo_Caso
        c_tipo = col_idx(hm, "Tipo_Caso")
        if c_tipo:
            ws.update_cell(lead_row, c_tipo, incoming)

        resp.message("De acuerdo. Empecemos, por favor dime tu *nombre*.")
        ws.update_cell(lead_row, c_est, "NOMBRE")
        return str(resp)

    if estatus == "NOMBRE":
        c_nom = col_idx(hm, "Nombre")
        if c_nom:
            ws.update_cell(lead_row, c_nom, incoming)

        resp.message("Gracias. Ahora tu *apellido*.")
        ws.update_cell(lead_row, c_est, "APELLIDO")
        return str(resp)

    if estatus == "APELLIDO":
        c_ap = col_idx(hm, "Apellido")
        if c_ap:
            ws.update_cell(lead_row, c_ap, incoming)

        resp.message("Describe brevemente tu situación (mínimo 10 caracteres).")
        ws.update_cell(lead_row, c_est, "DESCRIPCION")
        return str(resp)

    if estatus == "DESCRIPCION":
        if len(incoming) < 10:
            resp.message("Escribe un poco más (mínimo 10 caracteres).")
            return str(resp)

        c_desc = col_idx(hm, "Descripcion_Situacion")
        if c_desc:
            ws.update_cell(lead_row, c_desc, incoming)

        resp.message("Gracias. Dime el *AÑO* de inicio (ej. 2020).")
        ws.update_cell(lead_row, c_est, "INI_ANIO")
        return str(resp)

    if estatus == "INI_ANIO":
        if not re.match(r"^(19\d{2}|20\d{2})$", incoming):
            resp.message("Escribe un año válido (ej. 2018, 2020, 2024).")
            return str(resp)
        c = col_idx(hm, "Inicio_Anio")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("Ahora el *MES* de inicio (1 a 12).")
        ws.update_cell(lead_row, c_est, "INI_MES")
        return str(resp)

    if estatus == "INI_MES":
        if not re.match(r"^(1[0-2]|[1-9])$", incoming):
            resp.message("Escribe un mes del 1 al 12.")
            return str(resp)
        c = col_idx(hm, "Inicio_Mes")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("Ahora el *DÍA* de inicio (1 a 31).")
        ws.update_cell(lead_row, c_est, "INI_DIA")
        return str(resp)

    if estatus == "INI_DIA":
        if not re.match(r"^(3[01]|[12]\d|[1-9])$", incoming):
            resp.message("Escribe un día del 1 al 31.")
            return str(resp)
        c = col_idx(hm, "Inicio_Dia")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("Dime el *AÑO* de término (ej. 2025).")
        ws.update_cell(lead_row, c_est, "FIN_ANIO")
        return str(resp)

    if estatus == "FIN_ANIO":
        if not re.match(r"^(19\d{2}|20\d{2})$", incoming):
            resp.message("Escribe un año válido (ej. 2020, 2023, 2025).")
            return str(resp)
        c = col_idx(hm, "Fin_Anio")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("Ahora el *MES* de término (1 a 12).")
        ws.update_cell(lead_row, c_est, "FIN_MES")
        return str(resp)

    if estatus == "FIN_MES":
        if not re.match(r"^(1[0-2]|[1-9])$", incoming):
            resp.message("Escribe un mes del 1 al 12.")
            return str(resp)
        c = col_idx(hm, "Fin_Mes")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("Finalmente el *DÍA* de término (1 a 31).")
        ws.update_cell(lead_row, c_est, "FIN_DIA")
        return str(resp)

    if estatus == "FIN_DIA":
        if not re.match(r"^(3[01]|[12]\d|[1-9])$", incoming):
            resp.message("Escribe un día del 1 al 31.")
            return str(resp)
        c = col_idx(hm, "Fin_Dia")
        if c: ws.update_cell(lead_row, c, incoming)
        resp.message("¿Cuál era tu salario mensual en MXN? (solo número, ej. 15000)")
        ws.update_cell(lead_row, c_est, "SALARIO")
        return str(resp)

    if estatus == "SALARIO":
        if not re.match(r"^\d{3,8}(\.\d{1,2})?$", incoming):
            resp.message("Escribe un número válido (ej. 15000).")
            return str(resp)
        c_sal = col_idx(hm, "Salario_Mensual")
        if c_sal: ws.update_cell(lead_row, c_sal, incoming)

        resp.message(
            "Aviso importante: La información que te brindamos es orientativa y no constituye asesoría legal.\n\n"
            "¿Deseas continuar?\n"
            "1) Continuar\n"
            "2) No deseo continuar"
        )
        ws.update_cell(lead_row, c_est, "DISCLAIMER")
        return str(resp)

    if estatus == "DISCLAIMER":
        if incoming not in ("1", "2"):
            resp.message("Responde con 1 o 2.")
            return str(resp)

        if incoming == "2":
            resp.message("Entendido. Si deseas retomar tu caso, escríbenos cuando gustes.")
            ws.update_cell(lead_row, c_est, "FIN_NO_CONTINUA")
            return str(resp)

        # 🔥🔥🔥 PASA A EN_PROCESO Y ENCOLA 1 JOB
        ws.update_cell(lead_row, c_est, "EN_PROCESO")

        encolado = enqueue_process_lead(lead_row, lead_id)

        resp.message(
            "Gracias, ya tengo lo necesario ✅\n\n"
            "Estoy preparando tu *estimación preliminar* y asignando a la abogada que llevará tu caso.\n"
            "En un momento te envío el resultado por este medio."
            + ("" if encolado else "\n\n(Ya estoy procesándolo, dame un momento 🙏)")
        )
        return str(resp)

    # Si ya está procesado y quedó en CLIENTE_MENU:
    if estatus == "CLIENTE_MENU":
        resp.message(
            "✅ Ya tengo tu estimación preliminar.\n\n"
            "¿Qué deseas hacer?\n"
            "1) Ver informe\n"
            "2) Hablar con un abogado\n"
            "3) Terminar"
        )
        return str(resp)

    resp.message("Estoy en pruebas. Escribe 'Hola' para iniciar.")
    return str(resp)

@app.get("/")
def health():
    return "OK", 200
