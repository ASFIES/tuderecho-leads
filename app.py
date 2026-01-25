import os
import json
import base64
import uuid
from datetime import datetime, timezone

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

import gspread
from google.oauth2.service_account import Credentials

# =========================
# Configuración de App
# =========================
app = Flask(__name__)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()

# =========================
# Helpers de Utilidad
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def normalize_text(text: str) -> str:
    return (text or "").strip()

def normalize_phone(raw: str) -> str:
    raw = (raw or "").strip()
    return raw.replace("whatsapp:", "").strip()

# =========================
# Gestión de Credenciales
# =========================
def get_gspread_client():
    json_creds = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    path_creds = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()
    
    if json_creds:
        try:
            if json_creds.startswith("{"):
                creds_dict = json.loads(json_creds)
            else:
                creds_dict = json.loads(base64.b64decode(json_creds).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Error en formato de credenciales JSON: {e}")
    elif path_creds:
        with open(path_creds, "r") as f:
            creds_dict = json.load(f)
    else:
        raise RuntimeError("No se encontraron credenciales de Google.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

# =========================
# Motor de Datos (Optimizado)
# =========================
class SheetManager:
    def __init__(self, gc, sheet_name):
        self.sh = gc.open(sheet_name)

    def get_worksheet_data(self, title):
        ws = self.sh.worksheet(title)
        all_values = ws.get_all_values()
        if not all_values:
            return ws, {}, []
        
        headers = [normalize_text(h) for h in all_values[0]]
        header_map = {name: i + 1 for i, name in enumerate(headers) if name}
        data_rows = all_values[1:]
        return ws, header_map, data_rows

    def update_row_batch(self, ws, header_map, row_idx, updates: dict):
        """Actualiza múltiples celdas en una sola fila de forma eficiente."""
        cells_to_update = []
        for col_name, value in updates.items():
            if col_name in header_map:
                col_idx = header_map[col_name]
                cells_to_update.append(gspread.Cell(row=row_idx, col=col_idx, value=value))
        if cells_to_update:
            ws.update_cells(cells_to_update, value_input_option="USER_ENTERED")

# =========================
# Webhook Principal
# =========================
@app.post("/whatsapp")
def whatsapp_webhook():
    from_phone = normalize_phone(request.form.get("From"))
    msg_in = normalize_text(request.form.get("Body"))
    
    try:
        gc = get_gspread_client()
        sm = SheetManager(gc, GOOGLE_SHEET_NAME)
        
        # 1. Cargar Datos (Una sola lectura por pestaña)
        ws_leads, leads_headers, leads_rows = sm.get_worksheet_data(TAB_LEADS)
        ws_config, config_headers, config_rows = sm.get_worksheet_data(TAB_CONFIG)
        ws_logs = sm.sh.worksheet(TAB_LOGS)

        # 2. Buscar o Crear Lead en memoria
        lead_row_idx = None
        estatus_actual = "INICIO"
        lead_id = str(uuid.uuid4())
        created = True

        tel_col_idx = leads_headers.get("Telefono")
        if tel_col_idx:
            for i, row in enumerate(leads_rows, start=2):
                if len(row) >= tel_col_idx and row[tel_col_idx-1] == from_phone:
                    lead_row_idx = i
                    lead_id = row[leads_headers.get("ID_Lead")-1] if "ID_Lead" in leads_headers else lead_id
                    estatus_actual = row[leads_headers.get("ESTATUS")-1] if "ESTATUS" in leads_headers else "INICIO"
                    created = False
                    break

        if created:
            # Crear nueva fila si no existe
            new_row = [""] * len(leads_headers)
            new_row[leads_headers["ID_Lead"]-1] = lead_id if "ID_Lead" in leads_headers else ""
            new_row[leads_headers["Telefono"]-1] = from_phone
            new_row[leads_headers["ESTATUS"]-1] = "INICIO"
            if "Fecha_Registro" in leads_headers:
                new_row[leads_headers["Fecha_Registro"]-1] = now_iso()
            ws_leads.append_row(new_row, value_input_option="USER_ENTERED")
            lead_row_idx = len(leads_rows) + 2

        # 3. Lógica de Navegación
        # Buscamos la config del paso actual en los datos cargados
        current_cfg = {}
        for row in config_rows:
            if row[config_headers["ID_Paso"]-1] == estatus_actual:
                current_cfg = {h: row[idx-1] for h, idx in config_headers.items() if idx <= len(row)}
                break
        
        if not current_cfg and estatus_actual != "INICIO":
            # Fallback a INICIO si el paso no existe
            for row in config_rows:
                if row[config_headers["ID_Paso"]-1] == "INICIO":
                    current_cfg = {h: row[idx-1] for h, idx in config_headers.items() if idx <= len(row)}
                    break

        # 4. Procesar Respuesta y Determinar Siguiente Paso
        tipo = current_cfg.get("Tipo_Entrada", "").upper()
        opciones = [o.strip().upper() for o in current_cfg.get("Opciones_Validas", "").split(",") if o.strip()]
        
        next_paso = current_cfg.get("Siguiente_Si_1", "INICIO")
        out_text = ""
        error_msg = current_cfg.get("Mensaje_Error", "Opción no válida.")

        if created or estatus_actual == "INICIO":
            out_text = current_cfg.get("Texto_Bot", "Hola")
            next_paso = current_cfg.get("Siguiente_Si_1", "INICIO")
        elif tipo == "OPCIONES":
            if msg_in.upper() in opciones:
                # Si es la primera opción, va a Siguiente_Si_1, si no a Siguiente_Si_2
                next_paso = current_cfg.get("Siguiente_Si_1") if msg_in.upper() == opciones[0] else current_cfg.get("Siguiente_Si_2")
                # Cargar texto del siguiente paso para fluidez
                for row in config_rows:
                    if row[config_headers["ID_Paso"]-1] == next_paso:
                        out_text = row[config_headers["Texto_Bot"]-1]
                        break
            else:
                out_text = f"{current_cfg.get('Texto_Bot')}\n\n⚠️ {error_msg}"
                next_paso = estatus_actual # Se queda en el mismo
        else:
            # Texto libre
            next_paso = current_cfg.get("Siguiente_Si_1")
            for row in config_rows:
                if row[config_headers["ID_Paso"]-1] == next_paso:
                    out_text = row[config_headers["Texto_Bot"]-1]
                    break

        # 5. Actualización Batch del Lead
        updates = {
            "ESTATUS": next_paso,
            "Ultima_Actualizacion": now_iso(),
            "Ultimo_Mensaje_Cliente": msg_in
        }
        # Guardar en campo personalizado si aplica
        campo_repo = current_cfg.get("Campo_BD_Leads_A_Actualizar")
        if campo_repo and tipo != "OPCIONES":
            updates[campo_repo] = msg_in
            
        sm.update_row_batch(ws_leads, leads_headers, lead_row_idx, updates)

        # 6. Log y Respuesta
        log_row = [str(uuid.uuid4()), now_iso(), from_phone, lead_id, next_paso, msg_in, out_text[:100], "WHATSAPP", ""]
        ws_logs.append_row(log_row)

        resp = MessagingResponse()
        resp.message(out_text)
        return str(resp)

    except Exception as e:
        print(f"ERROR: {e}")
        error_resp = MessagingResponse()
        error_resp.message("Lo siento, tuve un problema técnico. Por favor intenta más tarde.")
        return str(error_resp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))