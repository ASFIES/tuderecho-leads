import os
import json
import base64
import uuid
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo
import time

from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

import gspread
from google.oauth2.service_account import Credentials

# OpenAI
from openai import OpenAI

# =========================
# App Config
# =========================
app = Flask(__name__)

# =========================
# Environment Variables
# =========================
# Google Sheets
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "TDLM_Sistema_Leads_v1").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_CONFIG = os.environ.get("TAB_CONFIG", "Config_XimenaAI").strip()
TAB_LOGS = os.environ.get("TAB_LOGS", "Logs").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_PARAM = os.environ.get("TAB_PARAM", "Parametros_Legales").strip()

# Credentials
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "credentials.json").strip()

# OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

# Twilio
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_NUMBER = os.environ.get("TWILIO_NUMBER", "").strip()

# Timezone
MX_TZ = ZoneInfo("America/Mexico_City")

# =========================
# Global Cache (Simple)
# =========================
# En entornos serverless esto se puede reiniciar, pero ayuda en requests seguidos si el contenedor vive.
# Para producción estricta, usar Redis o Memcached.
CACHE_TTL = 300  # 5 minutos
_config_cache = {"data": None, "ts": 0}
_sys_cache = {"data": None, "ts": 0}
_abogados_cache = {"data": None, "ts": 0}

# =========================
# Utils
# =========================
def now_iso_mx():
    return datetime.now(MX_TZ).isoformat(timespec="seconds")

def normalize_text(text: str) -> str:
    """Normaliza texto eliminando acentos y caracteres especiales básicos para comparación."""
    s = (text or "").strip()
    s = unicodedata.normalize("NFKC", s)
    return s

def clean_phone(phone: str) -> str:
    """Elimina 'whatsapp:' y espacios."""
    if not phone: return ""
    return phone.replace("whatsapp:", "").strip()

def detect_source(msg: str) -> str:
    msg = (msg or "").lower()
    if any(x in msg for x in ["facebook", "anuncio", "fb"]):
        return "FACEBOOK"
    if any(x in msg for x in ["web", "sitio", "página", "pagina"]):
        return "WEB"
    return "DESCONOCIDA"

# =========================
# Google Sheets Core
# =========================
def get_gspread_client():
    creds = None
    if GOOGLE_CREDENTIALS_JSON:
        try:
            # Detectar si es raw JSON o base64
            if GOOGLE_CREDENTIALS_JSON.lstrip().startswith("{"):
                info = json.loads(GOOGLE_CREDENTIALS_JSON)
            else:
                info = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_JSON).decode("utf-8"))
            creds = Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        except Exception as e:
            print(f"Error cargando credenciales JSON env: {e}")

    if not creds and os.path.exists(GOOGLE_CREDENTIALS_PATH):
        try:
            creds = Credentials.from_service_account_file(
                GOOGLE_CREDENTIALS_PATH,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
        except Exception as e:
            print(f"Error cargando credenciales FILE: {e}")
            
    if not creds:
        raise RuntimeError("No se encontraron credenciales de Google válidas.")
        
    return gspread.authorize(creds)

def get_sheet_data(gc, sheet_name, tab_name, cache_ref=None):
    """
    Obtiene todos los registros de una hoja. 
    Usa caché simple en memoria si se provee.
    """
    if cache_ref:
        if (time.time() - cache_ref["ts"]) < CACHE_TTL and cache_ref["data"]:
            return cache_ref["data"]

    try:
        sh = gc.open(sheet_name)
        ws = sh.worksheet(tab_name)
        data = ws.get_all_records() # Devuelve lista de diccionarios
        
        if cache_ref is not None:
            cache_ref["data"] = data
            cache_ref["ts"] = time.time()
        return data, ws
    except Exception as e:
        print(f"Error leyendo {tab_name}: {e}")
        return [], None

# =========================
# Logic Helpers
# =========================
def get_config_step(config_data, step_id):
    """Busca la configuración de un paso específico en memoria."""
    step_id = (step_id or "INICIO").strip()
    for row in config_data:
        if str(row.get("ID_Paso", "")).strip() == step_id:
            return row
    # Fallback a INICIO si no se encuentra
    if step_id != "INICIO":
        return get_config_step(config_data, "INICIO")
    return {}

def validate_input(value, rule):
    """Valida la entrada del usuario contra reglas simples."""
    value = (value or "").strip()
    rule = (rule or "").strip()
    
    if not rule:
        return True
        
    if rule.startswith("REGEX:"):
        pattern = rule.split(":", 1)[1]
        return bool(re.match(pattern, value))
        
    if rule == "EMAIL":
        return bool(re.match(r"[^@]+@[^@]+\.[^@]+", value))
        
    if rule == "MONEY":
        try:
            float(value.replace("$", "").replace(",", ""))
            return True
        except:
            return False
            
    if rule == "DATE_YYYY_MM_DD":
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except:
            return False
            
    return True

def calculate_compensation(lead_data, params_data):
    """
    Realiza el cálculo aproximado dependiendo del tipo de caso.
    """
    try:
        tipo = str(lead_data.get("Tipo_Caso", "1")).strip()
        salario = float(str(lead_data.get("Salario_Mensual", "0")).replace("$", "").replace(",", ""))
        
        fi = lead_data.get("Fecha_Inicio_Laboral")
        ff = lead_data.get("Fecha_Fin_Laboral")
        if not fi or not ff:
            return 0.0
            
        d1 = datetime.strptime(fi, "%Y-%m-%d")
        d2 = datetime.strptime(ff, "%Y-%m-%d")
        anios = (d2 - d1).days / 365.25
        
        sdi = (salario / 30) * 1.0452 # Factor integrado básico
        
        # Parámetros (usar valores por defecto si fallan)
        try:
            indemn_dias = float(next((item['Valor'] for item in params_data if item['Concepto'] == 'Indemnizacion'), 90))
            prima_ant_dias = float(next((item['Valor'] for item in params_data if item['Concepto'] == 'Prima_Antiguedad'), 12))
            veinte_dias = float(next((item['Valor'] for item in params_data if item['Concepto'] == 'Veinte_Dias_Por_Anio'), 20))
        except:
            indemn_dias, prima_ant_dias, veinte_dias = 90, 12, 20

        total = 0.0
        
        # Base: Aguinaldo proporcional + Vacaciones + Prima Vacacional (Simplificado en Indemnización Constitucional para este MVP)
        # La formula real es mucho más compleja, aquí usamos la estimación solicitada.
        
        # Indemnización Constitucional (90 días)
        if tipo == "1": # Despido
            total += (indemn_dias * sdi)
            total += (veinte_dias * sdi * anios) # 20 días por año (solo si aplica, aquí asumimos estimación max)
        
        # Prima Antigüedad (12 días por año)
        total += (prima_ant_dias * sdi * anios)
        
        return round(total, 2)
    except Exception as e:
        print(f"Error cálculo: {e}")
        return 0.0

def assign_lawyer(salario, abogados_data):
    """
    Asigna abogado. Regla: > 50k -> A01.
    Si no, rotación o primero disponible.
    """
    try:
        salario_val = float(str(salario).replace("$", "").replace(",", ""))
    except:
        salario_val = 0

    # REGLA ORO: Si salario > 50,000 asignar a Veronica Zavala (A01)
    if salario_val >= 50000:
        target_id = "A01"
    else:
        # Aquí se podría implementar Round Robin real guardando el último asignado en SysConfig
        # Por ahora, tomamos el primero activo que NO sea A01 si hay, o A01 si es el único.
        # Simplificación: Retornar A01 por defecto o buscar otro.
        target_id = None
        for ab in abogados_data:
            if str(ab.get("Activo", "")).upper() == "SI" and str(ab.get("ID_Abogado")) != "A01":
                target_id = ab.get("ID_Abogado")
                break
        if not target_id: target_id = "A01"

    # Buscar detalles del ID seleccionado
    for ab in abogados_data:
        if str(ab.get("ID_Abogado")) == target_id:
            return target_id, ab.get("Nombre_Abogado", "Abogado"), ab.get("Telefono", "")
            
    return "A01", "Veronica Zavala", ""

def generate_ai_analysis(lead_data, monto, system_prompt):
    """Genera resumen con OpenAI."""
    if not OPENAI_API_KEY:
        return "Gracias. Hemos recibido tu información y un abogado analizará tu caso."

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        user_context = f"""
        Nombre: {lead_data.get('Nombre')}
        Caso: {'Despido' if lead_data.get('Tipo_Caso')=='1' else 'Renuncia'}
        Descripción: {lead_data.get('Descripcion_Situacion')}
        Fecha Inicio: {lead_data.get('Fecha_Inicio_Laboral')}
        Fecha Fin: {lead_data.get('Fecha_Fin_Laboral')}
        Salario: {lead_data.get('Salario_Mensual')}
        Estimación Preliminar: ${monto:,.2f}
        """

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_context}
            ],
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error OpenAI: {e}")
        return "Gracias. Tu caso ha sido registrado y será revisado por un experto."

# =========================
# Main Webhook
# =========================
@app.route("/webhook", methods=["POST"])
def whatsapp_webhook():
    start_time = time.time()
    
    # 1. Parse Input
    form = request.form
    from_number = form.get("From", "")
    body = form.get("Body", "").strip()
    
    clean_number = clean_phone(from_number)
    
    # 2. Init Resources (Bulk Read)
    try:
        gc = get_gspread_client()
        # Cargar configuración primero (cacheada)
        config_data, _ = get_sheet_data(gc, GOOGLE_SHEET_NAME, TAB_CONFIG, _config_cache)
        
        # Cargar Leads (No cacheable, muy volátil, pero leemos hoja para buscar)
        # OPTIMIZACIÓN: Si la hoja es gigante esto es lento. Mejor usar find de gspread si es posible.
        # Pero para evitar API Chattering de 'cell by cell', leeremos las columnas clave o usaremos la API search de gspread.
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws_leads = sh.worksheet(TAB_LEADS)
        
        # Buscar lead existente
        cell = None
        try:
            # Intentar búsqueda exacta en columna Telefono (Col 2 aprox)
            # Asumimos Telefono es columna 2 (B) y Telefono_Normalizado es C
            cell = ws_leads.find(clean_number) # Busca en toda la hoja, retorna primer match
        except gspread.exceptions.CellNotFound:
            pass
            
        is_new = False
        lead_row_idx = 0
        lead_data = {}
        
        headers = ws_leads.row_values(1)
        col_map = {h: i+1 for i, h in enumerate(headers)}
        
        if cell:
            lead_row_idx = cell.row
            # Leer SOLO la fila del lead
            row_values = ws_leads.row_values(lead_row_idx)
            lead_data = {h: (row_values[i] if i < len(row_values) else "") for i, h in enumerate(headers)}
        else:
            # Crear nuevo lead
            is_new = True
            lead_id = str(uuid.uuid4())
            new_row = [""] * len(headers)
            
            # Llenar datos básicos
            def set_val(key, val):
                if key in col_map: new_row[col_map[key]-1] = val
                
            set_val("ID_Lead", lead_id)
            set_val("Telefono", clean_number) # Guardar limpio
            set_val("Telefono_Normalizado", clean_number)
            set_val("Fuente_Lead", detect_source(body))
            set_val("Fecha_Registro", now_iso_mx())
            set_val("Ultima_Actualizacion", now_iso_mx())
            set_val("ESTATUS", "INICIO")
            
            ws_leads.append_row(new_row)
            # Recuperar row index (asumimos que es la última)
            # Para estar seguros, mejor no hacer reload. Construimos lead_data local.
            lead_data = {h: (new_row[i] if i < len(new_row) else "") for i, h in enumerate(headers)}
            # Necesitamos el ROW index real para updates futuros en este mismo request
            lead_row_idx = len(ws_leads.col_values(1)) # Esto puede ser costoso, mejor append retorna range en versiones nuevas
            # Si hay concurrencia alta, esto es peligroso. Pero para este MVP:
    except Exception as e:
        print(f"FATAL ERROR INIT: {e}")
        return str(MessagingResponse().message("Lo siento, hubo un error interno. Intenta más tarde."))

    # 3. Determine Flow
    current_status = lead_data.get("ESTATUS", "INICIO")
    
    # Manejar reinicio
    if is_new:
        current_status = "INICIO"
    
    step_config = get_config_step(config_data, current_status)
    
    # Variables a actualizar en Batch
    updates = {}
    next_step = current_status
    bot_reply = ""
    
    # 4. Process Logic
    # Si es nuevo, solo saludar
    if is_new:
        bot_reply = step_config.get("Texto_Bot", "Hola")
        # No avanza paso automáticamente en INICIO hasta que responda
    
    else:
        # A. Validar Respuesta Anterior
        req_type = step_config.get("Tipo_Entrada", "TEXTO")
        valid_opts = str(step_config.get("Opciones_Validas", "")).split(",")
        validation_rule = step_config.get("Regla_Validacion", "")
        field_to_update = step_config.get("Campo_BD_Leads_A_Actualizar", "")
        
        input_valid = True
        
        # Extracción de opción numérica simple si aplica
        selected_opt = body
        if req_type == "OPCIONES":
            # Intentar ver si el usuario mandó "1", "1. opcion", etc.
            match = re.search(r"^\s*(\d+)", body)
            if match:
                selected_opt = match.group(1)
            
            if valid_opts and valid_opts != [''] and selected_opt not in [o.strip() for o in valid_opts]:
                input_valid = False
        
        elif req_type == "TEXTO":
            if not validate_input(body, validation_rule):
                input_valid = False

        # Acciones según validez
        if not input_valid:
            bot_reply = step_config.get("Mensaje_Error", "Respuesta no válida.") + "\n\n" + step_config.get("Texto_Bot", "")
            # Se queda en el mismo paso
        else:
            # GUARDAR DATO
            if field_to_update:
                updates[field_to_update] = body if req_type == "TEXTO" else selected_opt
                lead_data[field_to_update] = updates[field_to_update] # Actualizar local context

            # CALCULAR SIGUIENTE PASO
            if req_type == "OPCIONES":
                # Ver si hay override específico Siguiente_Si_X
                cond_next = step_config.get(f"Siguiente_Si_{selected_opt}")
                if cond_next:
                    next_step = cond_next
                else:
                    # Fallback genérico a opción 1 (común en sheets simples) o mantenerse
                    next_step = step_config.get("Siguiente_Si_1", "INICIO") 
            else:
                # Texto normal
                next_step = step_config.get("Siguiente_Si_1", "INICIO")

    # 5. Handle "System Steps" (Skipping logic & Processing)
    # Loop para avanzar pasos automáticos (e.g. validación, saltar correo, generar AI)
    # Máximo 3 saltos para evitar loops infinitos
    for _ in range(5):
        next_conf = get_config_step(config_data, next_step)
        next_type = next_conf.get("Tipo_Entrada", "")
        
        # SKIP LOGIC: CORREO
        if next_step == "CORREO":  # Hardcode business rule
            next_step = next_conf.get("Siguiente_Si_1", "DESCRIPCION")
            continue
            
        if next_type == "SISTEMA":
            if next_step == "GENERAR_RESULTADOS":
                # Cargar Params y Abogados
                params_data, _ = get_sheet_data(gc, GOOGLE_SHEET_NAME, TAB_PARAM)
                abogados_data, _ = get_sheet_data(gc, GOOGLE_SHEET_NAME, TAB_ABOGADOS, _abogados_cache)
                
                # Calcular
                monto = calculate_compensation(lead_data, params_data)
                updates["Resultado_Calculo"] = monto
                
                # Asignar Abogado
                aid, anom, atel = assign_lawyer(lead_data.get("Salario_Mensual"), abogados_data)
                updates["Abogado_Asignado_ID"] = aid
                updates["Abogado_Asignado_Nombre"] = anom
                
                # Prompt AI Mejorado
                prompt = """
                Eres Ximena, asistente legal empática de 'Tu Derecho Laboral México'.
                Redacta un mensaje final para el cliente.
                1. Muestra empatía genuina por su situación (despido/renuncia).
                2. Menciona que sus derechos están protegidos por la Ley Federal del Trabajo.
                3. Indica que el cálculo es una estimación preliminar de sus prestaciones (aguinaldo, vacaciones, prima antiguedad, etc).
                4. Cierra diciendo que el abogado asignado revisará el caso a detalle.
                Mantén un tono profesional pero cálido y humano. Máximo 150 palabras.
                """
                analisis = generate_ai_analysis(lead_data, monto, prompt)
                updates["Analisis_AI"] = analisis
                updates["Link_Reporte_Web"] = f"{os.environ.get('BASE_URL_WEB', '')}/reporte/{lead_data.get('ID_Lead')}"
                
                # Construir Mensaje Final
                bot_reply = (
                    f"✅ *Análisis Preliminar Finalizado*\n\n"
                    f"{analisis}\n\n"
                    f"💰 *Estimación:* ${monto:,.2f} MXN\n"
                    f"⚖️ *Abogado Asignado:* {anom}\n\n"
                    "Te contactaremos en breve para los siguientes pasos."
                )
                
                # Notificar al Abogado (Twilio)
                if TWILIO_SID and atel:
                    try:
                        tcli = Client(TWILIO_SID, TWILIO_TOKEN)
                        tcli.messages.create(
                            from_=TWILIO_NUMBER,
                            to=f"whatsapp:{atel}",
                            body=f"🔔 Nuevo Lead: {lead_data.get('Nombre')}\nCaso: {lead_data.get('Tipo_Caso')}\nEst: ${monto}"
                        )
                    except Exception as te:
                        print(f"Twilio error: {te}")
                
                next_step = next_conf.get("Siguiente_Si_1", "CLIENTE_MENU")
                
            else:
                # Otros pasos sistema
                next_step = next_conf.get("Siguiente_Si_1", "INICIO")
        else:
            # Es un paso interactivo, nos detenemos aquí y preparamos el mensaje
            if not bot_reply: # Si no lo generó el sistema
                bot_reply = next_conf.get("Texto_Bot", "")
            break

    # 6. Final Batch Update
    updates["ESTATUS"] = next_step
    updates["Ultima_Actualizacion"] = now_iso_mx()
    updates["Ultimo_Mensaje_Cliente"] = body
    
    # Convertir dict updates a lista de celdas para update_cells o hacer update por celda mapeando col
    # GSpread batch_update espera rangos. Para hacer esto eficiente en 1 llamada:
    # update_cells es mejor si tenemos los idx.
    
    cells_to_update = []
    for k, v in updates.items():
        if k in col_map:
            col_idx = col_map[k]
            # row, col, val
            cells_to_update.append(gspread.Cell(lead_row_idx, col_idx, v))
            
    if cells_to_update:
        try:
            ws_leads.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        except Exception as e:
            print(f"Error saving updates: {e}")

    # 7. Log (Fire and forget, o try catch simple)
    try:
        ws_logs = sh.worksheet(TAB_LOGS)
        ws_logs.append_row([
            str(uuid.uuid4()), now_iso_mx(), clean_number, lead_data.get("ID_Lead"), 
            current_status, body, bot_reply, "WHATSAPP", lead_data.get("Fuente_Lead"), "", ""
        ])
    except:
        pass

    # 8. Reply
    return str(MessagingResponse().message(bot_reply))


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 8080)))
