import os
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.sheets import open_worksheet, find_row_by_value, update_row_dict

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TZ = os.environ.get("TZ", "America/Mexico_City")


def process_lead(lead_id: str):
    """
    Job principal: termina cálculo + asignación y deja todo listo.
    """
    ws = open_worksheet(TAB_LEADS)

    row = find_row_by_value(ws, "ID_Lead", lead_id)
    if not row:
        return {"ok": False, "error": f"No encontré el lead {lead_id} en {TAB_LEADS}"}

    # Aquí puedes leer solo lo mínimo necesario (evita muchas lecturas)
    # OJO: get_all_records() lee todo; mejor leer fila completa:
    values = ws.row_values(row)
    headers = ws.row_values(1)
    data = {headers[i]: (values[i] if i < len(values) else "") for i in range(len(headers))}

    tipo = str(data.get("Tipo_Caso", "")).strip()
    desc = str(data.get("Descripcion_Situacion", "")).strip()
    salario = str(data.get("Salario_Mensual", "")).strip()
    ini = str(data.get("Fecha_Inicio_Laboral", "")).strip()
    fin = str(data.get("Fecha_Fin_Laboral", "")).strip()

    # ---------- Resultado preliminar (puedes sofisticarlo luego) ----------
    # Por ahora: genera texto de análisis y un “resultado_calculo” numérico placeholder
    if tipo == "1":
        caso_txt = "despido"
    elif tipo == "2":
        caso_txt = "renuncia"
    else:
        caso_txt = "caso laboral"

    analisis_ai = (
        f"Con la información que compartiste, revisaremos tu caso como '{caso_txt}' conforme a la LFT.\n"
        f"Datos clave: inicio {ini}, fin {fin}, salario ${salario}.\n"
        f"Descripción: {desc}\n\n"
        "Este análisis es preliminar. Un abogado confirmará contigo los datos y el enfoque legal."
    )

    # Puedes calcular luego; de momento deja 0.0 para no bloquear
    resultado_calculo = "0.0"

    now = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%dT%H:%M:%S%z")

    update_row_dict(ws, row, {
        "Analisis_AI": analisis_ai,
        "Resultado_Calculo": resultado_calculo,
        "ESTATUS": "CLIENTE_MENU",
        "Ultima_Actualizacion": now,
    })

    return {"ok": True, "lead_id": lead_id, "estatus": "CLIENTE_MENU"}
