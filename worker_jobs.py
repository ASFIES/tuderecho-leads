import os
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo

from utils.sheets import (
    open_spreadsheet,
    open_worksheet,
    get_all_values_safe,
    header_map,
    row_to_dict,
    find_row_by_col_value,
    update_row_cells,
    with_backoff,
)
from utils.abogados import list_abogados, pick_abogado, incrementar_carga
from utils.ai import generate_ai_summary
from utils.whatsapp import send_whatsapp_message

TZ = os.environ.get("TZ", "America/Mexico_City").strip()
MX_TZ = ZoneInfo(TZ)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()
TAB_ABOGADOS = os.environ.get("TAB_ABOGADOS", "Cat_Abogados").strip()
TAB_SYS = os.environ.get("TAB_SYS", "Config_Sistema").strip()
TAB_GESTION = os.environ.get("TAB_GESTION_ABOGADOS", "Gestion_Abogados").strip()

def now_iso() -> str:
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _to_int(x, default=0):
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default

def _to_float(x, default=0.0):
    try:
        s = str(x).strip().replace(",", "")
        return float(s)
    except Exception:
        return default

def _build_date(yy, mm, dd) -> date | None:
    y = _to_int(yy, 0); m = _to_int(mm, 0); d = _to_int(dd, 0)
    if y <= 0 or m <= 0 or d <= 0:
        return None
    try:
        return date(y, m, d)
    except Exception:
        return None

def _tipo_caso_humano(tipo: str) -> str:
    t = (tipo or "").strip()
    if t == "1": return "Despido"
    if t == "2": return "Renuncia"
    return "Caso laboral"

def calcular_estimacion(tipo_caso: str, salario_mensual: float, f_ini: date | None, f_fin: date | None) -> dict:
    sd = salario_mensual / 30.0 if salario_mensual > 0 else 0.0

    dias = None
    anios = None
    if f_ini and f_fin:
        dias = (f_fin - f_ini).days + 1
        anios = max(dias / 365.0, 0.0)

    aguinaldo_dias = 15.0
    aguinaldo_prop = 0.0
    if dias is not None:
        aguinaldo_prop = sd * aguinaldo_dias * (dias / 365.0)

    base = {
        "salario_diario": sd,
        "dias_trabajados": dias,
        "anios_aprox": anios,
        "aguinaldo_proporcional": aguinaldo_prop,
    }

    if tipo_caso == "1":
        ind_90 = sd * 90.0
        veinte = (sd * 20.0 * anios) if anios is not None else 0.0
        total = ind_90 + veinte + aguinaldo_prop
        base.update({
            "indemnizacion_90_dias": ind_90,
            "veinte_dias_por_anio": veinte,
            "total_estimado": total,
        })
        return base

    total = aguinaldo_prop
    base.update({
        "indemnizacion_90_dias": 0.0,
        "veinte_dias_por_anio": 0.0,
        "total_estimado": total,
    })
    return base

def fmt_money(x: float) -> str:
    try:
        return f"${x:,.2f} MXN"
    except Exception:
        return f"${x} MXN"

def build_result_text(est: dict, tipo_caso: str) -> str:
    parts = []
    parts.append(f"Salario mensual reportado: {fmt_money(est.get('salario_diario', 0.0)*30)}")
    if est.get("dias_trabajados") is not None:
        parts.append(f"Tiempo aproximado trabajado: {int(est['dias_trabajados'])} días (~{est.get('anios_aprox',0):.2f} años)")
    if tipo_caso == "1":
        parts.append(f"Indemnización 90 días (aprox): {fmt_money(est.get('indemnizacion_90_dias',0.0))}")
        parts.append(f"20 días por año (aprox): {fmt_money(est.get('veinte_dias_por_anio',0.0))}")
    parts.append(f"Aguinaldo proporcional (aprox): {fmt_money(est.get('aguinaldo_proporcional',0.0))}")
    parts.append(f"**Total preliminar estimado:** {fmt_money(est.get('total_estimado',0.0))}")
    return " | ".join(parts)

def append_gestion(ws_gestion, payload: dict):
    values = get_all_values_safe(ws_gestion)
    if not values or not values[0]:
        return
    hdr = values[0]
    hmap = header_map(hdr)
    row = [""] * len(hdr)

    def setv(col, val):
        if col in hmap:
            row[hmap[col] - 1] = str(val)

    for k, v in payload.items():
        setv(k, v)

    with_backoff(ws_gestion.append_row, row, value_input_option="USER_ENTERED")

def process_lead(lead_id: str):
    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)

    values = get_all_values_safe(ws_leads)
    idx = find_row_by_col_value(values, "ID_Lead", (lead_id or "").strip())
    if idx is None:
        return

    lead = row_to_dict(values[0], values[idx])

    telefono = (lead.get("Telefono") or "").strip().replace("whatsapp:", "")
    nombre = (lead.get("Nombre") or "").strip()
    apellido = (lead.get("Apellido") or "").strip()
    tipo_caso = (lead.get("Tipo_Caso") or "").strip()
    descripcion = (lead.get("Descripcion_Situacion") or "").strip()
    salario = _to_float(lead.get("Salario_Mensual"), 0.0)

    f_ini = _build_date(lead.get("Inicio_Anio"), lead.get("Inicio_Mes"), lead.get("Inicio_Dia"))
    f_fin = _build_date(lead.get("Fin_Anio"), lead.get("Fin_Mes"), lead.get("Fin_Dia"))

    est = calcular_estimacion(tipo_caso, salario, f_ini, f_fin)
    resultado_txt = build_result_text(est, tipo_caso)

    # asignar abogado
    abogado_id = ""
    abogado_nombre = ""
    try:
        ws_abogados = open_worksheet(sh, TAB_ABOGADOS)
        ab_list = list_abogados(ws_abogados)
        abogado = pick_abogado(ab_list, salario)
        if abogado:
            abogado_id = (abogado.get("ID_Abogado") or "").strip()
            abogado_nombre = (abogado.get("Nombre") or abogado.get("Nombre_Abogado") or "").strip()
            if abogado_id:
                incrementar_carga(ws_abogados, abogado_id)
    except Exception:
        pass

    token = (lead.get("Token_Reporte") or "").strip() or str(uuid.uuid4())[:12]

    # Link reporte desde Config_Sistema (KEY/VALUE) si existe RUTA_REPORTE
    link_reporte = (lead.get("Link_Reporte_Web") or "").strip()
    if not link_reporte:
        try:
            ws_sys = open_worksheet(sh, TAB_SYS)
            sys_values = get_all_values_safe(ws_sys)
            base_url = ""
            if sys_values and len(sys_values) >= 2:
                h = header_map(sys_values[0])
                key_col = h.get("KEY") or h.get("Key") or 1
                val_col = h.get("VALUE") or h.get("Value") or 2
                for r in sys_values[1:]:
                    k = (r[key_col-1] if key_col-1 < len(r) else "").strip()
                    v = (r[val_col-1] if val_col-1 < len(r) else "").strip()
                    if k.upper() in ("RUTA_REPORTE", "BASE_URL_REPORTE", "REPORTE_URL"):
                        base_url = v
                        break
            if base_url:
                sep = "&" if "?" in base_url else "?"
                link_reporte = f"{base_url}{sep}token={token}"
        except Exception:
            link_reporte = ""

    full_name = f"{nombre} {apellido}".strip()
    sumario = generate_ai_summary(full_name, _tipo_caso_humano(tipo_caso), descripcion, resultado_txt)

    msg_cliente = sumario
    if link_reporte:
        msg_cliente += f"\n\n📄 Tu reporte preliminar: {link_reporte}"
    if abogado_nombre or abogado_id:
        msg_cliente += f"\n\n👩‍⚖️ Abogada asignada (preliminar): {abogado_nombre or abogado_id}"

    updates = {
        "Resultado_Calculo": resultado_txt,
        "Analisis_AI": sumario[:2000],
        "Token_Reporte": token,
        "Link_Reporte_Web": link_reporte,
        "Abogado_Asignado_ID": abogado_id,
        "Abogado_Asignado_Nombre": abogado_nombre,
        "Ultima_Actualizacion": now_iso(),
        "ESTATUS": "CLIENTE_MENU",
        "Ultimo_Error": "",
    }
    update_row_cells(ws_leads, idx, updates)

    # Gestion_Abogados (best effort)
    try:
        ws_gestion = open_worksheet(sh, TAB_GESTION)
        append_gestion(ws_gestion, {
            "Fecha_Hora": now_iso(),
            "ID_Lead": lead_id,
            "Telefono": telefono,
            "Abogado_ID": abogado_id,
            "Abogado_Nombre": abogado_nombre,
            "Estatus": "ASIGNADO",
        })
    except Exception:
        pass

    # Envío proactivo de WhatsApp
    try:
        if telefono:
            send_whatsapp_message(telefono, msg_cliente)
    except Exception as e:
        update_row_cells(ws_leads, idx, {"Ultimo_Error": f"WHATSAPP_SEND: {str(e)[:220]}", "Ultima_Actualizacion": now_iso()})
