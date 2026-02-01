import os
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, request
from utils.sheets import open_spreadsheet, open_worksheet, get_all_values_safe, row_to_dict, find_row_by_col_value

TZ = os.environ.get("TZ", "America/Mexico_City").strip()
MX_TZ = ZoneInfo(TZ)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()

app = Flask(__name__)

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%d %H:%M:%S")

@app.get("/")
def home():
    return {"ok": True, "service": "tuderecho-reporte", "ts": now_iso()}

@app.get("/reporte")
def reporte():
    token = (request.args.get("token") or "").strip()
    lead_id = (request.args.get("lead") or "").strip()

    if not token and not lead_id:
        return ("Falta token.", 400)

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws_leads = open_worksheet(sh, TAB_LEADS)
    values = get_all_values_safe(ws_leads)

    idx = None
    if token:
        idx = find_row_by_col_value(values, "Token_Reporte", token)
    if idx is None and lead_id:
        idx = find_row_by_col_value(values, "ID_Lead", lead_id)

    if idx is None:
        return ("Reporte no encontrado.", 404)

    lead = row_to_dict(values[0], values[idx])

    nombre = html.escape((lead.get("Nombre") or "").strip())
    apellido = html.escape((lead.get("Apellido") or "").strip())
    tipo = html.escape((lead.get("Tipo_Caso") or "").strip())
    desc = html.escape((lead.get("Descripcion_Situacion") or "").strip())
    res = html.escape((lead.get("Resultado_Calculo") or "").strip())
    ai = html.escape((lead.get("Analisis_AI") or "").strip())

    tipo_h = "Despido" if tipo == "1" else ("Renuncia" if tipo == "2" else "Caso laboral")

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Reporte preliminar</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b0f14; color:#f2f4f7; margin:0; }}
    .wrap {{ max-width:900px; margin:0 auto; padding:24px; }}
    .card {{ background:#111827; border:1px solid #1f2937; border-radius:16px; padding:18px; margin-bottom:16px; }}
    h1 {{ margin:0 0 8px 0; font-size:22px; }}
    h2 {{ margin:0 0 8px 0; font-size:16px; color:#93c5fd; }}
    p {{ margin:0; line-height:1.45; white-space:pre-wrap; }}
    .muted {{ color:#9ca3af; font-size:12px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Reporte preliminar</h1>
      <p class="muted">Generado: {now_iso()} · Este reporte es informativo y no constituye asesoría legal.</p>
    </div>

    <div class="card">
      <h2>Datos del caso</h2>
      <p><b>Nombre:</b> {nombre} {apellido}</p>
      <p><b>Tipo:</b> {tipo_h}</p>
      <p><b>Descripción:</b> {desc}</p>
    </div>

    <div class="card">
      <h2>Estimación preliminar</h2>
      <p>{res}</p>
    </div>

    <div class="card">
      <h2>Orientación (IA)</h2>
      <p>{ai}</p>
    </div>
  </div>
</body>
</html>
"""



