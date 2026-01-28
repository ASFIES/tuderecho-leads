import os
from flask import Flask, request, abort

from utils.sheets import open_worksheet, find_row_by_value

TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()

app = Flask(__name__)

@app.get("/")
def home():
    return "Reporte OK"

@app.get("/r")
def report():
    token = (request.args.get("t") or "").strip()
    if not token:
        abort(400, "Falta token ?t=")

    ws = open_worksheet(TAB_LEADS)
    row = find_row_by_value(ws, "Token_Reporte", token)
    if not row:
        abort(404, "Token no encontrado")

    headers = ws.row_values(1)
    values = ws.row_values(row)
    data = {headers[i]: (values[i] if i < len(values) else "") for i in range(len(headers))}

    nombre = data.get("Nombre", "")
    analisis = data.get("Analisis_AI", "")
    resultado = data.get("Resultado_Calculo", "")

    html = f"""
    <html>
      <head><meta charset="utf-8"><title>Reporte</title></head>
      <body style="font-family: Arial; padding: 20px;">
        <h2>Reporte preliminar</h2>
        <p><b>Cliente:</b> {nombre}</p>
        <p><b>Resultado cálculo:</b> {resultado}</p>
        <h3>Análisis</h3>
        <pre style="white-space: pre-wrap;">{analisis}</pre>
      </body>
    </html>
    """
    return html
