import os
from flask import Flask, render_template
from utils.sheets import get_gspread_client, open_spreadsheet, open_worksheet, get_all_records_cached

app = Flask(__name__)

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()

@app.get("/")
def health():
    return "ok", 200

@app.get("/reporte/<token>")
def ver_reporte(token):
    gc = get_gspread_client()
    sh = open_spreadsheet(gc, GOOGLE_SHEET_NAME)
    ws = open_worksheet(sh, TAB_LEADS)

    rows = get_all_records_cached(ws, cache_key="bd_leads_report", ttl=60)
    lead = next((r for r in rows if (r.get("Token_Reporte") or "").strip() == token.strip()), None)

    if not lead:
        return "Reporte no encontrado.", 404

    return render_template("reporte.html", lead=lead)
