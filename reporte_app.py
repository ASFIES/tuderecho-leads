import os
from flask import Flask, jsonify, request
from utils.sheets import open_spreadsheet, open_worksheet, with_backoff

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "").strip()
TAB_LEADS = os.environ.get("TAB_LEADS", "BD_Leads").strip()

app = Flask(__name__)

@app.get("/")
def health():
    return jsonify({"ok": True, "service": "reporte"})

@app.get("/reporte")
def reporte():
    lead_id = (request.args.get("lead_id") or "").strip()
    if not lead_id:
        return jsonify({"ok": False, "error": "Falta lead_id"}), 400

    sh = open_spreadsheet(GOOGLE_SHEET_NAME)
    ws = open_worksheet(sh, TAB_LEADS)
    rows = with_backoff(ws.get_all_records)

    for r in rows:
        if str(r.get("ID_Lead", "")).strip() == lead_id:
            return jsonify({"ok": True, "lead": r})

    return jsonify({"ok": False, "error": "Lead no encontrado"}), 404
