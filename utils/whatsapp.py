# utils/whatsapp.py
import os
from twilio.rest import Client

def _get_twilio_client():
    sid = (os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid or not token:
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN.")
    return Client(sid, token)

def get_whatsapp_from_number() -> str:
    num = (os.environ.get("TWILIO_WHATSAPP_NUMBER") or "").strip()
    if not num:
        raise RuntimeError("Falta TWILIO_WHATSAPP_NUMBER (ej: whatsapp:+14155238886).")
    return num

def send_whatsapp_message(to_phone: str, body: str):
    client = _get_twilio_client()
    from_num = get_whatsapp_from_number()
    to_phone = (to_phone or "").strip()
    if not to_phone.startswith("whatsapp:"):
        to_phone = "whatsapp:" + to_phone
    return client.messages.create(from_=from_num, to=to_phone, body=body)
