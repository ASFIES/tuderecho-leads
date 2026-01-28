import os
from twilio.rest import Client

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
WHATSAPP_NUMBER = os.environ.get("WHATSAPP_NUMBER", "").strip()  # whatsapp:+1415...

_client = None


def _get_client():
    global _client
    if _client is None:
        if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
            raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN.")
        _client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _client


def send_whatsapp_message(to_number: str, body: str):
    """
    to_number puede venir como:
    - whatsapp:+52...
    - +52...
    """
    if not WHATSAPP_NUMBER:
        raise RuntimeError("Falta WHATSAPP_NUMBER (tu número habilitado en Twilio).")

    to = to_number.strip()
    if not to.startswith("whatsapp:"):
        to = "whatsapp:" + to

    c = _get_client()
    c.messages.create(
        from_=WHATSAPP_NUMBER,
        to=to,
        body=body
    )
