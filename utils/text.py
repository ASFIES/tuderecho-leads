# utils/text.py
import re
from typing import Dict

def normalize_option(msg: str) -> str:
    s = (msg or "").strip()
    m = re.search(r"([1-9])", s)
    return m.group(1) if m else ""

def render_text(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1]
    return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t").strip()

def template_fill(template: str, lead: Dict[str, str]) -> str:
    t = template or ""
    for k, v in (lead or {}).items():
        if k:
            t = t.replace("{" + k + "}", str(v))
    return t

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "página" in t or "pagina" in t:
        return "WEB"
    if "instagram" in t or "ig" in t:
        return "INSTAGRAM"
    return "DESCONOCIDA"
