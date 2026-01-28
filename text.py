import re
import unicodedata
from datetime import datetime

def render_text(s: str) -> str:
    s = s or ""
    return s.replace("\\n", "\n")

def phone_raw(raw: str) -> str:
    return (raw or "").strip()

def phone_norm(raw: str) -> str:
    s = (raw or "").strip()
    s = s.replace("whatsapp:", "").strip()
    return s

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_option(s: str) -> str:
    s = normalize_msg(s)
    m = re.search(r"\d", s)
    if m:
        return m.group(0)
    return s

def detect_fuente(msg: str) -> str:
    t = (msg or "").lower()
    if "facebook" in t or "anuncio" in t or "fb" in t:
        return "FACEBOOK"
    if "sitio" in t or "web" in t or "pagina" in t or "página" in t:
        return "WEB"
    return "DESCONOCIDA"

def is_valid_by_rule(value: str, rule: str) -> bool:
    value = (value or "").strip()
    rule = (rule or "").strip()

    if not rule:
        return True

    if rule.startswith("REGEX:"):
        pattern = rule.replace("REGEX:", "", 1).strip()
        try:
            return re.match(pattern, value) is not None
        except:
            return False

    if rule == "MONEY":
        try:
            x = float(value.replace("$", "").replace(",", "").strip())
            return x >= 0
        except:
            return False

    return True

def build_date_from_parts(y: str, m: str, d: str) -> str:
    y = (y or "").strip()
    m = (m or "").strip()
    d = (d or "").strip()
    if not (y and m and d):
        return ""
    try:
        yy = int(y); mm = int(m); dd = int(d)
        dt = datetime(yy, mm, dd)
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

def money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or "0")
    except:
        return 0.0

def safe_name(nombre: str) -> str:
    n = (nombre or "").strip()
    if not n:
        return "Hola"
    return n[:1].upper() + n[1:]
