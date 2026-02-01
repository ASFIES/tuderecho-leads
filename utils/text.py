import re
from typing import Dict

def normalize_msg(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[ \t]+", " ", s)
    return s

def normalize_option(msg: str) -> str:
    msg = (msg or "").strip()
    m = re.search(r"([1-9])", msg)
    return m.group(1) if m else ""

def render_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")
    return s.strip()

def detect_fuente(first_msg: str) -> str:
    msg = (first_msg or "").lower()
    if "fb" in msg or "facebook" in msg or "anuncio" in msg:
        return "FACEBOOK"
    if "ig" in msg or "instagram" in msg:
        return "INSTAGRAM"
    return "DESCONOCIDA"

def is_valid_by_rule(text: str, rule: str) -> bool:
    text = (text or "").strip()
    rule = (rule or "").strip()
    if not rule:
        return True
    if rule.upper() == "MONEY":
        return bool(re.fullmatch(r"\d{1,12}", text))
    if rule.upper().startswith("REGEX:"):
        pattern = rule.split(":", 1)[1].strip()
        try:
            return bool(re.fullmatch(pattern, text))
        except re.error:
            return True
    return True

def template_fill(template: str, lead: Dict[str, str]) -> str:
    t = template or ""
    for k, v in (lead or {}).items():
        if not k:
            continue
        t = t.replace("{" + k + "}", str(v))
    return t
