import re

def _pick_knowledge(tipo_txt: str, conocimiento_rows: list[dict]) -> str:
    """
    Selecciona 1-3 temas relevantes desde Conocimiento_AI
    """
    keys = []
    if tipo_txt == "despido":
        keys = ["despido", "liquidación", "indemnización", "artículo 47", "artículo 48"]
    else:
        keys = ["renuncia", "finiquito", "prestaciones", "aguinaldo", "vacaciones"]

    picked = []
    for r in conocimiento_rows or []:
        content = (r.get("Contenido_Legal") or "")
        title = (r.get("Titulo_Visible") or "")
        kw = (r.get("Palabras_Clave") or "")
        blob = f"{title}\n{kw}\n{content}".lower()
        if any(k in blob for k in keys):
            picked.append(f"### {title}\n{content}")
        if len(picked) >= 2:
            break

    if not picked and conocimiento_rows:
        r = conocimiento_rows[0]
        picked.append(f"### {r.get('Titulo_Visible','')}\n{r.get('Contenido_Legal','')}")

    # recorta para no meter demasiado
    joined = "\n\n".join(picked)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined[:1800]

def generar_resumen_legal_empatico(
    ai_client,
    model: str,
    tipo_txt: str,
    descripcion_usuario: str,
    conocimiento_rows: list[dict],
    max_words: int = 220,
) -> str:
    """
    Resumen largo, humano, con base legal. Si no hay OpenAI, fallback.
    """
    fallback = (
        "Con lo que me compartes, revisaremos tu caso para identificar prestaciones pendientes "
        "(como salarios devengados, aguinaldo proporcional, vacaciones y prima vacacional) y, "
        "si aplica, la indemnización correspondiente. Un abogado confirmará contigo los datos clave "
        "y te orientará sobre el camino más seguro para proteger tus derechos."
    )

    if not ai_client:
        return fallback

    contexto = _pick_knowledge(tipo_txt, conocimiento_rows)

    system = (
        "Eres una recepcionista legal empática de un despacho laboral en México. "
        "Redacta un resumen claro, humano y profesional en español, con base legal general "
        "(sin dar asesoría definitiva). "
        "No pidas correo. No prometas tiempos exactos. "
        f"Extensión objetivo: {max_words-40} a {max_words+40} palabras."
    )

    user = (
        f"Tipo de caso: {tipo_txt}\n"
        f"Situación del cliente: {descripcion_usuario}\n\n"
        f"Contexto legal del despacho (para apoyar el texto): {contexto}\n\n"
        "Incluye:\n"
        "- 1 frase de comprensión (empatía)\n"
        "- 1 frase de tranquilidad (acompañamiento)\n"
        "- 1 mención general a LFT (arts. 47/48 si es despido; si renuncia, finiquito)\n"
        "- 1 cierre indicando que una abogada revisará el caso."
    )

    try:
        resp = ai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=340,
        )
        txt = (resp.choices[0].message.content or "").strip()
        return txt or fallback
    except:
        return fallback
