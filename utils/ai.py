import os

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

def build_summary_prompt(nombre: str, tipo_caso: str, descripcion: str, resultado: str) -> str:
    return f"""
Eres una asistente legal (recepcionista) de un despacho laboral en México.
Redacta un mensaje cálido, humano y claro (180 a 230 palabras) dirigido a {nombre or "la persona"}, en español.
Objetivo: explicar de forma preliminar (no asesoría) qué significa su caso, qué se calculó y qué sigue.

Contexto del caso:
- Tipo de caso: {tipo_caso}
- Descripción (cliente): {descripcion}
- Resultado preliminar (cifras/resumen): {resultado}

Reglas:
- NO pidas correo.
- Incluye un disclaimer breve de que es informativo y no constituye asesoría.
- No prometas resultados; sí invita a confirmar datos con un abogado.
- Termina con una pregunta cerrada: "¿Deseas que un abogado revise tu caso hoy? 1) Sí  2) No"
"""

def generate_ai_summary(nombre: str, tipo_caso: str, descripcion: str, resultado: str) -> str:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    model = (os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()

    if not api_key or OpenAI is None:
        nombre = nombre or "Hola"
        return (
            f"{nombre}, gracias por compartir tu información. Con los datos que nos diste, preparamos una estimación preliminar "
            f"para tu caso ({tipo_caso}). {resultado}\n\n"
            "Aviso: esta información es únicamente informativa y no constituye asesoría legal. Un abogado confirmará los datos y "
            "te explicará opciones y estrategia.\n\n"
            "¿Deseas que un abogado revise tu caso hoy?\n1) Sí\n2) No"
        )

    client = OpenAI(api_key=api_key)
    prompt = build_summary_prompt(nombre, tipo_caso, descripcion, resultado)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Eres una asistente legal empática y precisa."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,
    )
    return (resp.choices[0].message.content or "").strip()
