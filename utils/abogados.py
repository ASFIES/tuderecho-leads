import random
from typing import Dict, List, Optional

from utils.sheets import get_all_values_safe, header_map, row_to_dict, with_backoff

def list_abogados(ws_abogados) -> List[Dict[str, str]]:
    values = get_all_values_safe(ws_abogados)
    if not values or len(values) < 2:
        return []
    hdr = values[0]
    out = []
    for row in values[1:]:
        d = row_to_dict(hdr, row)
        if (d.get("ID_Abogado") or "").strip():
            out.append(d)
    return out

def pick_abogado(abogados: List[Dict[str, str]], salario_mensual: float) -> Optional[Dict[str, str]]:
    def is_active(a):
        v = (a.get("Activo") or a.get("ACTIVO") or "1").strip()
        return v not in ("0", "NO", "FALSE", "False", "")

    if salario_mensual >= 50000:
        for a in abogados:
            if (a.get("ID_Abogado") or "").strip() == "A01" and is_active(a):
                return a

    activos = [a for a in abogados if is_active(a)]
    if not activos:
        return None

    def load(a):
        try:
            return int(float((a.get("Leads_Asignados_Hoy") or "0").strip() or "0"))
        except Exception:
            return 0

    activos.sort(key=load)
    top = activos[: min(3, len(activos))]
    return random.choice(top)

def incrementar_carga(ws_abogados, abogado_id: str):
    values = get_all_values_safe(ws_abogados)
    if not values or len(values) < 2:
        return
    hdr = values[0]
    hmap = header_map(hdr)
    if "ID_Abogado" not in hmap or "Leads_Asignados_Hoy" not in hmap:
        return

    col_id = hmap["ID_Abogado"] - 1
    col_load = hmap["Leads_Asignados_Hoy"] - 1

    for i, row in enumerate(values[1:], start=1):
        if col_id < len(row) and row[col_id].strip() == abogado_id:
            current = 0
            try:
                current = int(float((row[col_load] or "0").strip() or "0"))
            except Exception:
                current = 0
            new_val = current + 1
            with_backoff(ws_abogados.update_cell, i + 1, col_load + 1, str(new_val))
            return
