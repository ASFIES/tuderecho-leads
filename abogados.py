from utils.sheets import build_header_map, col_idx

def pick_abogado_from_sheet(ws_abogados, salario_mensual: float):
    """
    Regla: salario >= 50000 => abogado con ID_Abogado = A01 (desde sheet)
    Si no existe A01 o no activo, se cae a primer activo.
    """
    h = build_header_map(ws_abogados)
    idc = col_idx(h, "ID_Abogado")
    nc = col_idx(h, "Nombre_Abogado")
    tc = col_idx(h, "Telefono_Abogado")
    ac = col_idx(h, "Activo")

    rows = ws_abogados.get_all_values()[1:]  # list of lists

    def row_to_tuple(r):
        aid = (r[idc-1] if idc and idc-1 < len(r) else "").strip()
        an = (r[nc-1] if nc and nc-1 < len(r) else "").strip()
        at = (r[tc-1] if tc and tc-1 < len(r) else "").strip()
        activo = (r[ac-1] if ac and ac-1 < len(r) else "SI").strip().upper()
        return aid, an, at, activo

    # 1) si salario >= 50k => A01
    if salario_mensual >= 50000:
        for r in rows:
            aid, an, at, activo = row_to_tuple(r)
            if aid == "A01" and activo == "SI":
                return aid, an or "Abogada A01", at
        # si A01 no está activo o no existe, continuamos a fallback

    # 2) primer activo
    for r in rows:
        aid, an, at, activo = row_to_tuple(r)
        if activo == "SI" and aid:
            return aid, an or f"Abogada {aid}", at

    # 3) fallback duro
    return "A01", "Abogada asignada", ""
