from utils.sheets import build_header_map, col_idx, find_row_by_value

def load_config_row(ws_config, paso_actual: str):
    cfg_headers = build_header_map(ws_config)
    idpaso_col = col_idx(cfg_headers, "ID_Paso")
    if not idpaso_col:
        raise RuntimeError("En Config_XimenaAI falta la columna 'ID_Paso'.")

    paso_actual = (paso_actual or "").strip() or "INICIO"
    row = find_row_by_value(ws_config, idpaso_col, paso_actual)
    if not row and paso_actual != "INICIO":
        row = find_row_by_value(ws_config, idpaso_col, "INICIO")
    if not row:
        raise RuntimeError(f"No existe configuración para el paso '{paso_actual}'.")

    row_vals = ws_config.row_values(row)

    base_fields = [
        "ID_Paso", "Texto_Bot", "Tipo_Entrada", "Opciones_Validas",
        "Siguiente_Si_1", "Siguiente_Si_2",
        "Campo_BD_Leads_A_Actualizar", "Regla_Validacion", "Mensaje_Error"
    ]
    extra_siguientes = [f"Siguiente_Si_{i}" for i in range(3, 10)]

    def get_field(name):
        idx = col_idx(cfg_headers, name)
        return (row_vals[idx-1] if idx and idx-1 < len(row_vals) else "").strip()

    out = {k: get_field(k) for k in base_fields}
    for k in extra_siguientes:
        out[k] = get_field(k)
    return out

def pick_next_step_from_option(cfg: dict, msg_opt: str, default_step: str):
    k = f"Siguiente_Si_{msg_opt}"
    if cfg.get(k):
        return cfg.get(k).strip()
    if msg_opt == "1" and cfg.get("Siguiente_Si_1"):
        return cfg.get("Siguiente_Si_1").strip()
    if msg_opt == "2" and cfg.get("Siguiente_Si_2"):
        return cfg.get("Siguiente_Si_2").strip()
    return default_step
