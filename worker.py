import time
# ... (Importar las funciones de cálculo, OpenAI y Twilio que estaban en el script original)

def background_worker():
    print("Worker iniciado: Buscando leads pendientes...")
    while True:
        try:
            gc = get_gspread_client()
            sh = open_spreadsheet(gc)
            ws_leads = sh.worksheet(TAB_LEADS)
            all_leads = ws_leads.get_all_records()

            for i, lead in enumerate(all_leads, start=2):
                if lead.get("Procesar_AI_Status") == "PENDIENTE":
                    print(f"Procesando Lead: {lead.get('ID_Lead')}")
                    
                    # 1. Ejecutar cálculos legales y OpenAI
                    # Usamos la lógica de run_system_step_if_needed que ya tienes
                    # Pero ahora con prompts más empáticos y largos
                    
                    # 2. Enviar mensaje por Twilio manualmente aquí 
                    # ya que este script no responde al Webhook.
                    enviar_resultado_final_whatsapp(lead)
                    
                    # 3. Marcar como completado
                    headers = build_header_map(ws_leads)
                    update_lead_batch(ws_leads, headers, i, {
                        "Procesar_AI_Status": "LISTO",
                        "ESTATUS": "FIN_RESULTADOS"
                    })
                    print(f"Lead {lead.get('ID_Lead')} procesado con éxito.")

        except Exception as e:
            print(f"Error en Worker: {e}")
        
        time.sleep(10) # Espera 10 segundos para la siguiente vuelta

if __name__ == "__main__":
    background_worker()