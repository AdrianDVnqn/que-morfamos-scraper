
from db_utils import log_scraping_event, get_connection

print("Prueba de inserción en scraping_logs...")
try:
    # URL de prueba (una que sepamos que existe o una dummy)
    url_test = "https://www.google.com/maps/place/TEST_DEBUG/data=!4m7!3m6!1s0x0:0x0!8m2!3d0!4d0!16s%2Fg%2F11b6d1j111?hl=es"
    
    success = log_scraping_event(
        url=url_test,
        estado="TEST_DEBUG",
        mensaje="Prueba manual de inserción desde Antigravity",
        reviews_detectadas=0,
        nuevas_reviews=0,
        intentos=1
    )
    
    if success:
        print("✅ ÉXITO: Log insertado correctamente.")
    else:
        print("❌ FALLO: La función retornó False (revisar logs internos o consola).")

except Exception as e:
    print(f"❌ EXCEPCIÓN: {e}")
