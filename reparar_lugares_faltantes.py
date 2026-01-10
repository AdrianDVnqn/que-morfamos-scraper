import pandas as pd
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from db_utils import upsert_lugar
from geo_utils import extraer_coordenadas_url, asignar_barrio
from monitor_reviews import extraer_metricas_rapido, setup_driver 

# Se reusa setup_driver de monitor_reviews que ya tiene headless y opciones correctas
# Se reusa extraer_metricas_rapido para obtener count y rating
# Se reusa upsert_lugar para insertar en DB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reparar_lugares(csv_path):
    """
    Lee validacion_reviews.csv, filtra los lugares con problemas (0 reviews reales o diferencia negativa significativa),
    y los vuelve a procesar para asegurarlos en la DB.
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"No se pudo leer el archivo {csv_path}: {e}")
        return

    # Filtrar lugares sospechosos:
    # 1. Tienen 0 reviews reales en DB (reales == 0) y habían reportado tener (reportadas > 0).
    # 2. O la diferencia es negativa significativa (reales - reportadas < -5), es decir, faltan reviews.
    
    # diferencia = reales - reportadas
    # Si diferencia < 0, significa que reportadas > reales (Faltan reviews en DB)
    
    candidatos = df[ 
        ((df['reales'] == 0) & (df['reportadas'] > 0)) | 
        (df['diferencia'] < -5) 
    ]
    
    if candidatos.empty:
        logger.info("No se encontraron lugares discrepantes críticos (reales=0) para reparar.")
        return

    logger.info(f"Encontrados {len(candidatos)} lugares faltantes para reparar.")
    
    driver = setup_driver()
    
    procesados = 0
    errores = 0
    
    try:
        for idx, row in candidatos.iterrows():
            url = row['url']
            nombre_reportado = row['nombre']
            
            logger.info(f"Reparando ({idx+1}/{len(candidatos)}): {nombre_reportado}")
            
            try:
                # 1. Extraer métricas actuales (Rating, Count)
                count, rating = extraer_metricas_rapido(driver, url)
                
                # 2. Extraer Geo
                lat, lon = extraer_coordenadas_url(url)
                barrio_info = {}
                if lat and lon:
                    try:
                        barrio_info = asignar_barrio(lat, lon)
                    except:
                        pass
                
                # 3. Construir objeto lugar
                # No tenemos categoría ni dirección exacta desde 'extraer_metricas_rapido', 
                # pero upsert_lugar maneja updates parciales. 
                # OJO: Si el lugar no existe en absoluto en DB, insertar con dirección=None es mejor que nada.
                # Idealmente deberíamos scrapear la dirección y categoría si es una inserción nueva.
                # Para simplificar y aprovechar lo que tenemos:
                
                datos = {
                    'url': url,
                    'nombre': nombre_reportado, # Usamos el nombre del CSV como base
                    'total_reviews_google': count,
                    'rating_gral': rating,
                    'latitud': lat,
                    'longitud': lon,
                    'barrio': barrio_info.get('barrio'),
                    'zona': barrio_info.get('zona'),
                    'cerca_rio': barrio_info.get('cerca_rio'),
                    # Faltantes (se intentarán mantener si ya existen, o quedar null)
                    'categoria': None, 
                    'direccion': None 
                }
                
                if upsert_lugar(datos):
                    logger.info("   ✅ Lugar insertado/actualizado en DB")
                    procesados += 1
                    
                    # Dejar constancia en DB (scapping_logs)
                    try:
                        from db_utils import log_scraping_event
                        mensaje = f"Reparación Manual. Reviews: {count}. Rating: {rating}"
                        log_scraping_event(url, 'REPARADO', mensaje, reviews_detectadas=count)
                            
                    except Exception as e:
                        logger.warning(f"   ⚠️ No se pudo loguear evento: {e}")
                        
                else:
                    logger.warning("   ❌ Falló upsert_lugar")
                    errores += 1
                
            except Exception as e:
                logger.error(f"   Error procesando {url}: {e}")
                errores += 1
                
    finally:
        driver.quit()
        logger.info("="*40)
        logger.info(f"Proceso finalizado. Reparados: {procesados}. Errores: {errores}")
        
        # Generar resumen para Discord
        with open("discord_summary_reparacion.txt", "w", encoding="utf-8") as f:
            f.write(f"🔧 **Reparación Manual Finalizada**\n")
            f.write(f"Lugares Procesados: {procesados}\n")
            f.write(f"Errores: {errores}\n")
            if procesados == 0 and errores == 0:
                f.write("No hubo acciones requeridas.")
            elif errores > 0:
                f.write("⚠️ Revisar logs por errores.")

if __name__ == "__main__":
    # Ruta por defecto asumiendo ejecución en root del repo
    # Ajustar según entorno
    CSV_PATH = "data/validacion_reviews.csv"
    import os
    if not os.path.exists(CSV_PATH):
        # Intentar ruta local windows absoluta si falla (para pruebas locales)
        CSV_PATH = r"d:\MCD_24-26\restaurant-scraping\backend-render\data\validacion_reviews.csv"
    
    reparar_lugares(CSV_PATH)
