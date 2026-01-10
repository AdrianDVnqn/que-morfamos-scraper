import time
import logging
import re
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db_utils import (
    get_lugares_para_monitoreo, 
    upsert_lugar, 
    ensure_history_table_exists, 
    log_review_history
)

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=es-AR")
    # Estrategia de carga rápida: no esperar a que cargue todo (imágenes, scripts pesados)
    options.page_load_strategy = 'eager'
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def extraer_metricas_rapido(driver, url):
    """
    Extrae (count, rating) de la URL.
    Returns: (int, float or None)
    """
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        
        # Selectores testeados (Vista Detalle de Google Maps)
        # Class común header: .F7nice (contiene stars y count)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main']")))
            
            # Opción 1: Fallback a clase .F7nice (Rating + Count) que es muy estable
            header = driver.find_element(By.CSS_SELECTOR, "div.F7nice")
            text = header.text # "4.5(2.300)"
            
            rating = None
            count = 0
            
            # Extraer Rating (primer número decimal o entero antes del paréntesis)
            # Ej: "4.5(200)" -> 4.5
            rating_match = re.search(r'^([\d\.,]+)', text)
            if rating_match:
                try:
                    rating_str = rating_match.group(1).replace(',', '.')
                    rating = float(rating_str)
                except:
                    pass
            
            # Extraer Count (entre paréntesis)
            count_match = re.search(r'\(([\d\.]+)\)', text)
            if count_match:
                raw_num = count_match.group(1).replace('.', '')
                if raw_num.isdigit():
                    count = int(raw_num)
            
            # Si F7nice funcionó, retornamos
            if count > 0:
                return count, rating

            # Opción 2: Botones (Backup para count)
            botones = driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='opiniones'], button[aria-label*='reviews']")
            for btn in botones:
                lbl = btn.get_attribute("aria-label")
                nums = re.findall(r'[\d\.]+', lbl)
                if nums:
                    candidatos = [int(n.replace('.', '')) for n in nums if n.replace('.', '').isdigit()]
                    if candidatos:
                        return max(candidatos), rating # Rating quizás sea None si falló F7nice
                        
        except Exception as e:
            pass

        return 0, None
    except Exception as e:
        logger.warning(f"Error extrayendo metricas de {url}: {e}")
        return 0, None

def run_monitor():
    if not ensure_history_table_exists():
        logger.error("No se pudo verificar la tabla de historial.")
        return

    lugares = get_lugares_para_monitoreo(limit=50) 
    if not lugares:
        logger.info("No hay lugares pendientes para monitorear.")
        return

    logger.info(f"Comenzando monitoreo de {len(lugares)} lugares...")
    
    driver = setup_driver()
    processed = 0
    total_growth = 0
    
    try:
        for lugar in lugares:
            url = lugar['url']
            nombre = lugar['nombre']
            
            logger.info(f"Checking: {nombre}")
            
            current_count, current_rating = extraer_metricas_rapido(driver, url)
            
            if current_count > 0:
            # Loguear historial
                delta = log_review_history(url, current_count, current_rating, nombre=nombre, direccion=lugar.get('direccion'))
                
                # Actualizar main table
                update_data = {
                    'url': url,
                    'nombre': nombre,
                    'total_reviews_google': current_count,
                    'rating_gral': current_rating, # Actualizamos rating también
                    'categoria': None,
                    'direccion': lugar.get('direccion'), # Mantener dirección si la tenemos
                    'latitud': None,
                    'longitud': None,
                    'barrio': None,
                    'zona': None,
                    'cerca_rio': None
                }
                upsert_lugar(update_data)
                
                if delta > 0:
                    logger.info(f"   📈 Crece: +{delta} | Total: {current_count} | Rating: {current_rating} | {lugar.get('direccion')}")
                    total_growth += delta
                else:
                    logger.info(f"   = Static | Total: {current_count} | Rating: {current_rating}")
                
                processed += 1
            else:
                logger.warning("   ⚠️ No se pudo obtener metricas")
                
            time.sleep(2) 
            
    finally:
        driver.quit()
        logger.info(f"Fin del ciclo. Procesados: {processed}. Crecimiento total: +{total_growth}")

if __name__ == "__main__":
    run_monitor()
