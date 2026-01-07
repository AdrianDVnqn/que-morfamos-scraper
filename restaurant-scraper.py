import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import os

# --- CONFIGURACIÓN ---

# Archivos de salida
ARCHIVO_LINKS = "lugares_encontrados.csv"
ARCHIVO_LOG = "logs/scraper_run.log"

# Crear directorio de logs si no existe
os.makedirs("logs", exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(ARCHIVO_LOG, encoding='utf-8'),
        logging.StreamHandler()  # También mostrar en consola
    ]
)
logger = logging.getLogger(__name__)

# Categorías de establecimientos gastronómicos
CATEGORIAS = [
    # Tipos generales
    "Restaurante", "Restaurant", "Parrilla", "Pizzería", "Pizzeria",
    "Hamburguesería", "Hamburgueseria", "Cafetería", "Cafeteria", 
    "Bar", "Pub", "Cervecería", "Heladería", "Pastelería", "Panadería",
    "Rotisería", "Delivery comida", "Fast food",
    # Cocinas específicas
    "Sushi", "Comida japonesa", "Comida china", "Comida mexicana",
    "Comida italiana", "Comida árabe", "Empanadas", "Milanesas",
    "Comida vegana", "Comida vegetariana", "Comida saludable",
    "Brunch", "Desayunos", "Medialunas", "Lomitería",
    # Términos alternativos que Google reconoce bien
    "Donde comer", "Lugares para comer", "Gastronomía",
]

# Zonas generales de Neuquén
ZONAS = [
    "Zona Oeste Neuquén",
    "Zona Centro Neuquén", 
    "Zona Alto Neuquén",
    "cerca del río",
]

def generar_busquedas():
    """
    Genera combinaciones de búsquedas para máxima cobertura.
    Combina categorías con zonas, priorizando las más relevantes.
    """
    busquedas = set()
    
    # Combinaciones principales: categoría + zona
    categorias_principales = CATEGORIAS[:15]  # Las más importantes
    for cat in categorias_principales:
        for zona in ZONAS[:3]:  # Zonas principales
            busquedas.add(f"{cat} en {zona}")
    
    # Búsquedas generales para el resto de zonas
    for zona in ZONAS[3:]:
        busquedas.add(f"Restaurante en {zona}")
        busquedas.add(f"Donde comer en {zona}")
    
    # Categorías específicas solo en zona principal
    for cat in CATEGORIAS[15:]:
        busquedas.add(f"{cat} en Neuquén Capital")
    
    return list(busquedas)


def obtener_links_de_busqueda(query, max_reintentos=3):
    """
    Busca lugares en Google Maps y extrae los links.
    Retorna una lista de diccionarios con metadata.
    """
    # Configuración del navegador
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") 
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=es-AR")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    resultados = []
    timestamp_busqueda = datetime.now().isoformat()
    
    for intento in range(max_reintentos):
        driver = None
        try:
            logger.info(f"Buscando: '{query}' (intento {intento + 1}/{max_reintentos})")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.get("https://www.google.com/maps")
            
            # 1. Encontrar la barra de búsqueda e ingresar la query
            wait = WebDriverWait(driver, 15)
            search_box = wait.until(EC.element_to_be_clickable((By.ID, "searchboxinput")))
            search_box.clear()
            time.sleep(0.5)
            search_box.send_keys(query)
            search_box.send_keys(Keys.ENTER)
            
            # 2. Esperar a que cargue el panel de resultados
            logger.info("Esperando resultados...")
            time.sleep(3)
            
            # Intentar encontrar el feed de resultados
            try:
                feed = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@role='feed']")))
            except:
                logger.warning(f"No se encontró panel de resultados para '{query}'")
                break
            
            # 3. Scroll infinito mejorado
            logger.info("Scrolleando para cargar todos los locales...")
            scroll_pausas = 0
            max_scroll_pausas = 3
            last_height = 0
            
            while scroll_pausas < max_scroll_pausas:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
                time.sleep(1.5)
                
                new_height = driver.execute_script("return arguments[0].scrollHeight", feed)
                
                try:
                    driver.find_element(By.XPATH, "//*[contains(text(), 'llegado al final')]")
                    logger.info("Se detectó el final de la lista.")
                    break
                except:
                    pass
                
                if new_height == last_height:
                    scroll_pausas += 1
                    time.sleep(0.5)
                else:
                    scroll_pausas = 0
                    last_height = new_height
            
            # 4. Extraer los links con metadata
            logger.info("Extrayendo URLs...")
            elements = feed.find_elements(By.TAG_NAME, "a")
            
            for elem in elements:
                try:
                    href = elem.get_attribute("href")
                    if href and "/maps/place/" in href:
                        # Intentar extraer el nombre del lugar del aria-label
                        nombre = elem.get_attribute("aria-label") or "Desconocido"
                        
                        resultados.append({
                            "link": href,
                            "nombre": nombre,
                            "query": query,
                            "fecha_busqueda": timestamp_busqueda,
                            "intento_exitoso": intento + 1
                        })
                except:
                    continue
            
            logger.info(f"Se encontraron {len(resultados)} locales en esta búsqueda.")
            break  # Éxito
            
        except Exception as e:
            logger.error(f"Error en intento {intento + 1}: {e}")
            if intento < max_reintentos - 1:
                logger.info("Reintentando...")
                time.sleep(2)
        finally:
            if driver:
                driver.quit()
    
    return resultados


def guardar_resultados(todos_los_resultados, archivo):
    """
    Guarda los resultados en un CSV con toda la metadata.
    Elimina duplicados por link pero mantiene la primera aparición.
    """
    # Eliminar duplicados manteniendo el primero encontrado
    links_vistos = set()
    resultados_unicos = []
    
    for resultado in todos_los_resultados:
        if resultado["link"] not in links_vistos:
            links_vistos.add(resultado["link"])
            resultados_unicos.append(resultado)
    
    # Guardar en CSV
    with open(archivo, 'w', newline='', encoding='utf-8') as f:
        campos = ["link", "nombre", "query", "fecha_busqueda", "intento_exitoso"]
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(resultados_unicos)
    
    return len(resultados_unicos)


# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # Generar búsquedas
    BUSQUEDAS = generar_busquedas()
    
    # Log de inicio
    inicio_script = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIO DE EJECUCIÓN DEL SCRAPER")
    logger.info(f"Fecha y hora: {inicio_script.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total de búsquedas programadas: {len(BUSQUEDAS)}")
    logger.info("=" * 60)
    
    # Ejecutar búsquedas
    todos_los_resultados = []
    busquedas_exitosas = 0
    busquedas_fallidas = 0
    
    for i, busqueda in enumerate(BUSQUEDAS, 1):
        logger.info(f"Progreso: {i}/{len(BUSQUEDAS)}")
        
        resultados = obtener_links_de_busqueda(busqueda)
        
        if resultados:
            todos_los_resultados.extend(resultados)
            busquedas_exitosas += 1
        else:
            busquedas_fallidas += 1
        
        time.sleep(1)  # Pausa entre búsquedas
    
    # Guardar resultados
    lugares_unicos = guardar_resultados(todos_los_resultados, ARCHIVO_LINKS)
    
    # Log de resumen final
    fin_script = datetime.now()
    duracion = fin_script - inicio_script
    
    logger.info("=" * 60)
    logger.info("RESUMEN DE EJECUCIÓN")
    logger.info("=" * 60)
    logger.info(f"Hora de inicio: {inicio_script.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Hora de fin: {fin_script.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duración total: {duracion}")
    logger.info("-" * 40)
    logger.info(f"Búsquedas realizadas: {len(BUSQUEDAS)}")
    logger.info(f"Búsquedas exitosas: {busquedas_exitosas}")
    logger.info(f"Búsquedas fallidas: {busquedas_fallidas}")
    logger.info("-" * 40)
    logger.info(f"Total de resultados encontrados: {len(todos_los_resultados)}")
    logger.info(f"Lugares ÚNICOS guardados: {lugares_unicos}")
    logger.info(f"Duplicados eliminados: {len(todos_los_resultados) - lugares_unicos}")
    logger.info("-" * 40)
    logger.info(f"Archivo de salida: {ARCHIVO_LINKS}")
    logger.info(f"Archivo de log: {ARCHIVO_LOG}")
    logger.info("=" * 60)