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
from urllib.parse import unquote

# Integración con Supabase y Geo
try:
    from db_utils import upsert_lugar
    from geo_utils import extraer_coordenadas_url, asignar_barrio
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = True # Para que no falle si DB_UTILS está pero geo_utils no (aunque estarán ambos)
    # Mejor manejo de error:
    try:
        from db_utils import upsert_lugar
    except ImportError:
        DB_AVAILABLE = False

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
    "Zona Oeste Neuquén Capital",
    "Centro Neuquén Capital", 
    "Zona Alto Neuquén Capital",
    "Zona Este Neuquén Capital",
    "cerca del río en Neuquén Capital",
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
    # Configuración del navegador (compatible con GitHub Actions)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")  # Nuevo modo headless
    options.add_argument("--no-sandbox")  # Requerido en GitHub Actions
    options.add_argument("--disable-dev-shm-usage")  # Evita errores de memoria compartida
    options.add_argument("--disable-gpu")  # Requerido en algunos entornos Linux
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
            
            # 4. Extraer los links con metadata enriquecida
            logger.info("Extrayendo información detallada de los resultados...")
            
            # Iterar sobre las tarjetas de resultados (clase .Nv2PK suele ser el contenedor del item)
            # Si cambia la clase, esto podría fallar, así que mantenemos un fallback o try/except robusto.
            cards = feed.find_elements(By.CSS_SELECTOR, ".Nv2PK")
            
            if not cards:
                # Fallback por si la estructura de clases cambió: buscar solo por tag 'a' como antes
                logger.warning("No se encontraron tarjetas .Nv2PK, usando método fallback (solo links).")
                elements = feed.find_elements(By.TAG_NAME, "a")
                for elem in elements:
                    try:
                        href = elem.get_attribute("href")
                        if href and "/maps/place/" in href:
                            nombre = elem.get_attribute("aria-label") or "Desconocido"
                            resultados.append({
                                "link": href,
                                "nombre": nombre,
                                "categoria": None,
                                "rating_gral": None,
                                "total_reviews_google": 0,
                                "direccion": None,
                                "query": query,
                                "fecha_busqueda": timestamp_busqueda,
                                "intento_exitoso": intento + 1
                            })
                    except:
                        continue
            else:
                for card in cards:
                    try:
                        # Link y Nombre
                        link_elem = card.find_element(By.CSS_SELECTOR, "a.hfpxzc")
                        href = link_elem.get_attribute("href")
                        nombre = link_elem.get_attribute("aria-label") or "Desconocido"

                        # Rating (ej: "4.5" o "4,5")
                        try:
                            rating_text = card.find_element(By.CSS_SELECTOR, ".MW4etd").text
                            if rating_text:
                                # Normalizar y convertir a float
                                rating_clean = rating_text.replace(',', '.')
                                rating_gral = float(rating_clean)
                            else:
                                rating_gral = None
                        except:
                            rating_gral = None

                        # Reviews (ej: "(1.234)")
                        try:
                            reviews_text = card.find_element(By.CSS_SELECTOR, ".UY7F9").text
                            # Limpiar paréntesis y puntos de mil
                            reviews_clean = re.sub(r'[()\.]', '', reviews_text)
                            total_reviews = int(reviews_clean) if reviews_clean.isdigit() else 0
                        except:
                            total_reviews = 0

                        # Categoría y Dirección (suelen estar en contenedores .W4Efsd)
                        categoria = None
                        direccion = None
                        try:
                            # Buscar todos los contenedores de texto secundarios
                            text_containers = card.find_elements(By.CSS_SELECTOR, ".W4Efsd")
                            if len(text_containers) > 0:
                                # El primer W4Efsd suele tener Rating y (Reviews) en un hijo, 
                                # y Categoría y Precio en otro. Esto es variable.
                                # Estrategia: Buscar texto que NO sea rating/reviews
                                full_text = text_containers[0].text
                                parts = full_text.split('·')
                                if len(parts) > 1:
                                    # Posible formato: "4.5(500) · Hamburguesería · $$"
                                    # El rating/reviews extraídos arriba son más precisos por selectores, 
                                    # aquí intentamos sacar la categoría.
                                    # A veces la categoría es el segundo elemento del split
                                    categoria_cand = parts[-1].strip() # A veces es el último
                                    # Refinamiento simplificado: Tomar todo el texto contenido en el span correspondiente
                                    # Mejor aproximación: buscar info dentro de los spans hijos de W4Efsd
                                    spans = text_containers[1].find_elements(By.TAG_NAME, "span") if len(text_containers) > 1 else []
                                    if spans:
                                        categoria = spans[0].text
                                        if len(spans) > 1:
                                            direccion = spans[1].text
                        except:
                            pass

                        if href and "/maps/place/" in href:
                            resultados.append({
                                "link": href,
                                "nombre": nombre,
                                "categoria": categoria,
                                "rating_gral": rating_gral,
                                "total_reviews_google": total_reviews,
                                "direccion": direccion,
                                "query": query,
                                "fecha_busqueda": timestamp_busqueda,
                                "intento_exitoso": intento + 1
                            })
                    except Exception as e_card:
                        # logger.debug(f"Error parseando tarjeta: {e_card}")
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
    
    # Guardar en DB (Supabase)
    if DB_AVAILABLE:
        logger.info(f"Guardando {len(resultados_unicos)} lugares en Base de Datos...")
        upserts_ok = 0
        for lugar in resultados_unicos:
            url_map = lugar['link']
            
            # Extraer geo
            lat, lon = None, None
            try:
                from geo_utils import extraer_coordenadas_url, asignar_barrio
                lat, lon = extraer_coordenadas_url(url_map)
            except ImportError:
                pass
            
            # Asignar barrio
            barrio_info = {}
            if lat and lon:
                try:
                    barrio_info = asignar_barrio(lat, lon)
                except:
                    pass

            # Adaptar dict para upsert_lugar
            datos_db = {
                'url': url_map,
                'nombre': lugar['nombre'],
                'categoria': lugar.get('categoria'),
                'rating_gral': lugar.get('rating_gral'),
                'total_reviews_google': lugar.get('total_reviews_google', 0),
                'direccion': lugar.get('direccion'),
                'latitud': lat,
                'longitud': lon,
                'barrio': barrio_info.get('barrio'),
                'zona': barrio_info.get('zona'),
                'cerca_rio': barrio_info.get('cerca_rio')
            }
            if upsert_lugar(datos_db):
                upserts_ok += 1
        
        logger.info(f"✅ DB: {upserts_ok}/{len(resultados_unicos)} lugares actualizados")
    
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