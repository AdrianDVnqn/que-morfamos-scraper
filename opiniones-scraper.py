import time
import sys
import re
import os
import random
import datetime
import logging
import csv
import hashlib
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

# Integración con Supabase/PostgreSQL
try:
    from db_utils import (
        get_connection, close_connection, insertar_reviews_batch,
        obtener_ids_existentes_por_restaurante,
        upsert_lugar, get_ultima_review_restaurante
    )
    from geo_utils import asignar_barrio, extraer_coordenadas_url
    DB_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ No se pudo importar db_utils o geo_utils: {e}")
    DB_AVAILABLE = False

# Importar módulo compartido de scraping
try:
    from scraping_utils import (
        crear_driver as crear_driver_shared,
        forzar_entrada_pestana_opiniones as forzar_entrada_shared,
        ordenar_por_recientes as ordenar_shared,
        detectar_total_reviews as detectar_total_shared,
        expandir_resenas_largas as expandir_shared,
        parsear_fecha_relativa as parsear_fecha_shared,
        generar_id_review as generar_id_shared,
        extraer_coordenadas_url as extraer_coords_shared
    )
    SCRAPING_UTILS_AVAILABLE = True
except ImportError:
    SCRAPING_UTILS_AVAILABLE = False

# ==========================================
# ⚙️ CONFIGURACIÓN GENERAL
# ==========================================

# Archivos de entrada/salida
ARCHIVO_LUGARES = 'lugares_validados.csv'  # Entrada: lugares validados del enrichment
ARCHIVO_REVIEWS = 'reviews_neuquen.csv'    # Salida: reseñas extraídas
ARCHIVO_ESTADO = 'estado_reviews.csv'       # Estado de procesamiento por URL
ARCHIVO_LOG = 'logs/reviews_run.log'

# Límite de tiempo para GitHub Actions (5.5 horas = dejar margen)
TIEMPO_LIMITE_SEGUNDOS = 5.5 * 60 * 60  # 19800 segundos

# Crear directorio de logs
os.makedirs("logs", exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(ARCHIVO_LOG, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/92.0.4515.107 Safari/537.36"
]

# ==========================================
# 0. FUNCIONES DE EXTRACCIÓN DE METADATA
# ==========================================
# Función de coordenadas movida a geo_utils.py
# Se mantiene referencia local por compatibilidad si es necesario, o se usa directamente la importada.


def parsear_fecha_relativa(fecha_texto):
    """
    Convierte fechas relativas de Google Maps a fechas absolutas aproximadas.
    Ejemplos: "Hace 1 día", "una semana atrás", "Hace 3 meses", "un año atrás"
    Retorna: (fecha_absoluta_str, fecha_original)
    """
    if not fecha_texto:
        return None, None
    
    fecha_lower = fecha_texto.lower().strip()
    ahora = datetime.datetime.now()
    
    # Patrones para números
    numeros = {
        'un': 1, 'una': 1, 'uno': 1,
        'dos': 2, 'tres': 3, 'cuatro': 4, 'cinco': 5,
        'seis': 6, 'siete': 7, 'ocho': 8, 'nueve': 9, 'diez': 10,
        'once': 11, 'doce': 12
    }
    
    # Extraer número
    match_num = re.search(r'(\d+)', fecha_lower)
    if match_num:
        cantidad = int(match_num.group(1))
    else:
        # Buscar palabras numéricas
        cantidad = 1
        for palabra, num in numeros.items():
            if palabra in fecha_lower:
                cantidad = num
                break
    
    # Determinar unidad de tiempo y calcular fecha
    fecha_calculada = None
    
    if 'día' in fecha_lower or 'dia' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(days=cantidad)
    elif 'semana' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(weeks=cantidad)
    elif 'mes' in fecha_lower or 'meses' in fecha_lower:
        # Aproximación: 30 días por mes
        fecha_calculada = ahora - datetime.timedelta(days=cantidad * 30)
    elif 'año' in fecha_lower or 'años' in fecha_lower:
        # Aproximación: 365 días por año
        fecha_calculada = ahora - datetime.timedelta(days=cantidad * 365)
    elif 'hora' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(hours=cantidad)
    elif 'minuto' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(minutes=cantidad)
    
    if fecha_calculada:
        return fecha_calculada.strftime('%Y-%m-%d'), fecha_texto
    
    # Si no se pudo parsear, devolver None
    return None, fecha_texto


def safe_int(value, default=0):
    """Convierte un valor a int de forma segura, retornando default si falla"""
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


# ==========================================
# 1. SISTEMA DE ESTADO INCREMENTAL
# ==========================================
# ==========================================
# 1. SISTEMA DE ESTADO INCREMENTAL (DB)
# ==========================================
from db_utils import get_latest_scraping_states, log_scraping_event, ensure_log_tables_exists

def cargar_estado():
    """Carga el estado de procesamiento de URLs desde la DB"""
    if DB_AVAILABLE:
        ensure_log_tables_exists()
        return get_latest_scraping_states()
    else:
        # Fallback a CSV muy básico si no hay DB (opcional, o retornar vacío)
        return {}

def actualizar_estado(url, estado, mensaje="", reviews_detectadas=0, nuevas_reviews=0, incrementar_intento=False):
    """
    Actualiza el estado de una URL logueando en la DB.
    
    Args:
        url: URL del lugar procesado
        estado: Estado del procesamiento (EXITO, ERROR_TEMPORAL, etc.)
        mensaje: Mensaje descriptivo
        reviews_detectadas: Total de reviews detectadas en la página
        nuevas_reviews: Reviews nuevas insertadas
        incrementar_intento: Si incrementar contador de intentos
    """
    intentos = 1
    
    if DB_AVAILABLE:
        log_scraping_event(
            url=url, 
            estado=estado, 
            mensaje=str(mensaje)[:200],
            reviews_detectadas=reviews_detectadas,
            nuevas_reviews=nuevas_reviews,
            intentos=intentos
        )



def generar_id_review(url, autor, fecha, texto):
    """
    Genera un ID único para una reseña basado en sus datos.
    Usa hash de: URL del lugar + autor + fecha + primeros 50 chars del texto.
    """
    # Normalizar datos
    texto_norm = (texto or "")[:50].lower().strip()
    autor_norm = (autor or "").lower().strip()
    fecha_norm = (fecha or "").lower().strip()
    
    # Crear string único
    unique_str = f"{url}|{autor_norm}|{fecha_norm}|{texto_norm}"
    
    # Generar hash corto
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:16]


def cargar_reviews_existentes_por_restaurante(restaurante_nombre):
    """
    Carga los IDs de reseñas ya existentes para un restaurante específico.
    Primero intenta desde la base de datos (Supabase), luego fallback a CSV.
    Retorna un set de IDs para búsqueda rápida.
    """
    ids_existentes = set()
    
    # Primero intentar desde la base de datos
    if DB_AVAILABLE:
        try:
            ids_db = obtener_ids_existentes_por_restaurante(restaurante_nombre)
            if ids_db:
                logger.info(f"   📊 {len(ids_db)} reviews existentes en DB")
                return ids_db
        except Exception as e:
            logger.warning(f"Error consultando DB: {e}")
    
    # Fallback: cargar desde CSV local (busca por restaurante)
    if not os.path.exists(ARCHIVO_REVIEWS):
        return ids_existentes
    
    try:
        with open(ARCHIVO_REVIEWS, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('restaurante') == restaurante_nombre:
                    review_id = generar_id_review(
                        row.get('url', ''),
                        row.get('autor', ''),
                        row.get('fecha', ''),
                        row.get('texto', '')
                    )
                    ids_existentes.add(review_id)
    except Exception as e:
        logger.warning(f"Error cargando reviews existentes de CSV: {e}")
    
    return ids_existentes


def guardar_reviews(reviews_data):
    """
    Guarda reseñas de forma incremental en Supabase/PostgreSQL.
    El logging a scraping_logs se hace en actualizar_estado(), no aquí.
    """
    if not reviews_data:
        return 0
    
    insertadas_db = 0
    duplicadas_db = 0
    
    # Insertar en base de datos (Supabase)
    if DB_AVAILABLE:
        try:
            insertadas_db, duplicadas_db = insertar_reviews_batch(reviews_data)
            logger.info(f"   💾 DB: {insertadas_db} nuevas, {duplicadas_db} duplicadas")
        except Exception as e:
            logger.warning(f"   ⚠️ Error insertando en DB: {e}")
    
    return insertadas_db if DB_AVAILABLE and insertadas_db > 0 else len(reviews_data)

# ==========================================
# 2. FUNCIONES DE NAVEGACIÓN
# ==========================================
def crear_driver():
    """Crea driver de Chrome compatible con GitHub Actions"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,4000")
    options.add_argument("--lang=es-AR")
    options.add_argument("--log-level=3")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(60)
    return driver



def forzar_entrada_pestana_opiniones(driver):
    """Intenta entrar a la pestaña de Opiniones"""
    xpaths = [
        "//button[@role='tab'][contains(@aria-label, 'Reseñas')]",
        "//button[@role='tab'][contains(@aria-label, 'Reviews')]",
        "//button[@role='tab'][contains(@aria-label, 'Opiniones')]",
        "//button[@role='tab'][contains(@aria-label, 'Revisiones')]", 
        "//button[@role='tab']//div[contains(text(), 'Reseñas')]",
        "//button[@role='tab']//div[contains(text(), 'Opiniones')]",
        "//button[@role='tab'][contains(., 'Reviews')]",
        "//button[@role='tab'][contains(., 'Opiniones')]",
        "//button[@role='tab'][contains(., 'Reseñas')]"
    ]

    for intento in range(3):
        boton_encontrado = None
        for xpath in xpaths:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                boton_encontrado = btn
                break
            except: 
                continue

        if not boton_encontrado:
            try:
                botones = driver.find_elements(By.CSS_SELECTOR, "button[role='tab']")
                for btn in botones:
                    try:
                        txt = (btn.get_attribute("textContent") or "").lower()
                        aria = (btn.get_attribute("aria-label") or "").lower()
                        # Buscar en español e inglés
                        if any(word in txt for word in ["opiniones", "reviews", "reseñas"]):
                            boton_encontrado = btn
                            break
                        if any(word in aria for word in ["opiniones", "reviews", "reseñas", "revisiones"]):
                            boton_encontrado = btn
                            break
                    except: 
                        continue
            except: 
                pass

        if boton_encontrado:
            try:
                driver.execute_script("arguments[0].click();", boton_encontrado)
                time.sleep(2)
                
                # VERIFICACIÓN MULTIPLE
                # 1. Verificar si el botón cambió a seleccionado
                is_selected = boton_encontrado.get_attribute("aria-selected")
                if is_selected == "true":
                    return True

                # 2. Verificar presencia de botón Ordenar o Escribir
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label, 'Ordenar') or contains(@aria-label, 'Sort') or contains(@aria-label, 'Escribir') or contains(@aria-label, 'Write')]"))
                    )
                    return True
                except:
                    pass
                
                # 3. Verificar presencia de puntaje
                try:
                    driver.find_element(By.CLASS_NAME, "fontDisplayLarge")
                    return True
                except:
                    pass

                # Si llegamos acá, el click no pareció surtir efecto, reintentar loop
            except:
                time.sleep(1.5)
                continue
        time.sleep(1.5)
    return False


def ordenar_por_recientes(driver):
    """Ordena reseñas por más recientes"""
    for intento in range(3):
        try:
            # Buscar botón ordenar en español o inglés
            boton_ordenar = None
            try:
                boton_ordenar = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Ordenar') or contains(@aria-label, 'Sort')]"))
                )
            except:
                pass
            
            if not boton_ordenar:
                return True  # Si no hay botón de ordenar, continuar
                
            driver.execute_script("arguments[0].click();", boton_ordenar)
            time.sleep(1)

            opciones = driver.find_elements(By.XPATH, "//div[@role='menuitemradio']")
            
            for op in opciones:
                try:
                    texto = op.text.lower()
                    if "recientes" in texto or "newest" in texto or "más nuevas" in texto:
                        driver.execute_script("arguments[0].click();", op)
                        time.sleep(3)
                        return True
                except StaleElementReferenceException: 
                    continue
            
            # Fallback: segunda opción
            if len(opciones) >= 2:
                driver.execute_script("arguments[0].click();", opciones[1])
                time.sleep(3)
                return True

                
        except Exception:
            time.sleep(1)
            continue
    return False

def detectar_total_reviews(driver):
    """Detecta cantidad total de reseñas"""
    try:
        score = driver.find_element(By.XPATH, "//div[contains(@class, 'fontDisplayLarge')]")
        total_txt = score.find_element(By.XPATH, "..").find_element(By.CLASS_NAME, "fontBodySmall").text
        clean = re.search(r'([\d.,]+)', total_txt).group(1).replace('.', '').replace(',', '')
        return int(clean)
    except: 
        return 0

def expandir_resenas_largas(driver):
    """Expande textos truncados con 'Ver más'"""
    try:
        botones = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Ver más')]")
        for btn in botones:
            try: 
                driver.execute_script("arguments[0].click();", btn)
            except: 
                pass
        time.sleep(1)
    except: 
        pass

# ==========================================
# 3. PROCESAMIENTO PRINCIPAL
# ==========================================
# NOTA (01-sep-2026): aca vivia `procesar_restaurante(lugar, indice, total, tiempo_inicio)`,
# 228 lineas que abrian su propio driver de Selenium por cada lugar. Estaba DEFINIDA y nunca
# llamada: el unico camino vivo es `procesar_restaurante_con_driver`, que reusa un driver ya
# abierto. Se elimino en vez de dejarla 'por las dudas' porque una copia muerta de la logica
# de scraping es peor que no tenerla — invita a arreglar bugs en el lugar equivocado, que es
# exactamente lo que paso el 28-ago cuando se parcheo este archivo creyendo que era el que
# corre el cron (el que corre es monitor_reviews.py). Esta en el historial de git si hace falta.

def procesar_restaurante_con_driver(driver, lugar, tiempo_inicio):
    """
    Procesa un restaurante usando un driver ya inicializado (no lo cierra).
    El driver ya debe haber navegado a la URL del lugar.
    """
    url = lugar['link']
    nombre = lugar.get('nombre', 'Desconocido')
    categoria = lugar.get('categoria', '')
    
    # Verificar tiempo restante
    tiempo_transcurrido = time.time() - tiempo_inicio
    if tiempo_transcurrido > TIEMPO_LIMITE_SEGUNDOS:
        return None, "TIMEOUT_GLOBAL"
    
    reviews_data = []
    estado = "ERROR_TEMPORAL"
    mensaje = ""
    
    metadata = {
        "nombre": nombre,
        "categoria": categoria,
        "rating_gral": None,
        "total_google": 0,
        "direccion": None,
        "latitud": None,
        "longitud": None
    }
    
    # Coordenadas de URL
    lat, lon = extraer_coordenadas_url(url)
    metadata['latitud'] = lat
    metadata['longitud'] = lon

    try:
        try: 
            driver.execute_script("document.body.style.zoom='50%'")
        except: 
            pass

        # Esperar carga
        logger.info("   ⏳ Esperando carga de página...")
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[role='tab']")))
        except TimeoutException:
            logger.info("   ⚠️ Timeout, refrescando...")
            driver.refresh()
            time.sleep(5)

        # Extraer dirección
        try:
            boton_direccion = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='address']")
            direccion_elem = boton_direccion.find_element(By.CLASS_NAME, "Io6YTe")
            metadata['direccion'] = direccion_elem.text.strip()
        except:
            pass

        # Navegar a pestaña Opiniones
        logger.info("   🔍 Buscando pestaña Opiniones...")
        if not forzar_entrada_pestana_opiniones(driver):
            driver.refresh()
            time.sleep(5)
            if not forzar_entrada_pestana_opiniones(driver):
                # Marcar para reintento (no como estado final)
                logger.info("   ⚠️ No encontró pestaña - marcado para reintento")
                estado = "RETRY_PESTANA"
                mensaje = "No encontró pestaña de Opiniones - reintentar"
                actualizar_estado(url, estado, mensaje)
                return [], estado


        # Ordenar por recientes
        ordenar_por_recientes(driver)

        # Metadata
        try: 
            metadata['rating_gral'] = driver.find_element(By.CLASS_NAME, "fontDisplayLarge").text
        except: 
            pass
        
        metadata['total_google'] = detectar_total_reviews(driver)
        # Añadir URL a metadata para upsert
        metadata['url'] = url
        # Mapeo para consistencia
        metadata['total_reviews_google'] = metadata['total_google'] 
        
        # Enriquecer con Barrio/Zona
        if metadata['latitud'] and metadata['longitud']:
            try:
                geo_info = asignar_barrio(metadata['latitud'], metadata['longitud'])
                metadata.update(geo_info)
            except Exception as e:
                logger.warning(f"   ⚠️ Error asignando barrio: {e}")

        # Upsert del Lugar en DB
        if DB_AVAILABLE:
            try:
                upsert_lugar(metadata)
                logger.info("   🏪 Lugar actualizado en DB")
            except Exception as e:
                logger.warning(f"   ⚠️ Error upsert lugar: {e}")

        target = min(metadata['total_google'], 500)
        
        logger.info(f"   Rating: {metadata['rating_gral']} | Reviews: {metadata['total_google']} | Target: {target}")

        # Scroll para cargar reseñas
        ultimo_conteo = 0
        tiempo_estancado = 0
        scroll_start = time.time()
        
        while True:
            if time.time() - tiempo_inicio > TIEMPO_LIMITE_SEGUNDOS:
                logger.warning("Tiempo límite global alcanzado durante scroll")
                break
            
            conteo_actual = len(driver.find_elements(By.CSS_SELECTOR, "div.jftiEf"))
            
            # Log de progreso cada 10 reseñas
            if conteo_actual > 0 and conteo_actual != ultimo_conteo and conteo_actual % 10 == 0:
                logger.info(f"   📜 Cargando reseñas: {conteo_actual}/{target}")
            
            if conteo_actual >= target:
                break
            
            if conteo_actual == ultimo_conteo:
                tiempo_estancado += 1
                if tiempo_estancado > 8:
                    break
            else:
                tiempo_estancado = 0
                ultimo_conteo = conteo_actual
            
            if time.time() - scroll_start > 180:
                break
            
            try:
                contenedor = driver.find_element(By.CSS_SELECTOR, "div.m6QErb.DxyBCb")
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", contenedor)
            except:
                pass
            
            time.sleep(1)

        
        # Expandir reseñas largas
        expandir_resenas_largas(driver)
        
        # Cargar IDs de reseñas existentes
        ids_existentes = cargar_reviews_existentes_por_restaurante(metadata['nombre'])
        if ids_existentes:
            logger.info(f"   Reseñas existentes en dataset: {len(ids_existentes)}")
        
        # Early-stop optimization: obtener última reseña de DB para comparar
        ultima_review_db = None
        if DB_AVAILABLE:
            try:
                ultima_review_db = get_ultima_review_restaurante(metadata['nombre'])
                if ultima_review_db:
                    logger.info(f"   🎯 Early-stop activado: buscando match con última review de DB")
            except:
                pass
        
        # Extraer datos con BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        bloques = soup.find_all('div', class_='jftiEf')
        
        fecha_scraping = datetime.datetime.now().isoformat()
        reviews_nuevas = 0
        reviews_duplicadas = 0
        early_stopped = False
        
        for bloque in bloques:
            t_autor = bloque.find('div', class_='d4r55')
            autor = t_autor.text.strip() if t_autor else "Anónimo"
            
            t_texto = bloque.find('span', class_='wiI7pd')
            texto = t_texto.text.strip() if t_texto else ""
            
            # Early-stop check: si esta review coincide con la última en DB, parar
            if ultima_review_db:
                autor_norm = autor.strip().lower()
                texto_norm = ' '.join(texto[:100].lower().split())
                if (autor_norm == ultima_review_db['autor'] and 
                    texto_norm.startswith(ultima_review_db['texto_inicio'][:50])):
                    logger.info(f"   ⏹️ Early-stop: encontrada review existente de '{autor[:20]}...'")
                    early_stopped = True
                    break
            
            t_fecha = bloque.find('span', class_='rsqaWe')
            fecha_texto = t_fecha.text.strip() if t_fecha else None
            
            fecha_aproximada, fecha_original = parsear_fecha_relativa(fecha_texto)
            
            review_id = generar_id_review(url, autor, fecha_texto, texto)
            
            if review_id in ids_existentes:
                reviews_duplicadas += 1
                continue
            
            row = {
                'restaurante': metadata['nombre'],
                'categoria': metadata['categoria'],
                'rating_gral': metadata['rating_gral'],
                'total_reviews_google': metadata['total_google'],
                'direccion': metadata['direccion'],
                'latitud': metadata['latitud'],
                'longitud': metadata['longitud'],
                'autor': autor,
                'rating_user': None,
                'texto': texto,
                'fecha_aproximada': fecha_aproximada,
                'fecha_original': fecha_original,
                'url': url,
                'fecha_scraping': fecha_scraping,
                'review_id': review_id
            }
            
            tags_img = bloque.find_all('span', role='img')
            for tag in tags_img:
                lbl = (tag.get('aria-label') or "").lower()
                if 'estrella' in lbl or 'star' in lbl:
                    match = re.search(r'(\d+[.,]?\d*)', lbl)
                    if match:
                        try: 
                            row['rating_user'] = float(match.group(1).replace(',', '.'))
                        except: 
                            pass
                        break
            
            reviews_data.append(row)
            reviews_nuevas += 1
        
        estado = "EXITO"
        early_msg = " (early-stop)" if early_stopped else ""
        mensaje = f"Nuevas: {reviews_nuevas}, Duplicadas: {reviews_duplicadas}{early_msg}"
        logger.info(f"   ✓ {reviews_nuevas} reseñas NUEVAS | {reviews_duplicadas} duplicadas{early_msg}")

    except Exception as e:
        estado = "ERROR_TEMPORAL"
        mensaje = str(e)[:200]
        logger.error(f"   Error: {e}")
    
    # NO cerramos el driver aquí - se reutiliza
    
    actualizar_estado(
        url, estado, mensaje,
        reviews_detectadas=metadata.get('total_google', 0),
        nuevas_reviews=reviews_nuevas if estado == 'EXITO' else 0
    )
    return reviews_data, estado


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    tiempo_inicio = time.time()
    
    logger.info("=" * 60)
    logger.info("INICIO DEL SCRAPER DE RESEÑAS")
    logger.info(f"Fecha: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Límite de tiempo: {TIEMPO_LIMITE_SEGUNDOS/3600:.1f} horas")
    logger.info("=" * 60)
    
    # Verificar conexión a base de datos
    if DB_AVAILABLE:
        try:
            conn = get_connection()
            if conn:
                logger.info("✅ Conexión a Supabase/PostgreSQL establecida")
                # Asegurar que existe el índice único
                ensure_review_id_unique_constraint()
            else:
                logger.warning("⚠️ DB_AVAILABLE pero no hay conexión - usando solo CSV")
        except Exception as e:
            logger.warning(f"⚠️ Error conectando a DB: {e} - usando solo CSV")
    else:
        logger.info("📁 Modo CSV (DATABASE_URL no configurada)")
    
    # Cargar lugares validados
    if not os.path.exists(ARCHIVO_LUGARES):
        logger.error(f"No se encontró {ARCHIVO_LUGARES}")
        sys.exit(1)
    
    lugares = []
    with open(ARCHIVO_LUGARES, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        lugares = list(reader)
    
    logger.info(f"Lugares validados encontrados: {len(lugares)}")
    
    # Cargar estado de URLs procesadas
    estado_urls = cargar_estado()
    
    # Filtrar: solo procesar pendientes (no EXITO ni SIN_OPINIONES_DEFINITIVO)
    pendientes = []
    exitos = 0
    sin_opiniones_def = 0
    
    for lugar in lugares:
        url = lugar['link']
        data_estado = estado_urls.get(url)
        
        status = None
        if data_estado:
            if isinstance(data_estado, dict):
                status = data_estado.get('estado')
            else:
                status = str(data_estado)
        
        if status == 'EXITO':
            exitos += 1
            continue
            
        if status == 'SIN_OPINIONES_DEFINITIVO':
            sin_opiniones_def += 1
            continue
            
        # Cualquier otro estado (None, ERROR_TEMPORAL, RETRY_PESTANA, SIN_OPINIONES viejo) se procesa
        pendientes.append(lugar)
    
    logger.info(f"Ya procesados con éxito: {exitos}")
    logger.info(f"Sin opiniones definitivo (skip): {sin_opiniones_def}")
    logger.info(f"Pendientes: {len(pendientes)}")
    logger.info("-" * 40)
    
    # Crear driver compartido para evitar crear uno nuevo cada vez
    logger.info("Creando driver de Chrome...")
    driver = None
    
    def obtener_driver():
        """Obtiene o crea un driver de Chrome"""
        global driver
        if driver is None:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,4000")
            options.add_argument("--lang=es-AR")
            options.add_argument("--log-level=3")
            options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.set_page_load_timeout(60)
        return driver
    
    def reiniciar_driver():
        """Reinicia el driver si crashea"""
        global driver
        try:
            driver.quit()
        except:
            pass
        driver = None
        time.sleep(3)
        return obtener_driver()
    
    # Procesar
    procesados = 0
    total_reviews = 0
    errores_consecutivos = 0
    
    try:
        driver = obtener_driver()
        logger.info("✓ Driver creado exitosamente")
    except Exception as e:
        logger.error(f"No se pudo crear el driver: {e}")
        sys.exit(1)
    
    for i, lugar in enumerate(pendientes, 1):
        # Verificar tiempo
        tiempo_transcurrido = time.time() - tiempo_inicio
        if tiempo_transcurrido > TIEMPO_LIMITE_SEGUNDOS:
            logger.warning(f"LÍMITE DE TIEMPO ALCANZADO: {tiempo_transcurrido/3600:.2f}h")
            break
        
        url = lugar['link']
        nombre = lugar.get('nombre', 'Desconocido')
        
        logger.info(f"[{i}/{len(pendientes)}] {nombre[:40]}...")
        
        try:
            # Forzar idioma español en la URL (reemplazar hl=en por hl=es)
            url_es = url.replace('hl=en', 'hl=es').replace('&hl=en-US', '&hl=es')
            if 'hl=' not in url_es:
                # Si no hay parámetro hl, agregarlo
                if '?' in url_es:
                    url_es += '&hl=es'
                else:
                    url_es += '?hl=es'
            
            # Navegar a la URL en español
            driver.get(url_es)
            time.sleep(2)

            
            # Verificar que el driver sigue funcionando
            _ = driver.title
            
            # Procesar
            reviews, estado = procesar_restaurante_con_driver(driver, lugar, tiempo_inicio)
            
            if estado == "TIMEOUT_GLOBAL":
                break
            
            if reviews:
                guardar_reviews(reviews)
                total_reviews += len(reviews)
            
            procesados += 1
            errores_consecutivos = 0
            
            # Guardar progreso cada 10 restaurantes
            if procesados % 10 == 0:
                logger.info(f"   💾 Guardando progreso ({procesados} procesados, {total_reviews} reseñas)...")
                try:
                    import subprocess
                    subprocess.run(["cp", "estado_reviews.csv", "private-repo/data/"], check=False)
                    subprocess.run(["cp", "reviews_neuquen.csv", "private-repo/data/"], check=False)
                    subprocess.run(["git", "-C", "private-repo", "config", "user.name", "GitHub Actions Bot"], check=False)
                    subprocess.run(["git", "-C", "private-repo", "config", "user.email", "actions@github.com"], check=False)
                    subprocess.run(["git", "-C", "private-repo", "add", "."], check=False)
                    subprocess.run(["git", "-C", "private-repo", "commit", "-m", f"Progreso: {procesados} lugares, {total_reviews} reseñas"], check=False)
                    subprocess.run(["git", "-C", "private-repo", "push"], check=False)
                    logger.info("   ✓ Progreso guardado")
                except Exception as save_err:
                    logger.warning(f"   No se pudo guardar progreso: {save_err}")
            
        except Exception as e:
            errores_consecutivos += 1
            logger.warning(f"   Error en lugar: {str(e)[:50]}")
            actualizar_estado(url, "ERROR_TEMPORAL", str(e)[:100])
            
            if errores_consecutivos >= 3:
                logger.warning("3 errores consecutivos - reiniciando driver...")
                try:
                    driver = reiniciar_driver()
                    logger.info("✓ Driver reiniciado")
                    errores_consecutivos = 0
                except Exception as restart_error:
                    logger.error(f"No se pudo reiniciar el driver: {restart_error}")
                    break
        
        if i % 20 == 0:
            logger.info(f"♻️ Reiniciando driver preventivamente (ciclo {i})...")
            try:
                driver = reiniciar_driver()
            except Exception as e:
                logger.error(f"Error reiniciando driver: {e}")
        
        time.sleep(2)  # Pausa entre lugares

    
    # Cerrar driver
    try:
        driver.quit()
    except:
        pass
    
    # Cerrar conexión a base de datos
    if DB_AVAILABLE:
        try:
            close_connection()
        except:
            pass
    
    # Resumen
    tiempo_total = time.time() - tiempo_inicio
    logger.info("=" * 60)
    logger.info("RESUMEN DE EJECUCIÓN")
    logger.info("=" * 60)
    logger.info(f"Tiempo total: {tiempo_total/60:.1f} minutos")
    logger.info(f"Lugares procesados: {procesados} / {len(pendientes)}")
    logger.info(f"Reseñas extraídas: {total_reviews}")
    logger.info(f"Archivo de salida: {ARCHIVO_REVIEWS}")
    logger.info("=" * 60)
    
    # Si quedan pendientes (no se procesaron todos), crear archivo flag para que el workflow se reinicie
    if procesados < len(pendientes):
        logger.info("⚠️ Quedan lugares pendientes: creando flag para continuar workflow")
        with open(".continue", "w") as f:
            f.write("true")