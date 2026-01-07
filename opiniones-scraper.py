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
def extraer_coordenadas_url(url):
    """Extrae latitud y longitud de una URL de Google Maps"""
    # Patrón 1: @lat,lon,zoom
    patron = r'@(-?\d+\.\d+),(-?\d+\.\d+),\d+\.?\d*z?'
    match = re.search(patron, url)
    if match:
        return float(match.group(1)), float(match.group(2))
    
    # Patrón 2: @lat,lon (sin zoom)
    patron_alt = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
    match_alt = re.search(patron_alt, url)
    if match_alt:
        return float(match_alt.group(1)), float(match_alt.group(2))
    
    # Patrón 3: !3dlat!4dlon
    patron_data = r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
    match_data = re.search(patron_data, url)
    if match_data:
        return float(match_data.group(1)), float(match_data.group(2))
    
    return None, None


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


# ==========================================
# 1. SISTEMA DE ESTADO INCREMENTAL
# ==========================================
def cargar_estado():
    """Carga el estado de procesamiento de URLs"""
    if os.path.exists(ARCHIVO_ESTADO):
        estados = {}
        with open(ARCHIVO_ESTADO, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                estados[row['url']] = row['estado']
        return estados
    return {}

def actualizar_estado(url, estado, mensaje=""):
    """Actualiza el estado de una URL (EXITO, SIN_OPINIONES, ERROR_TEMPORAL, TIMEOUT)"""
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nueva_fila = {
        'url': url,
        'estado': estado,
        'fecha': ahora,
        'mensaje': str(mensaje).replace('\n', ' ').strip()[:200]
    }
    
    # Leer estado actual
    filas = []
    if os.path.exists(ARCHIVO_ESTADO):
        with open(ARCHIVO_ESTADO, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            filas = [row for row in reader if row['url'] != url]
    
    filas.append(nueva_fila)
    
    # Escribir
    with open(ARCHIVO_ESTADO, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'estado', 'fecha', 'mensaje'])
        writer.writeheader()
        writer.writerows(filas)

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


def cargar_reviews_existentes_por_url(url):
    """
    Carga los IDs de reseñas ya existentes para una URL específica.
    Retorna un set de IDs para búsqueda rápida.
    """
    ids_existentes = set()
    
    if not os.path.exists(ARCHIVO_REVIEWS):
        return ids_existentes
    
    try:
        with open(ARCHIVO_REVIEWS, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('url') == url:
                    # Regenerar el ID de la reseña existente
                    review_id = generar_id_review(
                        row.get('url', ''),
                        row.get('autor', ''),
                        row.get('fecha', ''),
                        row.get('texto', '')
                    )
                    ids_existentes.add(review_id)
    except Exception as e:
        logger.warning(f"Error cargando reviews existentes: {e}")
    
    return ids_existentes


def guardar_reviews(reviews_data):
    """Guarda reseñas de forma incremental"""
    if not reviews_data:
        return 0
    
    es_nuevo = not os.path.exists(ARCHIVO_REVIEWS)
    campos = ['restaurante', 'categoria', 'rating_gral', 'total_reviews_google', 
              'direccion', 'latitud', 'longitud', 'autor', 'rating_user', 
              'texto', 'fecha_aproximada', 'fecha_original', 'url', 'fecha_scraping', 'review_id']
    
    with open(ARCHIVO_REVIEWS, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        if es_nuevo:
            writer.writeheader()
        writer.writerows(reviews_data)
    
    return len(reviews_data)

# ==========================================
# 2. FUNCIONES DE NAVEGACIÓN
# ==========================================
def crear_driver():
    """Crea driver de Chrome compatible con GitHub Actions"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--window-size=1920,4000")
    options.add_argument("--lang=es-AR")
    options.add_argument("--log-level=3")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    # Más estabilidad en GitHub Actions
    options.add_argument("--single-process")
    options.add_argument("--disable-features=VizDisplayCompositor")
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def forzar_entrada_pestana_opiniones(driver):
    """Intenta entrar a la pestaña de Opiniones"""
    xpaths = [
        "//button[@role='tab'][contains(@aria-label, 'Revisiones')]", 
        "//button[@role='tab'][@data-tab-index='2']",
        "//button[@role='tab']//div[contains(text(), 'Opiniones')]",
        "//button[@role='tab'][contains(@aria-label, 'Opiniones')]"
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
                        txt = btn.get_attribute("textContent").lower()
                        aria = (btn.get_attribute("aria-label") or "").lower()
                        if "opiniones" in txt or "revisiones" in aria or "reviews" in aria:
                            boton_encontrado = btn
                            break
                    except: 
                        continue
            except: 
                pass

        if boton_encontrado:
            try:
                driver.execute_script("arguments[0].click();", boton_encontrado)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label, 'Ordenar') or contains(@aria-label, 'Escribir')]"))
                )
                return True
            except:
                time.sleep(1.5)
                continue
        time.sleep(1.5)
    return False

def ordenar_por_recientes(driver):
    """Ordena reseñas por más recientes"""
    for intento in range(3):
        try:
            boton_ordenar = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Ordenar')]"))
            )
            driver.execute_script("arguments[0].click();", boton_ordenar)
            time.sleep(1)

            opciones = driver.find_elements(By.XPATH, "//div[@role='menuitemradio']")
            
            for op in opciones:
                try:
                    if "recientes" in op.text.lower() or "newest" in op.text.lower():
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
def procesar_restaurante(lugar, indice, total, tiempo_inicio):
    """Procesa un restaurante y extrae sus reseñas"""
    url = lugar['link']
    nombre = lugar.get('nombre', 'Desconocido')
    categoria = lugar.get('categoria', '')
    
    # Verificar tiempo restante
    tiempo_transcurrido = time.time() - tiempo_inicio
    if tiempo_transcurrido > TIEMPO_LIMITE_SEGUNDOS:
        logger.warning(f"TIEMPO LÍMITE ALCANZADO ({tiempo_transcurrido/3600:.1f}h)")
        return None, "TIMEOUT_GLOBAL"
    
    logger.info(f"[{indice}/{total}] Procesando: {nombre[:40]}...")
    
    driver = crear_driver()
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
        driver.get(url)
        try: 
            driver.execute_script("document.body.style.zoom='50%'")
        except: 
            pass

        # Esperar carga
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[role='tab']")))
        except TimeoutException:
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
        if not forzar_entrada_pestana_opiniones(driver):
            driver.refresh()
            time.sleep(5)
            if not forzar_entrada_pestana_opiniones(driver):
                estado = "SIN_OPINIONES"
                mensaje = "No tiene pestaña de Opiniones"
                actualizar_estado(url, estado, mensaje)
                driver.quit()
                return [], estado

        # Ordenar por recientes
        ordenar_por_recientes(driver)

        # Metadata
        try: 
            metadata['rating_gral'] = driver.find_element(By.CLASS_NAME, "fontDisplayLarge").text
        except: 
            pass
        
        metadata['total_google'] = detectar_total_reviews(driver)
        target = min(metadata['total_google'], 500)  # Límite por lugar por ejecución
        
        logger.info(f"   Rating: {metadata['rating_gral']} | Reviews: {metadata['total_google']} | Target: {target}")

        # Scroll para cargar reseñas
        ultimo_conteo = 0
        tiempo_estancado = 0
        scroll_start = time.time()
        
        while True:
            # Verificar tiempo global
            if time.time() - tiempo_inicio > TIEMPO_LIMITE_SEGUNDOS:
                logger.warning("Tiempo límite global alcanzado durante scroll")
                break
            
            reviews = driver.find_elements(By.CLASS_NAME, 'jftiEf')
            actual = len(reviews)
            
            if actual >= target:
                break
            
            if actual == ultimo_conteo:
                tiempo_estancado += 1.5
                if tiempo_estancado > 20:
                    break
            else:
                tiempo_estancado = 0
            
            ultimo_conteo = actual
            
            if reviews:
                driver.execute_script("arguments[0].scrollIntoView(true);", reviews[-1])
            
            time.sleep(1.2)

        expandir_resenas_largas(driver)
        
        # Cargar IDs de reseñas existentes para este lugar
        ids_existentes = cargar_reviews_existentes_por_url(url)
        if ids_existentes:
            logger.info(f"   Reseñas existentes en dataset: {len(ids_existentes)}")
        
        # Extraer datos con BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        bloques = soup.find_all('div', class_='jftiEf')
        
        fecha_scraping = datetime.datetime.now().isoformat()
        reviews_nuevas = 0
        reviews_duplicadas = 0
        
        for bloque in bloques:
            # Extraer datos básicos primero para generar ID
            t_autor = bloque.find('div', class_='d4r55')
            autor = t_autor.text.strip() if t_autor else "Anónimo"
            
            t_texto = bloque.find('span', class_='wiI7pd')
            texto = t_texto.text.strip() if t_texto else ""
            
            t_fecha = bloque.find('span', class_='rsqaWe')
            fecha_texto = t_fecha.text.strip() if t_fecha else None
            
            # Convertir fecha relativa a absoluta
            fecha_aproximada, fecha_original = parsear_fecha_relativa(fecha_texto)
            
            # Generar ID único para esta reseña (usando fecha original para consistencia)
            review_id = generar_id_review(url, autor, fecha_texto, texto)
            
            # Verificar si ya existe
            if review_id in ids_existentes:
                reviews_duplicadas += 1
                continue  # Saltar reseña duplicada
            
            # Crear registro
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
            
            # Rating del usuario
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
        mensaje = f"Nuevas: {reviews_nuevas}, Duplicadas: {reviews_duplicadas}"
        logger.info(f"   ✓ {reviews_nuevas} reseñas NUEVAS | {reviews_duplicadas} duplicadas (skip)")

    except Exception as e:
        estado = "ERROR_TEMPORAL"
        mensaje = str(e)[:200]
        logger.error(f"   Error: {e}")
    
    finally:
        driver.quit()
    
    actualizar_estado(url, estado, mensaje)
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
    
    # Filtrar: solo procesar pendientes (no EXITO ni SIN_OPINIONES)
    pendientes = []
    for lugar in lugares:
        url = lugar['link']
        estado = estado_urls.get(url)
        if estado not in ['EXITO', 'SIN_OPINIONES']:
            pendientes.append(lugar)
    
    logger.info(f"Ya procesados con éxito: {sum(1 for e in estado_urls.values() if e == 'EXITO')}")
    logger.info(f"Sin opiniones (skip): {sum(1 for e in estado_urls.values() if e == 'SIN_OPINIONES')}")
    logger.info(f"Pendientes: {len(pendientes)}")
    logger.info("-" * 40)
    
    # Procesar
    procesados = 0
    total_reviews = 0
    
    for i, lugar in enumerate(pendientes, 1):
        # Verificar tiempo
        tiempo_transcurrido = time.time() - tiempo_inicio
        if tiempo_transcurrido > TIEMPO_LIMITE_SEGUNDOS:
            logger.warning(f"LÍMITE DE TIEMPO ALCANZADO: {tiempo_transcurrido/3600:.2f}h")
            break
        
        reviews, estado = procesar_restaurante(lugar, i, len(pendientes), tiempo_inicio)
        
        if estado == "TIMEOUT_GLOBAL":
            break
        
        if reviews:
            guardar_reviews(reviews)
            total_reviews += len(reviews)
        
        procesados += 1
        time.sleep(1)  # Pausa entre lugares
    
    # Resumen
    tiempo_total = time.time() - tiempo_inicio
    logger.info("=" * 60)
    logger.info("RESUMEN DE EJECUCIÓN")
    logger.info("=" * 60)
    logger.info(f"Tiempo total: {tiempo_total/60:.1f} minutos")
    logger.info(f"Lugares procesados: {procesados}")
    logger.info(f"Reseñas extraídas: {total_reviews}")
    logger.info(f"Archivo de salida: {ARCHIVO_REVIEWS}")
    logger.info("=" * 60)