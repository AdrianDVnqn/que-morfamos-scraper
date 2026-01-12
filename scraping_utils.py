"""
Módulo de utilidades compartidas para scraping de Google Maps.
Contiene funciones de navegación, extracción de datos y utilidades comunes.
"""
import time
import re
import datetime
import hashlib
import random
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

logger = logging.getLogger(__name__)

# User agents para rotación
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


# ==========================================
# FUNCIONES DE DRIVER
# ==========================================

def crear_driver(headless=True, window_height=4000):
    """Crea driver de Chrome compatible con GitHub Actions"""
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument(f"--window-size=1920,{window_height}")
    options.add_argument("--lang=es-AR")
    options.add_argument("--log-level=3")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(60)
    return driver


# ==========================================
# FUNCIONES DE NAVEGACIÓN
# ==========================================

def forzar_entrada_pestana_opiniones(driver):
    """Intenta entrar a la pestaña de Opiniones/Reseñas"""
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
                
                # Verificar si el botón cambió a seleccionado
                is_selected = boton_encontrado.get_attribute("aria-selected")
                if is_selected == "true":
                    return True

                # Verificar presencia de botón Ordenar o Escribir
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label, 'Ordenar') or contains(@aria-label, 'Sort') or contains(@aria-label, 'Escribir') or contains(@aria-label, 'Write')]"))
                    )
                    return True
                except:
                    pass
                
                # Verificar presencia de puntaje
                try:
                    driver.find_element(By.CLASS_NAME, "fontDisplayLarge")
                    return True
                except:
                    pass

            except:
                time.sleep(1.5)
                continue
        time.sleep(1.5)
    return False


def ordenar_por_recientes(driver):
    """Ordena reseñas por más recientes"""
    for intento in range(3):
        try:
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
    """Detecta cantidad total de reseñas desde la página"""
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


def extraer_rating_page(driver):
    """Extrae el rating general de la página"""
    try: 
        return driver.find_element(By.CLASS_NAME, "fontDisplayLarge").text
    except: 
        return None


# ==========================================
# FUNCIONES DE EXTRACCIÓN DE DATOS
# ==========================================

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
    
    numeros = {
        'un': 1, 'una': 1, 'uno': 1,
        'dos': 2, 'tres': 3, 'cuatro': 4, 'cinco': 5,
        'seis': 6, 'siete': 7, 'ocho': 8, 'nueve': 9, 'diez': 10,
        'once': 11, 'doce': 12
    }
    
    match_num = re.search(r'(\d+)', fecha_lower)
    if match_num:
        cantidad = int(match_num.group(1))
    else:
        cantidad = 1
        for palabra, num in numeros.items():
            if palabra in fecha_lower:
                cantidad = num
                break
    
    fecha_calculada = None
    
    if 'día' in fecha_lower or 'dia' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(days=cantidad)
    elif 'semana' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(weeks=cantidad)
    elif 'mes' in fecha_lower or 'meses' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(days=cantidad * 30)
    elif 'año' in fecha_lower or 'años' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(days=cantidad * 365)
    elif 'hora' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(hours=cantidad)
    elif 'minuto' in fecha_lower:
        fecha_calculada = ahora - datetime.timedelta(minutes=cantidad)
    
    if fecha_calculada:
        return fecha_calculada.strftime('%Y-%m-%d'), fecha_texto
    
    return None, fecha_texto


def generar_id_review(url, autor, fecha, texto):
    """
    Genera un ID único para una reseña basado en sus datos.
    Usa hash de: URL del lugar + autor + fecha + primeros 50 chars del texto.
    """
    texto_norm = (texto or "")[:50].lower().strip()
    autor_norm = (autor or "").lower().strip()
    fecha_norm = (fecha or "").lower().strip()
    
    unique_str = f"{url}|{autor_norm}|{fecha_norm}|{texto_norm}"
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:16]


def extraer_reviews_de_pagina(driver, url, metadata, ultimas_reviews_db=None):
    """
    Extrae reseñas de la página actual con early-stop optimization.
    
    Args:
        driver: Selenium driver
        url: URL del lugar
        metadata: dict con nombre, categoria, rating_gral, total_google, direccion, lat, lon
        ultimas_reviews_db: lista de dicts con 'autor' y 'texto_inicio' para early-stop
        
    Returns:
        tuple: (lista_reviews, early_stopped)
    """
    expandir_resenas_largas(driver)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    bloques = soup.find_all('div', class_='jftiEf')
    
    fecha_scraping = datetime.datetime.now().isoformat()
    reviews_data = []
    early_stopped = False
    
    for bloque in bloques:
        t_autor = bloque.find('div', class_='d4r55')
        autor = t_autor.text.strip() if t_autor else "Anónimo"
        
        t_texto = bloque.find('span', class_='wiI7pd')
        texto = t_texto.text.strip() if t_texto else ""
        
        # Early-stop check
        if ultimas_reviews_db:
            autor_norm = autor.strip().lower()
            texto_norm = ' '.join(texto[:100].lower().split())
            
            for review_db in ultimas_reviews_db:
                if (autor_norm == review_db['autor'] and 
                    texto_norm.startswith(review_db['texto_inicio'][:50])):
                    logger.info(f"   ⏹️ Early-stop: match con review de '{autor[:20]}...'")
                    early_stopped = True
                    break
            
            if early_stopped:
                break
        
        t_fecha = bloque.find('span', class_='rsqaWe')
        fecha_texto = t_fecha.text.strip() if t_fecha else None
        
        fecha_aproximada, fecha_original = parsear_fecha_relativa(fecha_texto)
        review_id = generar_id_review(url, autor, fecha_texto, texto)
        
        row = {
            'restaurante': metadata.get('nombre', 'Desconocido'),
            'categoria': metadata.get('categoria', ''),
            'rating_gral': metadata.get('rating_gral'),
            'total_reviews_google': metadata.get('total_google', 0),
            'direccion': metadata.get('direccion'),
            'latitud': metadata.get('latitud'),
            'longitud': metadata.get('longitud'),
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
    
    return reviews_data, early_stopped


def scroll_para_cargar_reviews(driver, target, tiempo_limite_segundos=180):
    """
    Hace scroll para cargar reseñas hasta alcanzar el target o agotar tiempo.
    
    Args:
        driver: Selenium driver
        target: Número objetivo de reseñas a cargar
        tiempo_limite_segundos: Tiempo máximo para scroll
        
    Returns:
        int: Cantidad de reseñas cargadas
    """
    ultimo_conteo = 0
    tiempo_estancado = 0
    scroll_start = time.time()
    
    while True:
        if time.time() - scroll_start > tiempo_limite_segundos:
            break
        
        conteo_actual = len(driver.find_elements(By.CSS_SELECTOR, "div.jftiEf"))
        
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
        
        try:
            contenedor = driver.find_element(By.CSS_SELECTOR, "div.m6QErb.DxyBCb")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", contenedor)
        except:
            pass
        
        time.sleep(1)
    
    return len(driver.find_elements(By.CSS_SELECTOR, "div.jftiEf"))


def extraer_coordenadas_url(url):
    """Extrae latitud y longitud de una URL de Google Maps"""
    try:
        match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
        if match:
            return float(match.group(1)), float(match.group(2))
    except:
        pass
    return None, None
