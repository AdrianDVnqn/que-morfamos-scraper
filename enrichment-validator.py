import time
import logging
import csv
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openai import OpenAI
import json

# --- CONFIGURACIÓN ---

ARCHIVO_ENTRADA = "lugares_encontrados.csv"
ARCHIVO_VALIDADOS = "lugares_validados.csv"
ARCHIVO_RECHAZADOS = "lugares_rechazados.csv"
ARCHIVO_LOG = "logs/enrichment_run.log"

# Crear directorio de logs si no existe
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

# Configurar DeepSeek API
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
if DEEPSEEK_API_KEY:
    client = OpenAI(
        api_key=DEEPSEEK_API_KEY,
        base_url="https://api.deepseek.com"
    )
else:
    logger.warning("DEEPSEEK_API_KEY no configurada. La validación LLM estará deshabilitada.")
    client = None



def extraer_categoria_de_lugar(driver, url, max_intentos=2):
    """
    Visita un lugar en Google Maps y extrae su categoría.
    Retorna la categoría o None si no se pudo extraer.
    """
    for intento in range(max_intentos):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            
            # Esperar a que cargue la página
            time.sleep(3)
            
            # Buscar el botón de categoría (puede tener diferentes selectores)
            selectores = [
                "button[jsaction*='category']",
                "button.DkEaL",
                "[data-tooltip='Copiar la categoría']",
            ]
            
            for selector in selectores:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, selector)
                    categoria = elem.text.strip()
                    if categoria:
                        return categoria
                except:
                    continue
            
            # Backup: buscar en spans con clase específica
            try:
                # A veces la categoría está en un span dentro del header
                spans = driver.find_elements(By.CSS_SELECTOR, "span.DkEaL, span.mgr77e")
                for span in spans:
                    texto = span.text.strip()
                    if texto and len(texto) < 50:  # Categorías suelen ser cortas
                        return texto
            except:
                pass
            
            logger.warning(f"No se encontró categoría para: {url}")
            return None
            
        except Exception as e:
            logger.error(f"Error al extraer categoría (intento {intento + 1}): {e}")
            if intento < max_intentos - 1:
                time.sleep(2)
    
    return None


def validar_con_llm(lugares_con_categoria, batch_size=10):
    """
    Usa DeepSeek para validar si las categorías corresponden a lugares gastronómicos.
    Procesa en batches para eficiencia.
    """
    if not client:
        logger.warning("LLM no disponible, marcando todos como válidos por defecto.")
        return {l["link"]: {"es_valido": True, "razon": "Sin validación LLM"} for l in lugares_con_categoria}
    
    resultados = {}
    
    # Procesar en batches
    for i in range(0, len(lugares_con_categoria), batch_size):
        batch = lugares_con_categoria[i:i + batch_size]
        
        # Construir el prompt
        items = "\n".join([
            f'{j+1}. Nombre: "{l["nombre"]}" | Categoría: "{l["categoria"]}"'
            for j, l in enumerate(batch)
        ])
        
        prompt = f"""Analiza cada lugar y determina si es un establecimiento GASTRONÓMICO 
(restaurante, bar, cafetería, heladería, pizzería, parrilla, delivery de comida, etc.).

IMPORTANTE: 
- Responde SOLO con un JSON válido, sin texto adicional.
- Un lugar es gastronómico si su función principal es vender comida o bebidas para consumir.
- NO son gastronómicos: balnearios, tiendas de ropa, farmacias, estaciones de servicio (a menos que sea su minimarket), supermercados, etc.

Lugares a analizar:
{items}

Responde con este formato JSON exacto:
[
  {{"indice": 1, "es_gastronomico": true, "razon": "Es un restaurante"}},
  {{"indice": 2, "es_gastronomico": false, "razon": "Es un balneario, no vende comida"}}
]"""

        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "Eres un asistente que clasifica lugares. Responde SOLO con JSON válido."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            texto = response.choices[0].message.content.strip()
            
            # Limpiar el texto si viene con markdown
            if texto.startswith("```"):
                texto = texto.split("```")[1]
                if texto.startswith("json"):
                    texto = texto[4:]
            
            validaciones = json.loads(texto)
            
            for v in validaciones:
                idx = v["indice"] - 1
                if 0 <= idx < len(batch):
                    lugar = batch[idx]
                    resultados[lugar["link"]] = {
                        "es_valido": v["es_gastronomico"],
                        "razon": v["razon"]
                    }
                    
        except Exception as e:
            logger.error(f"Error en validación LLM: {e}")
            # En caso de error, marcar como válidos para no perder datos
            for l in batch:
                if l["link"] not in resultados:
                    resultados[l["link"]] = {"es_valido": True, "razon": "Error en validación, se asume válido"}
        
        # Pausa entre batches para no exceder rate limits
        time.sleep(1)
    
    return resultados


def procesar_lugares():
    """
    Proceso principal: lee CSV, extrae categorías, valida con LLM, guarda resultados.
    """
    # Leer lugares del CSV
    lugares = []
    try:
        with open(ARCHIVO_ENTRADA, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            lugares = list(reader)
        logger.info(f"Leídos {len(lugares)} lugares del CSV.")
    except FileNotFoundError:
        logger.error(f"No se encontró el archivo {ARCHIVO_ENTRADA}")
        return
    
    if not lugares:
        logger.warning("No hay lugares para procesar.")
        return
    
    # Configurar navegador
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=es-AR")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # Etapa 1: Extraer categorías
        logger.info("=" * 50)
        logger.info("ETAPA 1: Extrayendo categorías de Google Maps")
        logger.info("=" * 50)
        
        lugares_con_categoria = []
        for i, lugar in enumerate(lugares, 1):
            logger.info(f"Procesando {i}/{len(lugares)}: {lugar['nombre'][:40]}...")
            
            categoria = extraer_categoria_de_lugar(driver, lugar["link"])
            
            lugares_con_categoria.append({
                **lugar,
                "categoria": categoria or "Sin categoría"
            })
            
            time.sleep(0.5)  # Pausa entre requests
        
    finally:
        driver.quit()
    
    # Etapa 2: Validar con LLM
    logger.info("=" * 50)
    logger.info("ETAPA 2: Validando con LLM")
    logger.info("=" * 50)
    
    validaciones = validar_con_llm(lugares_con_categoria)
    
    # Etapa 3: Separar y guardar resultados
    logger.info("=" * 50)
    logger.info("ETAPA 3: Guardando resultados")
    logger.info("=" * 50)
    
    validados = []
    rechazados = []
    timestamp = datetime.now().isoformat()
    
    for lugar in lugares_con_categoria:
        validacion = validaciones.get(lugar["link"], {"es_valido": True, "razon": "No validado"})
        
        registro = {
            "link": lugar["link"],
            "nombre": lugar["nombre"],
            "categoria": lugar["categoria"],
            "query_original": lugar.get("query", ""),
            "fecha_scraping": lugar.get("fecha_busqueda", ""),
            "fecha_validacion": timestamp,
        }
        
        if validacion["es_valido"]:
            validados.append(registro)
        else:
            registro["razon_rechazo"] = validacion["razon"]
            rechazados.append(registro)
    
    # Guardar validados
    with open(ARCHIVO_VALIDADOS, 'w', newline='', encoding='utf-8') as f:
        campos = ["link", "nombre", "categoria", "query_original", "fecha_scraping", "fecha_validacion"]
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(validados)
    
    # Guardar rechazados
    with open(ARCHIVO_RECHAZADOS, 'w', newline='', encoding='utf-8') as f:
        campos = ["link", "nombre", "categoria", "razon_rechazo", "query_original", "fecha_scraping", "fecha_validacion"]
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(rechazados)
    
    # Resumen
    logger.info("=" * 50)
    logger.info("RESUMEN DE VALIDACIÓN")
    logger.info("=" * 50)
    logger.info(f"Total procesados: {len(lugares)}")
    logger.info(f"Lugares VALIDADOS: {len(validados)}")
    logger.info(f"Lugares RECHAZADOS: {len(rechazados)}")
    logger.info(f"Archivo validados: {ARCHIVO_VALIDADOS}")
    logger.info(f"Archivo rechazados: {ARCHIVO_RECHAZADOS}")
    logger.info("=" * 50)


# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIO DEL PROCESO DE ENRIQUECIMIENTO Y VALIDACIÓN")
    logger.info(f"Fecha y hora: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    procesar_lugares()
    
    fin = datetime.now()
    logger.info(f"Duración total: {fin - inicio}")
