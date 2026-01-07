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
    Retorna: (categoria, esta_cerrado)
    """
    for intento in range(max_intentos):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            
            # Esperar a que cargue la página
            time.sleep(3)
            
            # Verificar si está cerrado permanentemente
            esta_cerrado = False
            try:
                page_text = driver.page_source.lower()
                if "permanently closed" in page_text or "cerrado permanentemente" in page_text:
                    esta_cerrado = True
                    logger.info(f"   ⚠️ Lugar cerrado permanentemente detectado")
            except:
                pass
            
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
                        return categoria, esta_cerrado
                except:
                    continue
            
            # Backup: buscar en spans con clase específica
            try:
                spans = driver.find_elements(By.CSS_SELECTOR, "span.DkEaL, span.mgr77e")
                for span in spans:
                    texto = span.text.strip()
                    if texto and len(texto) < 50:
                        return texto, esta_cerrado
            except:
                pass
            
            logger.warning(f"No se encontró categoría para: {url}")
            return None, esta_cerrado
            
        except Exception as e:
            logger.error(f"Error al extraer categoría (intento {intento + 1}): {e}")
            if intento < max_intentos - 1:
                time.sleep(2)
    
    return None, False

def validar_categorias_con_llm(categorias_unicas):
    """
    Valida las categorías ÚNICAS con el LLM.
    Retorna un dict {categoria: {"es_valido": bool, "razon": str}}
    
    Esto es MUCHO más eficiente: en lugar de 1200 llamadas, hacemos 1 sola.
    """
    if not client:
        logger.warning("LLM no disponible, marcando todas las categorías como válidas.")
        return {cat: {"es_valido": True, "razon": "Sin validación LLM"} for cat in categorias_unicas}
    
    # Filtrar categorías vacías o None
    categorias = [c for c in categorias_unicas if c and c != "Sin categoría"]
    
    if not categorias:
        logger.warning("No hay categorías para validar")
        return {}
    
    logger.info(f"Validando {len(categorias)} categorías únicas con LLM...")
    
    # Construir lista de categorías
    items = "\n".join([f'{i+1}. "{cat}"' for i, cat in enumerate(categorias)])
    
    prompt = f"""Analiza cada CATEGORÍA de Google Maps y determina si corresponde a un establecimiento GASTRONÓMICO.

IMPORTANTE: 
- Responde SOLO con un JSON válido, sin texto adicional.
- Es gastronómico si su función principal es vender comida o bebidas para consumir.
- SÍ son gastronómicos: Restaurante, Bar, Cafetería, Heladería, Pizzería, Parrilla, 
  Hamburguesería, Pub, Cervecería, Pastelería, Rotisería, Sushi, Delivery de comida, etc.
- NO son gastronómicos: Balneario, Tienda de ropa, Farmacia, Estación de servicio, 
  Supermercado, Hotel, Agencia de viajes, Gimnasio, Peluquería, Spa, etc.

Categorías a analizar:
{items}

Responde con este formato JSON exacto:
[
  {{"indice": 1, "categoria": "Restaurante", "es_gastronomico": true, "razon": "Vende comida"}},
  {{"indice": 2, "categoria": "Balneario", "es_gastronomico": false, "razon": "Es un lugar de recreación, no vende comida"}}
]"""

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "Eres un asistente que clasifica categorías de establecimientos. Responde SOLO con JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        texto = response.choices[0].message.content.strip()
        
        # Limpiar markdown si viene
        if texto.startswith("```"):
            texto = texto.split("```")[1]
            if texto.startswith("json"):
                texto = texto[4:]
        
        validaciones = json.loads(texto)
        
        # Construir dict de resultados
        resultados = {}
        for v in validaciones:
            idx = v["indice"] - 1
            if 0 <= idx < len(categorias):
                categoria = categorias[idx]
                resultados[categoria] = {
                    "es_valido": v["es_gastronomico"],
                    "razon": v["razon"]
                }
        
        logger.info(f"   ✓ {sum(1 for r in resultados.values() if r['es_valido'])} categorías gastronómicas")
        logger.info(f"   ✗ {sum(1 for r in resultados.values() if not r['es_valido'])} categorías no gastronómicas")
        
        return resultados
                
    except Exception as e:
        logger.error(f"Error en validación LLM: {e}")
        # En caso de error, marcar todas como válidas para no perder datos
        return {cat: {"es_valido": True, "razon": "Error LLM - marcado como válido"} for cat in categorias}




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
    
    # Configurar navegador (compatible con GitHub Actions)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")  # Nuevo modo headless
    options.add_argument("--no-sandbox")  # Requerido en GitHub Actions
    options.add_argument("--disable-dev-shm-usage")  # Evita errores de memoria compartida
    options.add_argument("--disable-gpu")  # Requerido en algunos entornos Linux
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
        lugares_cerrados = []
        
        for i, lugar in enumerate(lugares, 1):
            logger.info(f"Procesando {i}/{len(lugares)}: {lugar['nombre'][:40]}...")
            
            categoria, esta_cerrado = extraer_categoria_de_lugar(driver, lugar["link"])
            
            if esta_cerrado:
                # Lugar cerrado permanentemente - rechazar directamente
                lugares_cerrados.append({
                    **lugar,
                    "categoria": categoria or "Desconocida",
                    "razon_rechazo": "Lugar cerrado permanentemente"
                })
                logger.info(f"   ❌ Rechazado: Cerrado permanentemente")
            else:
                lugares_con_categoria.append({
                    **lugar,
                    "categoria": categoria or "Sin categoría"
                })
            
            time.sleep(0.5)  # Pausa entre requests

        
    finally:
        driver.quit()
    
    # Etapa 2: Validar CATEGORÍAS ÚNICAS con LLM (¡mucho más eficiente!)
    logger.info("=" * 50)
    logger.info("ETAPA 2: Validando CATEGORÍAS ÚNICAS con LLM")
    logger.info("=" * 50)
    
    # Extraer categorías únicas
    categorias_unicas = set(l["categoria"] for l in lugares_con_categoria)
    logger.info(f"Categorías únicas encontradas: {len(categorias_unicas)}")
    
    # Validar categorías (1 sola llamada al LLM)
    validaciones_categorias = validar_categorias_con_llm(categorias_unicas)
    
    # Etapa 3: Separar y guardar resultados
    logger.info("=" * 50)
    logger.info("ETAPA 3: Guardando resultados")
    logger.info("=" * 50)
    
    validados = []
    rechazados = []
    timestamp = datetime.now().isoformat()
    
    # Agregar lugares cerrados a rechazados
    for lugar_cerrado in lugares_cerrados:
        registro = {
            "link": lugar_cerrado["link"],
            "nombre": lugar_cerrado["nombre"],
            "categoria": lugar_cerrado["categoria"],
            "razon_rechazo": lugar_cerrado["razon_rechazo"],
            "query_original": lugar_cerrado.get("query", ""),
            "fecha_scraping": lugar_cerrado.get("fecha_busqueda", ""),
            "fecha_validacion": timestamp,
        }
        rechazados.append(registro)
    
    # Aplicar validación de categoría a cada lugar
    for lugar in lugares_con_categoria:
        categoria = lugar["categoria"]
        # Buscar validación de esta categoría
        validacion = validaciones_categorias.get(categoria, {"es_valido": True, "razon": "Categoría no validada"})

        
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
    logger.info(f"Cerrados permanentemente: {len(lugares_cerrados)}")
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
