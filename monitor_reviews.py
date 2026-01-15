"""
Monitor de Reviews - Actualización Diaria
Verifica cambios en el conteo de reseñas y scrapea las nuevas.
"""
import time
import logging
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db_utils import (
    get_lugares_para_monitoreo, 
    upsert_lugar, 
    get_ultimas_N_reviews_restaurante,
    insertar_reviews_batch,
    get_connection,
    close_connection,
    log_review_history,
    ensure_history_table_exists,
    log_scraping_event
)
from scraping_utils import (
    crear_driver,
    forzar_entrada_pestana_opiniones,
    ordenar_por_recientes,
    detectar_total_reviews,
    extraer_rating_page,
    extraer_reviews_de_pagina,
    scroll_para_cargar_reviews,
    extraer_coordenadas_url
)

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Límite de tiempo (5 horas para margen de seguridad)
MAX_RUNTIME_HOURS = 5
MAX_RUNTIME_SECONDS = MAX_RUNTIME_HOURS * 3600

# Máximo de reseñas a cargar por lugar (para limitar scroll)
MAX_REVIEWS_POR_LUGAR = 100


def procesar_lugar(driver, lugar, ultimas_reviews_db):
    """
    Procesa un lugar: verifica si hay nuevas reseñas y las scrapea si las hay.
    
    Returns:
        tuple: (nuevas_reviews, estado)
            nuevas_reviews: lista de dicts con las reviews nuevas
            estado: 'SIN_CAMBIOS', 'NUEVAS_REVIEWS', 'ERROR', 'SIN_PESTANA'
    """
    url = lugar['url']
    nombre = lugar['nombre']
    count_db = lugar.get('last_count', 0) or 0
    
    try:
        # Navegar a la URL
        url_es = url.replace('hl=en', 'hl=es')
        if 'hl=' not in url_es:
            url_es += ('&' if '?' in url_es else '?') + 'hl=es'
        
        driver.get(url_es)
        
        # Esperar carga completa (h1 + div.F7nice con el conteo)
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.F7nice")))
        except:
            # Si no carga, refrescar y reintentar
            driver.refresh()
            time.sleep(3)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.F7nice")))
            except:
                pass
        
        time.sleep(2)  # Espera adicional para carga completa
        
        # Extraer conteo actual de la página
        count_actual = detectar_total_reviews(driver)
        rating_actual = extraer_rating_page(driver)
        
        logger.info(f"   DB: {count_db} | Actual: {count_actual} | Rating: {rating_actual}")
        
        # Preparar datos para actualizar lugar
        lat, lon = extraer_coordenadas_url(url)
        update_data = {
            'url': url,
            'nombre': nombre,
            'total_reviews_google': count_actual if count_actual > 0 else count_db,
            'rating_gral': rating_actual,
            'categoria': None,
            'direccion': lugar.get('direccion'),
            'latitud': lat,
            'longitud': lon,
            'barrio': None,
            'zona': None,
            'cerca_rio': None
        }
        
        # Si el conteo es igual, no hay nada nuevo
        if count_actual > 0 and count_actual == count_db:
            upsert_lugar(update_data)  # Actualiza fecha_scraping
            return [], 'SIN_CAMBIOS'
        
        # Si hay diferencia (o no teníamos conteo), ir a scrapear
        if count_actual == 0:
            logger.warning("   ⚠️ No se pudo obtener conteo, intentando scrapear igual...")
        
        # Navegar a pestaña Opiniones
        if not forzar_entrada_pestana_opiniones(driver):
            driver.refresh()
            time.sleep(3)
            if not forzar_entrada_pestana_opiniones(driver):
                logger.warning("   ❌ No se encontró pestaña de opiniones")
                return [], 'SIN_PESTANA'
        
        # Ordenar por recientes
        ordenar_por_recientes(driver)
        
        # Calcular cuántas reseñas nuevas esperamos (máximo)
        diferencia = max(count_actual - count_db, 20)  # Al menos 20 para estar seguros
        target = min(diferencia + 10, MAX_REVIEWS_POR_LUGAR)  # Un poco más por seguridad
        
        logger.info(f"   📜 Cargando ~{target} reseñas (diferencia: {diferencia})...")
        
        # Scroll para cargar reseñas
        scroll_para_cargar_reviews(driver, target, tiempo_limite_segundos=60)
        
        # Metadata para las reviews
        metadata = {
            'nombre': nombre,
            'categoria': '',
            'rating_gral': rating_actual,
            'total_google': count_actual,
            'direccion': lugar.get('direccion'),
            'latitud': lat,
            'longitud': lon
        }
        
        # Extraer reviews con early-stop
        reviews, early_stopped = extraer_reviews_de_pagina(driver, url, metadata, ultimas_reviews_db)
        
        # Actualizar lugar en DB
        upsert_lugar(update_data)
        
        if reviews:
            logger.info(f"   ✅ {len(reviews)} reseñas nuevas encontradas" + 
                       (" (early-stop)" if early_stopped else ""))
            return reviews, 'NUEVAS_REVIEWS'
        else:
            logger.info("   = Sin reseñas nuevas")
            return [], 'SIN_CAMBIOS'
            
    except Exception as e:
        logger.error(f"   ❌ Error: {str(e)[:100]}")
        return [], 'ERROR'


def run_monitor():
    """Ejecuta el monitoreo diario de reviews"""
    logger.info("=" * 60)
    logger.info("INICIO DEL MONITOR DE REVIEWS")
    logger.info(f"Límite de tiempo: {MAX_RUNTIME_HOURS} horas")
    logger.info("=" * 60)
    
    # Verificar conexión a DB
    conn = get_connection()
    if not conn:
        logger.error("❌ No se pudo conectar a la base de datos")
        return
    logger.info("✅ Conexión a Supabase establecida")
    
    # Asegurar que existe la tabla review_history
    ensure_history_table_exists()
    logger.info("✅ Tabla review_history verificada")
    
    # Obtener todos los lugares (ordenados por fecha_scraping más antigua)
    lugares = get_lugares_para_monitoreo(limit=10000)
    if not lugares:
        logger.info("No hay lugares para monitorear.")
        return
    
    logger.info(f"📍 Lugares a monitorear: {len(lugares)}")
    logger.info("-" * 40)
    
    # Crear driver
    driver = None
    try:
        driver = crear_driver()
        logger.info("✅ Driver de Chrome creado")
    except Exception as e:
        logger.error(f"❌ No se pudo crear el driver: {e}")
        return
    
    # Contadores
    start_time = time.time()
    procesados = 0
    con_cambios = 0
    total_nuevas_reviews = 0
    errores = 0
    errores_consecutivos = 0
    timed_out = False
    
    try:
        for i, lugar in enumerate(lugares, 1):
            # Verificar tiempo
            elapsed = time.time() - start_time
            if elapsed >= MAX_RUNTIME_SECONDS:
                logger.warning(f"⏰ Tiempo límite alcanzado ({MAX_RUNTIME_HOURS}h)")
                timed_out = True
                break
            
            nombre = lugar['nombre']
            logger.info(f"[{i}/{len(lugares)}] {nombre[:50]}")
            
            # Obtener últimas 2 reviews para early-stop
            ultimas_reviews = get_ultimas_N_reviews_restaurante(nombre, n=2)
            
            try:
                # Procesar lugar
                reviews, estado = procesar_lugar(driver, lugar, ultimas_reviews)
                
                # Registrar en review_history para el dashboard Monitor
                # Esto se hace siempre que procesemos un lugar exitosamente
                count_actual = lugar.get('last_count', 0) or 0
                if reviews:
                    count_actual = count_actual + len(reviews)
                
                delta = log_review_history(
                    url=lugar['url'],
                    current_count=count_actual,
                    current_rating=None,  # Ya se actualizó en procesar_lugar
                    nombre=nombre,
                    direccion=lugar.get('direccion')
                )
                if delta and delta > 0:
                    logger.info(f"   📊 Historial: +{delta} reviews")
                
                if estado == 'NUEVAS_REVIEWS' and reviews:
                    # Insertar reviews en DB
                    insertadas, duplicadas = insertar_reviews_batch(reviews)
                    total_nuevas_reviews += insertadas
                    con_cambios += 1
                    logger.info(f"   💾 Guardadas: {insertadas} | Duplicadas: {duplicadas}")
                
                # INTEGRACIÓN DASHBOARD: Loguear en scraping_logs para visualización histórica
                estado_dash = "EXITO" 
                if estado == 'ERROR': estado_dash = "ERROR_TEMPORAL"
                elif estado == 'SIN_PESTANA': estado_dash = "SIN_OPINIONES"
                
                nuevas_count_dash = len(reviews) if reviews else 0
                
                log_scraping_event(
                    url=lugar['url'],
                    estado=estado_dash,
                    mensaje=f"Monitor: {estado} (+{nuevas_count_dash})",
                    reviews_detectadas=count_actual,
                    nuevas_reviews=nuevas_count_dash,
                    intentos=1
                )

                procesados += 1
                errores_consecutivos = 0
                
            except Exception as e:
                errores += 1
                errores_consecutivos += 1
                logger.error(f"   ❌ Error procesando: {str(e)[:80]}")
                
                # Si hay muchos errores seguidos, reiniciar driver
                if errores_consecutivos >= 3:
                    logger.warning("⚠️ 3 errores consecutivos, reiniciando driver...")
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = crear_driver()
                    errores_consecutivos = 0
            
            # Pausa entre lugares
            time.sleep(1.5)
            
            # Reinicio preventivo cada 50 lugares
            if i % 50 == 0:
                logger.info("♻️ Reinicio preventivo del driver...")
                try:
                    driver.quit()
                except:
                    pass
                driver = crear_driver()
                
    finally:
        # Cerrar driver
        try:
            driver.quit()
        except:
            pass
        
        # Cerrar conexión DB
        close_connection()
        
        # Resumen
        elapsed_mins = (time.time() - start_time) / 60
        logger.info("=" * 60)
        logger.info("RESUMEN DE EJECUCIÓN")
        logger.info("=" * 60)
        logger.info(f"Tiempo: {elapsed_mins:.1f} minutos")
        logger.info(f"Lugares procesados: {procesados}/{len(lugares)}")
        logger.info(f"Lugares con cambios: {con_cambios}")
        logger.info(f"Reseñas nuevas: {total_nuevas_reviews}")
        logger.info(f"Errores: {errores}")
        logger.info(f"Crecimiento total: +{total_nuevas_reviews}")
        logger.info("=" * 60)
        
        # Señal para el workflow de que hay que continuar
        if timed_out and procesados < len(lugares):
            logger.info("⚠️ Quedan lugares pendientes")
            print("CONTINUE_NEEDED")


if __name__ == "__main__":
    run_monitor()
