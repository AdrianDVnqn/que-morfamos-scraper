import os
import json
import csv
import datetime
import requests
import logging

# ==========================================
# CONFIGURACIÓN
# ==========================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Archivos a analizar
ARCHIVO_LUGARES = "lugares_encontrados.csv"
ARCHIVO_VALIDADOS = "lugares_validados.csv"
ARCHIVO_RECHAZADOS = "lugares_rechazados.csv"
ARCHIVO_REVIEWS = "reviews_neuquen.csv"
ARCHIVO_ESTADO_REVIEWS = "estado_reviews.csv"
ARCHIVO_RESUMEN = "resumen_run.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def contar_lineas_csv(archivo):
    """Cuenta las líneas de un CSV (excluyendo header)"""
    if not os.path.exists(archivo):
        return 0
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return sum(1 for _ in reader)
    except:
        return 0


def obtener_reviews_por_lugar():
    """Obtiene conteo de reseñas por lugar"""
    reviews_por_lugar = {}
    if not os.path.exists(ARCHIVO_REVIEWS):
        return reviews_por_lugar
    
    try:
        with open(ARCHIVO_REVIEWS, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                nombre = row.get('restaurante', 'Desconocido')
                reviews_por_lugar[nombre] = reviews_por_lugar.get(nombre, 0) + 1
    except:
        pass
    
    return reviews_por_lugar


def obtener_estado_reviews():
    """Obtiene estadísticas del estado de procesamiento de reviews"""
    estados = {'EXITO': 0, 'SIN_OPINIONES': 0, 'ERROR_TEMPORAL': 0, 'PENDIENTE': 0}
    
    if not os.path.exists(ARCHIVO_ESTADO_REVIEWS):
        return estados
    
    try:
        with open(ARCHIVO_ESTADO_REVIEWS, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                estado = row.get('estado', 'PENDIENTE')
                estados[estado] = estados.get(estado, 0) + 1
    except:
        pass
    
    return estados


def generar_resumen():
    """Genera el resumen completo del run"""
    ahora = datetime.datetime.now()
    
    # Conteos básicos
    lugares_encontrados = contar_lineas_csv(ARCHIVO_LUGARES)
    lugares_validados = contar_lineas_csv(ARCHIVO_VALIDADOS)
    lugares_rechazados = contar_lineas_csv(ARCHIVO_RECHAZADOS)
    total_reviews = contar_lineas_csv(ARCHIVO_REVIEWS)
    
    # Estado de reviews
    estado_reviews = obtener_estado_reviews()
    
    # Reviews por lugar
    reviews_por_lugar = obtener_reviews_por_lugar()
    
    resumen = {
        "fecha_ejecucion": ahora.isoformat(),
        "fecha_legible": ahora.strftime("%d/%m/%Y %H:%M"),
        "lugares": {
            "encontrados": lugares_encontrados,
            "validados": lugares_validados,
            "rechazados_llm": lugares_rechazados,
            "tasa_aprobacion": f"{(lugares_validados/(lugares_encontrados or 1))*100:.1f}%"
        },
        "reviews": {
            "total": total_reviews,
            "lugares_con_exito": estado_reviews.get('EXITO', 0),
            "lugares_sin_opiniones": estado_reviews.get('SIN_OPINIONES', 0),
            "lugares_con_error": estado_reviews.get('ERROR_TEMPORAL', 0)
        },
        "reviews_por_lugar": reviews_por_lugar,
        "top_10_lugares": dict(sorted(reviews_por_lugar.items(), key=lambda x: x[1], reverse=True)[:10])
    }
    
    # Guardar resumen detallado
    with open(ARCHIVO_RESUMEN, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Resumen guardado en {ARCHIVO_RESUMEN}")
    return resumen


def enviar_discord(mensaje, color=0x00ff00):
    """Envía mensaje a Discord via webhook"""
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL no configurado")
        return False
    
    payload = {
        "embeds": [{
            "description": mensaje,
            "color": color,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }]
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            logger.info("✓ Mensaje enviado a Discord")
            return True
        else:
            logger.error(f"Error Discord: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error enviando a Discord: {e}")
        return False


def enviar_mensaje_lugares(resumen):
    """Envía mensaje de resumen de LUGARES"""
    lugares = resumen['lugares']
    
    mensaje = f"""**🤖 QUE MORFAMOS - Resumen de Lugares**
📅 {resumen['fecha_legible']}

📍 **Lugares encontrados:** {lugares['encontrados']}
✅ **Validados:** {lugares['validados']}
❌ **Rechazados por LLM:** {lugares['rechazados_llm']}
📊 **Tasa de aprobación:** {lugares['tasa_aprobacion']}
"""
    
    return enviar_discord(mensaje, color=0x3498db)  # Azul


def enviar_mensaje_reviews(resumen):
    """Envía mensaje de resumen de RESEÑAS"""
    reviews = resumen['reviews']
    
    mensaje = f"""**⭐ QUE MORFAMOS - Resumen de Reseñas**
📅 {resumen['fecha_legible']}

📝 **Total en dataset:** {reviews['total']}
✅ **Lugares procesados:** {reviews['lugares_con_exito']}
🚫 **Sin opiniones:** {reviews['lugares_sin_opiniones']}
⚠️ **Con errores:** {reviews['lugares_con_error']}
"""
    
    # Agregar top 5 lugares
    if resumen.get('top_10_lugares'):
        mensaje += "\n🏆 **Top 5 por reseñas:**\n"
        for i, (nombre, count) in enumerate(list(resumen['top_10_lugares'].items())[:5], 1):
            mensaje += f"> {i}. {nombre[:30]}: **{count}**\n"
    
    return enviar_discord(mensaje, color=0xf1c40f)  # Amarillo


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    logger.info("=== Generando resumen y notificación ===")
    
    # Generar resumen
    resumen = generar_resumen()
    
    # Enviar mensaje de LUGARES
    logger.info("Enviando resumen de lugares...")
    enviar_mensaje_lugares(resumen)
    
    # Pequeña pausa entre mensajes
    import time
    time.sleep(1)
    
    # Enviar mensaje de RESEÑAS
    logger.info("Enviando resumen de reseñas...")
    enviar_mensaje_reviews(resumen)
    
    # Imprimir resumen en consola
    print("\n" + "="*50)
    print("RESUMEN DEL RUN")
    print("="*50)
    print(f"Lugares encontrados: {resumen['lugares']['encontrados']}")
    print(f"Lugares validados: {resumen['lugares']['validados']}")
    print(f"Lugares rechazados: {resumen['lugares']['rechazados_llm']}")
    print(f"Total reseñas: {resumen['reviews']['total']}")
    print("="*50)
