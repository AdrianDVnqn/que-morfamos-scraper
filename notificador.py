import os
import json
import csv
import datetime
import requests
import logging
import argparse
import re
import sys

# ==========================================
# CONFIGURACIÓN
# ==========================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Archivos a analizar (por defecto)
ARCHIVO_LUGARES = "lugares_encontrados.csv"
ARCHIVO_VALIDADOS = "lugares_validados.csv"
ARCHIVO_RECHAZADOS = "lugares_rechazados.csv"
ARCHIVO_REVIEWS = "reviews_neuquen.csv"
ARCHIVO_ESTADO_REVIEWS = "estado_reviews.csv"
ARCHIVO_RESUMEN = "resumen_run.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
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


def generar_resumen_lugares():
    """Genera el resumen completo del run de lugares"""
    ahora = datetime.datetime.now()
    
    # Conteos básicos
    lugares_encontrados = contar_lineas_csv(ARCHIVO_LUGARES)
    lugares_validados = contar_lineas_csv(ARCHIVO_VALIDADOS)
    lugares_rechazados = contar_lineas_csv(ARCHIVO_RECHAZADOS)
    
    resumen = {
        "fecha_ejecucion": ahora.isoformat(),
        "fecha_legible": ahora.strftime("%d/%m/%Y %H:%M"),
        "lugares": {
            "encontrados": lugares_encontrados,
            "validados": lugares_validados,
            "rechazados_llm": lugares_rechazados,
            "tasa_aprobacion": f"{(lugares_validados/(lugares_encontrados or 1))*100:.1f}%"
        }
    }
    return resumen


def enviar_discord(mensaje, color=0x00ff00, dry_run=False):
    """Envía mensaje a Discord via webhook"""
    payload = {
        "embeds": [{
            "description": mensaje,
            "color": color,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }]
    }
    
    if dry_run:
        logger.info(f"[DRY-RUN] Enviando a Discord:\n{json.dumps(payload, indent=2)}")
        return True

    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL no configurado")
        return False
    
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


def procesar_modo_lugares(args):
    """Logica para resumen de scraping de lugares"""
    logger.info("Generando resumen de LUGARES...")
    resumen = generar_resumen_lugares()
    lugares = resumen['lugares']
    
    mensaje = f"""**🤖 QUE MORFAMOS - Resumen de Lugares**
📅 {resumen['fecha_legible']}

📍 **Lugares encontrados:** {lugares['encontrados']}
✅ **Validados:** {lugares['validados']}
❌ **Rechazados por LLM:** {lugares['rechazados_llm']}
📊 **Tasa de aprobación:** {lugares['tasa_aprobacion']}
"""
    enviar_discord(mensaje, color=0x3498db, dry_run=args.dry_run)


def procesar_modo_monitor(args):
    """Logica para resumen de monitor de reviews (parseando log)"""
    file_path = args.file or 'run.log'
    if not os.path.exists(file_path):
        logger.error(f"No se encontró el archivo de log: {file_path}")
        return

    logger.info(f"Analizando log de MONITOR: {file_path}")
    
    # Valores por defecto
    stats = {
        'total_lugares': '?',
        'procesados': '?',
        'con_cambios': '?',
        'nuevas_reviews': '?',
        'tiempo': '?'
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            log_content = f.read()
            
            # Regex patterns
            patterns = {
                'total_lugares': r'Lugares a monitorear: (\d+)',
                'procesados': r'Lugares procesados: (\d+)/(\d+)', # Captura el primero pero muestra "X/Y"
                'con_cambios': r'Lugares con cambios: (\d+)',
                'nuevas_reviews': r'Reseñas nuevas: (\d+)',
                'tiempo': r'Tiempo: ([\d.]+) minutos'
            }
            
            for key, pattern in patterns.items():
                m = re.search(pattern, log_content)
                if m:
                    if key == 'procesados':
                        stats[key] = f"{m.group(1)}/{m.group(2)}"
                    else:
                        stats[key] = m.group(1)
        
        msg = f"""📊 **Monitoreo Diario de Reviews**
📅 {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}

📍 **Lugares totales:** {stats['total_lugares']}
✅ **Procesados:** {stats['procesados']}
🔄 **Con cambios:** {stats['con_cambios']}
⭐ **Reseñas nuevas:** {stats['nuevas_reviews']}
⏱️ **Tiempo:** {stats['tiempo']} min

[Ver log completo en GitHub Actions]"""

        color = 0x2ecc71 if stats['nuevas_reviews'] != '0' and stats['nuevas_reviews'] != '?' else 0x3498db
        enviar_discord(msg, color=color, dry_run=args.dry_run)

    except Exception as e:
        logger.error(f"Error parseando log monitoreo: {e}")


def procesar_modo_validacion(args):
    """Logica para enviar reporte de validacion (txt)"""
    file_path = args.file or 'discord_summary.txt'
    
    content = 'Proceso finalizado.'
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            logger.warning(f"No se pudo leer {file_path}: {e}")
    else:
        logger.warning(f"Archivo {file_path} no existe, enviando mensaje default.")

    msg = f"""{content}
📅 {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}
"""
    enviar_discord(msg, color=0xe74c3c, dry_run=args.dry_run)


def procesar_modo_generico(args):
    """Envía un mensaje genérico pasado por argumento"""
    if not args.message:
        logger.error("Debe especificar --message para modo genérico")
        return
    
    msg = f"""{args.title or '📢 Notificación'}
📅 {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}

{args.message}
"""
    color_map = {'info': 0x3498db, 'success': 0x2ecc71, 'error': 0xe74c3c, 'warning': 0xf1c40f}
    enviar_discord(msg, color=color_map.get(args.type, 0x3498db), dry_run=args.dry_run)


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Notificador Discord para Que Morfamos Scraper')
    
    parser.add_argument('--mode', choices=['lugares', 'monitor_reviews', 'validacion', 'generic'], 
                        required=True, help='Modo de operación')
    parser.add_argument('--file', help='Archivo de entrada (opcional, depende del modo)')
    parser.add_argument('--dry-run', action='store_true', help='Ejecutar sin enviar a Discord')
    parser.add_argument('--message', help='Mensaje para modo genérico')
    parser.add_argument('--title', help='Título para modo genérico')
    parser.add_argument('--type', choices=['info', 'success', 'error', 'warning'], default='info', help='Tipo de mensaje genérico')

    args = parser.parse_args()

    try:
        if args.mode == 'lugares':
            procesar_modo_lugares(args)
        elif args.mode == 'monitor_reviews':
            procesar_modo_monitor(args)
        elif args.mode == 'validacion':
            procesar_modo_validacion(args)
        elif args.mode == 'generic':
            procesar_modo_generico(args)
            
    except Exception as e:
        logger.error(f"Fallo global en notificador: {e}")
        sys.exit(1)

