import pandas as pd
import logging
import json
import argparse
import os
from datetime import datetime
from db_utils import get_connection, log_validation_report, ensure_log_tables_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def validar_db(export_csv=True):
    """
    Compara la metadata en 'lugares' vs el conteo real en 'reviews'.
    Genera reporte en DB y CSV.
    """
    logger.info("Iniciando validación de inconsistencias en DB...")
    
    conn = get_connection()
    if not conn:
        logger.error("No hay conexión a DB")
        return

    try:
        ensure_log_tables_exists()
        
        cursor = conn.cursor()
        
        # Query: Comparar lo que dice el lugar que tiene vs lo que realmente hay en reviews
        # Asumimos join por nombre (restaurante)
        query = """
            SELECT 
                l.url,
                l.nombre,
                l.total_reviews_google as reportadas,
                COUNT(r.review_id) as reales
            FROM lugares l
            LEFT JOIN reviews r ON l.nombre = r.restaurante
            GROUP BY l.url, l.nombre, l.total_reviews_google
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        resultados = []
        for row in rows:
            url, nombre, reportadas, reales = row
            reportadas = reportadas or 0
            reales = reales or 0
            diferencia = reales - reportadas
            
            resultados.append({
                'nombre': nombre,
                'url': url,
                'reportadas': reportadas,
                'reales': reales,
                'diferencia': diferencia,
                'fecha': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
        df = pd.DataFrame(resultados)
        
        # Análisis
        total_reportadas = df['reportadas'].sum()
        total_reales = df['reales'].sum()
        
        # Discrepancias significativas (ej: faltan reviews)
        # Filtramos donde faltan al menos 5 reviews, o donde hay 0 reales pero se esperaban >0
        discrepantes = df[ (df['diferencia'] < -5) | ((df['reales'] == 0) & (df['reportadas'] > 0)) ]
        
        logger.info("=" * 60)
        logger.info(f"Total Lugares Evaluados: {len(df)}")
        logger.info(f"Total Reviews 'Reportadas' (metadata): {total_reportadas}")
        logger.info(f"Total Reviews 'Reales' (count rows):   {total_reales}")
        logger.info(f"Lugares con Discrepancias Criticas:    {len(discrepantes)}")
        logger.info("=" * 60)
        
        # Loguear Reporte en DB
        detalle = discrepantes[['nombre', 'reportadas', 'reales', 'url']].to_dict(orient='records')
        log_validation_report(
            total_reportadas=int(total_reportadas), 
            total_reales=int(total_reales), 
            discrepancias_count=len(discrepantes), 
            detalle_json=detalle
        )
        logger.info("✅ Reporte guardado en tabla validation_reports")

        # Generar resumen para Discord
        with open("discord_summary.txt", "w", encoding="utf-8") as f:
            f.write(f"🔍 **Validación Semanal**\n")
            f.write(f"Total Evaluados: {len(df)}\n")
            f.write(f"Discrepancias Críticas: {len(discrepantes)}\n")
            if not discrepantes.empty:
                f.write(f"Ejemplos: {', '.join(discrepantes['nombre'].head(3).tolist())}...")
            else:
                f.write("✅ Todo en orden.")

        # Exportar CSV (para que el script de reparación lo consuma si se desea)
        if export_csv:
            csv_path = "data/validacion_reviews.csv"
            # Asegurar directorio
            os.makedirs("data", exist_ok=True)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"📁 CSV de validación exportado a: {csv_path}")
            
    except Exception as e:
        logger.error(f"Error en validación DB: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    validar_db()
