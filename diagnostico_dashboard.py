"""
Script de diagnóstico para las tablas del dashboard.
Verifica el estado de scraping_logs y review_history.

Uso: python diagnostico_dashboard.py
"""
import os
import sys
from datetime import datetime

# Cargar credenciales desde mis_claves.env
def load_env_file(filepath):
    """Carga variables de entorno desde un archivo."""
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")
        print(f"✅ Credenciales cargadas desde {filepath}")
        return True
    except FileNotFoundError:
        print(f"❌ No se encontró {filepath}")
        return False

# Cargar mis_claves.env primero
if not load_env_file("mis_claves.env"):
    print("   Nota: Asegúrate de tener el archivo mis_claves.env con DATABASE_URL")
    sys.exit(1)

if not os.getenv("DATABASE_URL"):
    print("❌ DATABASE_URL no encontrada en mis_claves.env")
    sys.exit(1)

from db_utils import get_connection, close_connection

def diagnostico_scraping_logs():
    """Analiza la tabla scraping_logs."""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Estadísticas generales
        cursor.execute("SELECT COUNT(*) FROM scraping_logs")
        total = cursor.fetchone()[0]
        
        # Fechas extremas
        cursor.execute("SELECT MIN(fecha), MAX(fecha) FROM scraping_logs")
        row = cursor.fetchone()
        fecha_min, fecha_max = row
        
        # Registros por estado
        cursor.execute("""
            SELECT estado, COUNT(*) 
            FROM scraping_logs 
            GROUP BY estado 
            ORDER BY COUNT(*) DESC
        """)
        por_estado = cursor.fetchall()
        
        # Últimos 5 registros
        cursor.execute("""
            SELECT fecha, url, estado, nuevas_reviews 
            FROM scraping_logs 
            ORDER BY fecha DESC 
            LIMIT 5
        """)
        ultimos = cursor.fetchall()
        
        # Registros de los últimos 7 días
        cursor.execute("""
            SELECT DATE(fecha), COUNT(*), SUM(nuevas_reviews) 
            FROM scraping_logs 
            WHERE fecha > NOW() - INTERVAL '7 days'
            GROUP BY DATE(fecha) 
            ORDER BY DATE(fecha) DESC
        """)
        ultimos_7_dias = cursor.fetchall()
        
        cursor.close()
        
        return {
            'total': total,
            'fecha_min': fecha_min,
            'fecha_max': fecha_max,
            'por_estado': por_estado,
            'ultimos': ultimos,
            'ultimos_7_dias': ultimos_7_dias
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def diagnostico_review_history():
    """Analiza la tabla review_history."""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Verificar si existe la tabla
        cursor.execute("SELECT to_regclass('review_history');")
        exists = cursor.fetchone()[0] is not None
        
        if not exists:
            return {'existe': False}
        
        # Estadísticas generales
        cursor.execute("SELECT COUNT(*) FROM review_history")
        total = cursor.fetchone()[0]
        
        # Fechas extremas
        cursor.execute("SELECT MIN(recorded_at), MAX(recorded_at) FROM review_history")
        row = cursor.fetchone()
        fecha_min, fecha_max = row
        
        # Últimos 5 registros
        cursor.execute("""
            SELECT recorded_at, nombre, review_count, delta_since_last 
            FROM review_history 
            ORDER BY recorded_at DESC 
            LIMIT 5
        """)
        ultimos = cursor.fetchall()
        
        # Registros con delta positivo (nuevas reviews detectadas)
        cursor.execute("""
            SELECT COUNT(*) FROM review_history WHERE delta_since_last > 0
        """)
        con_delta = cursor.fetchone()[0]
        
        # Registros de los últimos 7 días
        cursor.execute("""
            SELECT DATE(recorded_at), COUNT(*), SUM(delta_since_last) 
            FROM review_history 
            WHERE recorded_at > NOW() - INTERVAL '7 days'
            GROUP BY DATE(recorded_at) 
            ORDER BY DATE(recorded_at) DESC
        """)
        ultimos_7_dias = cursor.fetchall()
        
        cursor.close()
        
        return {
            'existe': True,
            'total': total,
            'fecha_min': fecha_min,
            'fecha_max': fecha_max,
            'ultimos': ultimos,
            'con_delta': con_delta,
            'ultimos_7_dias': ultimos_7_dias
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def diagnostico_reviews():
    """Analiza la tabla reviews (fuente de 'Reseñas 24h')."""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Total de reviews
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total = cursor.fetchone()[0]
        
        # Reviews últimas 24h (lo que muestra el dashboard)
        # fecha_scraping es TEXT, hay que castear
        cursor.execute("""
            SELECT COUNT(*) FROM reviews 
            WHERE fecha_scraping::timestamp > NOW() - INTERVAL '24 hours'
        """)
        ultimas_24h = cursor.fetchone()[0]
        
        # Fechas extremas
        cursor.execute("SELECT MIN(fecha_scraping), MAX(fecha_scraping) FROM reviews")
        row = cursor.fetchone()
        fecha_min, fecha_max = row
        
        # Reviews por día (últimos 7 días)
        cursor.execute("""
            SELECT DATE(fecha_scraping::timestamp), COUNT(*) 
            FROM reviews 
            WHERE fecha_scraping::timestamp > NOW() - INTERVAL '7 days'
            GROUP BY DATE(fecha_scraping::timestamp) 
            ORDER BY DATE(fecha_scraping::timestamp) DESC
        """)
        por_dia = cursor.fetchall()
        
        cursor.close()
        
        return {
            'total': total,
            'ultimas_24h': ultimas_24h,
            'fecha_min': fecha_min,
            'fecha_max': fecha_max,
            'por_dia': por_dia
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def main():
    print("=" * 60)
    print("DIAGNÓSTICO DE TABLAS DEL DASHBOARD")
    print(f"Fecha actual: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 0. reviews (fuente del contador "Reseñas 24h")
    print("\n📋 TABLA: reviews (fuente de 'Reseñas 24h')")
    print("-" * 40)
    reviews = diagnostico_reviews()
    if reviews:
        print(f"   Total registros: {reviews['total']:,}")
        print(f"   🔵 Reviews últimas 24h: {reviews['ultimas_24h']}")
        print(f"   Fecha más antigua: {reviews['fecha_min']}")
        print(f"   Fecha más reciente: {reviews['fecha_max']}")
        
        print("\n   Reviews por día (últimos 7 días):")
        if reviews['por_dia']:
            for fecha, count in reviews['por_dia']:
                print(f"      {fecha}: {count:,} reviews")
        else:
            print("      ⚠️ SIN REVIEWS en los últimos 7 días")
    else:
        print("   ❌ No se pudo obtener información")
    
    # 1. scraping_logs
    print("\n📋 TABLA: scraping_logs (fuente del gráfico 'Timeline')")
    print("-" * 40)
    logs = diagnostico_scraping_logs()
    if logs:
        print(f"   Total registros: {logs['total']:,}")
        print(f"   Fecha más antigua: {logs['fecha_min']}")
        print(f"   Fecha más reciente: {logs['fecha_max']}")
        
        print("\n   Por estado:")
        for estado, count in logs['por_estado']:
            print(f"      {estado}: {count}")
        
        print("\n   Últimos 5 registros:")
        for fecha, url, estado, nuevas in logs['ultimos']:
            url_corta = url[:40] + "..." if url and len(url) > 40 else url
            print(f"      {fecha} | {estado} | nuevas:{nuevas}")
        
        print("\n   Últimos 7 días:")
        if logs['ultimos_7_dias']:
            for fecha, count, nuevas in logs['ultimos_7_dias']:
                print(f"      {fecha}: {count} registros, {nuevas or 0} nuevas reviews")
        else:
            print("      ⚠️ SIN REGISTROS en los últimos 7 días")
    else:
        print("   ❌ No se pudo obtener información")
    
    # 2. review_history
    print("\n📋 TABLA: review_history (fuente del 'Monitor')")
    print("-" * 40)
    history = diagnostico_review_history()
    if history:
        if not history.get('existe'):
            print("   ⚠️ La tabla NO EXISTE")
        else:
            print(f"   Total registros: {history['total']:,}")
            print(f"   Fecha más antigua: {history['fecha_min']}")
            print(f"   Fecha más reciente: {history['fecha_max']}")
            print(f"   Registros con delta > 0: {history['con_delta']}")
            
            if history['ultimos']:
                print("\n   Últimos 5 registros:")
                for fecha, nombre, count, delta in history['ultimos']:
                    nombre_corto = nombre[:30] + "..." if nombre and len(nombre) > 30 else nombre
                    print(f"      {fecha} | {nombre_corto} | reviews:{count} | delta:{delta}")
            
            print("\n   Últimos 7 días:")
            if history['ultimos_7_dias']:
                for fecha, count, delta_total in history['ultimos_7_dias']:
                    print(f"      {fecha}: {count} registros, delta total: {delta_total or 0}")
            else:
                print("      ⚠️ SIN REGISTROS en los últimos 7 días")
    else:
        print("   ❌ No se pudo obtener información")
    
    # Cerrar conexión
    close_connection()
    
    print("\n" + "=" * 60)
    print("DIAGNÓSTICO COMPLETADO")
    print("=" * 60)

if __name__ == "__main__":
    main()
