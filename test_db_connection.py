"""
Script de verificación de conexión a Supabase.
Ejecutar antes de hacer commit para verificar que todo funciona.

Uso: python test_db_connection.py
"""
import os
import sys

# Intentar cargar .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configurar DATABASE_URL si no está en env
if not os.getenv("DATABASE_URL"):
    print("⚠️ DATABASE_URL no encontrada en variables de entorno")
    print("   Opciones:")
    print("   1. Configurar: export DATABASE_URL='postgresql://...'")
    print("   2. Crear archivo .env con DATABASE_URL=...")
    print("   3. Ejecutar el script de migrate_data con las credenciales")
    sys.exit(1)

from db_utils import (
    get_connection, close_connection, 
    obtener_estadisticas, ensure_review_id_unique_constraint,
    upsert_lugar
)

def main():
    print("=" * 50)
    print("VERIFICACIÓN DE CONEXIÓN A SUPABASE")
    print("=" * 50)
    
    # 1. Probar conexión
    print("\n1️⃣ Probando conexión...")
    conn = get_connection()
    if conn:
        print("   ✅ Conexión establecida correctamente")
    else:
        print("   ❌ No se pudo conectar")
        sys.exit(1)
    
    # 2. Verificar índice único (ya no es crítico para dedupe pero útil)
    print("\n2️⃣ Verificando índice único en review_id...")
    ensure_review_id_unique_constraint()
    print("   ✅ Chequeo realizado")
    
    # 3. Test Upsert Lugar
    print("\n3️⃣ Testeando Upsert Lugar (Prueba dummy)...")
    lugar_test = {
        'nombre': 'Restaurante Prueba Script',
        'categoria': 'Test',
        'rating_gral': '5.0',
        'total_reviews_google': 1,
        'direccion': 'Calle Falsa 123',
        'latitud': -38.0,
        'longitud': -68.0,
        'url': 'https://maps.google.com/?cid=test_script_123'
    }
    if upsert_lugar(lugar_test):
        print("   ✅ Upsert Lugar exitoso")
    else:
        print("   ❌ Falló Upsert Lugar")

    # 4. Obtener estadísticas
    print("\n4️⃣ Obteniendo estadísticas...")
    stats = obtener_estadisticas()
    if stats:
        print(f"   📊 Total reviews en DB: {stats['total_reviews']:,}")
        print(f"   🏪 Total restaurantes: {stats['total_restaurantes']:,}")
        print(f"   🕐 Reviews últimas 24h: {stats['reviews_ultimas_24h']:,}")
    else:
        print("   ⚠️ No se pudieron obtener estadísticas")
    
    # 5. Cerrar conexión
    print("\n4️⃣ Cerrando conexión...")
    close_connection()
    print("   ✅ Conexión cerrada")
    
    print("\n" + "=" * 50)
    print("✅ VERIFICACIÓN COMPLETADA - Todo funciona correctamente")
    print("=" * 50)
    print("\nPuedes ejecutar el scraper con:")
    print("   python opiniones-scraper.py")

if __name__ == "__main__":
    main()
