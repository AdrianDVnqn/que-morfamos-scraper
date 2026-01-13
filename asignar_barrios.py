import pandas as pd
import geopandas as gpd
import sys

# ==========================================
# CONFIGURACIÓN
# ==========================================
ARCHIVO_REVIEWS = 'reviews_neuquen.csv'

# Mapeo de Zonas (Definido globalmente para ser importable)
ZONAS_MAP = {
    # CENTRO
    'ÁREA CENTRO ESTE': 'Centro',
    'ÁREA CENTRO OESTE': 'Centro',
    'ÁREA CENTRO SUR': 'Centro',
    
    # ESTE
    'SANTA GENOVEVA': 'Este',
    'CONFLUENCIA URBANO': 'Este', 
    'MARIANO MORENO': 'Este',
    'VILLA FARRELL': 'Este',
    'SAPERE': 'Este',
    'PROVINCIAS UNIDAS': 'Este',
    'VILLA MARÍA': 'Este',
    'BELGRANO': 'Este',

    # RÍO / PASEO DE LA COSTA
    'RÍO GRANDE': 'Paseo de la Costa',
    'LIMAY': 'Paseo de la Costa',
    'ALTOS DEL LIMAY': 'Paseo de la Costa',
    'CONFLUENCIA RURAL': 'Paseo de la Costa', 
    
    # NORTE / EL ALTO
    'ALTA BARDA': 'Norte / Alto',
    'RINCÓN DE EMILIO': 'Norte / Alto',
    'PARQUE INDUSTRIAL': 'Norte / Alto', # Note: JSON has 'CIUDAD INDUSTRIAL OBISPO DON JAIME DE NEVARES'
    'CIUDAD INDUSTRIAL OBISPO DON JAIME DE NEVARES': 'Norte / Alto',
    '14 DE OCTUBRE y COPOL': 'Norte / Alto',
    'TERRAZAS DEL NEUQUÉN': 'Norte / Alto',
    'BARDAS SOLEADAS': 'Norte / Alto',
    
    # OESTE
    'VILLA FLORENCIA': 'Oeste',
    'VILLA CEFERINO': 'Oeste',
    'SAN LORENZO NORTE': 'Oeste',
    'SAN LORENZO SUR': 'Oeste',
    'GRAN NEUQUÉN NORTE': 'Oeste',
    'GRAN NEUQUÉN SUR': 'Oeste',
    'MELIPAL': 'Oeste',
    'UNIÓN DE MAYO': 'Oeste',
    'GREGORIO ÁLVAREZ': 'Oeste',
    'ISLAS MALVINAS': 'Oeste',
    'BOUQUET ROLDÁN': 'Oeste',
    'VALENTINA SUR RURAL': 'Oeste',
    'VALENTINA SUR URBANO': 'Oeste', 
    'VALENTINA NORTE URBANO': 'Oeste',
    'VALENTINA NORTE RURAL': 'Oeste',
    'ESFUERZO': 'Oeste',
    'HIBEPA': 'Oeste',
    'CUENCA XV': 'Oeste',
    'CANAL V': 'Oeste',
    'MILITAR': 'Oeste',
    'LA SIRENA': 'Oeste',
    'CUMELÉN': 'Oeste', 
    'EL PROGRESO': 'Oeste',
    'HUILICHES': 'Oeste',
    'DON BOSCO II': 'Oeste',
    'DON BOSCO III': 'Oeste',
    'NUEVO': 'Oeste'
}

BARRIOS_RIO = ['RÍO GRANDE', 'LIMAY', 'CONFLUENCIA RURAL', 'RINCÓN DE EMILIO', 'VALENTINA SUR RURAL']

def load_barrios():
    print("🌐 Descargando mapa de barrios de Neuquén...")
    try:
        gdf_barrios = gpd.read_file('https://www.estadisticaneuquen.gob.ar/apps/barrios/shapeBarrios.json') 
        print("   ✅ Mapa descargado") 
    except Exception as e:
        print(f"   ❌ Error descargando mapa: {e}")
        sys.exit(1)

    # ¡IMPORTANTE! Definimos que el archivo viene en "Web Mercator" (Metros)
    gdf_barrios.set_crs(epsg=3857, inplace=True, allow_override=True)
    # Lo convertimos a Latitud/Longitud (EPSG:4326) para que coincida con el GPS
    gdf_barrios = gdf_barrios.to_crs(epsg=4326)
    # Nos quedamos solo con lo útil y renombramos para claridad
    gdf_barrios = gdf_barrios[['NOMBRE', 'geometry']].rename(columns={'NOMBRE': 'barrio_oficial'})
    return gdf_barrios


def process_supabase(gdf_barrios):
    from db_utils import get_connection
    import psycopg2
    
    print("\n🐘 Conectando a Supabase...")
    conn = get_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos.")
        sys.exit(1)
        
    try:
        # Cargar Lugares
        query = "SELECT nombre, latitud, longitud FROM lugares WHERE latitud IS NOT NULL AND longitud IS NOT NULL"
        df_lugares = pd.read_sql(query, conn)
        print(f"   📊 Lugares cargados: {len(df_lugares)}")
        
        if df_lugares.empty:
            print("   ⚠️ No hay lugares con coordenadas para procesar.")
            return

        # Convertir a GeoDataFrame
        gdf_lugares = gpd.GeoDataFrame(
            df_lugares,
            geometry=gpd.points_from_xy(df_lugares.longitud, df_lugares.latitud),
            crs="EPSG:4326"
        )
        
        # Spatial Join
        print("🗺️  Realizando cruce espacial...")
        resultado = gpd.sjoin(gdf_lugares, gdf_barrios, how="left", predicate="within")
        
        # Asignar Zonas
        resultado['zona'] = resultado['barrio_oficial'].map(ZONAS_MAP).fillna('Otras Zonas')
        
        # Lógica extra: Cerca del Río (Promoted global constant)
        resultado['cerca_rio'] = resultado['barrio_oficial'].isin(BARRIOS_RIO)
        
        # Update en Batch
        print("💾 Actualizando base de datos...")
        cursor = conn.cursor()
        
        updates = 0
        for idx, row in resultado.iterrows():
            nombre = row['nombre']
            barrio = row['barrio_oficial'] if pd.notna(row['barrio_oficial']) else None
            zona = row['zona']
            cerca_rio = bool(row['cerca_rio'])
            
            sql = """
                UPDATE lugares 
                SET barrio = %s, zona = %s, cerca_rio = %s 
                WHERE nombre = %s
            """
            cursor.execute(sql, (barrio, zona, cerca_rio, nombre))
            updates += 1
            
        conn.commit()
        cursor.close()
        print(f"   ✅ {updates} lugares actualizados en Supabase.")
        
    except Exception as e:
        print(f"❌ Error procesando Supabase: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    # Ya no hay argumentos, siempre va a Supabase
    gdf_barrios = load_barrios()
    process_supabase(gdf_barrios)

if __name__ == "__main__":
    main()
