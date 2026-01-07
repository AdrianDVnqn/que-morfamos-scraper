import pandas as pd
import geopandas as gpd
import sys

# ==========================================
# CONFIGURACIÓN
# ==========================================
ARCHIVO_REVIEWS = 'reviews_neuquen.csv'

# ==========================================
# 1. CARGAR Y CORREGIR COORDENADAS (CRUCIAL)
# ==========================================

# Cargar el archivo de barrios desde la web
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

# ==========================================
# 2. CARGAR RESEÑAS DESDE CSV
# ==========================================
print(f"📂 Cargando {ARCHIVO_REVIEWS}...")
try:
    df_reviews = pd.read_csv(ARCHIVO_REVIEWS, low_memory=False)
except FileNotFoundError:
    print(f"   ❌ No se encontró {ARCHIVO_REVIEWS}")
    sys.exit(1)

# Convertir coordenadas a float (valores inválidos → NaN)
df_reviews['latitud'] = pd.to_numeric(df_reviews['latitud'], errors='coerce')
df_reviews['longitud'] = pd.to_numeric(df_reviews['longitud'], errors='coerce')

print(f"   Total de reseñas: {len(df_reviews)}")

# Verificar que existan las columnas de coordenadas
if 'latitud' not in df_reviews.columns or 'longitud' not in df_reviews.columns:
    print("❌ Error: El CSV no tiene columnas 'latitud' y 'longitud'")
    sys.exit(1)

# Filtrar filas con coordenadas válidas
df_validos = df_reviews[df_reviews['latitud'].notna() & df_reviews['longitud'].notna()].copy()
print(f"   Reseñas con coordenadas válidas: {len(df_validos)}")

if len(df_validos) == 0:
    print("   ⚠️ No hay reseñas con coordenadas válidas")
    sys.exit(0)

# Convertir a GeoDataFrame
gdf_reviews = gpd.GeoDataFrame(
    df_validos,
    geometry=gpd.points_from_xy(df_validos.longitud, df_validos.latitud),
    crs="EPSG:4326"
)

# ==========================================
# 3. SPATIAL JOIN (CRUCE ESPACIAL)
# ==========================================
print("\n🗺️  Realizando cruce espacial...")
resultado = gpd.sjoin(gdf_reviews, gdf_barrios, how="left", predicate="within")
print(f"   ✅ Cruce completado")

# ==========================================
# 4. AGREGAR DESCRIPCIONES (ZONAS)
# ==========================================
print("\n🏘️  Clasificando zonas...")

zonas_map = {
    # CENTRO
    'AREA CENTRO ESTE': 'Centro / Comercial',
    'AREA CENTRO OESTE': 'Centro / Comercial',
    'AREA CENTRO SUR': 'Bajo / Comercial Antiguo',
    'SANTA GENOVEVA': 'Residencial VIP / Cerca Centro',
    
    # RÍO / PASEO DE LA COSTA
    'RIO GRANDE': 'Paseo de la Costa / Turístico',
    'LIMAY': 'Ribereño / Balnearios',
    'CONFLUENCIA RURAL': 'Rural / Paseo Costero',
    'CONFLUENCIA URBANA': 'Residencial Denso / Sur',
    'VALENTINA SUR RURAL': 'Rural / Casas de Té',
    
    # NORTE / BARDAS
    'ALTA BARDA': 'Norte / Residencial',
    'RINCON DE EMILIO': 'Norte / Río Neuquén',
    'PARQUE INDUSTRIAL': 'Industrial',
    'CIUDAD INDUSTRIAL': 'Industrial',
    
    # OESTE (Alta densidad)
    'VILLA FLORENCIA': 'Oeste / Residencial',
    'VILLA CEFERINO': 'Oeste Profundo',
    'SAN LORENZO NORTE': 'Oeste Profundo',
    'SAN LORENZO SUR': 'Oeste Profundo',
    'GRAN NEUQUEN NORTE': 'Oeste / Alta Densidad',
    'GRAN NEUQUEN SUR': 'Oeste / Alta Densidad',
    'MELIPAL': 'Oeste / Residencial',
    'UNION DE MAYO': 'Oeste / Residencial',
    'GREGORIO ALVAREZ': 'Oeste / Bloques',
    'ISLAS MALVINAS': 'Oeste / Alta Densidad',
    'BOUQUET ROLDAN': 'Oeste / Cerca Centro'
}

# Aplicar el mapeo
resultado['zona'] = resultado['barrio_oficial'].map(zonas_map).fillna('Otras Zonas')

# Lógica extra: Cerca del Río
barrios_rio = ['RIO GRANDE', 'LIMAY', 'CONFLUENCIA RURAL', 'RINCON DE EMILIO', 'VALENTINA SUR RURAL']
resultado['cerca_rio'] = resultado['barrio_oficial'].isin(barrios_rio)

# ==========================================
# 5. INTEGRAR RESULTADOS AL DATASET ORIGINAL
# ==========================================
print("\n💾 Integrando barrios al dataset...")

# Agregar columnas al DataFrame original
df_reviews['barrio'] = None
df_reviews['zona'] = None
df_reviews['cerca_rio'] = False

# Mapear resultados por índice
for idx in resultado.index:
    df_reviews.loc[idx, 'barrio'] = resultado.loc[idx, 'barrio_oficial']
    df_reviews.loc[idx, 'zona'] = resultado.loc[idx, 'zona']
    df_reviews.loc[idx, 'cerca_rio'] = resultado.loc[idx, 'cerca_rio']

# Guardar dataset actualizado
df_reviews.to_csv(ARCHIVO_REVIEWS, index=False, encoding='utf-8')
print(f"   ✅ Archivo actualizado: {ARCHIVO_REVIEWS}")

# ==========================================
# 6. REPORTE FINAL
# ==========================================
print("\n" + "=" * 60)
print("✅ PROCESO COMPLETADO")
print("=" * 60)

barrios_asignados = df_reviews['barrio'].notna().sum()
sin_barrio = df_reviews['barrio'].isna().sum()

print(f"Reseñas con barrio asignado: {barrios_asignados}")
print(f"Reseñas sin barrio (fuera de polígonos): {sin_barrio}")

# Mostrar distribución por zona
print("\n📍 Distribución por zona:")
print(df_reviews['zona'].value_counts().head(10))
