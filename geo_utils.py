import re
import logging
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

logger = logging.getLogger(__name__)

# Cache para el GeoDataFrame de barrios
_GDF_BARRIOS = None

ZONAS_MAP = {
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

BARRIOS_RIO = ['RIO GRANDE', 'LIMAY', 'CONFLUENCIA RURAL', 'RINCON DE EMILIO', 'VALENTINA SUR RURAL']

def extraer_coordenadas_url(url):
    """Extrae latitud y longitud de una URL de Google Maps"""
    if not url:
        return None, None
        
    # Patrón 1: @lat,lon,zoom
    patron = r'@(-?\d+\.\d+),(-?\d+\.\d+),\d+\.?\d*z?'
    match = re.search(patron, url)
    if match:
        return float(match.group(1)), float(match.group(2))
    
    # Patrón 2: @lat,lon (sin zoom)
    patron_alt = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
    match_alt = re.search(patron_alt, url)
    if match_alt:
        return float(match_alt.group(1)), float(match_alt.group(2))
    
    # Patrón 3: !3dlat!4dlon
    patron_data = r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
    match_data = re.search(patron_data, url)
    if match_data:
        return float(match_data.group(1)), float(match_data.group(2))
    
    return None, None

def _cargar_barrios():
    """Carga y cachea el GeoDataFrame de barrios."""
    global _GDF_BARRIOS
    if _GDF_BARRIOS is not None:
        return _GDF_BARRIOS
        
    url_geojson = 'https://www.estadisticaneuquen.gob.ar/apps/barrios/shapeBarrios.json'
    try:
        logger.info("🌐 Descargando mapa de barrios de Neuquén...")
        gdf = gpd.read_file(url_geojson)
        
        # Corrección de CRS: Viene en Web Mercator (3857), pasar a Lat/Lon (4326)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
             # Asumimos que si no es 4326 es 3857 como dice el script original
             # Si no tiene CRS definido, forzamos 3857 primero
            if gdf.crs is None:
                gdf.set_crs(epsg=3857, inplace=True)
            gdf = gdf.to_crs(epsg=4326)
            
        _GDF_BARRIOS = gdf[['NOMBRE', 'geometry']].rename(columns={'NOMBRE': 'barrio_oficial'})
        logger.info("   ✅ Mapa cargado y procesado.")
        return _GDF_BARRIOS
    except Exception as e:
        logger.error(f"   ❌ Error cargando mapa de barrios: {e}")
        return None

def asignar_barrio(lat, lon):
    """
    Dado lat, lon, determina el barrio, zona y si está cerca del río.
    Retorna un diccionario con las claves: barrio, zona, cerca_rio.
    """
    resultado = {
        'barrio': None,
        'zona': 'Otras Zonas',
        'cerca_rio': False
    }
    
    if lat is None or lon is None:
        return resultado
        
    gdf_barrios = _cargar_barrios()
    if gdf_barrios is None:
        return resultado
        
    try:
        punto = Point(lon, lat)  # Ojo: Point es (x, y) = (lon, lat)
        
        # Buscar en qué polígono cae el punto
        # Usamos check simple iterando si son pocos, o sjoin si fuera masivo.
        # Al ser un solo punto a la vez, iterar es rápido y evita crear GDF del punto.
        
        barrio_encontrado = None
        for idx, row in gdf_barrios.iterrows():
            if row.geometry.contains(punto):
                barrio_encontrado = row['barrio_oficial']
                break
        
        if barrio_encontrado:
            resultado['barrio'] = barrio_encontrado
            resultado['zona'] = ZONAS_MAP.get(barrio_encontrado, 'Otras Zonas')
            resultado['cerca_rio'] = barrio_encontrado in BARRIOS_RIO
            
    except Exception as e:
        logger.warning(f"Error asignando barrio para ({lat}, {lon}): {e}")
        
    return resultado
