import geopandas as gpd
import folium
import sys
from asignar_barrios import ZONAS_MAP

# ==========================================
# CONFIGURACIÓN
# ==========================================
URL_BARRIOS = 'https://www.estadisticaneuquen.gob.ar/apps/barrios/shapeBarrios.json'
MAPA_SALIDA = 'mapa_zonas_neuquen.html'

def main():
    print("🌐 Descargando mapa de barrios de Neuquén...")
    try:
        gdf = gpd.read_file(URL_BARRIOS)
        print("   ✅ Mapa descargado")
    except Exception as e:
        print(f"   ❌ Error descargando mapa: {e}")
        sys.exit(1)

    # Normalización de Proyección y Nombres
    gdf.set_crs(epsg=3857, inplace=True, allow_override=True)
    gdf = gdf.to_crs(epsg=4326)
    gdf = gdf[['NOMBRE', 'geometry']].rename(columns={'NOMBRE': 'barrio'})

    # Asignar Zonas usando el mapa importado
    print("🎨 Asignando zonas...")
    gdf['zona'] = gdf['barrio'].map(ZONAS_MAP).fillna('Otras Zonas')

    # 1. IMPRIMIR REPORTE DE ZONAS
    print("\n" + "="*40)
    print("📋 REPORTE DE ZONAS Y BARRIOS")
    print("="*40)
    
    zonas_ordenadas = sorted(gdf['zona'].unique())
    for zona in zonas_ordenadas:
        barrios_en_zona = gdf[gdf['zona'] == zona]['barrio'].tolist()
        barrios_en_zona.sort()
        print(f"\n📍 {zona}:")
        print(f"   {', '.join(barrios_en_zona)}")

    # 2. GENERAR MAPA INTERACTIVO
    print("\n" + "="*40)
    print("🗺️  GENERANDO MAPA VISUAL...")
    print("="*40)
    
    centro_neuquen = [-38.9516, -68.0591]
    m = folium.Map(location=centro_neuquen, zoom_start=13, tiles="CartoDB positron")

    # Función de estilo basada en la zona
    colores_zona = {
        'Centro': '#e41a1c',       # Rojo
        'Este': '#4daf4a',         # Verde
        'Paseo de la Costa': '#984ea3', # Violeta
        'Norte / Alto': '#377eb8', # Azul (Norte / Alto)
        'Oeste': '#ff7f00',        # Naranja
        'Otras Zonas': '#cccccc'   # Gris claro
    }

    def style_function(feature):
        zona = feature['properties']['zona']
        color = colores_zona.get(zona, '#cccccc')
        return {
            'fillColor': color,
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6
        }

    # Añadir GeoJSON al mapa con tooltip
    folium.GeoJson(
        gdf,
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(fields=['barrio', 'zona'], aliases=['Barrio:', 'Zona:'])
    ).add_to(m)

    # Añadir leyenda (básica, usando fit bounds)
    m.fit_bounds(m.get_bounds())
    
    m.save(MAPA_SALIDA)
    print(f"✅ Mapa guardado en: {MAPA_SALIDA}")
    print(f"👉 Abrí este archivo en tu navegador para ver la distribución.")

if __name__ == "__main__":
    main()
