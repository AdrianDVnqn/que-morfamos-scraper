"""
Script de prueba para ver resúmenes generados por DeepSeek.
Lugares FIJOS para comparar metodologías.
"""
import os
import random
from datetime import datetime

# Cargar .env si existe
if os.path.exists("mis_claves.env"):
    from dotenv import load_dotenv
    load_dotenv("mis_claves.env")

from db_utils import get_connection, get_todas_reviews_lugar, migrate_embedding_columns
from deepseek_utils import generar_resumen_reviews

# Lugares fijos para comparar metodologías
LUGARES_FIJOS = [
    "Grido Helado",
    "Restaurante El Ciervo", 
    "Growler Bar",
    "BIO ZEN",
    "Sureña Restaurante"
]

# Nombre con timestamp
OUTPUT_FILE = f"test_resumenes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

def main():
    print("🧪 Probando generación de resúmenes con DeepSeek...\n")
    
    # Verificar API key
    if not os.getenv("DEEPSEEK_API_KEY"):
        print("❌ DEEPSEEK_API_KEY no configurada")
        return
    
    # Conectar a DB
    conn = get_connection()
    if not conn:
        print("❌ No se pudo conectar a la DB")
        return
    
    # Migrar columnas
    migrate_embedding_columns()
    
    # Crear contenido Markdown
    md = f"""# 🧪 Test de Resúmenes con DeepSeek

**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  
**Lugares fijos:** {', '.join(LUGARES_FIJOS)}

---

"""
    
    for nombre in LUGARES_FIJOS:
        print(f"📍 Procesando: {nombre}...")
        
        # Obtener reviews
        reviews = get_todas_reviews_lugar(nombre)
        
        if not reviews:
            print(f"   ⚠️ No se encontraron reseñas para {nombre}")
            md += f"## 📍 {nombre}\n\n⚠️ No se encontraron reseñas\n\n---\n\n"
            continue
        
        # Estadísticas
        with_rating = len([r for r in reviews if r.get('rating')])
        extremos_5 = [r for r in reviews if r.get('rating') == 5]
        extremos_1_2 = [r for r in reviews if r.get('rating') in [1, 2]]
        por_largo = sorted(reviews, key=lambda x: len(str(x.get('texto', ''))), reverse=True)
        
        md += f"""## 📍 {nombre}

| Métrica | Valor |
|---------|-------|
| Reseñas totales | {len(reviews)} |
| Con rating | {with_rating} |
| 5 estrellas | {len(extremos_5)} |
| 1-2 estrellas | {len(extremos_1_2)} |

### 📋 Reseñas Seleccionadas (SIN duplicados)

"""
        # Marcar las ya mostradas para no repetir
        mostradas = set()
        
        # Recientes (3 que no estén en otras categorías prioritarias primero)
        md += "#### 🕐 Más recientes (3):\n"
        count = 0
        for r in reviews:
            if count >= 3:
                break
            rid = id(r)
            if rid not in mostradas:
                mostradas.add(rid)
                txt = (r.get('texto') or '').replace('\n', ' ')
                rating = r.get('rating') or '?'
                md += f"\n**{count+1}. [{rating}★]**\n> {txt}\n"
                count += 1
        
        # Más largas (2 que no se hayan mostrado)
        md += "\n#### 📏 Más largas (2):\n"
        count = 0
        for r in por_largo:
            if count >= 2:
                break
            rid = id(r)
            if rid not in mostradas:
                mostradas.add(rid)
                txt = (r.get('texto') or '').replace('\n', ' ')
                chars = len(r.get('texto', ''))
                rating = r.get('rating') or '?'
                md += f"\n**{count+1}. [{rating}★ - {chars} chars]**\n> {txt}\n"
                count += 1
        
        # Negativos (2 que no se hayan mostrado)
        md += "\n#### ⚠️ Ratings negativos (1-2★):\n"
        count = 0
        for r in extremos_1_2:
            if count >= 2:
                break
            rid = id(r)
            if rid not in mostradas:
                mostradas.add(rid)
                txt = (r.get('texto') or '').replace('\n', ' ')
                md += f"\n**{count+1}. [{r.get('rating')}★]**\n> {txt}\n"
                count += 1
        if count == 0:
            md += "*No hay reseñas negativas (o ya fueron mostradas)*\n"
        
        # Positivos (2 que no se hayan mostrado)
        md += "\n#### ⭐ Ratings positivos (5★):\n"
        sample_5 = extremos_5.copy()
        random.shuffle(sample_5)
        count = 0
        for r in sample_5:
            if count >= 2:
                break
            rid = id(r)
            if rid not in mostradas:
                mostradas.add(rid)
                txt = (r.get('texto') or '').replace('\n', ' ')
                md += f"\n**{count+1}. [{r.get('rating')}★]**\n> {txt}\n"
                count += 1
        if count == 0:
            md += "*No hay reseñas de 5★ (o ya fueron mostradas)*\n"
        
        # Generar resumen
        resumen = generar_resumen_reviews(reviews, nombre)
        
        md += f"""
### 🤖 Resumen Generado

```
{resumen if resumen else "⚠️ No se pudo generar resumen"}
```

---

"""
    
    # Guardar archivo
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(md)
    
    print(f"\n✅ Resultados guardados en: {OUTPUT_FILE}")
    print(f"   Abrilo en VS Code para verlo bonito!")

if __name__ == "__main__":
    main()
