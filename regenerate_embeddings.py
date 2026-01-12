"""
Regeneración INTELIGENTE de embeddings.
Usa DeepSeek para generar resúmenes y detectar si hay información nueva.

Modos:
- python regenerate_embeddings.py           # Incremental (solo cambios)
- python regenerate_embeddings.py --full    # Regenerar todo
"""
import os
import sys
import logging
from sqlalchemy import create_engine, text
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector
from langchain_core.documents import Document

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar .env si existe
if os.path.exists("mis_claves.env"):
    from dotenv import load_dotenv
    load_dotenv("mis_claves.env")

DATABASE_URL = os.getenv("DATABASE_URL")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "reviews_embeddings")

# Importar utilidades
from db_utils import (
    get_connection, close_connection, migrate_embedding_columns,
    get_lugares_para_embedding, get_reviews_nuevas_sin_embedding,
    get_todas_reviews_lugar, actualizar_resumen_lugar
)
from deepseek_utils import generar_resumen_reviews, detectar_info_nueva, limpiar_texto


def get_sqlalchemy_url(url):
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url and url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def delete_embeddings_for_lugares(engine, nombres):
    """Elimina embeddings de lugares específicos"""
    if not nombres:
        return 0
    
    nombres_escaped = [n.replace("'", "''") for n in nombres]
    nombres_str = "', '".join(nombres_escaped)
    
    query = f"""
        DELETE FROM langchain_pg_embedding 
        WHERE collection_id IN (
            SELECT uuid FROM langchain_pg_collection WHERE name = '{COLLECTION_NAME}'
        )
        AND cmetadata->>'nombre' IN ('{nombres_str}')
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        return result.rowcount


def delete_all_embeddings(engine):
    """Elimina todos los embeddings de la colección"""
    query = f"""
        DELETE FROM langchain_pg_embedding 
        WHERE collection_id IN (
            SELECT uuid FROM langchain_pg_collection WHERE name = '{COLLECTION_NAME}'
        )
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        return result.rowcount


def create_document(lugar, resumen):
    """Crea un Document de LangChain para un lugar"""
    if not resumen or len(resumen.strip()) < 20:
        return None
    
    rating_raw = lugar.get('rating_gral', 0)
    try:
        if isinstance(rating_raw, str):
            rating_raw = rating_raw.replace(',', '.')
        rating = float(rating_raw) if rating_raw else 0.0
    except:
        rating = 0.0
    
    return Document(
        page_content=resumen,
        metadata={
            "nombre": str(lugar['nombre']),
            "rating": rating,
            "direccion": str(lugar.get('direccion', '') or ''),
            "zona": str(lugar.get('zona', '') or ''),
            "barrio": str(lugar.get('barrio', '') or ''),
            "categoria": str(lugar.get('categoria', '') or '')
        }
    )


def regenerate_full():
    """Regenera TODOS los resúmenes y embeddings desde cero"""
    logger.info("🔄 Regeneración COMPLETA de embeddings con resúmenes")
    
    # Migrar columnas si es necesario
    migrate_embedding_columns()
    
    engine = create_engine(get_sqlalchemy_url(DATABASE_URL))
    
    # Eliminar todos los embeddings existentes
    logger.info("🗑️ Eliminando embeddings existentes...")
    deleted = delete_all_embeddings(engine)
    logger.info(f"   Eliminados: {deleted}")
    
    # Obtener todos los lugares
    lugares = get_lugares_para_embedding()
    logger.info(f"📍 Lugares a procesar: {len(lugares)}")
    
    docs = []
    procesados = 0
    
    for i, lugar in enumerate(lugares):
        nombre = lugar['nombre']
        
        # Obtener todas las reseñas
        reviews = get_todas_reviews_lugar(nombre)
        
        if not reviews:
            continue
        
        # Generar resumen con DeepSeek
        logger.info(f"[{i+1}/{len(lugares)}] {nombre[:40]}... ({len(reviews)} reviews)")
        resumen = generar_resumen_reviews(reviews, nombre)
        
        if resumen:
            # Guardar resumen en DB
            actualizar_resumen_lugar(nombre, resumen)
            
            # Crear documento para embedding
            doc = create_document(lugar, resumen)
            if doc:
                docs.append(doc)
                procesados += 1
        
        # Log de progreso
        if (i + 1) % 50 == 0:
            logger.info(f"   ⏳ Progreso: {i+1}/{len(lugares)}")
    
    # Generar embeddings
    if docs:
        logger.info(f"🚀 Generando embeddings para {len(docs)} lugares...")
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        
        vectorstore = PGVector.from_documents(
            documents=docs,
            embedding=embeddings,
            connection=DATABASE_URL,
            collection_name=COLLECTION_NAME,
            use_jsonb=True
        )
        
        logger.info(f"✅ {len(docs)} embeddings generados!")
    else:
        logger.warning("⚠️ No hay documentos para generar embeddings")
    
    close_connection()


def regenerate_incremental():
    """Regenera solo los lugares que tienen información nueva"""
    logger.info("🔄 Regeneración INCREMENTAL de embeddings")
    
    # Migrar columnas si es necesario
    migrate_embedding_columns()
    
    engine = create_engine(get_sqlalchemy_url(DATABASE_URL))
    
    # Obtener todos los lugares con metadata de embedding
    lugares = get_lugares_para_embedding()
    logger.info(f"📍 Lugares en DB: {len(lugares)}")
    
    lugares_a_actualizar = []
    nuevos_resumenes = {}
    
    for i, lugar in enumerate(lugares):
        nombre = lugar['nombre']
        resumen_actual = lugar.get('resumen_reviews')
        embedding_date = lugar.get('embedding_updated_at')
        
        # Caso 1: No tiene resumen → generar
        if not resumen_actual:
            reviews = get_todas_reviews_lugar(nombre)
            if reviews:
                logger.info(f"[NUEVO] {nombre[:40]}...")
                resumen = generar_resumen_reviews(reviews, nombre)
                if resumen:
                    nuevos_resumenes[nombre] = resumen
                    lugares_a_actualizar.append(lugar)
            continue
        
        # Caso 2: Tiene resumen → verificar si hay reviews nuevas QUALIFICADAS
        reviews_nuevas = get_reviews_nuevas_sin_embedding(nombre, embedding_date)
        
        if not reviews_nuevas:
            continue
        
        # Filtro de CALIDAD: > 30 caracteres (después de limpiar repetidos/puntuación)
        reviews_validas = [r for r in reviews_nuevas if r and len(limpiar_texto(str(r))) > 30]
        
        # Umbral MÍNIMO de cantidad: al menos 20 reviews nuevas válidas para justificar análisis
        # (Optimización de costos para estudiante: solo regenerar cuando hay mucho volumen nuevo)
        if len(reviews_validas) < 20:
            # Si hay pocas reviews nuevas, solo actualizamos la fecha para no chequear mañana lo mismo
            # (A menos que pasaran > 30 días, eso se podría agregar luego)
            if reviews_nuevas: # Si hay reviews pero son cortas o pocas
                 actualizar_resumen_lugar(nombre, resumen_actual) # Actualiza timestamp sin cambiar resumen
            continue

        logger.info(f"[CHECK] {nombre[:40]}... ({len(reviews_validas)} reviews nuevas válidas)")
        
        # Verificar si aportan info nueva con DeepSeek
        if detectar_info_nueva(resumen_actual, reviews_validas):
            # Regenerar resumen completo
            todas_reviews = get_todas_reviews_lugar(nombre) # Devuelve dicts con rating
            resumen = generar_resumen_reviews(todas_reviews, nombre)
            if resumen:
                nuevos_resumenes[nombre] = resumen
                lugares_a_actualizar.append(lugar)
                logger.info(f"   ✅ Info nueva detectada, regenerando")
        else:
            logger.info(f"   ⏭️ Sin info nueva relevante, actualizando solo timestamp")
            actualizar_resumen_lugar(nombre, resumen_actual) # Actualiza solo fecha
    
    logger.info(f"\n📊 Lugares a actualizar: {len(lugares_a_actualizar)}")
    
    if not lugares_a_actualizar:
        logger.info("✅ Todo está actualizado, no hay cambios necesarios")
        close_connection()
        return
    
    # Actualizar resúmenes en DB
    logger.info("💾 Guardando resúmenes en DB...")
    for nombre, resumen in nuevos_resumenes.items():
        actualizar_resumen_lugar(nombre, resumen)
    
    # Eliminar embeddings viejos de los lugares a actualizar
    nombres_actualizar = [l['nombre'] for l in lugares_a_actualizar]
    logger.info(f"🗑️ Eliminando embeddings viejos de {len(nombres_actualizar)} lugares...")
    delete_embeddings_for_lugares(engine, nombres_actualizar)
    
    # Crear nuevos documentos
    docs = []
    for lugar in lugares_a_actualizar:
        nombre = lugar['nombre']
        resumen = nuevos_resumenes.get(nombre)
        if resumen:
            doc = create_document(lugar, resumen)
            if doc:
                docs.append(doc)
    
    # Generar embeddings
    if docs:
        logger.info(f"🚀 Generando {len(docs)} embeddings nuevos...")
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        
        vectorstore = PGVector.from_documents(
            documents=docs,
            embedding=embeddings,
            connection=DATABASE_URL,
            collection_name=COLLECTION_NAME,
            use_jsonb=True
        )
        
        logger.info(f"✅ {len(docs)} embeddings actualizados!")
    
    close_connection()


if __name__ == "__main__":
    if "--full" in sys.argv:
        regenerate_full()
    else:
        regenerate_incremental()
