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
import time
import requests
from datetime import timedelta
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
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Importar utilidades
from db_utils import (
    get_connection, close_connection, migrate_embedding_columns,
    get_lugares_para_embedding, get_reviews_nuevas_sin_embedding,
    get_todas_reviews_lugar, actualizar_resumen_lugar
)
from llm_utils import generar_resumen_reviews, detectar_info_nueva, limpiar_texto


# Se llena si la generacion de resumenes fallo de forma masiva. Hace que el script termine con
# codigo != 0 para que el workflow salga en ROJO: un fallo del proveedor no puede verse igual
# que una semana sin novedades.
RESUMENES_ROTOS = []


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
    # Si es el mensaje genérico de falta de info, NO generar embedding
    if not resumen or len(resumen.strip()) < 20 or "insuficiente información" in resumen:
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


def send_discord_report(stats):
    """Envía reporte de ejecución a Discord"""
    if not DISCORD_WEBHOOK_URL:
        return

    color = 0x00ff00 if stats['status'] == 'success' else 0xff0000
    
    mensaje = f"""**🧠 QUE MORFAMOS - Regeneración de Embeddings**
📊 **Tipo:** {stats['tipo']}
⏱️ **Duración:** {stats['duration']}

📍 **Lugares procesados:** {stats['lugares_procesados']}
📝 **Resúmenes generados:** {stats['resumenes_generados']}
🚀 **Embeddings creados:** {stats['embeddings_creados']}
"""
    
    payload = {
        "embeds": [{
            "description": mensaje,
            "color": color,
            "timestamp": datetime.now().isoformat() if 'datetime' in globals() else None
        }]
    }
    
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Error enviando reporte a Discord: {e}")


def regenerate_full(resume=False):
    """Regenera TODOS los resúmenes y embeddings desde cero"""
    start_time = time.time()
    logger.info(f"🔄 Regeneración COMPLETA de embeddings (Resume={resume})")
    
    # Migrar columnas si es necesario
    migrate_embedding_columns()
    
    engine = create_engine(get_sqlalchemy_url(DATABASE_URL))
    
    # Eliminar todos los embeddings existentes (correct even for resume, we rebuild the vector store)
    logger.info("🗑️ Eliminando embeddings existentes...")
    deleted = delete_all_embeddings(engine)
    logger.info(f"   Eliminados: {deleted}")
    
    # Obtener todos los lugares
    lugares = get_lugares_para_embedding()
    logger.info(f"📍 Lugares a procesar: {len(lugares)}")
    
    docs = []
    procesados = 0
    resumenes_count = 0
    fallos_resumen = 0
    skipped_count = 0
    
    limit_date = datetime.now() - timedelta(hours=24)
    
    for i, lugar in enumerate(lugares):
        nombre = lugar['nombre']
        
        # Lógica RESUME: Si ya se actualizó en las últimas 24hs, usamos lo que hay
        if resume and lugar.get('embedding_updated_at'):
             last_update = lugar['embedding_updated_at']
             # Asegurar que es datetime
             if isinstance(last_update, str):
                 try:
                     last_update = datetime.fromisoformat(last_update)
                 except:
                     pass
             
             if isinstance(last_update, datetime) and last_update > limit_date:
                 resumen = lugar.get('resumen_reviews')
                 if resumen and len(resumen) > 20:
                     logger.info(f"[{i+1}/{len(lugares)}] ⏭️ SKIPPING {nombre[:30]}... (Updated: {last_update})")
                     doc = create_document(lugar, resumen)
                     if doc:
                         docs.append(doc)
                         skipped_count += 1
                     continue

        # Obtener todas las reseñas
        reviews = get_todas_reviews_lugar(nombre)
        
        if not reviews:
            continue
        
        # Generar resumen con DeepSeek
        logger.info(f"[{i+1}/{len(lugares)}] {nombre[:40]}... ({len(reviews)} reviews)")
        resumen = generar_resumen_reviews(reviews, nombre)
        
        if resumen:
            resumenes_count += 1
            # Guardar resumen en DB
            actualizar_resumen_lugar(nombre, resumen)
            
            # Crear documento para embedding
            doc = create_document(lugar, resumen)
            if doc:
                docs.append(doc)
        else:
            # Se cuenta el fallo. `generar_resumen_reviews` devuelve "" cuando la llamada al LLM
            # falla, y antes eso se salteaba en silencio: con el proveedor caido el job terminaba
            # en VERDE habiendo generado cero resumenes. Paso de verdad — el saldo de DeepSeek se
            # agoto y nadie se entero hasta que se fue a buscar por que los resumenes estaban
            # viejos.
            fallos_resumen += 1
        
        # Log de progreso
        if (i + 1) % 50 == 0:
            logger.info(f"   ⏳ Progreso: {i+1}/{len(lugares)}")
    
    # Generar embeddings
    embeddings_count = 0
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
        embeddings_count = len(docs)
        logger.info(f"✅ {embeddings_count} embeddings generados!")
    else:
        logger.warning("⚠️ No hay documentos para generar embeddings")
    
    close_connection()
    
    # Enviar reporte
    duration = str(timedelta(seconds=int(time.time() - start_time)))
    send_discord_report({
        'status': 'success',
        'tipo': 'FULL (Manual)',
        'duration': duration,
        'lugares_procesados': len(lugares),
        'resumenes_generados': resumenes_count,
        'embeddings_creados': embeddings_count
    })


def regenerate_incremental():
    """Regenera solo los lugares que tienen información nueva"""
    start_time = time.time()
    logger.info("🔄 Regeneración INCREMENTAL de embeddings")
    
    # Migrar columnas si es necesario
    migrate_embedding_columns()
    
    engine = create_engine(get_sqlalchemy_url(DATABASE_URL))
    
    # Obtener todos los lugares con metadata de embedding
    lugares = get_lugares_para_embedding()
    logger.info(f"📍 Lugares en DB: {len(lugares)}")
    
    lugares_a_actualizar = []
    nuevos_resumenes = {}
    fallos_resumen = 0
    
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
                # Se CUENTA el fallo. Antes esto se salteaba en silencio, y como
                # `detectar_info_nueva` devuelve True ante un error, con el proveedor caído todos
                # los lugares pasaban la puerta, fallaban al generar, y `lugares_a_actualizar`
                # quedaba vacío: el job reportaba "no hubo cambios", que es INDISTINGUIBLE de una
                # semana tranquila. Paso de verdad al agotarse el saldo de DeepSeek.
                fallos_resumen += 1
                logger.warning(f"   ⚠️ Falló la generación del resumen de {nombre[:40]}")
        else:
            logger.info(f"   ⏭️ Sin info nueva relevante, actualizando solo timestamp")
            actualizar_resumen_lugar(nombre, resumen_actual) # Actualiza solo fecha
    
    logger.info(f"\n📊 Lugares a actualizar: {len(lugares_a_actualizar)}")

    # "Todo falló" y "no hubo novedades" se veían EXACTAMENTE IGUAL desde afuera: los dos
    # terminaban con la lista vacía y el job en verde. Acá se separan. Si hubo intentos y la
    # mayoría falló, es el proveedor —sin saldo o credenciales mal—, no una semana tranquila.
    intentos = len(lugares_a_actualizar) + fallos_resumen
    if fallos_resumen and fallos_resumen >= max(3, intentos * 0.5):
        proveedor = os.getenv("SUMMARY_PROVIDER", "deepseek")
        logger.error(
            f"🔴 RESÚMENES ROTOS: fallaron {fallos_resumen} de {intentos} intentos. "
            f"Con esa proporción no es un caso aislado — revisar saldo y credenciales de "
            f"'{proveedor}'. Los resúmenes NO se actualizaron esta corrida."
        )
        RESUMENES_ROTOS.append((fallos_resumen, intentos, proveedor))
    elif fallos_resumen:
        logger.warning(f"⚠️ {fallos_resumen} de {intentos} resúmenes fallaron.")
    
    # Enviar reporte si NO hubo cambios (para saber que corrió)
    if not lugares_a_actualizar:
        logger.info("✅ Todo está actualizado, no hay cambios necesarios")
        close_connection()
        duration = str(timedelta(seconds=int(time.time() - start_time)))
        send_discord_report({
            'status': 'success',
            'tipo': 'Incremental (Sin cambios)',
            'duration': duration,
            'lugares_procesados': len(lugares),
            'resumenes_generados': 0,
            'embeddings_creados': 0
        })
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
    
    # Reporte final con cambios
    duration = str(timedelta(seconds=int(time.time() - start_time)))
    send_discord_report({
        'status': 'success',
        'tipo': 'Incremental (Con actualizaciones)',
        'duration': duration,
        'lugares_procesados': len(lugares),
        'resumenes_generados': len(nuevos_resumenes),
        'embeddings_creados': len(docs)
    })


def regenerate_embeddings_only():
    """Solo regenera embeddings usando los resúmenes YA GUARDADOS en DB"""
    start_time = time.time()
    logger.info("🧩 Regeneración SOLO de embeddings (Desde DB existente)")
    
    # Migrar columnas si es necesario
    migrate_embedding_columns()
    engine = create_engine(get_sqlalchemy_url(DATABASE_URL))
    
    # Obtener lugares que YA tienen resumen
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT nombre, rating_gral, direccion, zona, barrio, categoria, resumen_reviews FROM lugares WHERE resumen_reviews IS NOT NULL AND length(resumen_reviews) > 20")
    rows = cursor.fetchall()
    
    lugares_con_resumen = []
    for r in rows:
        lugares_con_resumen.append({
            'nombre': r[0],
            'rating_gral': r[1],
            'direccion': r[2],
            'zona': r[3],
            'barrio': r[4],
            'categoria': r[5],
            'resumen_reviews': r[6]
        })
    conn.close()
    
    logger.info(f"📍 Lugares con resumen apto: {len(lugares_con_resumen)}")
    
    if not lugares_con_resumen:
        logger.warning("No hay lugares con resumen para procesar.")
        return

    # Eliminar todos los embeddings existentes para empezar limpio
    logger.info("🗑️ Eliminando embeddings existentes...")
    delete_all_embeddings(engine)
    
    # Crear documentos
    docs = []
    for l in lugares_con_resumen:
        doc = create_document(l, l['resumen_reviews'])
        if doc:
            docs.append(doc)
            
    # Generar embeddings
    embeddings_count = 0
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
        embeddings_count = len(docs)
        logger.info(f"✅ {embeddings_count} embeddings generados!")
    
    close_connection()
    
    duration = str(timedelta(seconds=int(time.time() - start_time)))
    send_discord_report({
        'status': 'success',
        'tipo': 'Embeddings Only (Recuperación)',
        'duration': duration,
        'lugares_procesados': len(lugares_con_resumen),
        'resumenes_generados': 0,
        'embeddings_creados': embeddings_count
    })


if __name__ == "__main__":
    from datetime import datetime
    if "--full" in sys.argv:
        resume = "--resume" in sys.argv
        regenerate_full(resume=resume)
    elif "--embed-only" in sys.argv:
        regenerate_embeddings_only()
    else:
        regenerate_incremental()

    # El workflow tiene que salir en ROJO si los resumenes no se generaron. Sin esto el job
    # terminaba en verde con cero resumenes y el problema podia pasar semanas sin que nadie lo
    # viera — que es exactamente lo que paso al agotarse el saldo de DeepSeek.
    if RESUMENES_ROTOS:
        fallos, intentos, proveedor = RESUMENES_ROTOS[0]
        print(f"::error::Resumenes rotos: fallaron {fallos}/{intentos} con proveedor "
              f"'{proveedor}'. Revisar saldo y credenciales.")
        sys.exit(1)
