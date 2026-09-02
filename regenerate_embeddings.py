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
from datetime import datetime, timedelta
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

# Cuantas resenas nuevas VALIDAS (>30 caracteres) hacen falta para preguntarle al LLM si el
# resumen quedo desactualizado. El LLM decide DESPUES si vale regenerar
# (`detectar_info_nueva`), asi que este numero no es "cada cuanto se regenera" sino "cada cuanto
# vale la pena preguntar".
#
# El valor sale de medir el corpus, no de la intuicion. Sobre las 91.447 resenas de mas de 30
# caracteres, el 38% menciona algun concepto del vocabulario (features, dietas, ocasiones); el
# resto es "muy rico", "buena atencion" y demas, que no aporta nada buscable. Ojo tambien que la
# MITAD de las resenas no tiene texto: son solo estrellas (percentiles 10 y 25 = 0 caracteres).
#
# Con p = 0.38 por resena:
#
#     N resenas   prob. de >=1 concepto   conceptos esperados   lugares/semana
#         3               76%                    1.1                 45
#         5               91%                    1.9                 19
#        10               99%                    3.8                  8
#        15              100%                    5.7                  3
#        20              100%                    7.6                  0   <- el valor viejo
#
# 5 es el punto justo: 1.9 conceptos esperados cae en el mismo liston de >=2 menciones que usa la
# metrica de evidencia para dar un concepto por confirmado — es la misma pregunta. Exigir 15
# pedia 5.7 conceptos para molestarse en mirar, o sea el triple de la evidencia necesaria, y la
# diferencia de costo entre 5 y 15 es menos de un centavo por semana.
UMBRAL_RESENAS_NUEVAS = int(os.getenv("UMBRAL_RESENAS_NUEVAS", "5"))

# Tope de dias sin evaluar un lugar. Es la valvula contra el congelamiento silencioso: sin ella,
# un lugar de poco movimiento no llega nunca al umbral y su resumen queda viejo indefinidamente.
MAX_DIAS_SIN_EVALUAR = int(os.getenv("MAX_DIAS_SIN_EVALUAR", "90"))

# TOPE DURO de lugares a EVALUAR por corrida. No es un limite de presupuesto -en regimen normal
# son ~19 lugares por semana, muy por debajo- sino una red contra sorpresas: si algo vuelve a
# resetear los timestamps en masa, una corrida sin tope regeneraria los 929 lugares de una.
# Cuenta EVALUACIONES y no regeneraciones, porque el lugar donde el LLM contesta "no aporta nada"
# igual gasto su llamada.
# Con 100, el peor caso son ~$0.05 por corrida y el backlog acumulado desde julio (294 lugares)
# se drena en unas 3 corridas. No causa inanicion: al evaluarse, el lugar resetea su ventana y
# sale de la cola, asi que la siguiente corrida agarra a los que quedaron.
MAX_LUGARES_POR_CORRIDA = int(os.getenv("MAX_LUGARES_POR_CORRIDA", "100"))


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
    tope_avisado = False
    # Cuenta llamadas al LLM, no regeneraciones: preguntar "¿hay info nueva?" cuesta aunque la
    # respuesta sea que no.
    evaluados = 0
    
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
        
        # Umbral MÍNIMO para justificar el análisis. Era 20 y NUNCA se alcanzaba: medido el
        # 01-sep, con 245 lugares que recibieron reseñas nuevas en la semana, los que llegaban a
        # 20 válidas eran CERO. Los resúmenes no se regeneraban desde julio.
        # Tope de la corrida. Se chequea ANTES de mirar las resenas nuevas para no gastar ni la
        # consulta de conteo de mas.
        if evaluados >= MAX_LUGARES_POR_CORRIDA:
            if not tope_avisado:
                logger.info(f"🛑 Tope de {MAX_LUGARES_POR_CORRIDA} lugares alcanzado. El resto "
                            f"queda para la proxima corrida (su ventana sigue acumulando).")
                tope_avisado = True
            continue

        # Válvula de seguridad contra el congelamiento. Un lugar de poco movimiento puede no
        # llegar nunca a UMBRAL_RESENAS_NUEVAS, y uno cuyo LLM contesta "no aporta nada" varias
        # veces seguidas resetea la ventana cada vez: en los dos casos el resumen se queda viejo
        # para siempre sin que nada lo señale. Pasado MAX_DIAS_SIN_EVALUAR se evalúa igual, con
        # tal de que haya alguna reseña nueva que justifique mirar.
        # Hoy no dispara para nadie (todos fueron evaluados hace ~51 días); es para que no vuelva
        # a pasar lo de julio-agosto, donde los resúmenes se congelaron siete semanas sin aviso.
        vencido = False
        if embedding_date and len(reviews_validas) >= 5:
            edad = datetime.now() - (embedding_date if isinstance(embedding_date, datetime)
                                     else datetime.fromisoformat(str(embedding_date)))
            vencido = edad.days > MAX_DIAS_SIN_EVALUAR
            if vencido:
                logger.info(f"[VENCIDO] {nombre[:40]}... ({edad.days} días sin evaluar, "
                            f"{len(reviews_validas)} reseñas nuevas)")

        if len(reviews_validas) < UMBRAL_RESENAS_NUEVAS and not vencido:
            # NO se toca el timestamp. Esta es la mitad importante del arreglo: antes acá se
            # llamaba a `actualizar_resumen_lugar(nombre, resumen_actual)` "para no chequear
            # mañana lo mismo", y eso ponía `embedding_updated_at = NOW()` aunque el resumen no
            # cambiara. Como la ventana de "reseñas nuevas" se calcula contra esa fecha, se
            # REINICIABA cada semana: las 3 reseñas de esta semana no se sumaban a las 4 de la
            # que viene, así que ningún lugar acumulaba nunca lo suficiente. Era una condición
            # imposible de cumplir, no un umbral exigente.
            # Medido, la diferencia que hace: con umbral 15, la ventana que se reinicia alcanza a
            # 3 lugares por semana; acumulando, a 119.
            # El costo de no cortar acá es una consulta de conteo por lugar y por corrida — el
            # LLM sólo se toca pasando el umbral.
            continue

        logger.info(f"[CHECK] {nombre[:40]}... ({len(reviews_validas)} reviews nuevas válidas)")
        
        # Verificar si aportan info nueva con DeepSeek
        evaluados += 1
        if detectar_info_nueva(resumen_actual, reviews_validas):
            # Regenerar resumen completo
            todas_reviews = get_todas_reviews_lugar(nombre) # Devuelve dicts con rating
            resumen = generar_resumen_reviews(todas_reviews, nombre)
            if resumen:
                # Se persiste ACA, no al final. Antes los resúmenes se acumulaban en memoria y se
                # escribían recién después del bucle: una corrida cortada a la mitad —timeout del
                # runner, error de red, lo que sea— perdía TODO el trabajo del LLM ya pagado. Con
                # 100 lugares y varios minutos cada uno, esa ventana de riesgo son horas.
                # Escribir por lugar hace la corrida reanudable de hecho: lo ya hecho queda hecho,
                # y como el timestamp se actualiza en la misma operación, la siguiente corrida no
                # lo vuelve a tomar.
                actualizar_resumen_lugar(nombre, resumen)
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
    # Los resúmenes ya se guardaron uno por uno dentro del bucle, para que una corrida cortada no
    # pierda el trabajo del LLM. Este bloque quedaba como segunda escritura idéntica de todo:
    # inofensiva, pero engañosa al leer el código —parecía que ACÁ era donde se persistía— y
    # duplicaba una UPDATE por lugar sin motivo.
    logger.info(f"💾 {len(nuevos_resumenes)} resúmenes ya persistidos durante el recorrido.")
    
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
