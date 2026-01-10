"""
Módulo de utilidades para conexión a Supabase/PostgreSQL.
Proporciona funciones para insertar reviews y gestionar estado de procesamiento.
"""
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Variable global para la conexión
_connection = None

def get_database_url():
    """Obtiene la URL de la base de datos desde variables de entorno."""
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.warning("⚠️ DATABASE_URL no configurada - usando modo CSV")
        return None
    return url


def get_connection():
    """Obtiene o crea una conexión a PostgreSQL."""
    global _connection
    
    database_url = get_database_url()
    if not database_url:
        return None
    
    if _connection is not None:
        try:
            # Verificar que la conexión sigue activa
            cursor = _connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return _connection
        except:
            _connection = None
    
    try:
        import psycopg2
        _connection = psycopg2.connect(database_url)
        _connection.autocommit = False
        logger.info("✅ Conexión a PostgreSQL establecida")
        return _connection
    except ImportError:
        logger.error("❌ psycopg2 no instalado. Ejecutar: pip install psycopg2-binary")
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        return None


def close_connection():
    """Cierra la conexión a la base de datos."""
    global _connection
    if _connection:
        try:
            _connection.close()
            logger.info("✅ Conexión a PostgreSQL cerrada")
        except:
            pass
        _connection = None


def _simplificar_direccion(direccion):
    """
    Retorna la primera parte de la dirección (antes de la coma).
    Ej: 'Eugenio Perticone 545, Q8300 Neuquén' -> 'Eugenio Perticone 545'
    """
    if not direccion:
        return ""
    return direccion.split(',')[0].strip()

def upsert_lugar(lugar_data):
    """
    Inserta o actualiza un lugar en la tabla 'lugares'.
    Maneja duplicados de nombre agregando la dirección simplificada.
    """
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        raw_nombre = lugar_data.get('nombre') or lugar_data.get('restaurante', 'Desconocido')
        url = lugar_data.get('url')
        direccion = lugar_data.get('direccion')
        
        # --- Lógica de Desambiguación de Nombres ---
        if direccion: # Solo podemos desambiguar si hay dirección
            # Buscar otros lugares con el mismo nombre y distinta URL
            cursor.execute("SELECT url, direccion, nombre FROM lugares WHERE nombre = %s AND url != %s", (raw_nombre, url))
            collisions = cursor.fetchall()
            
            if collisions:
                logger.info(f"⚠️ Detectada colisión de nombre para '{raw_nombre}' con {len(collisions)} lugares.")
                
                # 1. Modificar nombre del actual
                simp_dir_actual = _simplificar_direccion(direccion)
                nuevo_nombre_actual = f"{raw_nombre} - {simp_dir_actual}"
                lugar_data['nombre'] = nuevo_nombre_actual # Actualizamos para el insert abajo
                # logger.info(f"   -> Nombre ajustado a: {nuevo_nombre_actual}")
                
                # 2. Corregir los existentes mal nombrados (si aún tienen el nombre genérico)
                for col_url, col_dir, col_nom in collisions:
                    if col_nom == raw_nombre and col_dir: # Solo si se llaman igual exactamente
                        simp_dir_col = _simplificar_direccion(col_dir)
                        nuevo_nombre_col = f"{raw_nombre} - {simp_dir_col}"
                        try:
                            # Actualización recursiva ligera
                            cursor.execute("UPDATE lugares SET nombre = %s WHERE url = %s", (nuevo_nombre_col, col_url))
                            logger.info(f"   -> Corrigiendo nombre existente: {nuevo_nombre_col}")
                            
                            # También actualizar el historial para mantener consistencia? 
                            # Podría ser costoso, dejémoslo para lugares.
                        except Exception as e:
                            logger.warning(f"Error corrigiendo colisión {col_url}: {e}")
        
        # --- Fin Lógica Desambiguación ---

        query = """
            INSERT INTO lugares (
                nombre, categoria, rating_gral, total_reviews_google, 
                direccion, latitud, longitud, url, 
                barrio, zona, cerca_rio,
                fecha_scraping
            ) VALUES (
                %(nombre)s, %(categoria)s, %(rating_gral)s, %(total_reviews_google)s, 
                %(direccion)s, %(latitud)s, %(longitud)s, %(url)s, 
                %(barrio)s, %(zona)s, %(cerca_rio)s,
                NOW()
            )
            ON CONFLICT (url) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                categoria = COALESCE(EXCLUDED.categoria, lugares.categoria),
                rating_gral = COALESCE(EXCLUDED.rating_gral, lugares.rating_gral),
                total_reviews_google = GREATEST(lugares.total_reviews_google, EXCLUDED.total_reviews_google),
                direccion = COALESCE(EXCLUDED.direccion, lugares.direccion),
                latitud = COALESCE(EXCLUDED.latitud, lugares.latitud),
                longitud = COALESCE(EXCLUDED.longitud, lugares.longitud),
                barrio = COALESCE(EXCLUDED.barrio, lugares.barrio),
                zona = COALESCE(EXCLUDED.zona, lugares.zona),
                cerca_rio = COALESCE(EXCLUDED.cerca_rio, lugares.cerca_rio),
                fecha_scraping = NOW();
        """
        
        # Preparar datos (usando el nombre potencialmente modificado)
        datos = {
            'nombre': lugar_data.get('nombre'), # Ya fue actualizado arriba si hubo colisión
            'categoria': lugar_data.get('categoria'),
            'rating_gral': lugar_data.get('rating_gral'),
            'total_reviews_google': lugar_data.get('total_reviews_google', 0),
            'direccion': lugar_data.get('direccion'),
            'latitud': lugar_data.get('latitud'),
            'longitud': lugar_data.get('longitud'),
            'url': lugar_data.get('url'),
            'barrio': lugar_data.get('barrio'),
            'zona': lugar_data.get('zona'),
            'cerca_rio': lugar_data.get('cerca_rio')
        }
        
        cursor.execute(query, datos)
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error upsert lugar: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        cursor.close()


def insertar_reviews_batch(reviews_data):
    """
    Inserta un lote de reviews en la base de datos.
    Esquema: restaurante, review_id, autor, rating_user, texto, fecha_aproximada, fecha_original, fecha_scraping
    
    Deduplicación: (restaurante + autor + texto_inicio)
    """
    if not reviews_data:
        return 0, 0
    
    conn = get_connection()
    if not conn:
        return 0, 0
    
    insertadas = 0
    duplicadas = 0
    
    try:
        cursor = conn.cursor()
        
        for review in reviews_data:
            # Normalizar texto para comparación
            texto_raw = review.get('texto', '') or ''
            texto_norm = ' '.join(texto_raw[:100].lower().split())
            autor_norm = (review.get('autor', '') or '').strip().lower()
            restaurante = review.get('restaurante', '')
            
            # Verificar si ya existe (sin usar URL, usando nombre de restaurante)
            cursor.execute("""
                SELECT 1 FROM reviews 
                WHERE restaurante = %s 
                AND LOWER(TRIM(autor)) = %s 
                AND LOWER(LEFT(texto, 100)) LIKE %s
                LIMIT 1
            """, (restaurante, autor_norm, texto_norm[:50] + '%'))
            
            if cursor.fetchone():
                duplicadas += 1
                continue
            
            try:
                # Insertar sin columna 'url' que no existe en el esquema target
                cursor.execute("""
                    INSERT INTO reviews (
                        restaurante, autor, rating_user, texto, 
                        fecha_aproximada, fecha_original, 
                        fecha_scraping, review_id
                    ) VALUES (
                        %(restaurante)s, %(autor)s, %(rating_user)s, %(texto)s,
                        %(fecha_aproximada)s, %(fecha_original)s,
                        %(fecha_scraping)s, %(review_id)s
                    )
                """, {
                    'restaurante': restaurante,
                    'autor': review.get('autor', 'Anónimo'),
                    'rating_user': review.get('rating_user'),
                    'texto': review.get('texto', ''),
                    'fecha_aproximada': review.get('fecha_aproximada'),
                    'fecha_original': review.get('fecha_original'),
                    'fecha_scraping': review.get('fecha_scraping', datetime.now().isoformat()),
                    'review_id': review.get('review_id', '')
                })
                
                if cursor.rowcount > 0:
                    insertadas += 1
                else:
                    duplicadas += 1
                    
            except Exception as e:
                logger.warning(f"⚠️ Error insertando review: {e}")
                duplicadas += 1
        
        conn.commit()
        logger.info(f"✅ DB: {insertadas} insertadas, {duplicadas} duplicadas/skip")
        
    except Exception as e:
        logger.error(f"❌ Error en batch insert: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        cursor.close()
    
    return insertadas, duplicadas


def verificar_review_existe(review_id):
    """Verifica si una review ya existe en la base de datos."""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM reviews WHERE review_id = %s", (review_id,))
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    except Exception as e:
        logger.warning(f"⚠️ Error verificando review: {e}")
        return False


def obtener_ids_existentes_por_url(url):
    """
    Obtiene los review_ids existentes para una URL específica.
    Útil para deduplicación antes de insertar.
    
    Returns:
        set: Conjunto de review_ids existentes
    """
    conn = get_connection()
    if not conn:
        return set()
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT review_id FROM reviews WHERE url = %s",
            (url,)
        )
        ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return ids
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo IDs existentes: {e}")
        return set()


def obtener_estadisticas():
    """Obtiene estadísticas generales de la base de datos."""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT restaurante) FROM reviews")
        total_restaurantes = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM reviews 
            WHERE fecha_scraping > NOW() - INTERVAL '24 hours'
        """)
        reviews_24h = cursor.fetchone()[0]
        
        cursor.close()
        
        return {
            'total_reviews': total_reviews,
            'total_restaurantes': total_restaurantes,
            'reviews_ultimas_24h': reviews_24h
        }
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo estadísticas: {e}")
        return None


        return False

def ensure_history_table_exists():
    """Crea la tabla de historial de reviews si no existe."""
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        # Verificar si existe la tabla
        cursor.execute("SELECT to_regclass('review_history');")
        exists = cursor.fetchone()[0] is not None
        
        if not exists:
            cursor.execute("""
                CREATE TABLE review_history (
                    id SERIAL PRIMARY KEY,
                    lugar_url TEXT NOT NULL,
                    nombre TEXT, -- Nombre del lugar al momento del registro
                    direccion TEXT, -- Dirección para desambiguar
                    review_count INTEGER NOT NULL,
                    rating NUMERIC(3, 1), 
                    delta_since_last INTEGER DEFAULT 0,
                    recorded_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX idx_history_url ON review_history(lugar_url);
                CREATE INDEX idx_history_date ON review_history(recorded_at);
            """)
        else:
            # Migración: agregar columnas si faltan
            columns_to_add = [
                ("rating", "NUMERIC(3, 1)"),
                ("nombre", "TEXT"),
                ("direccion", "TEXT")
            ]
            for col_name, col_type in columns_to_add:
                try:
                    cursor.execute(f"ALTER TABLE review_history ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
                except Exception as e:
                    logger.warning(f"Error agregando columna {col_name}: {e}")
                
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f"⚠️ Error creando tabla historial: {e}")
        return False
    finally:
        cursor.close()

def log_review_history(url, current_count, current_rating=None, nombre=None, direccion=None):
    """
    Registra un snapshot de metricas (count, rating, nombre, direccion).
    """
    conn = get_connection()
    if not conn:
        return False
        
    try:
        cursor = conn.cursor()
        
        # Obtener último conteo para calcular delta
        prev_count = 0
        cursor.execute("""
            SELECT review_count FROM review_history 
            WHERE lugar_url = %s 
            ORDER BY recorded_at DESC LIMIT 1
        """, (url,))
        row = cursor.fetchone()
        if row:
            prev_count = row[0]
            
        delta = current_count - prev_count
        
        # Insertar nuevo registro con nombre y dirección
        cursor.execute("""
            INSERT INTO review_history (lugar_url, review_count, rating, delta_since_last, nombre, direccion)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (url, current_count, current_rating, delta, nombre, direccion))
        
        conn.commit()
        return delta
    except Exception as e:
        logger.error(f"Error logueando historial: {e}")
        return 0
    finally:
        cursor.close()

def get_lugares_para_monitoreo(limit=50):
    """
    Obtiene URLs de lugares para actualizar, incluyendo nombre y dirección.
    """
    conn = get_connection()
    if not conn:
        return []
        
    try:
        cursor = conn.cursor()
        # Se agrega 'direccion' al select
        cursor.execute("""
            SELECT url, nombre, total_reviews_google, direccion
            FROM lugares 
            ORDER BY fecha_scraping ASC NULLS FIRST 
            LIMIT %s
        """, (limit,))
        
        lugares = []
        for row in cursor.fetchall():
            lugares.append({
                'url': row[0], 
                'nombre': row[1], 
                'last_count': row[2],
                'direccion': row[3]
            })
            
        return lugares
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo lugares para monitoreo: {e}")
        return []
    finally:
        try:
            cursor.close()
        except:
            pass

def ensure_log_tables_exists():
    """Crea las tablas de logs y reportes si no existen."""
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        # Tabla scraping_logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_logs (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP DEFAULT NOW(),
                url TEXT,
                estado TEXT, 
                mensaje TEXT,
                reviews_detectadas INTEGER DEFAULT 0,
                nuevas_reviews INTEGER DEFAULT 0,
                intentos INTEGER DEFAULT 1 -- Para retry logic
            );
            CREATE INDEX IF NOT EXISTS idx_logs_fecha ON scraping_logs(fecha);
            CREATE INDEX IF NOT EXISTS idx_logs_estado ON scraping_logs(estado);
            CREATE INDEX IF NOT EXISTS idx_logs_url ON scraping_logs(url);
        """)
        
        # Migración: agregar intentos si no existe
        try:
            cursor.execute("ALTER TABLE scraping_logs ADD COLUMN IF NOT EXISTS intentos INTEGER DEFAULT 1;")
        except:
            pass
            
        # Tabla validation_reports (sin cambios)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_reports (
                id SERIAL PRIMARY KEY,
                fecha_reporte TIMESTAMP DEFAULT NOW(),
                total_reportadas INTEGER,
                total_reales INTEGER,
                discrepancias_count INTEGER,
                detalle JSONB 
            );
            CREATE INDEX IF NOT EXISTS idx_valid_fecha ON validation_reports(fecha_reporte);
        """)
        
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f"⚠️ Error creando tablas de logs: {e}")
        return False
    finally:
        try:
            cursor.close()
        except:
            pass

def log_scraping_event(url, estado, mensaje, reviews_detectadas=0, nuevas_reviews=0, intentos=1):
    """Guarda un evento de scraping en la base de datos."""
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scraping_logs (url, estado, mensaje, reviews_detectadas, nuevas_reviews, intentos)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (url, estado, mensaje, reviews_detectadas, nuevas_reviews, intentos))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error logueando evento: {e}")
        return False
    finally:
        cursor.close()

def get_latest_scraping_states():
    """
    Retorna un diccionario con el último estado y nro de intentos para cada URL.
    {url: {'estado': 'EXITO', 'intentos': 1, 'fecha': ...}}
    """
    conn = get_connection()
    if not conn:
        return {}
    try:
        cursor = conn.cursor()
        # DISTINCT ON (url) nos da la última entrada para cada URL si ordenamos por fecha DESC
        cursor.execute("""
            SELECT DISTINCT ON (url) url, estado, intentos, fecha 
            FROM scraping_logs 
            ORDER BY url, fecha DESC
        """)
        
        estados = {}
        for row in cursor.fetchall():
            estados[row[0]] = {
                'estado': row[1],
                'intentos': row[2],
                'fecha': row[3]
            }
        return estados
    except Exception as e:
        logger.error(f"Error obteniendo estados de scraping: {e}")
        return {}
    finally:
        cursor.close()

def log_validation_report(total_reportadas, total_reales, discrepancias_count, detalle_json):
    """Guarda un reporte de validación en la base de datos."""
    conn = get_connection()
    if not conn:
        return False
    try:
        import json
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO validation_reports (total_reportadas, total_reales, discrepancias_count, detalle)
            VALUES (%s, %s, %s, %s)
        """, (total_reportadas, total_reales, discrepancias_count, json.dumps(detalle_json)))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error logueando reporte validacion: {e}")
        return False
    finally:
        cursor.close()
