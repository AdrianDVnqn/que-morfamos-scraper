"""
Regeneración COMPLETA de resúmenes con el prompt corregido — con shadow write.

Estrategia (blue-green para datos derivados): NO se pisa `resumen_reviews` ni la colección de
embeddings que sirve producción. Se escribe a columnas y colección paralelas, se evalúa con el
golden dataset, y recién ahí se promueve. Si algo sale mal, producción nunca se entera.

    resumen_reviews          <- producción (intacto)
    resumen_reviews_v2       <- lo que genera este script
    resumen_prompt_version   <- marca qué prompt lo generó (para rollout parcial y rollback)
    resumen_generado_at      <- cuándo

Es idempotente y resumible: los lugares que ya tengan la versión objetivo se saltean, así que se
puede cortar y volver a correr sin repetir trabajo ni gasto.

Uso:
    SUMMARY_PROVIDER=openai python regenerar_resumenes.py --dry-run   # estima costo, no llama a la API
                                                                      # (sí crea las columnas shadow,
                                                                      #  que son aditivas y nullables)
    SUMMARY_PROVIDER=openai python regenerar_resumenes.py --golden    # SÓLO los ~48 del golden
                                                                      #  dataset (~$0.02): usar ESTO
                                                                      #  para iterar el prompt
    SUMMARY_PROVIDER=openai python regenerar_resumenes.py --todo      # los 791 (~$0.33, ~13 min):
                                                                      #  sólo para el benchmark final
    SUMMARY_PROVIDER=openai python regenerar_resumenes.py --limit 50  # sólo N lugares
    python regenerar_resumenes.py --embeddings                        # genera embeddings del v2
"""
import os
import sys
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

if os.path.exists("mis_claves.env"):
    from dotenv import load_dotenv
    load_dotenv("mis_claves.env")

from sqlalchemy import create_engine, text
from llm_utils import generar_resumen_reviews

# Cambiar cuando se toque el prompt: los lugares con otra versión se regeneran, los que ya tienen
# ésta se saltean. Es lo que hace el script resumible y permite rollouts parciales.
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "v5.1-sin-ausencias-2026-09-01")
COLLECTION_V2 = os.getenv("COLLECTION_NAME_V2", "reviews_embeddings_v2")
CONCURRENCIA = int(os.getenv("REGEN_CONCURRENCIA", "6"))

_lock = threading.Lock()
_stats = {"ok": 0, "vacios": 0, "errores": 0, "in_tok": 0, "out_tok": 0}


def asegurar_columnas(engine):
    """Crea las columnas shadow si no existen. Aditivo: no toca las que ya usa producción."""
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE lugares
                ADD COLUMN IF NOT EXISTS resumen_reviews_v2 TEXT,
                ADD COLUMN IF NOT EXISTS resumen_prompt_version TEXT,
                ADD COLUMN IF NOT EXISTS resumen_generado_at TIMESTAMPTZ
        """))
    print("✅ Columnas shadow verificadas (resumen_reviews_v2, resumen_prompt_version, resumen_generado_at)")


def nombres_del_golden_dataset():
    """Lugares que el golden dataset usa como ground truth (~48)."""
    ruta = os.getenv("GOLDEN_DATASET_PATH", r"D:\que-morfamos-backend\golden_dataset.json")
    with open(ruta, encoding="utf-8") as f:
        casos = json.load(f)
    nombres = set()
    for c in casos:
        nombres.update(c.get("expected_restaurants", []))
    return sorted(n for n in nombres if not n.startswith("<TODO"))


# Features a revisar y los términos con que aparecen. Se usa para armar la muestra dirigida:
# no sirve medir un prompt de recall sobre lugares que no tienen nada que recuperar.
FEATURES_A_MEDIR = {
    "pet friendly": ["pet friendly", "pet-friendly", "mascota", "perro"],
    "wifi": ["wifi", "wi-fi"],
    "terraza": ["terraza", "patio", "al aire libre"],
    "sin tacc": ["sin tacc", "sin gluten", "celiac"],
    "vegano": ["vegano", "vegana", "veggie"],
    "musica en vivo": ["musica en vivo", "shows en vivo", "banda en vivo"],
    "pelotero": ["pelotero", "juegos para chicos", "juegos infantiles"],
}


def lugares_con_feature_perdida(engine, limit=40):
    """Lugares donde las reseñas confirman una feature y el resumen NO la menciona.

    Es la muestra dirigida para iterar un prompt de recall: medir sobre el golden dataset o sobre
    una muestra al azar diluye la señal con lugares que no tienen nada que recuperar. Acá cada
    lugar es un fallo conocido, así que la mejora se lee directo.

    Medido el 01-sep-2026, el resumen sólo captura entre el 43% y el 77% de las features que las
    reseñas confirman — y el resumen es la única evidencia que lee el ranking del backend.
    """
    condiciones = []
    for terminos in FEATURES_A_MEDIR.values():
        rev = " OR ".join([f"r.texto ILIKE '%{t}%'" for t in terminos])
        res = " OR ".join([f"l.resumen_reviews ILIKE '%{t}%'" for t in terminos])
        condiciones.append(f"(SUM(CASE WHEN {rev} THEN 1 ELSE 0 END) >= 2 AND NOT ({res}))")
    having = " OR ".join(condiciones)
    sql = f"""
        SELECT l.id, l.nombre, count(r.*) AS n_reviews
        FROM lugares l
        JOIN reviews r ON r.lugar_id = l.id AND length(r.texto) > 30
        GROUP BY l.id, l.nombre, l.resumen_reviews
        HAVING count(r.*) >= 5 AND ({having})
        ORDER BY count(r.*) DESC
        LIMIT {int(limit)}
    """
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def lugares_pendientes(engine, limit=None, solo_golden=False):
    """Lugares con reseñas suficientes que todavía no tienen la versión objetivo."""
    sql = """
        SELECT l.id, l.nombre, count(r.*) AS n_reviews
        FROM lugares l
        JOIN reviews r ON r.lugar_id = l.id AND length(r.texto) > 30
        WHERE l.resumen_prompt_version IS DISTINCT FROM :v
    """
    params = {"v": PROMPT_VERSION}
    if solo_golden:
        sql += " AND l.nombre = ANY(:golden)"
        params["golden"] = nombres_del_golden_dataset()
    sql += """
        GROUP BY l.id, l.nombre
        HAVING count(r.*) >= 5
        ORDER BY count(r.*) DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def procesar_lugar(engine, lugar_id, nombre):
    with engine.connect() as conn:
        reviews = [{"texto": r[0], "rating": r[1]} for r in conn.execute(text("""
            SELECT texto, rating_user FROM reviews
            WHERE lugar_id = :id AND texto IS NOT NULL AND length(texto) > 30
        """), {"id": lugar_id}).fetchall()]

    resumen = generar_resumen_reviews(reviews, nombre_lugar=nombre)

    # generar_resumen_reviews devuelve "" si el texto quedó truncado incluso tras el reintento:
    # se prefiere no guardar nada antes que guardar un resumen amputado (así se generaron los 127
    # resúmenes cortados que hay hoy en producción).
    if not resumen or len(resumen.strip()) < 100:
        with _lock:
            _stats["vacios"] += 1
        return nombre, False, "resumen vacío o demasiado corto"

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE lugares
            SET resumen_reviews_v2 = :r,
                resumen_prompt_version = :v,
                resumen_generado_at = NOW()
            WHERE id = :id
        """), {"r": resumen, "v": PROMPT_VERSION, "id": lugar_id})

    with _lock:
        _stats["ok"] += 1
        _stats["in_tok"] += sum(min(len(x["texto"]), 1000) for x in reviews[:50]) // 4
        _stats["out_tok"] += len(resumen) // 4
    return nombre, True, None


def regenerar(engine, limit=None, solo_golden=False, objetivo=None):
    # `objetivo` permite pasar una lista ya elegida (ej. la muestra dirigida de --faltantes) en
    # vez de dejar que la seleccione la consulta de pendientes.
    pendientes = objetivo if objetivo is not None else lugares_pendientes(engine, limit, solo_golden)
    total = len(pendientes)
    if not total:
        print(f"✅ Nada pendiente: todos los lugares ya tienen la versión {PROMPT_VERSION}")
        return

    print(f"🔄 {total} lugares pendientes | proveedor={os.getenv('SUMMARY_PROVIDER','deepseek')} "
          f"| concurrencia={CONCURRENCIA} | versión={PROMPT_VERSION}\n")
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=CONCURRENCIA) as pool:
        futuros = {pool.submit(procesar_lugar, engine, lid, nom): nom
                   for lid, nom, _ in pendientes}
        for i, fut in enumerate(as_completed(futuros), 1):
            nombre = futuros[fut]
            try:
                _, ok, err = fut.result()
                marca = "✅" if ok else f"⚠️  {err}"
            except Exception as e:
                with _lock:
                    _stats["errores"] += 1
                marca = f"❌ {type(e).__name__}: {str(e)[:60]}"
            if i % 25 == 0 or not marca.startswith("✅"):
                print(f"  [{i}/{total}] {marca} {nombre[:45]}")

    dur = time.time() - t0
    costo = _stats["in_tok"] / 1e6 * 0.15 + _stats["out_tok"] / 1e6 * 0.60
    print(f"\n{'='*64}")
    print(f"OK: {_stats['ok']} | vacíos/truncados: {_stats['vacios']} | errores: {_stats['errores']}")
    print(f"Tiempo: {dur/60:.1f} min | costo estimado (gpt-4o-mini): ${costo:.3f}")
    print(f"\nProducción sigue leyendo `resumen_reviews` — nada se pisó.")
    print(f"Siguiente paso: python regenerar_resumenes.py --embeddings")


def estimar(engine):
    pendientes = lugares_pendientes(engine)
    ids = tuple(p[0] for p in pendientes) or (0,)
    with engine.connect() as conn:
        # `muestreo_estrategico` toma como MUCHO 50 reseñas por lugar (cada una truncada a 1000
        # chars), así que el input es min(n_reseñas, 50) × largo_promedio — no la suma de todas.
        chars = conn.execute(text("""
            SELECT coalesce(sum(least(n, 50) * avg_len), 0) FROM (
              SELECT lugar_id, count(*) AS n, avg(least(length(texto), 1000)) AS avg_len
              FROM reviews
              WHERE length(texto) > 30 AND lugar_id IN :ids
              GROUP BY lugar_id
            ) t
        """), {"ids": ids}).scalar()
    overhead_prompt = len(pendientes) * 400  # instrucciones fijas del prompt, por lugar
    in_tok = (float(chars) + overhead_prompt) / 4
    out_tok = len(pendientes) * 1250 / 4
    print(f"Lugares pendientes: {len(pendientes)}")
    print(f"Tokens estimados: ~{in_tok:,.0f} input / ~{out_tok:,.0f} output")
    for nombre, pin, pout in [("gpt-4o-mini", 0.15, 0.60), ("gpt-4.1-nano", 0.10, 0.40)]:
        print(f"  {nombre:<16} ${in_tok/1e6*pin + out_tok/1e6*pout:.3f}")
    print(f"  embeddings      ${out_tok/1e6*0.02:.4f}")
    print("\n(--dry-run: no se llamó a ninguna API ni se escribió nada)")


def generar_embeddings(engine):
    """Genera los embeddings del v2 en una COLECCIÓN NUEVA, sin tocar la que sirve producción."""
    from langchain_openai import OpenAIEmbeddings
    from langchain_postgres import PGVector
    from langchain_core.documents import Document

    with engine.connect() as conn:
        filas = conn.execute(text("""
            SELECT nombre, resumen_reviews_v2, rating_gral, direccion, zona, barrio, categoria
            FROM lugares
            WHERE resumen_prompt_version = :v AND length(resumen_reviews_v2) > 50
        """), {"v": PROMPT_VERSION}).fetchall()

    if not filas:
        print("❌ No hay resúmenes v2. Corré primero la regeneración.")
        return

    url = os.environ["DATABASE_URL"]
    for viejo, nuevo in (("postgres://", "postgresql+psycopg://"), ("postgresql://", "postgresql+psycopg://")):
        if url.startswith(viejo):
            url = url.replace(viejo, nuevo, 1)
            break

    store = PGVector(
        connection=url,
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        collection_name=COLLECTION_V2,
        use_jsonb=True,
    )

    # Vaciar la colección shadow antes de repoblarla: sin esto, re-correr tras una regeneración
    # nueva deja los vectores viejos conviviendo con los nuevos y el vector search devuelve
    # duplicados/versiones mezcladas. Sólo toca la colección v2, nunca la de producción.
    with engine.begin() as conn:
        borrados = conn.execute(text("""
            DELETE FROM langchain_pg_embedding
            WHERE collection_id IN (SELECT uuid FROM langchain_pg_collection WHERE name = :c)
        """), {"c": COLLECTION_V2}).rowcount
    if borrados:
        print(f"🧹 Colección '{COLLECTION_V2}' vaciada ({borrados} vectores viejos)")

    docs = [
        Document(
            page_content=f["resumen_reviews_v2"] if isinstance(f, dict) else f[1],
            metadata={
                "nombre": f[0], "rating": float(f[2] or 0), "direccion": f[3] or "",
                "zona": f[4] or "", "barrio": f[5] or "", "categoria": f[6] or "",
            },
        )
        for f in filas
    ]

    print(f"🧠 Generando {len(docs)} embeddings en la colección '{COLLECTION_V2}'...")
    for i in range(0, len(docs), 100):
        store.add_documents(docs[i:i + 100])
        print(f"   {min(i+100, len(docs))}/{len(docs)}")

    print(f"\n✅ Colección '{COLLECTION_V2}' lista. La de producción "
          f"('{os.getenv('COLLECTION_NAME', 'reviews_embeddings')}') quedó intacta.")
    print("   Para evaluar: correr el benchmark del backend apuntando a la colección nueva.")


def main():
    engine = create_engine(os.environ["DATABASE_URL"])

    # Se crean siempre, incluso en --dry-run: son columnas nuevas y nullables (aditivas), no tocan
    # ningún dato existente, y el conteo de pendientes necesita consultarlas.
    asegurar_columnas(engine)

    if "--dry-run" in sys.argv:
        estimar(engine)
        return

    if "--embeddings" in sys.argv:
        generar_embeddings(engine)
        return

    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    solo_golden = "--golden" in sys.argv

    # Muestra dirigida a los fallos conocidos de recall de features (ver la función).
    if "--faltantes" in sys.argv:
        objetivo = lugares_con_feature_perdida(engine, limit or 40)
        print(f"🎯 Muestra dirigida: {len(objetivo)} lugares con features en las reseñas que el "
              f"resumen actual NO menciona.")
        regenerar(engine, None, False, objetivo=objetivo)
        return

    # Guardrail de costo. Iterar el prompt NO requiere regenerar los 791 lugares: la señal para
    # decidir si un prompt mejoró ("¿las features del ground truth siguen siendo detectables?") se
    # obtiene con los ~48 del golden dataset, a ~2 centavos por iteración en vez de ~$0.33.
    # La corrida completa sólo hace falta para el BENCHMARK, porque ahí el vector search compite
    # contra el corpus entero. En la sesión del 26-ago-2026 se gastaron $1.32 en cuatro corridas
    # completas cuando alcanzaba con tres iteraciones en modo --golden y UNA completa al final.
    if not (solo_golden or limit or "--todo" in sys.argv):
        pendientes = lugares_pendientes(engine)
        costo = len(pendientes) * 0.00042  # medido: ~$0.33 / 791 lugares
        print(f"⚠️  Vas a regenerar los {len(pendientes)} lugares (~${costo:.2f}, ~13 min).")
        print("   Si estás ITERANDO el prompt, no hace falta: usá `--golden` (~48 lugares, ~$0.02)")
        print("   y mirá cuántas features del ground truth siguen siendo detectables.")
        print("   La corrida completa sólo hace falta para correr el benchmark de punta a punta.")
        print("\n   Para confirmar la corrida completa: agregá --todo")
        return

    regenerar(engine, limit, solo_golden)


if __name__ == "__main__":
    main()
