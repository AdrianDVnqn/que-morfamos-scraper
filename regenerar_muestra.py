"""
Regeneración de MUESTRA de resúmenes — paso previo a la regeneración completa.

Genera resúmenes nuevos con el prompt corregido (el que ya no inventa características) para un
puñado de lugares, y los compara contra los que están hoy en producción. **No escribe nada en la
base**: sólo produce un archivo de comparación para revisar a mano antes de gastar en el lote
completo.

Incluye además un chequeo automático de "groundedness": para cada característica verificable que
el resumen afirme (pelotero, sin TACC, estacionamiento, etc.), busca si alguna reseña CRUDA la
menciona. Es lo que se hizo a mano para descubrir el bug de "Parrillas Gatica", automatizado.

Uso:
    SUMMARY_PROVIDER=openai python regenerar_muestra.py
    SUMMARY_PROVIDER=openai python regenerar_muestra.py --n 30
"""
import os
import re
import sys
import unicodedata

if os.path.exists("mis_claves.env"):
    from dotenv import load_dotenv
    load_dotenv("mis_claves.env")

from sqlalchemy import create_engine, text
from llm_utils import generar_resumen_reviews

SALIDA = "comparacion_resumenes.md"

# Lugares donde ya confirmamos que el resumen viejo alucina (sesión 25-ago-2026): son el control
# del experimento — si el prompt nuevo funciona, acá tiene que dejar de afirmar el pelotero.
CONTROLES = [
    "Parrillas Gatica",
    "Frosts Frineve Creams",
    "Un Altra Volta",
    "Heladería Costa Piré",
]

# Características verificables: si el resumen las afirma, tiene que haber evidencia en reseñas.
# Cada entrada es (etiqueta, [variantes a buscar en el texto crudo]).
# OJO con las variantes: se buscan como substring, así que hay que cubrir las distintas formas
# de la palabra. La primera versión sólo buscaba el sustantivo "estacionamiento" y marcó a
# Heladería Costa Piré como alucinación, cuando en realidad sus reseñas dicen "difícil para
# estacionar" (verbo) — falso positivo del chequeo, no del resumen. Preferir raíces cortas.
CARACTERISTICAS = [
    ("pelotero", ["peloter", "juego", "hamaca", "tobogan", "calesita"]),
    ("sin TACC / celíaco", ["sin tacc", "celiac", "sin gluten", "libre de gluten"]),
    ("vegano", ["vegan", "vegetarian"]),
    ("estacionamiento", ["estacion", "cochera", "parking", "aparcar", "estacionar"]),
    ("wifi", ["wifi", "wi-fi", "internet"]),
    ("apto mascotas", ["mascota", "pet friendly", "perro"]),
]

NEGACION = re.compile(r"\b(no|ni|sin|carece|falta|ausencia)\b")


def norm(t):
    t = unicodedata.normalize("NFD", str(t or "").lower())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def afirma(resumen, variantes):
    """True si el resumen menciona la característica SIN negación cercana hacia atrás."""
    r = norm(resumen)
    for v in variantes:
        for m in re.finditer(re.escape(norm(v)), r):
            if not NEGACION.search(r[max(0, m.start() - 70):m.start()]):
                return True
    return False


def hay_evidencia(reviews, variantes):
    """Cuántas reseñas crudas mencionan alguna variante (sin importar polaridad)."""
    return sum(1 for rv in reviews if any(norm(v) in norm(rv.get("texto")) for v in variantes))


def auditar(resumen, reviews):
    """Devuelve las características que el resumen afirma sin respaldo en reseñas crudas."""
    sin_respaldo = []
    for etiqueta, variantes in CARACTERISTICAS:
        if afirma(resumen, variantes) and hay_evidencia(reviews, variantes) == 0:
            sin_respaldo.append(etiqueta)
    return sin_respaldo


def main():
    n = 20
    if "--n" in sys.argv:
        n = int(sys.argv[sys.argv.index("--n") + 1])

    engine = create_engine(os.environ["DATABASE_URL"])

    with engine.connect() as conn:
        controles = conn.execute(text("""
            SELECT id, nombre, resumen_reviews FROM lugares WHERE nombre = ANY(:n)
        """), {"n": CONTROLES}).fetchall()

        # El resto: los más reseñados (mayor impacto en producción), excluyendo los controles.
        resto = conn.execute(text("""
            SELECT id, nombre, resumen_reviews FROM lugares
            WHERE resumen_reviews IS NOT NULL AND length(resumen_reviews) > 50
              AND NOT (nombre = ANY(:n))
            ORDER BY total_reviews_google DESC LIMIT :lim
        """), {"n": CONTROLES, "lim": max(0, n - len(controles))}).fetchall()

        lugares = list(controles) + list(resto)
        print(f"Regenerando {len(lugares)} resúmenes "
              f"({len(controles)} controles + {len(resto)} más reseñados)...")
        print(f"Proveedor: {os.getenv('SUMMARY_PROVIDER', 'deepseek')}\n")

        salida = ["# Comparación de resúmenes: producción vs prompt corregido\n",
                  "Generado por `regenerar_muestra.py`. **No se escribió nada en la base.**\n"]
        alucinaciones_antes = alucinaciones_despues = 0

        for i, (lugar_id, nombre, resumen_viejo) in enumerate(lugares, 1):
            reviews = [dict(r._mapping) for r in conn.execute(text("""
                SELECT texto, rating_user AS rating FROM reviews
                WHERE lugar_id = :id AND texto IS NOT NULL AND length(texto) > 30
            """), {"id": lugar_id}).fetchall()]

            if len(reviews) < 5:
                print(f"  [{i}/{len(lugares)}] {nombre}: sólo {len(reviews)} reseñas válidas, salteado")
                continue

            try:
                resumen_nuevo = generar_resumen_reviews(reviews, nombre_lugar=nombre)
            except Exception as e:
                print(f"  [{i}/{len(lugares)}] {nombre}: ERROR -> {e}")
                continue

            malas_antes = auditar(resumen_viejo, reviews)
            malas_despues = auditar(resumen_nuevo, reviews)
            alucinaciones_antes += len(malas_antes)
            alucinaciones_despues += len(malas_despues)

            marca = "🔬 CONTROL" if nombre in CONTROLES else ""
            estado = "✅" if not malas_despues else "⚠️"
            print(f"  [{i}/{len(lugares)}] {estado} {nombre} {marca} "
                  f"| sin respaldo antes: {malas_antes or '-'} → después: {malas_despues or '-'}")

            salida.append(f"\n---\n\n## {nombre} {marca}\n")
            salida.append(f"*{len(reviews)} reseñas válidas*\n")
            salida.append(f"\n**Afirmaciones sin respaldo — ANTES:** {malas_antes or 'ninguna'}\n")
            salida.append(f"\n**Afirmaciones sin respaldo — DESPUÉS:** {malas_despues or 'ninguna'}\n")
            salida.append(f"\n### Resumen actual (producción)\n\n{resumen_viejo}\n")
            salida.append(f"\n### Resumen nuevo (prompt corregido)\n\n{resumen_nuevo}\n")

        salida.insert(2, f"\n**Total de afirmaciones sin respaldo en reseñas crudas: "
                         f"{alucinaciones_antes} antes → {alucinaciones_despues} después.**\n")

        with open(SALIDA, "w", encoding="utf-8") as f:
            f.write("\n".join(salida))

        print(f"\n{'='*60}")
        print(f"Afirmaciones sin respaldo: {alucinaciones_antes} antes → {alucinaciones_despues} después")
        print(f"Comparación completa en: {SALIDA}")


if __name__ == "__main__":
    main()
