"""
Utilidades para interactuar con el LLM que genera los resúmenes de reseñas.

Antes era `deepseek_utils.py` y estaba cableado a DeepSeek; se renombró y se hizo multi-proveedor
el 26-ago-2026, cuando DeepSeek se quedó sin saldo y el job semanal del scraper dejó de poder
generar resúmenes. El proveedor se elige con la env var SUMMARY_PROVIDER (deepseek | openai |
gemini) y el modelo con SUMMARY_MODEL; todos hablan el protocolo OpenAI-compatible.
"""
import os
import re
import random
import requests
import logging

logger = logging.getLogger(__name__)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

# Proveedor para generar resúmenes. Se agregó cuando DeepSeek se quedó sin saldo (26-ago-2026):
# el backend ya había migrado a OpenAI, pero este módulo seguía cableado a DeepSeek, así que el
# job semanal del scraper no podía generar resúmenes. Todos los proveedores soportados hablan el
# mismo protocolo (OpenAI-compatible), así que sólo cambian URL, key y modelo.
_PROVIDERS = {
    "deepseek": (DEEPSEEK_API_URL, "DEEPSEEK_API_KEY", "deepseek-chat"),
    "openai": ("https://api.openai.com/v1/chat/completions", "OPENAI_API_KEY", "gpt-4o-mini"),
    "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
               "GOOGLE_API_KEY", "gemini-3.5-flash-lite"),
}


def _resolver_proveedor():
    """Devuelve (url, api_key, modelo) según SUMMARY_PROVIDER (default: deepseek)."""
    nombre = os.getenv("SUMMARY_PROVIDER", "deepseek").lower()
    if nombre not in _PROVIDERS:
        raise ValueError(f"SUMMARY_PROVIDER desconocido: {nombre}. Opciones: {list(_PROVIDERS)}")
    url, key_env, modelo_default = _PROVIDERS[nombre]
    api_key = os.getenv(key_env)
    if not api_key:
        raise ValueError(f"Falta {key_env} para SUMMARY_PROVIDER={nombre}")
    return url, api_key, os.getenv("SUMMARY_MODEL", modelo_default)


def limpiar_texto(texto):
    """
    Limpia texto para reducir tokens innecesarios.
    - "....." → "."
    - "!!!!" → "!"
    - Múltiples espacios → uno solo
    """
    if not texto:
        return ""
    
    # Reducir puntuación repetida
    texto = re.sub(r'\.{2,}', '.', texto)  # ... → .
    texto = re.sub(r'!{2,}', '!', texto)   # !!! → !
    texto = re.sub(r'\?{2,}', '?', texto)  # ??? → ?
    texto = re.sub(r'-{2,}', '-', texto)   # --- → -
    
    # Reducir repetición excesiva de letras (ej: "holaaaa" -> "hola")
    # Detecta cualquier carácter repetido 3 o más veces y lo deja en 1
    texto = re.sub(r'(.)\1{2,}', r'\1', texto)
    
    # Reducir espacios
    texto = re.sub(r'\s+', ' ', texto)
    
    return texto.strip()


def _call_llm(messages, max_tokens=500, temperature=0.3, devolver_finish_reason=False):
    """Llama al proveedor configurado en SUMMARY_PROVIDER.

    Con devolver_finish_reason=True devuelve (texto, finish_reason) en vez de sólo el texto.
    """
    url, api_key, modelo = _resolver_proveedor()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": modelo,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    choice = response.json()["choices"][0]
    texto = choice["message"]["content"].strip()
    if devolver_finish_reason:
        # finish_reason == "length" significa que el modelo se quedó sin presupuesto de tokens y
        # el texto quedó cortado. Es la señal autoritativa de la API: mucho más confiable que
        # adivinar por puntuación final.
        return texto, choice.get("finish_reason")
    return texto


# Características concretas y buscables, agrupadas por concepto. Las reseñas que las mencionan
# tienen prioridad en el muestreo: son poquísimas (2 de 379 para "pelotero" en Parrilla Rancho
# Grande) y con un muestreo por recencia/largo/rating quedaban afuera SIEMPRE, así que el LLM
# nunca veía la evidencia y el resumen no podía mencionar la característica. El bug estaba
# enmascarado mientras el prompt alucinaba features; al corregirlo quedó a la vista.
#
# Se agrupa por concepto (y no una lista plana) para poder dar CUOTA POR CARACTERÍSTICA: con una
# lista plana, un lugar con 30 reseñas sobre "vegano" y 2 sobre "pelotero" llenaba todos los
# lugares reservados con las veganas y el pelotero se perdía igual.
FEATURE_GROUPS = {
    "niños": ["pelotero", "juegos", "juego para", "hamaca", "tobogan", "tobogán", "calesita",
              "infantil", "para chicos", "para niños"],
    "sin_gluten": ["celiac", "celíac", "gluten", "tacc"],
    "vegano": ["vegan", "vegetarian", "plant based"],
    "lactosa": ["lactosa", "deslactosad"],
    "estacionamiento": ["estacionamiento", "estacionar", "cochera", "parking"],
    "wifi": ["wifi", "wi-fi"],
    "mascotas": ["mascota", "pet friendly", "perro"],
    "accesibilidad": ["silla de ruedas", "accesib", "rampa"],
}

# Cuántas reseñas se reservan por característica encontrada. Con 3 alcanza para que el LLM vea
# evidencia repetida (y no la tome por un comentario suelto) sin comerse el muestreo general.
CUPO_POR_FEATURE = 3


def _features_de(texto):
    """Devuelve el conjunto de características que menciona una reseña."""
    t = str(texto or "").lower()
    return {g for g, terms in FEATURE_GROUPS.items() if any(x in t for x in terms)}


def muestreo_estrategico(reviews, total=50):
    """
    Selecciona reseñas estratégicamente para maximizar diversidad.

    Estrategia:
    - Hasta CUPO_POR_FEATURE reseñas por CADA característica encontrada (pelotero, sin TACC,
      vegano, estacionamiento...), para que ninguna quede sin evidencia por ser minoritaria
    - 20 más recientes (actualidad)
    - 10 más largas (más informativas)
    - 10 con ratings extremos (1-2★ y 5★)
    - el resto, aleatorias (diversidad)

    Args:
        reviews: lista de dicts con 'texto', 'rating', 'fecha'
        total: cantidad total a seleccionar

    Returns:
        lista de dicts (reviews completas) seleccionadas
    """
    if len(reviews) <= total:
        return [r for r in reviews if r.get('texto')]

    seleccionadas = set()
    items_seleccionados = []

    def agregar(review):
        if id(review) not in seleccionadas and review.get('texto'):
            seleccionadas.add(id(review))
            items_seleccionados.append(review)

    # 0. PRIORIDAD: reseñas que mencionan características concretas, con cupo POR característica.
    # Van primero justamente porque son escasas: si no se reservan, el resto de los criterios las
    # tapa. El cupo por grupo evita que una característica abundante (ej. "vegano") se coma los
    # lugares de una rara (ej. "pelotero", que puede tener 2 reseñas en 379).
    cupos = {g: 0 for g in FEATURE_GROUPS}
    for r in reviews:
        grupos = _features_de(r.get('texto'))
        if any(cupos[g] < CUPO_POR_FEATURE for g in grupos):
            agregar(r)
            for g in grupos:
                cupos[g] += 1

    # 1. 20 más recientes (ya vienen ordenadas por fecha DESC)
    for r in reviews[:20]:
        agregar(r)
    
    # 2. 10 más largas
    por_largo = sorted(reviews, key=lambda x: len(str(x.get('texto', ''))), reverse=True)
    for r in por_largo[:10]:
        agregar(r)
    
    # 3. 10 con ratings extremos
    extremos = [r for r in reviews if r.get('rating') in [1, 2, 5]]
    random.shuffle(extremos)
    for r in extremos[:10]:
        agregar(r)
    
    # 4. Rellenar con aleatorias hasta llegar al total
    restantes = [r for r in reviews if id(r) not in seleccionadas]
    random.shuffle(restantes)
    for r in restantes:
        if len(items_seleccionados) >= total:
            break
        agregar(r)
    
    return items_seleccionados


# El prompt está versionado porque `regenerate_embeddings.py` escribe en la columna de
# PRODUCCIÓN (`resumen_reviews`) en la corrida semanal: sin esto, iterar el prompt acá cambiaría
# en silencio lo que genera el cron. La variante se elige por env var y el default es la que hoy
# corre en producción, así que probar v5 no toca nada hasta que se promueva a mano.
VARIANTE_PROMPT = os.getenv("RESUMEN_PROMPT_VARIANT", "v4")

# v5 ataca un problema MEDIDO (01-sep-2026): el resumen se come entre un tercio y la mitad de las
# características que las reseñas sí confirman, y el resumen es la única evidencia que lee el
# ranking del backend. Cobertura del resumen sobre lugares con >=2 reseñas que mencionan la
# feature: pet friendly 54%, wifi 43%, música en vivo 44%, terraza 63%, sin TACC 63%, vegano 77%.
#
# La causa es que v4 sólo tiene un FILTRO ("mencionala nada más si las reseñas la confirman") y
# ninguna instrucción de RECALL: nunca le pide al modelo que las busque activamente, así que
# menciona las que le llamaron la atención. v5 agrega la lista a repasar, conservando intacto el
# filtro de confirmación — que es lo que evita que repasar la lista se convierta en inventarla.
EXTRA_V5 = """
     REPASO OBLIGATORIO antes de cerrar el párrafo 3: recorré esta lista y, POR CADA ÍTEM,
     preguntate si alguna reseña lo confirma. Si lo confirma, NOMBRALO; si ninguna lo menciona,
     omitilo en silencio (no escribas que falta, no lo aclares):
       opciones sin TACC / celíaco · opciones veganas o vegetarianas · sin lactosa ·
       juegos para chicos o pelotero · apto mascotas · estacionamiento · wifi ·
       mesas afuera, patio o terraza · música en vivo · delivery o takeaway ·
       accesibilidad para sillas de ruedas · apto para grupos grandes · desayuno o merienda
     No es una lista para completar: es una lista para REVISAR. Un ítem sin respaldo en las
     reseñas no va, y nombrar de más es peor que omitir.
     Preferí la palabra que usan las reseñas antes que un sinónimo tuyo.

     PROHIBIDO decir que algo NO se menciona. La lista de arriba es para vos, no para el lector:
     él no tiene que enterarse de que la revisaste. Si una característica no está respaldada,
     simplemente NO APARECE en el texto — sin aclaración, sin "no se especifica", sin "no hay
     información sobre".
       MAL:  "no se mencionan características como opciones sin TACC, juegos para niños o
              estacionamiento"
       MAL:  "aunque no se menciona la disponibilidad de estacionamiento"
       BIEN: (esa oración no existe; el párrafo habla sólo de lo que las reseñas sí confirman)
     Un resumen más corto pero enteramente afirmativo es MEJOR que uno largo lleno de ausencias."""


def generar_resumen_reviews(reviews_data, nombre_lugar=""):
    """
    Genera un resumen estructurado usando muestreo estratégico.
    """
    items = []
    
    # Compatibilidad hacia atrás: si recibe lista de strings
    if reviews_data and isinstance(reviews_data[0], str):
        items = [{'texto': t, 'rating': '?'} for t in reviews_data if t and len(str(t).strip()) > 20][:50]
    else:
        # 1. Filtrar primero las reseñas válidas (> 30 chars, consistencia con regenerate_embeddings)
        # Esto asegura que para lugares chicos (ej: 40 reseñas válidas) usemos TODAS
        valid_items = [i for i in reviews_data if i.get('texto') and len(str(i['texto']).strip()) > 30]
        
        # Si hay menos de 5 reseñas válidas, generamos un resumen genérico
        if len(valid_items) < 5:
            return "No se cuenta con suficiente información sobre este lugar para generar un resumen detallado."
        
        # 2. Muestreo estratégico solo sobre las válidas
        items = muestreo_estrategico(valid_items, total=50)
    
    if not items:
        return ""
    
    # Limpiar y concatenar reseñas con rating (1000 chars cada una)
    formatted_reviews = []
    for item in items:
        txt = limpiar_texto(str(item.get('texto', '')))[:1000]
        pts = item.get('rating', '?')
        formatted_reviews.append(f"[{pts}★] {txt}")
        
    reseñas_concat = "\n---\n".join(formatted_reviews)
    
    extra_v5 = EXTRA_V5 if VARIANTE_PROMPT == "v5" else ""

    prompt = f"""Actúa como un experto en SEO gastronómico y Data Science. 
Tu objetivo es generar un "Perfil Semántico Rico" para el restaurante "{nombre_lugar}" basado en sus reseñas.
Este texto será convertido en vectores (embeddings), por lo que debe estar optimizado para búsqueda semántica.

INSTRUCCIONES:
1. **Lenguaje Natural Denso:** No uses listas con viñetas ni JSON. Usa oraciones completas y fluidas.
2. **Palabras Clave (Keywords):** Usá vocabulario natural que ayude a la búsqueda semántica (ej: "económico", "romántico", "con amigos"), pero SOLO si está respaldado por las reseñas. NUNCA agregues una palabra clave específica y verificable (ej: "pelotero", "celíaco", "estacionamiento", "wifi", "apto mascotas") solo porque es un término "probable" para este tipo de negocio — si ninguna reseña la menciona, no la nombres ni en positivo ni en negativo.
3. **Manejo de Negaciones:** Los embeddings confunden "No es caro" con "Es caro".
   - MAL: "No tiene estacionamiento" → BIEN: "Sin estacionamiento propio"
   - MAL: "No es apto celíacos" → BIEN: "Solo opciones con gluten"
4. **Inconsistencias:** Si hay opiniones divididas (ej: algunos elogian la atención, otros la critican), usa frases como "atención variable según el turno" o "experiencia inconsistente en el servicio".
5. **Estructura del Texto:** Genera un solo bloque de texto con 3 párrafos lógicos sin títulos:
   - Párrafo 1: Tipo de lugar, especialidad, ambiente, ocasiones ideales
   - Párrafo 2: Puntos fuertes, puntos débiles, precio, atención
   - Párrafo 3: Características específicas SOLO si las reseñas las confirman (TACC, vegano, niños, estacionamiento, ubicación) — no completes esta lista "por las dudas": omití cualquiera que ninguna reseña respalde.{extra_v5}
6. **Ni inventar ni negar — reportá lo que dicen las reseñas:**
   - Si NINGUNA reseña menciona una característica verificable (pelotero, apto celíaco, sin TACC,
     estacionamiento, wifi, apto mascotas), NO la afirmes ni la niegues: omitila por completo.
   - Si AL MENOS UNA reseña la confirma, AFIRMALA de forma clara y positiva, aunque sea una sola
     mención entre muchas reseñas. Una característica poco comentada sigue siendo real: que pocos
     la mencionen no la convierte en escasa ni en inexistente.
   - MAL: una reseña dice "pedimos un desayuno sin TACC y tuvimos variedad" y el resumen escribe
     "escasez de opciones sin gluten". BIEN: "cuenta con opciones sin TACC".
   - MAL: una reseña dice "es la única que tiene juegos infantiles" y el resumen escribe "falta de
     opciones para ciertos grupos". BIEN: "tiene juegos infantiles".
   - Sólo describí una característica como ausente o limitada si alguna reseña lo dice
     explícitamente; nunca por no haberla encontrado.

RESEÑAS PARA ANALIZAR:
{reseñas_concat}

Genera SOLO el texto descriptivo final, sin introducción ni comentarios."""

    messages = [{"role": "user", "content": prompt}]

    # max_tokens=800 (antes 500): con el cap viejo el 14% de los resúmenes de producción quedaban
    # cortados a mitad de palabra y el 19% perdía el tercer párrafo — justo el que lista las
    # características (TACC, vegano, niños, estacionamiento) que el Juez del backend usa como
    # evidencia. Con el prompt corregido los resúmenes miden ~306 tokens (máx. medido: 336), así
    # que 800 da margen de sobra; aun así NO se confía en el margen: si la API avisa que cortó
    # (finish_reason == "length"), se reintenta con el doble de presupuesto. Subir el cap no
    # encarece nada por sí solo — se paga por tokens generados.
    for cap in (800, 1600):
        try:
            # temperature=0 (antes 0.3): esto es un pipeline de generación de DATOS, no de
            # redacción creativa. Con 0.3 dos corridas sobre las mismas reseñas producían resúmenes
            # distintos — a veces mencionando una característica y a veces no — lo que hace
            # imposible reproducir un resultado o atribuir una mejora al prompt en vez de al azar.
            texto, finish_reason = _call_llm(
                messages, max_tokens=cap, temperature=0, devolver_finish_reason=True
            )
        except Exception as e:
            logger.error(f"Error generando resumen: {e}")
            return ""

        if finish_reason != "length":
            return texto

        logger.warning(
            f"Resumen de '{nombre_lugar}' cortado por el cap de {cap} tokens "
            f"(finish_reason=length); reintentando con más presupuesto."
        )

    logger.error(
        f"Resumen de '{nombre_lugar}' sigue cortado con 1600 tokens. Se descarta para no guardar "
        f"un texto amputado en la base."
    )
    return ""


def detectar_info_nueva(resumen_actual, reseñas_nuevas_textos):
    """
    Detecta si las reseñas nuevas aportan información que no está en el resumen actual.
    
    Args:
        resumen_actual: str con el resumen existente del lugar
        reseñas_nuevas_textos: lista de strings con los textos de las reseñas nuevas
    
    Returns:
        bool: True si hay información nueva relevante, False si no
    """
    # Filtrar reseñas vacías o muy cortas
    textos_validos = [t for t in reseñas_nuevas_textos if t and len(str(t).strip()) > 20]
    
    if not textos_validos:
        return False
    
    if not resumen_actual or len(resumen_actual.strip()) < 10:
        return True  # Si no hay resumen, siempre regenerar
    
    reseñas_concat = "\n---\n".join([str(t)[:200] for t in textos_validos[:10]])
    
    prompt = f"""Tengo este resumen existente de un restaurante:
"{resumen_actual}"

Y estas reseñas nuevas:
{reseñas_concat}

¿Las reseñas nuevas mencionan algo IMPORTANTE que NO esté ya reflejado en el resumen? 
(Por ejemplo: un servicio nuevo, una queja diferente, una característica única no mencionada)

Responde SOLO con "SI" o "NO"."""

    messages = [{"role": "user", "content": prompt}]
    
    try:
        respuesta = _call_llm(messages, max_tokens=10, temperature=0.1)
        return respuesta.upper().startswith("SI")
    except Exception as e:
        logger.error(f"Error detectando info nueva: {e}")
        return True  # En caso de error, regenerar por seguridad
