"""
Utilidades para interactuar con DeepSeek API.
Usado para generar resúmenes de reseñas y detectar información nueva.
"""
import os
import re
import random
import requests
import logging

logger = logging.getLogger(__name__)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"


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


def _call_deepseek(messages, max_tokens=500, temperature=0.3):
    """Llama a la API de DeepSeek"""
    if not DEEPSEEK_API_KEY:
        raise ValueError("DEEPSEEK_API_KEY no configurada")
    
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "deepseek-chat",
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    
    response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    
    return response.json()["choices"][0]["message"]["content"].strip()


def muestreo_estrategico(reviews, total=50):
    """
    Selecciona reseñas estratégicamente para maximizar diversidad.
    
    Estrategia:
    - 20 más recientes (actualidad)
    - 10 más largas (más informativas)
    - 10 con ratings extremos (1-2★ y 5★)
    - 10 aleatorias (diversidad)
    
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


def generar_resumen_reviews(reviews_data, nombre_lugar=""):
    """
    Genera un resumen estructurado usando muestreo estratégico.
    """
    items = []
    
    # Compatibilidad hacia atrás: si recibe lista de strings
    if reviews_data and isinstance(reviews_data[0], str):
        items = [{'texto': t, 'rating': '?'} for t in reviews_data if t and len(str(t).strip()) > 20][:50]
    else:
        # 1. Filtrar primero las reseñas válidas (> 20 chars)
        # Esto asegura que para lugares chicos (ej: 40 reseñas válidas) usemos TODAS
        valid_items = [i for i in reviews_data if i.get('texto') and len(str(i['texto']).strip()) > 20]
        
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
    
    prompt = f"""Actúa como un experto en SEO gastronómico y Data Science. 
Tu objetivo es generar un "Perfil Semántico Rico" para el restaurante "{nombre_lugar}" basado en sus reseñas.
Este texto será convertido en vectores (embeddings), por lo que debe estar optimizado para búsqueda semántica.

INSTRUCCIONES:
1. **Lenguaje Natural Denso:** No uses listas con viñetas ni JSON. Usa oraciones completas y fluidas.
2. **Palabras Clave (Keywords):** Incluye explícitamente términos de búsqueda probables (ej: "económico", "romántico", "celíaco", "con amigos", "estacionamiento", "para niños", "pelotero").
3. **Manejo de Negaciones:** Los embeddings confunden "No es caro" con "Es caro". 
   - MAL: "No tiene estacionamiento" → BIEN: "Sin estacionamiento propio"
   - MAL: "No es apto celíacos" → BIEN: "Solo opciones con gluten"
4. **Inconsistencias:** Si hay opiniones divididas (ej: algunos elogian la atención, otros la critican), usa frases como "atención variable según el turno" o "experiencia inconsistente en el servicio".
5. **Estructura del Texto:** Genera un solo bloque de texto con 3 párrafos lógicos sin títulos:
   - Párrafo 1: Tipo de lugar, especialidad, ambiente, ocasiones ideales
   - Párrafo 2: Puntos fuertes, puntos débiles, precio, atención
   - Párrafo 3: Características específicas (TACC, vegano, niños, estacionamiento, ubicación)

RESEÑAS PARA ANALIZAR:
{reseñas_concat}

Genera SOLO el texto descriptivo final, sin introducción ni comentarios."""

    messages = [{"role": "user", "content": prompt}]
    
    try:
        return _call_deepseek(messages, max_tokens=500, temperature=0.3)
    except Exception as e:
        logger.error(f"Error generando resumen: {e}")
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
        respuesta = _call_deepseek(messages, max_tokens=10, temperature=0.1)
        return respuesta.upper().startswith("SI")
    except Exception as e:
        logger.error(f"Error detectando info nueva: {e}")
        return True  # En caso de error, regenerar por seguridad
