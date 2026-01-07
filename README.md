# Qué Morfamos - Data Extraction & Vectorization Pipeline

Este repositorio contiene la lógica de Ingeniería de Datos y ETL para el backend del sistema de recomendación "Qué Morfamos" (enfocado en la ciudad de Neuquén).

El objetivo principal es automatizar el mantenimiento de la base de conocimiento del asistente de IA, asegurando que las recomendaciones se basen en las reseñas más recientes de Google Maps, con mínima intervención manual.

## Arquitectura del Flujo de Datos

Para garantizar la seguridad de los datos y cumplir con buenas prácticas de desarrollo, este sistema opera con una arquitectura desacoplada:

1. **Código Público (Este Repo):** Contiene la lógica de scraping, limpieza y vectorización.
2. **Orquestación:** GitHub Actions ejecuta los scripts de forma programada (CRON).
3. **Almacenamiento Seguro:** Los datos crudos (CSV) se sincronizan automáticamente con un repositorio privado mediante Personal Access Tokens, mientras que los vectores se envían a Pinecone.

## Workflows Automatizados

El sistema consta de dos pipelines independientes:

### 1. Discovery Pipeline (Descubrimiento)
* **Frecuencia:** Mensual / A demanda.
* **Función:** Rastrea Google Maps buscando nuevos establecimientos en la zona objetivo.
* **Salida:** Genera un dataset de "Candidatos" en el repositorio privado para ser validados, evitando que lugares erróneos entren al sistema de producción.

### 2. Enrichment Pipeline (Enriquecimiento Vectorial)
* **Frecuencia:** Semanal.
* **Entrada:** Lee la lista maestra de restaurantes validados.
* **Proceso:**
    * Extrae las reseñas más recientes (ordenadas por fecha).
    * Genera IDs únicos (hashing) para evitar duplicados.
    * Genera Embeddings de texto.
* **Salida:** Realiza un **Upsert Incremental** a la base de datos vectorial (Pinecone), actualizando solo la información nueva.

## Tech Stack

* **Lenguaje:** Python 3.10
* **Extracción:** Selenium / Undetected-Chromedriver (Headless)
* **Procesamiento:** Pandas, BeautifulSoup
* **Base Vectorial:** Pinecone
* **CI/CD:** GitHub Actions

## Disclaimer

Este proyecto fue desarrollado con fines académicos y educativos como parte de un portafolio de Data Science. Los datos extraídos no se distribuyen en este repositorio.
