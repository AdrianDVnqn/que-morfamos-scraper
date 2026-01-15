# Qué Morfamos - Data Pipeline & Intelligent Embeddings

Este repositorio contiene la infraestructura de Ingeniería de Datos para el sistema de recomendación gastronómica "Qué Morfamos", enfocado en la ciudad de Neuquén, Argentina.

El objetivo es mantener actualizada la base de conocimiento de un asistente conversacional de IA, asegurando que las recomendaciones reflejen las opiniones más recientes de los usuarios de Google Maps.

## Contexto del Proyecto

Este desarrollo forma parte de mi portfolio como estudiante de la Maestría en Ciencia de Datos (MCD) de la Universidad Nacional del Comahue. El sistema integra conceptos de:

- Web Scraping y ETL automatizado
- Bases de datos relacionales (PostgreSQL/Supabase)
- Generación de embeddings semánticos con LLMs
- Orquestación de pipelines con GitHub Actions
- Diseño de arquitecturas de datos para RAG (Retrieval-Augmented Generation)

## Arquitectura General

El sistema opera con una arquitectura de tres capas:

```
┌─────────────────────────────────────────────────────────────────┐
│                     GITHUB ACTIONS (Orquestación)               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Discovery    │  │ Review       │  │ Embedding            │  │
│  │ Pipeline     │  │ Monitor      │  │ Regeneration         │  │
│  │ (mensual)    │  │ (semanal)    │  │ (condicional)        │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SUPABASE (PostgreSQL)                      │
│  ┌───────────┐    ┌───────────┐    ┌───────────────────────┐   │
│  │ lugares   │◄───│ reviews   │    │ langchain_pg_embedding│   │
│  │ (937)     │ FK │ (170k+)   │    │ (vectores semánticos) │   │
│  └───────────┘    └───────────┘    └───────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PRODUCTOS DERIVADOS                          │
│  ┌──────────────────┐    ┌──────────────────────────────────┐  │
│  │ Dashboard        │    │ API de Recomendaciones (FastAPI) │  │
│  │ (Next.js)        │    │ + Chatbot Conversacional         │  │
│  └──────────────────┘    └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Workflows Automatizados

### 1. Discovery Pipeline
- **Frecuencia:** Mensual
- **Script:** `restaurant-scraper.py`
- **Función:** Descubre nuevos establecimientos gastronómicos en Google Maps dentro de la zona de Neuquén.
- **Validación:** Los candidatos pasan por un clasificador LLM que filtra lugares no gastronómicos.

### 2. Review Monitor
- **Frecuencia:** Semanal (domingos)
- **Script:** `monitor_reviews.py`
- **Función:** Monitorea el crecimiento de reseñas en los lugares registrados. Utiliza early-stopping para evitar re-procesar reseñas existentes.
- **Optimización:** Solo scrapea reseñas nuevas detectando coincidencias con la última review almacenada.

### 3. Embedding Regeneration
- **Frecuencia:** Condicional (cuando hay nuevas reviews)
- **Script:** `regenerate_embeddings.py`
- **Función:** Genera resúmenes semánticos de cada lugar usando DeepSeek y los vectoriza con OpenAI Embeddings para alimentar el sistema RAG.

### 4. Auxiliary Workflows
- `asignar-barrios.yml`: Asigna barrio y zona geográfica a lugares nuevos usando geometría espacial.
- `check-discrepancies.yml`: Valida consistencia entre conteos reportados y reales.

## Estructura de la Base de Datos

El schema utiliza relaciones formales con foreign keys para mantener integridad referencial:

| Tabla | Descripción | Relación |
|-------|-------------|----------|
| `lugares` | Establecimientos gastronómicos (937 registros) | PK: `id` |
| `reviews` | Reseñas de usuarios (~170k registros) | FK: `lugar_id` → `lugares.id` |
| `review_history` | Histórico de crecimiento de reviews | FK: `lugar_id` → `lugares.id` |
| `scraping_logs` | Logs de ejecución de scrapers | FK: `lugar_id` → `lugares.id` |
| `langchain_pg_embedding` | Vectores semánticos para RAG | - |

## Stack Tecnológico

| Componente | Tecnología |
|------------|------------|
| Lenguaje | Python 3.10 |
| Scraping | Selenium + ChromeDriver (headless) |
| Base de Datos | Supabase (PostgreSQL) |
| Embeddings | OpenAI text-embedding-3-small |
| Summarization | DeepSeek Chat API |
| Orquestación | GitHub Actions |
| Notificaciones | Discord Webhooks |

## Scripts Principales

| Script | Función |
|--------|---------|
| `restaurant-scraper.py` | Descubrimiento de nuevos lugares |
| `opiniones-scraper.py` | Extracción de reseñas |
| `monitor_reviews.py` | Monitoreo de crecimiento |
| `regenerate_embeddings.py` | Generación de embeddings |
| `db_utils.py` | Utilidades de conexión a Supabase |
| `notificador.py` | Notificaciones centralizadas a Discord |

## Repositorios Relacionados

Este scraper forma parte de un ecosistema más amplio:

- **que-morfamos-scraper** (este repo): Pipeline de datos
- **que-morfamos** (backend): API FastAPI + lógica de recomendación
- **que-morfamos-web** (frontend): Interfaz de usuario React
- **que-morfamos-dashboard**: Panel de monitoreo Next.js

## Disclaimer

Este proyecto fue desarrollado con fines académicos como parte del portfolio de la Maestría en Ciencia de Datos (MCD) de la Universidad Nacional del Comahue. El código se comparte con propósitos educativos; los datos extraídos no se distribuyen públicamente.
