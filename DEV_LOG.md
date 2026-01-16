# 📔 Bitácora de Desarrollo - Scraper

Registro de cambios técnicos, fixes y decisiones de diseño en el scraper de reseñas.

## 📅 Sesión: 15 de Enero de 2026

### 🛠️ Fixes y Mejoras
- **Integración con Dashboard Legacy:** Se modificó `monitor_reviews.py` para que invoque `log_scraping_event`.
    - **Problema:** El nuevo script de monitoreo diario llenaba `review_history` pero ignoraba `scraping_logs`, lo que hacía que el gráfico "Timeline" del dashboard principal dejara de actualizarse el 10/01.
    - **Solución:** Ahora cada lote procesado por el monitor también genera una entrada en `scraping_logs` con estado "EXITO".
- **Comportamiento del Monitor:** Se confirmó que el "salto" de 362k reviews en el delta de hoy se debe a la inicialización de la tabla histórica (`Day 0`), y se espera que se normalice en la siguiente ejecución.

---
*Bitácora iniciada automáticamente por Antigravity Agent.*

## 📅 Sesión: 16 de Enero de 2026

### 🛠️ Fixes y Mejoras
- **Corrección de Tipos SQL en Logs:** Se solucionó el error `operator does not exist: text > timestamp` en `db_utils.py`.
    - **Causa:** La columna `fecha_scraping` en la tabla `reviews` es de tipo TEXT (ISO string), pero se comparaba directamente contra un objeto datetime en las consultas de "Info nueva" y estadísticas.
    - **Solución:** Se agregó un cast explícito `::timestamp` en las cláusulas WHERE afectadas (`get_reviews_nuevas_sin_embedding` y `obtener_estadisticas`).
