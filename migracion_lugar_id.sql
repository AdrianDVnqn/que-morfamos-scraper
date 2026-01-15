-- ============================================================
-- MIGRACIÓN: Agregar relación formal entre lugares y reviews
-- Fecha: 2026-01-14
-- ============================================================
-- INSTRUCCIONES:
-- 1. Correr este script en el SQL Editor de Supabase
-- 2. Ejecutar cada sección por separado para verificar
-- ============================================================

-- ============================================================
-- PASO 1: Diagnóstico previo (SOLO LECTURA)
-- ============================================================
-- Verificar cuántas reviews hay y cuántas matchean con lugares

SELECT 
    'Total reviews' as metrica,
    COUNT(*) as valor
FROM reviews

UNION ALL

SELECT 
    'Reviews con match en lugares' as metrica,
    COUNT(*) as valor
FROM reviews r
INNER JOIN lugares l ON r.restaurante = l.nombre

UNION ALL

SELECT 
    'Reviews SIN match (huérfanas)' as metrica,
    COUNT(*) as valor
FROM reviews r
LEFT JOIN lugares l ON r.restaurante = l.nombre
WHERE l.id IS NULL;

-- ============================================================
-- PASO 2: Ver reviews huérfanas (opcional, para investigar)
-- ============================================================
-- Descomentar si querés ver cuáles no matchean:

-- SELECT DISTINCT r.restaurante
-- FROM reviews r
-- LEFT JOIN lugares l ON r.restaurante = l.nombre
-- WHERE l.id IS NULL
-- ORDER BY r.restaurante;

-- ============================================================
-- PASO 3: Agregar columna lugar_id a reviews
-- ============================================================

ALTER TABLE reviews 
ADD COLUMN IF NOT EXISTS lugar_id INTEGER;

-- ============================================================
-- PASO 4: Migrar datos - Llenar lugar_id basándose en nombre
-- ============================================================

UPDATE reviews r
SET lugar_id = l.id
FROM lugares l
WHERE r.restaurante = l.nombre
  AND r.lugar_id IS NULL;  -- Solo actualizar los que no tienen

-- Verificar cuántos se actualizaron
SELECT 
    COUNT(*) FILTER (WHERE lugar_id IS NOT NULL) as "Con lugar_id",
    COUNT(*) FILTER (WHERE lugar_id IS NULL) as "Sin lugar_id (huérfanas)",
    COUNT(*) as "Total"
FROM reviews;

-- ============================================================
-- PASO 5: Crear índice para performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_reviews_lugar_id ON reviews(lugar_id);

-- ============================================================
-- PASO 6: Agregar Foreign Key (DESPUÉS de verificar que todo está bien)
-- ============================================================
-- ⚠️ IMPORTANTE: Solo correr esto si NO hay reviews huérfanas
-- o si estás OK con que esas reviews queden con lugar_id = NULL

-- Opción A: FK que permite NULL (recomendado si hay huérfanas)
ALTER TABLE reviews 
ADD CONSTRAINT fk_reviews_lugar 
FOREIGN KEY (lugar_id) REFERENCES lugares(id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- ============================================================
-- PASO 7: Verificación final
-- ============================================================

-- Ver que la relación existe
SELECT 
    tc.table_name, 
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name = 'reviews';

-- Estadísticas finales
SELECT 
    'Lugares totales' as metrica, COUNT(*) as valor FROM lugares
UNION ALL
SELECT 
    'Reviews totales' as metrica, COUNT(*) as valor FROM reviews
UNION ALL
SELECT 
    'Reviews vinculadas' as metrica, COUNT(*) as valor 
    FROM reviews WHERE lugar_id IS NOT NULL;

-- ============================================================
-- ¡LISTO! Ahora el diagrama de Supabase debería mostrar la relación
-- ============================================================
