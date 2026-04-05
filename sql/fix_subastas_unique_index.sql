-- Actualiza la llave unica de `subastas` para que el ETL haga upsert por
-- archivo + numero de lote, evitando colisiones entre lotes distintos.
--
-- Ejecutar en Supabase SQL Editor.

BEGIN;

DROP INDEX IF EXISTS idx_subastas_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_subastas_unique
ON subastas(archivo_fuente, numero_lote);

COMMIT;

-- Verificacion rapida
SELECT
    archivo_fuente,
    numero_lote,
    COUNT(*) AS repeticiones
FROM subastas
GROUP BY archivo_fuente, numero_lote
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC, archivo_fuente, numero_lote;
