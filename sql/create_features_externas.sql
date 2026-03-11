
-- EJECUTAR ESTO EN SUPABASE SQL EDITOR ANTES DE CORRER EL SCRIPT
-- Dashboard > SQL Editor > New query > pegar todo > Run

CREATE TABLE IF NOT EXISTS features_externas (
    fecha_mes            DATE        NOT NULL PRIMARY KEY,
    lluvia_acum_mm       FLOAT,
    temp_max_prom_c      FLOAT,
    et0_prom_mm          FLOAT,
    ipc_var_mensual_pct  FLOAT,
    precio_maiz_usd_ton  FLOAT,
    actualizado_en       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fe_fecha ON features_externas(fecha_mes);

-- Verificar:
SELECT * FROM features_externas ORDER BY fecha_mes DESC LIMIT 12;
