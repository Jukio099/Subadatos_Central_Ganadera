-- Tabla para precios agregados públicos de Subastar.
-- Ejecutar en Supabase antes de activar la opción "Subastar" en Streamlit.

create table if not exists public.subastar_precios_resumen (
  id bigserial primary key,
  fuente text not null default 'subastar',
  sede text not null,
  anio integer not null,
  evento_id text not null,
  evento_numero_reporte text,
  fecha_evento date not null,
  fecha_reporte date,
  clasificacion text not null,
  tipo_codigo text not null,
  edad text not null default '',
  cantidad_animales integer not null,
  precio_max_kg integer,
  precio_min_kg integer,
  precio_promedio_kg integer,
  peso_promedio_kg integer,
  valor_promedio_animal integer,
  url_fuente text,
  archivo_fuente text,
  hash_registro text not null,
  creado_en timestamptz not null default now(),
  actualizado_en timestamptz not null default now(),
  constraint subastar_precios_resumen_unique unique (
    sede,
    anio,
    evento_id,
    fecha_evento,
    clasificacion,
    tipo_codigo,
    edad
  )
);

create index if not exists idx_subastar_precios_resumen_fecha
  on public.subastar_precios_resumen (fecha_evento desc);

create index if not exists idx_subastar_precios_resumen_sede_fecha
  on public.subastar_precios_resumen (sede, fecha_evento desc);

create index if not exists idx_subastar_precios_resumen_tipo_fecha
  on public.subastar_precios_resumen (tipo_codigo, fecha_evento desc);
