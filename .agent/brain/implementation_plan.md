# Plan de Implementacion Vivo

Este archivo reemplaza el plan viejo del dashboard y resume el estado real del proyecto.

## Estado actual

### Dashboard

- `modelo/app.py` ya incluye el KPI `Tendencia 1 mes`.
- `modelo/app.py` ya incluye las tabs `Ultima Subasta` y `Tendencias`.
- La estructura actual de tabs ya no coincide con el plan historico anterior porque ese trabajo fue implementado.

### ETL principal

- `etl/extract.py` descarga PDFs desde Central Ganadera y consulta Supabase para evitar reprocesar archivos ya cargados.
- `etl/transform.py` extrae metadata desde el contenido del PDF y usa fallbacks por nombre de archivo.
- `etl/load.py` hace `upsert` por lotes a Supabase.
- `etl/main.py` orquesta el pipeline y publica resumen en GitHub Actions cuando corre en CI.

## Problemas operativos detectados

### 1. Cambios de formato en PDFs recientes

- Algunos PDFs nuevos usan horas tipo `AM` / `PM` en vez de `a. m.` / `p. m.`.
- Eso rompe el parser de lotes para subastas recientes si no se contempla el nuevo formato.

### 2. Ruido en la extraccion de links

- La pagina de resultados incluye otros PDFs no relacionados con subastas, como informes o politicas.
- Si no se filtran, el ETL intenta descargarlos y genera ruido, errores 404 y falsos positivos.

### 3. Exploracion innecesaria de paginas historicas

- Para operacion semanal normalmente basta con revisar `page/1`.
- Recorrer todas las paginas solo tiene sentido para backfill o recuperacion historica.

### 4. Observabilidad del workflow

- El workflow debe distinguir entre:
  - sin PDFs nuevos
  - PDFs nuevos con transformacion exitosa
  - PDFs nuevos pero 0 registros validos

## Prioridades actuales

### P1

- Mantener estable la deteccion y parseo de PDFs recientes.
- Filtrar mejor los links de extraccion.
- Hacer que el workflow falle o alerte claramente cuando haya PDFs nuevos pero 0 datos validos.

### P2

- Mejorar el soporte para formatos especiales de equinos y mulares si reaparecen como fuente activa.
- Seguir fortaleciendo resumentes de ejecucion y contexto de sesiones.

### P3

- Revisar futuras mejoras del dashboard solo despues de estabilizar ETL y workflows.

## Convenciones de trabajo

- `docs/session_log.md`: bitacora cronologica por sesion.
- `.agent/brain/project_context.md`: contexto tecnico estable.
- `cerrar sesion`: comando estandar para actualizar memoria del proyecto.
