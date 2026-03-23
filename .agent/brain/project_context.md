# Project Context

## Uso

Este archivo guarda contexto tecnico relativamente estable del proyecto.
No es una bitacora diaria; para eso usar `docs/session_log.md`.

## Regla de trabajo

- Comando de cierre sugerido: `cerrar sesion`
- Al cierre se actualiza `docs/session_log.md`
- Si hubo cambios estructurales o decisiones duraderas, tambien se actualiza este archivo

## Estado actual del proyecto

### ETL principal

- `etl/main.py` orquesta extraccion, transformacion y carga a Supabase.
- `etl/extract.py` descarga PDFs de forma incremental consultando `archivo_fuente` en Supabase antes de bajar cada archivo.
- `etl/transform.py` extrae metadata desde el contenido del PDF, usa fallbacks por nombre de archivo, normaliza procedencias y parsea lotes con estrategia estricta + fallback + diagnostico.
- `etl/load.py` hace `upsert` por lotes a `subastas` y elimina duplicados logicos antes de cargar.

### ETL de features externas

- `etl/fetch_features_externas.py` construye la tabla `features_externas` con clima, IPC y precio del maiz.
- El script guarda `sql/create_features_externas.sql` y puede pedir confirmacion interactiva si no se usa `--no-interactive`.

### GitHub Actions

- `.github/workflows/etl.yml` ejecuta el ETL programado miercoles y jueves, usando `python etl/main.py --solo-nuevos`.
- `.github/workflows/keepalive.yml` sigue apuntando a una URL placeholder `https://TU-APP.streamlit.app` y requiere ajuste para ser funcional.

### Observaciones importantes

- El plan en `.agent/brain/implementation_plan.md` no refleja el estado actual del ETL.
- El codigo actual esta mas avanzado en robustez operativa que lo descrito en ese plan.
