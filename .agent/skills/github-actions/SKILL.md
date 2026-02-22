---
name: github-actions
description: >
  Aplica este skill cuando el usuario pida crear, modificar o revisar
  workflows de GitHub Actions para este proyecto. Cubre el ETL automático
  (etl.yml), despliegue del dashboard y buenas prácticas de CI/CD para
  el stack Python + Supabase + Streamlit.
---

# Skill: GitHub Actions — Mercado Ganadero

## Contexto del proyecto
- **ETL**: `etl/main.py` — descarga PDFs de Central Ganadera, transforma y sube a Supabase
- **Dashboard**: `modelo/app.py` — Streamlit, desplegado en Streamlit Cloud
- **Base de datos**: Supabase (PostgreSQL gestionado)
- **Python**: 3.11 · **OS runner**: `ubuntu-latest`

## Secrets de GitHub requeridos

| Secret | Descripción |
|--------|-------------|
| `SUPABASE_URL` | URL del proyecto Supabase (ej. `https://xxxx.supabase.co`) |
| `SUPABASE_KEY` | Clave `service_role` o `anon` de Supabase |

> **IMPORTANTE**: Nunca usar el archivo `.env` en GitHub Actions.
> Las credenciales se inyectan como variables de entorno desde los Secrets.

### Cómo agregar los Secrets en GitHub
1. Ir al repositorio → **Settings** → **Secrets and variables** → **Actions**
2. Clic en **New repository secret**
3. Agregar `SUPABASE_URL` y `SUPABASE_KEY` con sus valores reales

---

## Workflow del ETL automático — `etl.yml`

### Archivo: `.github/workflows/etl.yml`

```yaml
name: ETL — Central Ganadera

on:
  schedule:
    - cron: "0 11 * * 1"   # Lunes 06:00 hora Colombia (UTC-5 = UTC+11 invertido → 11:00 UTC)
  workflow_dispatch:         # Ejecución manual desde GitHub

jobs:
  etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Instalar dependencias
        run: pip install -r requirements.txt

      - name: Ejecutar ETL
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        run: python etl/main.py --solo-nuevos
```

### Notas importantes
- **Cron**: `0 11 * * 1` = lunes 11:00 UTC = 06:00 hora Colombia (UTC-5)
- **`--solo-nuevos`**: Solo procesa PDFs que aún no están en Supabase, evitando duplicados
- **Notificación de fallo**: GitHub Actions envía email automáticamente al dueño del repo si el job falla
- **Ejecución manual**: `workflow_dispatch` permite correr el ETL en cualquier momento desde la UI de GitHub

---

## Agregar resumen al workflow

Para que GitHub muestre un resumen de cuántos registros se insertaron,
añade este step al final del job (el main.py ya imprime el reporte):

```yaml
      - name: Mostrar resumen del ETL
        if: always()
        run: |
          echo "### 📊 Reporte ETL" >> $GITHUB_STEP_SUMMARY
          echo "Revisa los logs del step 'Ejecutar ETL' para ver el detalle de registros insertados." >> $GITHUB_STEP_SUMMARY
```

---

## Buenas prácticas para este proyecto

1. **Nunca** hardcodear `SUPABASE_URL` o `SUPABASE_KEY` en el código ni en el yml
2. Usar `actions/setup-python@v5` con `cache: "pip"` para acelerar instalaciones
3. Usar `--solo-nuevos` en el schedule para evitar reprocesar datos históricos;
   usar el ETL completo (sin flag) solo para re-sincronización total manual
4. Los PDFs descargados **no** se guardan como artefactos (son temporales, ya están en Supabase)
5. El CSV de respaldo (`datos_subastas.csv`) se genera localmente pero no se sube al repo
   (está en `.gitignore`)

---

## Plantilla rápida para nuevo workflow

```yaml
name: <nombre>
on:
  schedule:
    - cron: "<cron UTC>"
  workflow_dispatch:
jobs:
  <job>:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"
      - run: pip install -r requirements.txt
      - name: <paso>
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        run: <comando>
```
