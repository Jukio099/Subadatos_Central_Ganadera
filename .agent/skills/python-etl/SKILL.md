---
name: python-etl
description: >
  Aplica este skill cuando el usuario pida escribir, modificar o revisar
  cualquier script del ETL: extract.py, transform.py, load.py, main.py,
  o cualquier archivo dentro de la carpeta etl/. También aplica cuando
  el usuario mencione scraping, PDFs, pdfplumber, pandas, limpieza de datos
  o carga a Supabase.
---

# Skill: Python ETL — Mercado Ganadero

## Contexto del proyecto
Estamos construyendo un ETL para extraer datos de subastas ganaderas de
centralganadera.com. Los datos vienen en PDFs públicos con tablas de lotes.
El stack es: requests + BeautifulSoup → pdfplumber → pandas → Supabase.

## Reglas obligatorias para todo código ETL

### 1. Entorno y dependencias
- Siempre asumir que el código corre dentro del entorno virtual `.venv`
- Nunca instalar librerías con `pip install X` directamente en el código
- Si se necesita una librería nueva, agregarla a `requirements.txt`
- Usar `python-dotenv` para todas las credenciales. Nunca hardcodear URLs o keys

### 2. Estructura de archivos
```
etl/
├── extract.py      # SOLO descarga y scraping. No procesa datos.
├── transform.py    # SOLO parsing de PDFs y limpieza. No descarga ni sube.
├── load.py         # SOLO carga a Supabase. No extrae ni transforma.
└── main.py         # Orquesta los 3 pasos en orden.
```
Cada archivo tiene UNA sola responsabilidad. No mezclar lógica entre archivos.

### 3. Estilo de código
- Comentarios y docstrings SIEMPRE en español
- Nombres de variables y funciones en snake_case en español:
  ✅ `precio_final_kg`, `extraer_links_pdf`, `limpiar_numero`
  ❌ `finalPrice`, `extractLinks`, `cleanNumber`
- Cada función debe tener docstring con: qué hace, parámetros, retorno
- Usar type hints en todas las funciones:
  ✅ `def limpiar_numero(texto: str) -> float | None:`
  ❌ `def limpiar_numero(texto):`

### 4. Manejo de errores
- SIEMPRE usar try/except en: requests, pdfplumber, operaciones de Supabase
- Usar emojis consistentes en los prints:
  - ✅ operación exitosa
  - ❌ error
  - ⚠️ advertencia / dato inesperado pero no fatal
  - ⬇️ descarga en progreso
  - 🚀 inicio de proceso
  - 📊 estadísticas / resumen
- Nunca dejar que un error en un PDF detenga el procesamiento de los demás

### 5. Scraping responsable
- Siempre incluir `time.sleep(1.5)` entre requests al mismo dominio
- Usar el User-Agent definido en extract.py. No cambiar sin razón.
- Verificar si un PDF ya fue descargado antes de volver a bajarlo
- Si una página no tiene PDFs, terminar el loop (no seguir paginando)

### 6. Procesamiento de PDFs
- Usar `pdfplumber` (no PyPDF2, no pdfminer directamente)
- El texto de los PDFs de Central Ganadera sale desordenado: las columnas
  se mezclan. Usar regex para reconstruir las filas.
- Tipos de animal válidos: HV, HL, MC, ML, AT, VH, T2, R, M1, M3, Y, P2
- Filtrar siempre lotes con peso = 0 o precio = 0 (son lotes no vendidos)
- Los números colombianos usan punto como separador de miles: 1.234.567
  Función para limpiarlos: `texto.replace(".", "").replace(",", ".")`

### 7. Carga a Supabase
- Siempre usar upsert (no insert) para evitar duplicados
- Subir en lotes de máximo 200 registros
- Convertir tipos de pandas a tipos nativos de Python antes de insertar
  (int64 → int, float64 → float, Timestamp → str)
- La tabla en Supabase se llama `subastas`

## Schema de la tabla `subastas`
```sql
fecha_subasta    DATE
numero_boletin   INTEGER
tipo_subasta     VARCHAR(50)   -- 'Tradicional', 'Especial GYR', 'Equina'
tipo_codigo      VARCHAR(5)    -- 'HV', 'ML', 'AT', etc.
tipo_descripcion VARCHAR(100)
peso_total_kg    NUMERIC
peso_promedio_kg NUMERIC
procedencia      VARCHAR(100)
hora_subasta     VARCHAR(30)
precio_base_kg   NUMERIC
precio_final_kg  NUMERIC
precio_total_cop NUMERIC
archivo_fuente   VARCHAR(200)
```

## Ejemplo de función bien escrita
```python
def limpiar_numero(texto: str) -> float | None:
    """
    Convierte texto de número colombiano a float.
    
    Los PDFs usan punto como separador de miles: '1.234.567'
    Esta función lo convierte a float: 1234567.0
    
    Parámetros:
        texto: string con el número a limpiar (ej: '1.234.567')
    
    Retorna:
        float con el valor numérico, o None si no es convertible
    """
    if not texto or str(texto).strip() in ("", "0", "-"):
        return None
    limpio = str(texto).replace(".", "").replace(",", ".").strip()
    try:
        return float(limpio)
    except ValueError:
        return None
```
