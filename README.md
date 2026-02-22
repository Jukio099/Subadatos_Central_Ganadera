# 🐄 ETL Mercado Ganadero — Central Ganadera de Medellín

## Estructura del proyecto

```
mercado_ganadero/
├── etl/
│   ├── extract.py      # Descarga los PDFs del sitio web
│   ├── transform.py    # Extrae los datos de los PDFs
│   ├── load.py         # Sube los datos a Supabase
│   └── main.py         # Ejecuta las 3 fases en orden
├── requirements.txt    # Librerías necesarias
├── .env                # Tus credenciales (NO subir a GitHub)
└── pdfs/               # Carpeta de PDFs descargados (se crea automáticamente)
```

---

## 🚀 Cómo empezar (paso a paso)

### 1. Instala las dependencias

```bash
pip install -r requirements.txt
```

### 2. Crea tu cuenta en Supabase

1. Ve a https://supabase.com y crea una cuenta gratis
2. Crea un nuevo proyecto (elige servidor en **US East** para menor latencia)
3. Espera 2 minutos a que se configure
4. Ve a **Settings → API** y copia:
   - `Project URL` (algo como `https://xxxx.supabase.co`)
   - `anon public key`

### 3. Crea el archivo `.env`

Crea un archivo llamado `.env` en la raíz del proyecto:

```
SUPABASE_URL=https://tuproyecto.supabase.co
SUPABASE_KEY=tu_anon_key_aqui
```

⚠️ **IMPORTANTE:** Agrega `.env` a tu `.gitignore` para no subir tus credenciales a GitHub.

### 4. Crea la tabla en Supabase

1. Abre tu proyecto en Supabase
2. Ve a **SQL Editor**
3. Pega y ejecuta este SQL:

```sql
CREATE TABLE IF NOT EXISTS subastas (
    id                  BIGSERIAL PRIMARY KEY,
    fecha_subasta       DATE,
    numero_boletin      INTEGER,
    tipo_subasta        VARCHAR(50),
    tipo_codigo         VARCHAR(5),
    tipo_descripcion    VARCHAR(100),
    peso_total_kg       NUMERIC,
    peso_promedio_kg    NUMERIC,
    procedencia         VARCHAR(100),
    hora_subasta        VARCHAR(30),
    precio_base_kg      NUMERIC,
    precio_final_kg     NUMERIC,
    precio_total_cop    NUMERIC,
    archivo_fuente      VARCHAR(200),
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subastas_fecha ON subastas(fecha_subasta);
CREATE INDEX IF NOT EXISTS idx_subastas_tipo  ON subastas(tipo_subasta, tipo_codigo);
```

### 5. Corre el ETL

```bash
# Desde la carpeta etl/
cd etl
python main.py
```

Esto va a:
1. Descargar todos los PDFs de la Central Ganadera (~12 páginas)
2. Extraer los datos de cada lote de cada subasta
3. Subir todo a Supabase

---

## 📊 Columnas del dato final

| Columna | Tipo | Descripción |
|---|---|---|
| fecha_subasta | DATE | Fecha de la subasta |
| numero_boletin | INT | Número del boletín (ej. 7) |
| tipo_subasta | TEXT | Tradicional / Especial GYR / Equina |
| tipo_codigo | TEXT | HV, ML, MC, AT, VH, HL... |
| tipo_descripcion | TEXT | Hembra de vientre, Macho de levante... |
| peso_total_kg | NUMERIC | Peso total del lote en kg |
| peso_promedio_kg | NUMERIC | Peso promedio por animal en kg |
| procedencia | TEXT | Municipio de origen |
| precio_base_kg | NUMERIC | Precio base por kg (COP) |
| precio_final_kg | NUMERIC | Precio final por kg (COP) |
| precio_total_cop | NUMERIC | Precio total del lote (COP) |

---

## ❓ Problemas frecuentes

**El script no extrae datos de un PDF:**
- Puede ser un PDF escaneado (imagen). Abre el PDF manualmente y mira si el texto es seleccionable.
- Reporta el nombre del archivo para ajustar el parser.

**Error de conexión a Supabase:**
- Verifica que el archivo `.env` existe y tiene las variables correctas.
- Verifica que la tabla `subastas` fue creada en el SQL Editor.

**El precio sale en 0:**
- Algunos lotes en el PDF tienen precio en 0 (animales sin vender). Son filtrados automáticamente.
