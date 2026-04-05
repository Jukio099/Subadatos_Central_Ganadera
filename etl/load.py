"""
load.py
=======
Paso 3 del ETL: sube los datos limpios a Supabase (PostgreSQL).

Antes de correr este script:
1. Crea una cuenta gratis en https://supabase.com
2. Crea un proyecto nuevo
3. Ve a Settings > API y copia tu URL y anon key
4. Crea un archivo .env con esas variables (ver instrucciones abajo)
5. Ejecuta el SQL de la tabla (ver TABLA_SQL abajo) en el SQL Editor de Supabase

Autor: Tu nombre
Fecha: 2026
"""

import os
import pandas as pd
from datetime import datetime, date
from supabase import create_client, Client
from dotenv import load_dotenv

# ─── CARGA DE VARIABLES DE ENTORNO ────────────────────────────────────────────
# Crea un archivo .env en la raíz del proyecto con este contenido:
#   SUPABASE_URL=https://xxxxxxxxxxxx.supabase.co
#   SUPABASE_KEY=tu_anon_key_aqui
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ─── SQL PARA CREAR LA TABLA EN SUPABASE ──────────────────────────────────────
# Pega este SQL en el SQL Editor de Supabase y ejecútalo UNA sola vez:
TABLA_SQL = """
CREATE TABLE IF NOT EXISTS subastas (
    id                  BIGSERIAL PRIMARY KEY,
    fecha_subasta       DATE,
    numero_boletin      INTEGER,
    tipo_subasta        VARCHAR(30),       -- 'Tradicional', 'Especial GYR', 'Equina'
    numero_lote         VARCHAR(10),       -- El ID del lote en el PDF (ej. '001')
    tipo_codigo         VARCHAR(5),        -- 'HV', 'ML', 'AT', etc.
    cantidad_animales   INTEGER,           -- Cantidad de animales en el lote
    peso_total_kg       REAL,              -- Peso total del lote en kg
    peso_promedio_kg    REAL,              -- Peso promedio por animal en kg
    procedencia         VARCHAR(100),      -- Municipio de origen
    hora_subasta        VARCHAR(30),       -- Hora de la puja
    precio_base_kg      REAL,              -- Precio base por kg en COP
    precio_final_kg     REAL,              -- Precio final por kg en COP
    archivo_fuente      VARCHAR(200),      -- Nombre del PDF original
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

-- Evitar duplicados si se corre el ETL varias veces
CREATE UNIQUE INDEX IF NOT EXISTS idx_subastas_unique 
ON subastas(archivo_fuente, numero_lote);

-- Índices para el dashboard
CREATE INDEX IF NOT EXISTS idx_subastas_fecha ON subastas(fecha_subasta);
CREATE INDEX IF NOT EXISTS idx_subastas_tipo ON subastas(tipo_subasta, tipo_codigo);
"""


def conectar_supabase() -> Client:
    """Crea y retorna el cliente de Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "❌ Faltan variables de entorno.\n"
            "Crea un archivo .env con:\n"
            "  SUPABASE_URL=https://xxxx.supabase.co\n"
            "  SUPABASE_KEY=tu_anon_key"
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def df_a_registros(df: pd.DataFrame) -> list[dict]:
    """
    Convierte el DataFrame a lista de dicts listos para insertar en Supabase.
    """
    registros = []
    for _, fila in df.iterrows():
        # Asegurar que la fecha sea string YYYY-MM-DD
        fecha = None
        if pd.notna(fila.get("fecha_subasta")):
            if isinstance(fila["fecha_subasta"], (datetime, date)):
                fecha = fila["fecha_subasta"].strftime("%Y-%m-%d")
            else:
                fecha = str(fila["fecha_subasta"])

        registro = {
            "fecha_subasta":     fecha,
            "numero_boletin":    int(fila["numero_boletin"]) if pd.notna(fila.get("numero_boletin")) else None,
            "tipo_subasta":      str(fila["tipo_subasta"]) if pd.notna(fila.get("tipo_subasta")) else None,
            "numero_lote":       str(fila["numero_lote"]) if pd.notna(fila.get("numero_lote")) else None,
            "tipo_codigo":       str(fila["tipo_codigo"]) if pd.notna(fila.get("tipo_codigo")) else None,
            "cantidad_animales": int(fila["cantidad_animales"]) if pd.notna(fila.get("cantidad_animales")) else None,
            "peso_total_kg":     float(fila["peso_total_kg"]) if pd.notna(fila.get("peso_total_kg")) else None,
            "peso_promedio_kg":  float(fila["peso_promedio_kg"]) if pd.notna(fila.get("peso_promedio_kg")) else None,
            "procedencia":       str(fila["procedencia"]) if pd.notna(fila.get("procedencia")) else None,
            "hora_subasta":      str(fila["hora_subasta"]) if pd.notna(fila.get("hora_subasta")) else None,
            "precio_base_kg":    float(fila["precio_base_kg"]) if pd.notna(fila.get("precio_base_kg")) else None,
            "precio_final_kg":   float(fila["precio_final_kg"]) if pd.notna(fila.get("precio_final_kg")) else None,
            "archivo_fuente":    str(fila["archivo_fuente"]) if pd.notna(fila.get("archivo_fuente")) else None,
        }
        registros.append(registro)
    return registros


def subir_a_supabase(df: pd.DataFrame, tabla: str = "subastas", batch_size: int = 200) -> dict:
    """
    Sube el DataFrame a Supabase en lotes.
    Usa upsert para evitar duplicados si el archivo ya fue procesado.
    
    Retorna un diccionario con estadísticas de inserción.
    """
    # Eliminar duplicados lógicos (clones) en el dataframe antes de subir
    # para evitar el error 'ON CONFLICT DO UPDATE cannot affect row a second time'
    n_antes = len(df)
    df = df.drop_duplicates(subset=["archivo_fuente", "numero_lote"], keep="first")
    n_duplicados = n_antes - len(df)
    if n_duplicados > 0:
        print(f"🧹 Se eliminaron {n_duplicados} filas duplicadas idénticas (mismo archivo y numero_lote).")

    stats = {"procesados": len(df), "exitosos": 0, "fallidos": 0, "errores": []}
    if df.empty:
        print("⚠️  No hay datos para subir.")
        return stats
    
    supabase = conectar_supabase()
    registros = df_a_registros(df)
    total = len(registros)
    insertados = 0
    
    print(f"\n🚀 Subiendo {total} registros a Supabase (tabla: {tabla})...")
    
    # Subir en lotes para no sobrecargar la API
    for i in range(0, total, batch_size):
        lote = registros[i : i + batch_size]
        try:
            # upsert: inserta si no existe, actualiza si ya existe
            # (basado en combinación archivo_fuente, numero_lote)
            resultado = supabase.table(tabla).upsert(
                lote, 
                on_conflict="archivo_fuente,numero_lote"
            ).execute()
            insertados += len(lote)
            stats["exitosos"] += len(lote)
            print(f"  ✅ Lote {i // batch_size + 1}: {len(lote)} registros ({insertados}/{total})")
        except Exception as e:
            stats["fallidos"] += len(lote)
            stats["errores"].append(str(e))
            print(f"  ❌ Error en lote {i // batch_size + 1}: {e}")
    
    print(f"\n✅ Carga completada: {insertados} registros en '{tabla}'")
    return stats


def verificar_registros_existentes(supabase: Client, archivo_fuente: str) -> int:
    """
    Verifica cuántos registros de un archivo ya existen en la BD.
    Útil para no duplicar datos en cargas incrementales.
    """
    resultado = (
        supabase.table("subastas")
        .select("id", count="exact")
        .eq("archivo_fuente", archivo_fuente)
        .execute()
    )
    return resultado.count or 0


# ─── EJECUCIÓN DIRECTA ────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Cargar el CSV generado por transform.py
    CSV_PATH = "datos_subastas.csv"
    
    if not os.path.exists(CSV_PATH):
        print(f"❌ No encontré '{CSV_PATH}'. Ejecuta transform.py primero.")
    else:
        df = pd.read_csv(CSV_PATH, parse_dates=["fecha_subasta"])
        print(f"📂 Cargando {len(df)} filas desde '{CSV_PATH}'...")
        
        subir_a_supabase(df)
        
        print("\n🎉 ¡Listo! Verifica los datos en tu dashboard de Supabase.")
        print("   → https://supabase.com/dashboard/project/TU_PROJECT_ID/table-editor")
