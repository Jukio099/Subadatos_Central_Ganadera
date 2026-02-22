"""
main.py
=======
Punto de entrada del ETL. Ejecuta las 3 fases en orden:
  1. extract.py  → descarga los PDFs
  2. transform.py → extrae los datos de los PDFs
  3. load.py     → sube los datos a Supabase

Uso:
  python main.py                # ETL completo
  python main.py --solo-nuevos  # Solo descarga PDFs que no estén en la BD aún
"""

import argparse
import os
import sys

# Asegurar que las importaciones funcionen desde cualquier directorio de trabajo
_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
_DIR_PROYECTO = os.path.join(_DIR_SCRIPT, "..")
sys.path.insert(0, _DIR_SCRIPT)

from extract import extraer_todos_los_pdfs
from transform import procesar_todos_los_pdfs
from load import subir_a_supabase

def main(solo_nuevos: bool = False):
    print("=" * 60)
    print("🐄  ETL CENTRAL GANADERA DE MEDELLÍN")
    print("=" * 60)

    # ── FASE 1: Extracción ──────────────────────────────────────
    print("\n📥 FASE 1: Descargando PDFs...")
    boletines = extraer_todos_los_pdfs()

    if not boletines:
        print("❌ No se encontraron PDFs. Verifica tu conexión a internet.")
        return

    # ── FASE 2: Transformación ──────────────────────────────────
    print("\n🔄 FASE 2: Procesando PDFs y extrayendo datos...")
    df = procesar_todos_los_pdfs(boletines_meta=boletines)

    if df.empty:
        print("❌ No se pudieron extraer datos de los PDFs.")
        return

    # Guardar CSV como respaldo siempre (en la raíz del proyecto)
    csv_path = os.path.join(_DIR_PROYECTO, "datos_subastas.csv")
    df.to_csv(csv_path, index=False)
    print(f"💾 Respaldo guardado: {csv_path} ({len(df)} filas)")

    print("\n🚀 FASE 3: Subiendo datos a Supabase...")
    stats = subir_a_supabase(df)

    # ── REPORTE FINAL ───────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 REPORTE FINAL DEL ETL")
    print("=" * 60)
    print(f"📄 PDFs procesados:        {len(boletines)}")
    print(f"✅ Registros procesados:   {stats.get('exitosos', 0)}")
    print(f"❌ Registros fallidos:     {stats.get('fallidos', 0)}")
    
    errores = set(stats.get('errores', []))
    if errores:
        print("   Errores detectados:")
        for err in errores:
            print(f"    - {err}")

    print("\n🎉 ETL completado exitosamente.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Central Ganadera de Medellín")
    parser.add_argument("--solo-nuevos", action="store_true", help="Solo procesa PDFs nuevos")
    args = parser.parse_args()
    main(solo_nuevos=args.solo_nuevos)
