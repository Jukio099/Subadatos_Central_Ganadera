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

    # ── FASE 1: Extracción ──────────────────────────────────────────────
    print("\n📥 FASE 1: Descargando PDFs...")
    resultado_extract = extraer_todos_los_pdfs()
    boletines = resultado_extract["boletines"]
    pdfs_nuevos = resultado_extract["nuevos"]
    pdfs_saltados = resultado_extract["saltados"]

    if not boletines and pdfs_nuevos == 0:
        print(f"⏭️  {pdfs_saltados} PDFs ya existían en la BD. No hay nuevos para procesar.")
        return

    # ── FASE 2: Transformación ──────────────────────────────────
    print("\n🔄 FASE 2: Procesando PDFs y extrayendo datos...")
    df, pdfs_sin_fecha = procesar_todos_los_pdfs(boletines_meta=boletines)

    if df.empty:
        print("❌ No se pudieron extraer datos de los PDFs.")
        return

    # Guardar CSV como respaldo siempre (en la raíz del proyecto)
    csv_path = os.path.join(_DIR_PROYECTO, "datos_subastas.csv")
    df.to_csv(csv_path, index=False)
    print(f"💾 Respaldo guardado: {csv_path} ({len(df)} filas)")

    print("\n🚀 FASE 3: Subiendo datos a Supabase...")
    stats = subir_a_supabase(df)

    # ── REPORTE FINAL ─────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 REPORTE FINAL DEL ETL")
    print("=" * 60)
    print(f"⬇️  PDFs nuevos procesados:   {pdfs_nuevos}")
    print(f"⏭️  PDFs saltados (en BD):    {pdfs_saltados}")
    print(f"✅ Registros nuevos insertados: {stats.get('exitosos', 0)}")
    print(f"❌ Registros fallidos:         {stats.get('fallidos', 0)}")
    
    errores = set(stats.get('errores', []))
    if errores:
        print("   Errores detectados:")
        for err in errores:
            print(f"    - {err}")

    # ── PDFs sin fecha — advertencia explícita y resumen en GitHub Actions ──
    if pdfs_sin_fecha:
        print(f"\n⚠️  PDFs sin fecha_subasta ({len(pdfs_sin_fecha)}) — requieren revisión manual:")
        for nombre in pdfs_sin_fecha:
            print(f"    - {nombre}")

        github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if github_summary:
            with open(github_summary, "a", encoding="utf-8") as f:
                f.write("\n## ⚠️ PDFs sin fecha detectados\n\n")
                f.write("Estos archivos no pudieron extraerse la fecha. Revisar manualmente:\n\n")
                for nombre in pdfs_sin_fecha:
                    f.write(f"- `{nombre}`\n")

    print("\n🎉 ETL completado exitosamente.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Central Ganadera de Medellín")
    parser.add_argument("--solo-nuevos", action="store_true", help="Solo procesa PDFs nuevos")
    args = parser.parse_args()
    main(solo_nuevos=args.solo_nuevos)
