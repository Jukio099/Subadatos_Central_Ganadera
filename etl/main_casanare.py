"""
etl/main_casanare.py
Orquestador del ETL de SubaCasanare.

Uso:
    python etl/main_casanare.py              # busca hasta 10 PDFs nuevos
    python etl/main_casanare.py --max 20     # busca hasta 20 PDFs nuevos
"""
from __future__ import annotations
import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from supabase import create_client

from etl.casanare import TABLA, procesar_pdf

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
_DIR_PROYECTO = os.path.join(_DIR_SCRIPT, "..")


def _get_ultimo_numero_pdf(client) -> int:
    """Retorna el número de PDF más alto ya cargado en subastas_casanare (0 si vacío)."""
    res = (
        client.table(TABLA)
        .select("numero_pdf")
        .order("numero_pdf", desc=True)
        .limit(1)
        .execute()
    )
    if res.data:
        return int(res.data[0]["numero_pdf"])
    return 0


def escribir_resumen_github(lineas: list[str]) -> None:
    """Escribe el resumen en $GITHUB_STEP_SUMMARY si está disponible."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n".join(lineas) + "\n")


def main(max_nuevos: int = 10) -> int:
    load_dotenv(os.path.join(_DIR_PROYECTO, ".env"))

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logger.error("Faltan SUPABASE_URL o SUPABASE_KEY en el entorno.")
        return 1

    client = create_client(supabase_url, supabase_key)
    ultimo = _get_ultimo_numero_pdf(client)
    logger.info("Último numero_pdf en BD: %s", ultimo)

    total_cargados = 0
    intentados = 0
    fallos_consecutivos = 0
    MAX_FALLOS_CONSECUTIVOS = 3  # para si la numeración tiene huecos

    for numero in range(ultimo + 1, ultimo + max_nuevos + 1):
        intentados += 1
        cargados = procesar_pdf(client, numero)
        if cargados > 0:
            total_cargados += cargados
            fallos_consecutivos = 0
            logger.info("PDF %s: %d filas insertadas", numero, cargados)
        else:
            fallos_consecutivos += 1
            logger.info("PDF %s: sin datos (fallo %d/%d)", numero, fallos_consecutivos, MAX_FALLOS_CONSECUTIVOS)
            if fallos_consecutivos >= MAX_FALLOS_CONSECUTIVOS:
                logger.info("Deteniendo búsqueda: %d fallos consecutivos.", MAX_FALLOS_CONSECUTIVOS)
                break

    escribir_resumen_github([
        "## 🐄 ETL SubaCasanare",
        "",
        "| Métrica | Valor |",
        "|---------|-------|",
        f"| Último PDF en BD antes del ETL | {ultimo} |",
        f"| PDFs intentados | {intentados} |",
        f"| Filas insertadas/actualizadas | {total_cargados} |",
        "",
        "> Si filas=0, puede que no haya subastas nuevas o que los PDFs aún no estén publicados.",
    ])

    logger.info("ETL Casanare finalizado. Filas cargadas: %d", total_cargados)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL SubaCasanare")
    parser.add_argument("--max", type=int, default=10, dest="max_nuevos",
                        help="Máximo de PDFs nuevos a intentar (default: 10)")
    args = parser.parse_args()
    sys.exit(main(max_nuevos=args.max_nuevos))
