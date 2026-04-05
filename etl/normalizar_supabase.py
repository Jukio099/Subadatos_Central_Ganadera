import argparse
import os
import sys
from collections import Counter, defaultdict

from dotenv import load_dotenv
from supabase import create_client

_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
_DIR_PROYECTO = os.path.join(_DIR_SCRIPT, "..")
if _DIR_PROYECTO not in sys.path:
    sys.path.insert(0, _DIR_PROYECTO)

from shared.data_cleaning import (
    FERIA_CASANARE,
    FERIA_CENTRAL,
    normalizar_procedencia,
    normalizar_tipo_subasta,
)


TABLAS = {
    "subastas": FERIA_CENTRAL,
    "subastas_casanare": FERIA_CASANARE,
}


def crear_cliente():
    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_KEY en el entorno.")
    return create_client(supabase_url, supabase_key)


def leer_tabla(client, tabla: str, columnas: str) -> list[dict]:
    filas = []
    inicio = 0
    while True:
        data = client.table(tabla).select(columnas).range(inicio, inicio + 999).execute().data
        if not data:
            break
        filas.extend(data)
        if len(data) < 1000:
            break
        inicio += 1000
    return filas


def detectar_cambios(filas: list[dict], feria: str) -> tuple[list[dict], Counter, Counter]:
    cambios = []
    cambios_tipo = Counter()
    cambios_proc = Counter()

    for fila in filas:
        tipo_actual = fila.get("tipo_subasta")
        proc_actual = fila.get("procedencia")
        tipo_nuevo = normalizar_tipo_subasta(tipo_actual, feria)
        proc_nuevo = normalizar_procedencia(proc_actual, feria)

        payload = {"id": fila["id"]}
        hubo_cambio = False

        if tipo_nuevo != tipo_actual:
            payload["tipo_subasta"] = tipo_nuevo
            cambios_tipo[(str(tipo_actual), tipo_nuevo)] += 1
            hubo_cambio = True

        if proc_nuevo != proc_actual:
            payload["procedencia"] = proc_nuevo
            cambios_proc[(str(proc_actual), str(proc_nuevo))] += 1
            hubo_cambio = True

        if hubo_cambio:
            cambios.append(payload)

    return cambios, cambios_tipo, cambios_proc


def imprimir_resumen(tabla: str, total: int, cambios: list[dict], cambios_tipo: Counter, cambios_proc: Counter):
    solo_tipo = sum(1 for c in cambios if "tipo_subasta" in c and "procedencia" not in c)
    solo_proc = sum(1 for c in cambios if "procedencia" in c and "tipo_subasta" not in c)
    ambos = sum(1 for c in cambios if "procedencia" in c and "tipo_subasta" in c)

    print(f"\nTabla: {tabla}")
    print(f"- filas leidas: {total}")
    print(f"- filas con cambios: {len(cambios)}")
    print(f"- solo tipo_subasta: {solo_tipo}")
    print(f"- solo procedencia: {solo_proc}")
    print(f"- ambos campos: {ambos}")

    if cambios_tipo:
        print("- cambios top en tipo_subasta:")
        for (antes, despues), cantidad in cambios_tipo.most_common(10):
            print(f"  {cantidad:>5} | {antes!r} -> {despues!r}")

    if cambios_proc:
        print("- cambios top en procedencia:")
        for (antes, despues), cantidad in cambios_proc.most_common(15):
            print(f"  {cantidad:>5} | {antes!r} -> {despues!r}")


def aplicar_cambios(client, tabla: str, cambios: list[dict], batch_size: int = 500):
    grupos = defaultdict(list)
    for cambio in cambios:
        payload = tuple(sorted((k, v) for k, v in cambio.items() if k != "id"))
        grupos[payload].append(cambio["id"])

    actualizados = 0
    for payload_items, ids in grupos.items():
        payload = dict(payload_items)
        for inicio in range(0, len(ids), batch_size):
            bloque = ids[inicio : inicio + batch_size]
            client.table(tabla).update(payload).in_("id", bloque).execute()
            actualizados += len(bloque)
            print(f"  OK {tabla}: {payload} -> {len(bloque)} filas")
    return actualizados


def main():
    parser = argparse.ArgumentParser(description="Normaliza tipo_subasta y procedencia en Supabase.")
    parser.add_argument("--apply", action="store_true", help="Aplica los cambios en Supabase.")
    args = parser.parse_args()

    client = crear_cliente()
    total_actualizados = 0

    for tabla, feria in TABLAS.items():
        filas = leer_tabla(client, tabla, "id,tipo_subasta,procedencia")
        cambios, cambios_tipo, cambios_proc = detectar_cambios(filas, feria)
        imprimir_resumen(tabla, len(filas), cambios, cambios_tipo, cambios_proc)

        if args.apply and cambios:
            print(f"- aplicando cambios sobre {tabla}...")
            total_actualizados += aplicar_cambios(client, tabla, cambios)

    if args.apply:
        print(f"\nListo. Filas actualizadas: {total_actualizados}")
    else:
        print("\nModo analisis: no se aplicaron cambios. Usa --apply para ejecutar el backfill.")


if __name__ == "__main__":
    main()
