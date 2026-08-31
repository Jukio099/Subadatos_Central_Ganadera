"""
publicar_precios_landing.py
===========================
Genera un resumen JSON con los precios más recientes de las tres fuentes
(Central Ganadera, SubaCasanare y Subastar) y lo publica en el bucket
público de Supabase Storage para que la landing (www.subadatos.com)
muestre datos reales sin exponer credenciales.

URL pública resultante:
  {SUPABASE_URL}/storage/v1/object/public/subadatos-publicaciones/landing/precios_latest.json

Se ejecuta por cron después de cada corrida del ETL (ver ejecutar_etl.sh).
"""

import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from supabase import create_client

_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"))

BUCKET = "subadatos-publicaciones"
OBJETO = "landing/precios_latest.json"

# Nombres legibles de las categorías (mismo mapeo que ganadero/etl/queries.py)
CODIGOS_GANADO = {
    "VH": "Vaca Horra", "HL": "Hembra Levante", "ML": "Macho Levante",
    "MC": "Macho Ceba", "MG": "Macho Gordo", "HV": "Hembra Vientre",
    "AT": "Añojo Toro", "T2": "Toro", "R": "Reproductor",
    "VG": "Vaca Gorda", "VC": "Vaca de Cría", "NC": "Novilla de Ceba",
    "NG": "Novilla Gorda", "BF": "Búfalo", "VP": "Vaca Parida",
    "XX": "Lote Mixto", "NV": "Novilla Vientre", "H": "Hembra", "M": "Macho",
}

# Rango sano de precios COP/kg en pie para descartar errores de parsing
PRECIO_MIN, PRECIO_MAX = 2_000, 30_000


def _nombre(codigo: str) -> str:
    return CODIGOS_GANADO.get((codigo or "").strip().upper(), codigo or "N/D")


def _promedios_por_codigo(filas, campo_codigo, campo_precio):
    grupos = defaultdict(list)
    for f in filas:
        precio = f.get(campo_precio)
        if precio and PRECIO_MIN <= precio <= PRECIO_MAX:
            grupos[_nombre(f.get(campo_codigo, ""))].append(precio)
    return {cat: round(sum(p) / len(p)) for cat, p in grupos.items() if p}


def resumen_central(client) -> dict:
    ultima_res = (
        client.table("subastas").select("fecha_subasta")
        .gt("precio_final_kg", 0).order("fecha_subasta", desc=True).limit(1).execute()
    )
    if not ultima_res.data:
        return {}
    fin = date.fromisoformat(ultima_res.data[0]["fecha_subasta"])
    inicio = fin - timedelta(days=6)
    fin_ant, inicio_ant = inicio - timedelta(days=1), inicio - timedelta(days=7)

    campos = "tipo_codigo,precio_final_kg,cantidad_animales,procedencia,numero_boletin"
    actual = client.table("subastas").select(campos) \
        .gte("fecha_subasta", inicio.isoformat()).lte("fecha_subasta", fin.isoformat()) \
        .gt("precio_final_kg", 0).execute().data or []
    anterior = client.table("subastas").select("tipo_codigo,precio_final_kg") \
        .gte("fecha_subasta", inicio_ant.isoformat()).lte("fecha_subasta", fin_ant.isoformat()) \
        .gt("precio_final_kg", 0).execute().data or []

    prom_actual = _promedios_por_codigo(actual, "tipo_codigo", "precio_final_kg")
    prom_anterior = _promedios_por_codigo(anterior, "tipo_codigo", "precio_final_kg")

    precios = []
    for cat, precio in sorted(prom_actual.items(), key=lambda x: -x[1]):
        prev = prom_anterior.get(cat)
        variacion = round((precio - prev) / prev * 100, 1) if prev else None
        precios.append({"categoria": cat, "precio_kg": precio, "variacion_pct": variacion})

    boletines = [f["numero_boletin"] for f in actual if f.get("numero_boletin")]
    return {
        "nombre": "Central Ganadera de Medellín",
        "fecha": fin.isoformat(),
        "boletin": max(boletines) if boletines else None,
        "total_animales": sum(f["cantidad_animales"] or 0 for f in actual),
        "total_lotes": len(actual),
        "regiones": len({f["procedencia"] for f in actual if f.get("procedencia")}),
        "precios": precios,
    }


def resumen_casanare(client) -> dict:
    ultima_res = (
        client.table("subastas_casanare").select("fecha_subasta,numero_pdf")
        .gt("precio_final_kg", 0).order("fecha_subasta", desc=True).limit(1).execute()
    )
    if not ultima_res.data:
        return {}
    fecha = ultima_res.data[0]["fecha_subasta"]
    feria = ultima_res.data[0]["numero_pdf"]
    filas = client.table("subastas_casanare") \
        .select("sexo_codigo,precio_final_kg,cantidad_animales") \
        .eq("numero_pdf", feria).gt("precio_final_kg", 0).execute().data or []

    prom = _promedios_por_codigo(filas, "sexo_codigo", "precio_final_kg")
    return {
        "nombre": "SubaCasanare (Yopal)",
        "fecha": fecha,
        "feria": feria,
        "total_animales": sum(f["cantidad_animales"] or 0 for f in filas),
        "total_lotes": len(filas),
        "precios": [
            {"categoria": c, "precio_kg": p, "variacion_pct": None}
            for c, p in sorted(prom.items(), key=lambda x: -x[1])
        ],
    }


def _resumen_tabla_agregada(client, fuente: str, nombre: str) -> dict:
    ultima_res = (
        client.table("subastar_precios_resumen").select("fecha_evento")
        .eq("fuente", fuente)
        .order("fecha_evento", desc=True).limit(1).execute()
    )
    if not ultima_res.data:
        return {}
    fecha_max = date.fromisoformat(ultima_res.data[0]["fecha_evento"])
    inicio = fecha_max - timedelta(days=6)
    filas = client.table("subastar_precios_resumen") \
        .select("sede,tipo_codigo,precio_promedio_kg,cantidad_animales") \
        .eq("fuente", fuente) \
        .gte("fecha_evento", inicio.isoformat()).execute().data or []

    prom = _promedios_por_codigo(filas, "tipo_codigo", "precio_promedio_kg")
    return {
        "nombre": nombre,
        "fecha": fecha_max.isoformat(),
        "sedes": sorted({f["sede"] for f in filas if f.get("sede")}),
        "total_animales": sum(f["cantidad_animales"] or 0 for f in filas),
        "precios": [
            {"categoria": c, "precio_kg": p, "variacion_pct": None}
            for c, p in sorted(prom.items(), key=lambda x: -x[1])
        ],
    }


def resumen_subastar(client) -> dict:
    return _resumen_tabla_agregada(
        client, "subastar", "Subastar (Montería · Guamal · Sahagún)"
    )


def main() -> int:
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    central = resumen_central(client)
    casanare = resumen_casanare(client)
    subastar = resumen_subastar(client)
    cencogan = _resumen_tabla_agregada(
        client, "cencogan", "Cencogán (Buenavista · Planeta Rica · Tarazá · Yolombó)"
    )
    asosubastas = _resumen_tabla_agregada(
        client, "asosubastas", "Asosubastas (promedio nacional)"
    )
    sugaberrio = _resumen_tabla_agregada(
        client, "sugaberrio", "Sugaberrío (Magdalena Medio)"
    )
    cogasucre = _resumen_tabla_agregada(
        client, "cogasucre", "Cogasucre (Sincelejo)"
    )

    payload = {
        "actualizado": datetime.utcnow().isoformat() + "Z",
        "moneda": "COP/kg en pie",
        "fuentes": {
            "central": central,
            "casanare": casanare,
            "subastar": subastar,
            "cogasucre": cogasucre,
            "cencogan": cencogan,
            "asosubastas": asosubastas,
            "sugaberrio": sugaberrio,
        },
    }

    cuerpo = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    storage = client.storage.from_(BUCKET)
    storage.upload(
        OBJETO, cuerpo,
        {"content-type": "application/json", "cache-control": "300", "upsert": "true"},
    )
    url = storage.get_public_url(OBJETO)
    print(f"✅ Publicado {len(cuerpo)} bytes → {url}")
    print(
        f"   Central: {central.get('fecha')} | Casanare: {casanare.get('fecha')} | "
        f"Subastar: {subastar.get('fecha')} | Cencogán: {cencogan.get('fecha')} | "
        f"Asosubastas: {asosubastas.get('fecha')} | Sugaberrío: {sugaberrio.get('fecha')} | "
        f"Cogasucre: {cogasucre.get('fecha')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
