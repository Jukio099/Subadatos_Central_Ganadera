"""
etl/casanare.py
Descarga, parsea y carga PDFs de SubaCasanare a Supabase.

URL patrón: https://www.subacasanare.com/Precio_Pdf/{numero_pdf}
Tabla destino: subastas_casanare
"""
from __future__ import annotations
import io
import re
import time
import logging
from datetime import date
from typing import Optional

import requests
import pdfplumber

logger = logging.getLogger(__name__)

BASE_URL = "https://www.subacasanare.com/Precio_Pdf/{}"
TABLA = "subastas_casanare"
DELAY_ENTRE_DESCARGAS = 1.5  # segundos

# Regex para detectar una fila de lote válida.
# Patrón: número  CODIGO  cantidad  peso_total  peso_prom  PROCEDENCIA  HH:MM:SS  base  final  [obs]
_LOT_RE = re.compile(
    r"^(\d+)\s+"                   # numero_lote
    r"([A-Z]{2,3})\s+"             # sexo_codigo
    r"(\d+)\s+"                    # cantidad_animales
    r"([\d.]+)\s+"                 # peso_total (miles con punto)
    r"(\d+)\s+"                    # peso_promedio
    r"(.+?)\s+"                    # procedencia (lazy)
    r"(\d{2}:\d{2}:\d{2})\s+"      # hora_entrada
    r"([\d.]+)\s+"                 # precio_base (miles con punto)
    r"([\d.]+)"                    # precio_final (miles con punto)
    r"(.*)?$"                      # observaciones (opcional)
)
_FERIA_RE = re.compile(
    r"FERIA NO\.\s+(\d+).*TIPO DE SUBASTA\.\s+(\S+).*CIUDAD\s+(.+)",
    re.IGNORECASE,
)
_FECHA_RE = re.compile(
    r"FECHA FERIA\.\s+(\d{4}-\d{2}-\d{2}).*MARTILLO\.\s+(.+)",
    re.IGNORECASE,
)


def _parse_miles(s: str) -> float:
    """Convierte '1.398' → 1398.0  y  '7.500' → 7500.0.
    El PDF usa punto como separador de miles, no como decimal."""
    s = s.strip().replace(".", "").replace(",", "")
    return float(s) if s else 0.0


def descargar_pdf(numero_pdf: int, timeout: int = 30) -> Optional[bytes]:
    """Descarga el PDF de SubaCasanare. Retorna bytes o None si falla."""
    url = BASE_URL.format(numero_pdf)
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200 and b"%PDF" in resp.content[:10]:
            return resp.content
        logger.warning("PDF %s: HTTP %s", numero_pdf, resp.status_code)
        return None
    except requests.RequestException as exc:
        logger.error("PDF %s: error de red: %s", numero_pdf, exc)
        return None


def parsear_pdf(contenido: bytes, numero_pdf: int) -> list[dict]:
    """Extrae filas de lotes del contenido binario de un PDF.
    Retorna lista de dicts listos para insertar en subastas_casanare."""
    meta = {
        "numero_pdf": numero_pdf,
        "fecha_subasta": None,
        "tipo_subasta": None,
        "ciudad": None,
        "martillo": None,
    }
    filas = []

    with pdfplumber.open(io.BytesIO(contenido)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            page_start = len(filas)
            texto = page.extract_text() or ""
            for linea in texto.splitlines():
                linea = linea.strip()
                if not linea:
                    continue

                m = _FERIA_RE.search(linea)
                if m:
                    meta["tipo_subasta"] = m.group(2).upper()
                    meta["ciudad"] = m.group(3).strip().upper()
                    continue

                m = _FECHA_RE.search(linea)
                if m:
                    try:
                        meta["fecha_subasta"] = date.fromisoformat(m.group(1))
                    except ValueError:
                        pass
                    meta["martillo"] = m.group(2).strip().upper()
                    continue

                if linea.startswith("Lote "):
                    continue

                m = _LOT_RE.match(linea)
                if m:
                    fila = {
                        **meta,
                        "numero_lote": m.group(1),
                        "sexo_codigo": m.group(2).upper(),
                        "cantidad_animales": int(m.group(3)),
                        "peso_total_kg": _parse_miles(m.group(4)),
                        "peso_promedio_kg": float(m.group(5)),
                        "procedencia": m.group(6).strip().upper(),
                        "hora_entrada": m.group(7),
                        "precio_base_kg": _parse_miles(m.group(8)),
                        "precio_final_kg": _parse_miles(m.group(9)),
                        "observaciones": m.group(10).strip() if m.group(10) else None,
                    }
                    filas.append(fila)
                else:
                    if filas[page_start:] and linea and not linea.startswith("SUBASTA"):
                        ultimo = filas[-1]
                        obs_actual = ultimo.get("observaciones") or ""
                        ultimo["observaciones"] = (
                            (obs_actual + " / " + linea).strip(" /")
                        )

            if page_num == 0 and meta["fecha_subasta"] is None:
                logger.warning(
                    "PDF %s: no se encontró fecha_subasta en la primera página",
                    numero_pdf,
                )

    return filas


def cargar_en_supabase(client, filas: list[dict]) -> int:
    """Inserta filas en subastas_casanare (upsert por numero_pdf + numero_lote).
    Retorna cantidad de filas insertadas/actualizadas."""
    if not filas:
        return 0

    for f in filas:
        if isinstance(f.get("fecha_subasta"), date):
            f["fecha_subasta"] = f["fecha_subasta"].isoformat()

    resp = (
        client.table(TABLA)
        .upsert(filas, on_conflict="numero_pdf,numero_lote")
        .execute()
    )
    return len(resp.data) if resp.data else 0


def procesar_pdf(client, numero_pdf: int) -> int:
    """Pipeline completo: descarga → parsea → carga. Retorna filas cargadas."""
    try:
        contenido = descargar_pdf(numero_pdf)
        if contenido is None:
            return 0
        filas = parsear_pdf(contenido, numero_pdf)
        if not filas:
            logger.warning("PDF %s: sin filas parseadas", numero_pdf)
            return 0
        cargadas = cargar_en_supabase(client, filas)
        logger.info("PDF %s: %d filas cargadas", numero_pdf, cargadas)
        return cargadas
    finally:
        time.sleep(DELAY_ENTRE_DESCARGAS)
