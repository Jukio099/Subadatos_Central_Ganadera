"""
Ingesta de subastas ganaderas adicionales:
  - Cogasucre (Sincelejo) — lotes HTML, misma plataforma de listado que SubaCasanare
  - Cencogán (Buenavista, Planeta Rica, Tarazá, Yolombó) — lotes HTML
  - Asosubastas — promedio nacional semanal (API JSON)
  - Sugaberrío (5 sedes) — tablas HTML semanales

Los datos se agregan y se hacen upsert en `subastar_precios_resumen`
usando la columna `fuente` para no mezclarlos con Subastar.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, "..", ".env"))
load_dotenv("/home/julio/ganadero/.env")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
TABLA = "subastar_precios_resumen"
ON_CONFLICT = "sede,anio,evento_id,fecha_evento,clasificacion,tipo_codigo,edad"

SEDE_CENCOGAN = {
    "GG": "BUENAVISTA",
    "PR": "PLANETA RICA",
    "TZ": "TARAZA",
    "YO": "YOLOMBO",
}
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
SEDES_SUGABERRIO = [
    "CIMITARRA", "SABANA DE TORRES", "AGUACHICA", "PUERTO SALGAR", "PUERTO BERRIO",
]
CATEGORIA_A_CODIGO = {
    "macho de levante": "ML",
    "macho levante": "ML",
    "macho de ceba": "MC",
    "macho ceba": "MC",
    "hembra de levante": "HL",
    "hembra levante": "HL",
    "novilla de vientre": "HV",
    "novilla vientre": "HV",
    "vaca horra": "VH",
    "vaca parida": "VP",
}


def _client():
    url = os.environ.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL")
    key = (
        os.environ.get("SUPABASE_KEY")
        or os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    )
    if not url or not key:
        raise RuntimeError("Faltan SUPABASE_URL y SUPABASE_KEY/SERVICE_KEY")
    return create_client(url, key)


def _hash(partes: list[str]) -> str:
    return hashlib.sha256("|".join(partes).encode("utf-8")).hexdigest()


def _parse_cop(texto: str) -> int | None:
    if not texto:
        return None
    limpio = re.sub(r"[^\d,.\-]", "", texto)
    if not limpio or limpio in {"-", "—"}:
        return None
    # $11.077 o 9,097 → enteros colombianos (separador de miles)
    limpio = limpio.replace(".", "").replace(",", "")
    try:
        valor = int(limpio)
    except ValueError:
        return None
    return valor if 1_000 <= valor <= 40_000 else valor if valor > 0 else None


def _fila_agregada(
    *,
    fuente: str,
    sede: str,
    anio: int,
    evento_id: str,
    fecha_evento: str,
    tipo_codigo: str,
    cantidad: int,
    precio_prom: int,
    precio_min: int | None = None,
    precio_max: int | None = None,
    peso_prom: int | None = None,
    url: str = "",
    clasificacion: str = "SEMANAL",
) -> dict:
    return {
        "fuente": fuente,
        "sede": sede,
        "anio": anio,
        "evento_id": evento_id,
        "evento_numero_reporte": evento_id,
        "fecha_evento": fecha_evento,
        "fecha_reporte": fecha_evento,
        "clasificacion": clasificacion,
        "tipo_codigo": tipo_codigo,
        "edad": "",
        "cantidad_animales": cantidad,
        "precio_max_kg": precio_max or precio_prom,
        "precio_min_kg": precio_min or precio_prom,
        "precio_promedio_kg": precio_prom,
        "peso_promedio_kg": peso_prom or 0,
        "valor_promedio_animal": 0,
        "url_fuente": url,
        "archivo_fuente": url,
        "hash_registro": _hash([fuente, sede, str(anio), evento_id, tipo_codigo, fecha_evento]),
        "creado_en": datetime.utcnow().isoformat(timespec="seconds"),
        "actualizado_en": datetime.utcnow().isoformat(timespec="seconds"),
    }


def subir(filas: list[dict]) -> int:
    if not filas:
        return 0
    supabase = _client()
    total = 0
    for i in range(0, len(filas), 200):
        chunk = filas[i : i + 200]
        supabase.table(TABLA).upsert(chunk, on_conflict=ON_CONFLICT).execute()
        total += len(chunk)
    return total


# ── Cogasucre ───────────────────────────────────────────────────────────────

def _fecha_cogasucre(html: str, headers: dict) -> str | None:
    m = re.search(
        r'Fecha</span>\s*(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})',
        html,
        re.I,
    )
    if m:
        raw = m.group(1)
        if "-" in raw:
            return raw
        partes = raw.split("/")
        if len(partes) == 3:
            d, mo, y = (int(p) for p in partes)
            if y < 100:
                y += 2000
            return f"{y:04d}-{mo:02d}-{d:02d}"
    last_mod = headers.get("Last-Modified") or headers.get("last-modified")
    if last_mod:
        try:
            return datetime.strptime(last_mod, "%a, %d %b %Y %H:%M:%S %Z").date().isoformat()
        except ValueError:
            pass
    return None


def scrape_cogasucre(desde: int = 2295, cuantas: int = 4) -> list[dict]:
    """Recorre ferias recientes hacia atrás y agrega precios por categoría."""
    filas: list[dict] = []
    halladas = 0
    for numero in range(desde, max(desde - 40, 2200), -1):
        if halladas >= cuantas:
            break
        url = f"https://app.cogasucre.com/lotes/administracion/detalle/{numero}"
        time.sleep(0.8)
        resp = requests.get(url, headers=HEADERS, timeout=25)
        if resp.status_code != 200 or "Listado de Lotes" not in resp.text:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        tablas = soup.find_all("table")
        if len(tablas) < 2:
            continue
        meta = tablas[0].get_text(" ", strip=True)
        if f"Feria No. {numero}" not in meta and f"Feria No.{numero}" not in meta:
            continue
        lotes = []
        for tr in tablas[1].find_all("tr")[1:]:
            celdas = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(celdas) < 10:
                continue
            precio = _parse_cop(celdas[9])
            codigo = celdas[1].strip().upper()
            if not codigo or precio is None or precio < 1000:
                continue
            try:
                cantidad = int(re.sub(r"\D", "", celdas[2]) or 0)
            except ValueError:
                cantidad = 0
            lotes.append((codigo, precio, cantidad))
        if not lotes:
            continue
        fecha = _fecha_cogasucre(resp.text, resp.headers) or datetime.utcnow().date().isoformat()
        grupos: dict[str, dict] = defaultdict(lambda: {"precios": [], "animales": 0})
        for codigo, precio, cantidad in lotes:
            grupos[codigo]["precios"].append(precio)
            grupos[codigo]["animales"] += cantidad
        anio = int(fecha[:4])
        for codigo, g in grupos.items():
            filas.append(
                _fila_agregada(
                    fuente="cogasucre",
                    sede="SINCELEJO",
                    anio=anio,
                    evento_id=str(numero),
                    fecha_evento=fecha,
                    tipo_codigo=codigo,
                    cantidad=g["animales"],
                    precio_prom=round(sum(g["precios"]) / len(g["precios"])),
                    precio_min=min(g["precios"]),
                    precio_max=max(g["precios"]),
                    url=url,
                    clasificacion="GENERAL",
                )
            )
        halladas += 1
        print(f"  ✅ Cogasucre feria {numero} {fecha}: {len(grupos)} categorías, {len(lotes)} lotes")
    return filas


# ── Cencogán ────────────────────────────────────────────────────────────────

def _fecha_cencogan(html: str, soup: BeautifulSoup) -> str | None:
    m = re.search(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})', html)
    if m:
        return m.group(1)
    texto = soup.get_text(" ", strip=True).lower()
    m = re.search(
        r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(\d{1,2})(?:\s*,\s*|\s+de\s+)?(\d{4})?",
        texto,
    )
    if not m:
        return None
    mes, dia, anio = m.group(1), int(m.group(2)), m.group(3)
    anio_n = int(anio) if anio else datetime.utcnow().year
    return f"{anio_n:04d}-{MESES_ES[mes]:02d}-{dia:02d}"


def scrape_cencogan(max_subastas: int = 8) -> list[dict]:
    indice = "https://cencogan.com/lotes-en-linea/"
    resp = requests.get(indice, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    slugs = []
    vistos = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/lotes-en-linea/subasta-([A-Z]{2})-(\d+)-(\d{4})/?", href, re.I)
        if not m:
            continue
        key = (m.group(1).upper(), m.group(2), m.group(3))
        if key in vistos:
            continue
        vistos.add(key)
        slugs.append(urljoin(indice, href))
        if len(slugs) >= max_subastas:
            break

    filas: list[dict] = []
    for url in slugs:
        time.sleep(1.0)
        det = requests.get(url, headers=HEADERS, timeout=30)
        if det.status_code != 200:
            print(f"  ⚠️  Cencogán {url}: HTTP {det.status_code}")
            continue
        ds = BeautifulSoup(det.text, "html.parser")
        fecha = _fecha_cencogan(det.text, ds)
        ident = re.search(r"subasta-([A-Z]{2})-(\d+)-(\d{4})", url, re.I)
        if not ident or not fecha:
            print(f"  ⚠️  Cencogán sin fecha/slug: {url}")
            continue
        prefijo, numero, anio = ident.group(1).upper(), ident.group(2), int(ident.group(3))
        sede = SEDE_CENCOGAN.get(prefijo, prefijo)
        evento_id = f"{prefijo}-{numero}"

        tabla = ds.find("table")
        if not tabla:
            continue
        grupos: dict[str, dict] = defaultdict(lambda: {"precios": [], "animales": 0, "pesos": []})
        for tr in tabla.find_all("tr")[1:]:
            celdas = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(celdas) < 10:
                continue
            codigo = celdas[1].strip().upper()
            try:
                cantidad = int(re.sub(r"\D", "", celdas[2]) or 0)
            except ValueError:
                cantidad = 0
            precio = _parse_cop(celdas[9])
            peso = _parse_cop(celdas[4]) or 0
            if not codigo or precio is None:
                continue
            # los precios de Cencogán vienen sin separador (7500), _parse_cop exige >=1000
            if precio < 1000:
                continue
            grupos[codigo]["precios"].append(precio)
            grupos[codigo]["animales"] += cantidad
            if peso:
                grupos[codigo]["pesos"].append(peso)

        for codigo, g in grupos.items():
            if not g["precios"]:
                continue
            filas.append(
                _fila_agregada(
                    fuente="cencogan",
                    sede=sede,
                    anio=anio,
                    evento_id=evento_id,
                    fecha_evento=fecha,
                    tipo_codigo=codigo,
                    cantidad=g["animales"],
                    precio_prom=round(sum(g["precios"]) / len(g["precios"])),
                    precio_min=min(g["precios"]),
                    precio_max=max(g["precios"]),
                    peso_prom=round(sum(g["pesos"]) / len(g["pesos"])) if g["pesos"] else 0,
                    url=url,
                    clasificacion="LOTE",
                )
            )
        print(f"  ✅ Cencogán {sede} {evento_id} {fecha}: {len(grupos)} categorías")
    return filas


# ── Asosubastas ─────────────────────────────────────────────────────────────

def scrape_asosubastas(anio: int = 2026) -> list[dict]:
    headers = {
        **HEADERS,
        "Content-Type": "application/json",
        "Origin": "https://asosubastas.com",
        "Referer": "https://asosubastas.com/",
    }
    semana_ok = None
    payload_ok = None
    for semana in range(40, 28, -1):
        resp = requests.post(
            "https://asosubastas.com/api/data/weekly-average",
            headers=headers,
            json={"payload": {"filter_year": anio, "filter_week": semana, "categories": [2, 3, 4, 5, 6, 7, 8]}},
            timeout=30,
        )
        if resp.status_code != 200:
            continue
        data = resp.json()
        if isinstance(data, list) and data and "categoria" in data[0]:
            semana_ok, payload_ok = semana, data
            break
        if isinstance(data, dict) and data.get("error"):
            continue
    if not payload_ok:
        print("  ⚠️  Asosubastas: sin semana con datos")
        return []

    # lunes de esa semana ISO
    fecha = datetime.fromisocalendar(anio, semana_ok, 1).date().isoformat()
    filas = []
    for item in payload_ok:
        codigo = str(item.get("categoria", "")).upper()
        precio = item.get("promedio_actual")
        if not codigo or precio is None:
            continue
        filas.append(
            _fila_agregada(
                fuente="asosubastas",
                sede="NACIONAL",
                anio=anio,
                evento_id=f"S{semana_ok}",
                fecha_evento=fecha,
                tipo_codigo=codigo,
                cantidad=0,
                precio_prom=round(float(precio)),
                precio_min=round(float(item["promedio_min_actual"])) if item.get("promedio_min_actual") else None,
                precio_max=round(float(item["promedio_max_actual"])) if item.get("promedio_max_actual") else None,
                url="https://asosubastas.com/",
                clasificacion="NACIONAL",
            )
        )
    print(f"  ✅ Asosubastas semana {semana_ok} ({fecha}): {len(filas)} categorías")
    return filas


# ── Sugaberrío ──────────────────────────────────────────────────────────────

def scrape_sugaberrio() -> list[dict]:
    url = "https://www.sugaberrio.co/pages/precio-del-ganado"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    texto = soup.get_text(" ", strip=True).lower()
    m = re.search(
        r"semana del\s+(\d{1,2})\s+al\s+(\d{1,2})\s+de\s+"
        r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})",
        texto,
    )
    if not m:
        print("  ⚠️  Sugaberrío: no se encontró la fecha de la semana")
        return []
    dia_fin, mes, anio = int(m.group(2)), MESES_ES[m.group(3)], int(m.group(4))
    fecha = f"{anio:04d}-{mes:02d}-{dia_fin:02d}"
    evento_id = f"{anio}{mes:02d}{dia_fin:02d}"

    tablas = soup.find_all("table")
    filas: list[dict] = []
    for i, tabla in enumerate(tablas):
        sede = SEDES_SUGABERRIO[i] if i < len(SEDES_SUGABERRIO) else f"SEDE_{i+1}"
        for tr in tabla.find_all("tr")[1:]:
            celdas = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(celdas) < 3:
                continue
            codigo = CATEGORIA_A_CODIGO.get(celdas[0].strip().lower())
            precio = _parse_cop(celdas[2])
            if not codigo or precio is None or precio < 1000:
                continue
            filas.append(
                _fila_agregada(
                    fuente="sugaberrio",
                    sede=sede,
                    anio=anio,
                    evento_id=evento_id,
                    fecha_evento=fecha,
                    tipo_codigo=codigo,
                    cantidad=0,
                    precio_prom=precio,
                    url=url,
                    clasificacion="SEMANAL",
                )
            )
    print(f"  ✅ Sugaberrío {fecha}: {len(filas)} filas en {min(len(tablas), 5)} sedes")
    return filas


def main(fuentes: list[str] | None = None) -> int:
    pedidas = fuentes or ["cogasucre", "cencogan", "asosubastas", "sugaberrio"]
    print("🐄  Ingesta de subastas nuevas")
    todas: list[dict] = []
    if "cogasucre" in pedidas:
        print("\n📥 Cogasucre")
        todas.extend(scrape_cogasucre())
    if "cencogan" in pedidas:
        print("\n📥 Cencogán")
        todas.extend(scrape_cencogan())
    if "asosubastas" in pedidas:
        print("\n📥 Asosubastas")
        todas.extend(scrape_asosubastas())
    if "sugaberrio" in pedidas:
        print("\n📥 Sugaberrío")
        todas.extend(scrape_sugaberrio())

    subidas = subir(todas)
    print(f"\n✅ Upsert {subidas} filas agregadas en {TABLA}")
    return 0 if todas else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta Cogasucre / Cencogán / Asosubastas / Sugaberrío")
    parser.add_argument("--fuente", action="append", choices=["cogasucre", "cencogan", "asosubastas", "sugaberrio"])
    args = parser.parse_args()
    raise SystemExit(main(args.fuente))
