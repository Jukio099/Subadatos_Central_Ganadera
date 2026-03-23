"""
extract.py
==========
Paso 1 del ETL: descarga todos los PDFs de subastas de la Central Ganadera
de Medellín y los guarda localmente en la carpeta /pdfs.

Ahora es incremental: antes de descargar un PDF consulta Supabase para ver
si archivo_fuente ya existe en la tabla subastas. Si ya existe, lo salta.

Autor: Tu nombre
Fecha: 2026
"""

import requests
from bs4 import BeautifulSoup
import os
import time
import re
from datetime import datetime
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
BASE_URL = "https://centralganadera.com/boletines/resultados-de-subasta/"
# Ruta absoluta a la carpeta pdfs/ en la raíz del proyecto (un nivel arriba de etl/)
_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(_DIR_SCRIPT, "..", "pdfs")
MAX_PAGES = 15            # número máximo de páginas a recorrer (ajusta según crezca el sitio)
DELAY = 1.5               # segundos de espera entre requests (para no sobrecargar el servidor)
DEFAULT_FAST_PAGES = 1    # en modo incremental operativo, casi siempre basta con la página 1

# ─── CLIENTE SUPABASE ─────────────────────────────────────────────────────────
_supabase_url = os.environ.get("SUPABASE_URL", "")
_supabase_key = os.environ.get("SUPABASE_KEY", "")

try:
    supabase: Client = create_client(_supabase_url, _supabase_key)
except Exception as _e:
    print(f"⚠️  No se pudo inicializar el cliente de Supabase: {_e}")
    supabase = None  # type: ignore

# ─── HEADERS ──────────────────────────────────────────────────────────────────
# Simulamos un navegador real para evitar bloqueos
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def crear_carpeta_pdfs():
    """Crea la carpeta /pdfs si no existe."""
    os.makedirs(PDF_DIR, exist_ok=True)
    print(f"📁 Carpeta de PDFs: '{PDF_DIR}/'")


def ya_existe_en_bd(nombre_archivo: str) -> bool:
    """
    Consulta Supabase para verificar si un PDF ya fue procesado y cargado.

    Busca el nombre del archivo en la columna archivo_fuente de la tabla subastas.
    Si ya existe al menos un registro con ese archivo, retorna True para saltarlo.

    Parámetros:
        nombre_archivo: nombre del archivo PDF (ej. 'resultado_17_02_26_cg.pdf')

    Retorna:
        True si el archivo ya existe en la BD, False si es nuevo o hubo un error.
    """
    if supabase is None:
        return False
    try:
        resultado = (
            supabase.table("subastas")
            .select("archivo_fuente")
            .eq("archivo_fuente", nombre_archivo)
            .limit(1)
            .execute()
        )
        return len(resultado.data) > 0
    except Exception as e:
        print(f"  ⚠️  Error al consultar BD para {nombre_archivo}: {e}")
        return False


def es_pdf_subasta_relevante(href: str, titulo: str) -> bool:
    """Filtra PDFs ajenos al boletín de resultados de subasta."""
    texto = f"{href} {titulo}".lower()
    if "/uploads/" not in href.lower() or not href.lower().endswith(".pdf"):
        return False

    excluidos = [
        "informe",
        "politica-de-tratamiento-de-datos",
        "terminos-de-uso",
        "politicas-de-privacidad",
        "politicas-corporativas",
    ]
    if any(token in texto for token in excluidos):
        return False

    requeridos = ["resultado", "subasta"]
    return all(token in texto for token in requeridos)


def obtener_links_pdf_de_pagina(url: str) -> list[dict]:
    """
    Dado un URL de página de boletines, retorna una lista de dicts con:
    - url: URL del PDF
    - titulo: título del boletín (ej. "Resultado de Subasta Tradicional N.° 07")
    - tipo: 'tradicional', 'especial-gyr', 'equina', u 'otro'
    - año: año extraído del URL del PDF
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠️  Error al acceder a {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Los PDFs están en <a href="...pdf"> dentro de los artículos/cards
    links_encontrados = []
    vistos: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        titulo = a_tag.get_text(strip=True)
        if es_pdf_subasta_relevante(href, titulo):
            href_normalizado = href.strip()
            if href_normalizado in vistos:
                continue
            vistos.add(href_normalizado)

            # Detectar tipo de subasta desde el URL
            href_lower = href_normalizado.lower()
            if "especial" in href_lower and "gyr" in href_lower:
                tipo = "Especial GYR"
            elif "especial" in href_lower:
                tipo = "Especial"
            elif "equina" in href_lower:
                tipo = "Equina"
            elif "mular" in href_lower:
                tipo = "Mulares"
            elif "tradicional" in href_lower or "comercial" in href_lower:
                tipo = "Tradicional"
            else:
                tipo = "Tradicional"

            # Extraer año del path del URL (ej. /2026/02/)
            match_anio = re.search(r"/uploads/(\d{4})/", href_normalizado)
            anio = int(match_anio.group(1)) if match_anio else None

            # Extraer fecha del nombre del archivo
            # Format 1: 17_02_26_cg or 17_02_26 (DD_MM_YY optionally followed by _something)
            # Tolera sufijos como -1, -v2, -compressed, -cg después de la fecha
            match_fecha = re.search(r"_(\d{2})_(\d{2})_(\d{2})(?:[_\-]|$|\.)", href_normalizado)
            
            # Format 2: 17_de-mayo_-2024 or similar (DD_de-MES_-YYYY)
            match_fecha_txt = re.search(r"_(\d{1,2})[_]*de[-_]*([a-zñáéíóú]{3,})[-_]*(\d{4})", href_normalizado, re.IGNORECASE)

            if match_fecha:
                dia, mes, anio_corto = match_fecha.groups()
                anio_completo = 2000 + int(anio_corto)
                try:
                    fecha = datetime(anio_completo, int(mes), int(dia)).date()
                except ValueError:
                    fecha = None
            elif match_fecha_txt:
                dia_str, mes_nombre, anio_str = match_fecha_txt.groups()
                mes_nombre = mes_nombre.lower().rstrip("-_")
                
                # Importar diccionarios de meses desde transform.py
                if _DIR_SCRIPT not in sys.path:
                    sys.path.insert(0, _DIR_SCRIPT)
                from transform import _MESES_ES, _MESES_ABREV

                mes_num = _MESES_ES.get(mes_nombre)
                if not mes_num:
                    mes_abrev = mes_nombre[:3] if len(mes_nombre) >= 3 else mes_nombre
                    mes_num = _MESES_ABREV.get(mes_abrev)
                if mes_num:
                    try:
                        fecha = datetime(int(anio_str), mes_num, int(dia_str)).date()
                    except ValueError:
                        fecha = None
                else:
                    fecha = None
            else:
                # Format 3: DD de MES (without year, assume current year)
                match_fecha_no_year = re.search(r"[_\-](\d{1,2})[_\s\-]*de[_\s\-]*([a-zñáéíóú]{3,})", href_normalizado, re.IGNORECASE)
                # Format 4: DD_MM without year
                match_fecha_num_no_year = re.search(r"[_\-](\d{1,2})[_\-](\d{1,2})(?:_|$|\.)", href_normalizado)

                if match_fecha_no_year:
                    dia_str, mes_nombre = match_fecha_no_year.groups()
                    mes_nombre = mes_nombre.lower().rstrip("-_ ")
                    
                    if _DIR_SCRIPT not in sys.path:
                        sys.path.insert(0, _DIR_SCRIPT)
                    from transform import _MESES_ES, _MESES_ABREV
                    
                    mes_num = _MESES_ES.get(mes_nombre)
                    if not mes_num:
                        mes_abrev = mes_nombre[:3] if len(mes_nombre) >= 3 else mes_nombre
                        mes_num = _MESES_ABREV.get(mes_abrev)
                        
                    if mes_num:
                        try:
                            fecha = datetime(datetime.now().year, mes_num, int(dia_str)).date()
                        except ValueError:
                            fecha = None
                    else:
                        fecha = None
                elif match_fecha_num_no_year:
                    dia_str, mes_str = match_fecha_num_no_year.groups()
                    try:
                        fecha = datetime(datetime.now().year, int(mes_str), int(dia_str)).date()
                    except ValueError:
                        fecha = None
                else:
                    fecha = None
                
            if fecha is None:
                print(f"  ⚠️  No se pudo extraer fecha del archivo: {os.path.basename(href_normalizado)}")

            links_encontrados.append({
                "url": href_normalizado,
                "titulo": titulo,
                "tipo": tipo,
                "anio": anio,
                "fecha": fecha,
                "nombre_archivo": os.path.basename(href_normalizado),
            })

    return links_encontrados


def descargar_pdf(url: str, nombre_archivo: str) -> str | None:
    """
    Descarga un PDF si no existe ya localmente.
    Retorna la ruta local del archivo, o None si falló.
    """
    ruta_local = os.path.join(PDF_DIR, nombre_archivo)

    # Si ya lo descargamos antes, no volvemos a descargarlo
    if os.path.exists(ruta_local):
        print(f"  ✅ Ya existe: {nombre_archivo}")
        return ruta_local

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        with open(ruta_local, "wb") as f:
            f.write(resp.content)

        print(f"  ⬇️  Descargado: {nombre_archivo}")
        return ruta_local

    except requests.RequestException as e:
        print(f"  ❌ Error descargando {nombre_archivo}: {e}")
        return None


def extraer_todos_los_pdfs(max_pages: int = MAX_PAGES, solo_nuevos: bool = False) -> dict:
    """
    Recorre todas las páginas del listado de boletines y descarga solo los PDFs nuevos.

    Para cada PDF encontrado consulta Supabase. Si archivo_fuente ya existe en la
    tabla subastas, lo salta completamente. Solo descarga PDFs genuinamente nuevos.

    Parámetros:
        max_pages: número máximo de páginas a recorrer
        solo_nuevos: si es True, usa modo rápido y revisa solo la página 1

    Retorna:
        dict con:
          - boletines: lista de dicts con metadata de cada PDF nuevo descargado
          - nuevos: cantidad de PDFs nuevos descargados
          - saltados: cantidad de PDFs que ya existían en la BD
    """
    crear_carpeta_pdfs()
    todos_los_boletines = []
    conteo_nuevos = 0
    conteo_saltados = 0

    paginas_a_recorrer = DEFAULT_FAST_PAGES if solo_nuevos else max_pages
    print(f"🔎 Modo de exploración: {'rápido (página 1)' if solo_nuevos else f'completo ({paginas_a_recorrer} páginas máx.)'}")

    vistos_en_corrida: set[str] = set()

    for num_pagina in range(1, paginas_a_recorrer + 1):
        # Construir URL de la página (página 1 no tiene /page/1/)
        if num_pagina == 1:
            url_pagina = BASE_URL
        else:
            url_pagina = f"{BASE_URL}page/{num_pagina}/"

        print(f"\n📄 Página {num_pagina}: {url_pagina}")

        links = obtener_links_pdf_de_pagina(url_pagina)

        if not links:
            print(f"  ⚠️  No se encontraron PDFs en la página {num_pagina}. Fin del recorrido.")
            break

        print(f"  🔗 {len(links)} PDFs encontrados")

        for boletin in links:
            if boletin["nombre_archivo"] in vistos_en_corrida:
                print(f"  🔁 Duplicado en la corrida: {boletin['nombre_archivo']} (saltando)")
                continue
            vistos_en_corrida.add(boletin["nombre_archivo"])

            nombre_archivo = boletin["nombre_archivo"]

            # Verificación incremental: consultar Supabase antes de descargar
            if ya_existe_en_bd(nombre_archivo):
                print(f"  ⏭️  Ya existe en BD: {nombre_archivo} (saltando)")
                conteo_saltados += 1
                continue

            print(f"  ⬇️  Nuevo PDF encontrado: {nombre_archivo} (descargando)")
            ruta = descargar_pdf(boletin["url"], nombre_archivo)
            if ruta:
                boletin["ruta_local"] = ruta
                todos_los_boletines.append(boletin)
                conteo_nuevos += 1

        time.sleep(DELAY)  # Pausa entre páginas

    print(f"\n✅ PDFs nuevos descargados: {conteo_nuevos}")
    print(f"⏭️  PDFs saltados (ya en BD):  {conteo_saltados}")
    return {
        "boletines": todos_los_boletines,
        "nuevos": conteo_nuevos,
        "saltados": conteo_saltados,
    }


# ─── EJECUCIÓN DIRECTA ────────────────────────────────────────────────────────
if __name__ == "__main__":
    resultado = extraer_todos_los_pdfs()
    boletines = resultado["boletines"]

    # Mostrar resumen
    print("\n📊 Resumen:")
    tipos = {}
    for b in boletines:
        tipos[b["tipo"]] = tipos.get(b["tipo"], 0) + 1
    for tipo, conteo in sorted(tipos.items()):
        print(f"   {tipo}: {conteo} subastas")
    print(f"\n📊 Nuevos descargados: {resultado['nuevos']} | Saltados: {resultado['saltados']}")
