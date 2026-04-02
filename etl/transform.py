"""
transform.py
============
Paso 2 del ETL: lee los PDFs descargados y extrae los datos de cada lote
de subasta en filas estructuradas.

El PDF tiene columnas: Lote | Cant | Tipo | P.Total | P.Prom | Procedencia
                        | Llegada | $Base kg | $Final kg | $Promedio | P.Total

Autor: Tu nombre
Fecha: 2026
"""

import pdfplumber
import pandas as pd
import re
import os
from datetime import datetime, date

# Ruta raíz del proyecto (un nivel arriba de etl/)
_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
_DIR_PROYECTO = os.path.join(_DIR_SCRIPT, "..")

# ─── MAPEO DE TIPOS DE ANIMAL ──────────────────────────────────────────────────
# Códigos que aparecen en la columna "Tipo" del PDF
TIPOS_ANIMAL = {
    # Bovinos
    "HV": "Hembra de vientre",
    "HL": "Hembra de levante",
    "MC": "Macho de ceba",
    "ML": "Macho de levante",
    "AT": "Añojo toro",
    "VH": "Vaca de horro",
    "T2": "Toro de 2 dientes",
    "R":  "Reproductor",
    "M1": "Macho 1 diente",
    "M3": "Macho 3 dientes",
    # Equinos / Mulares
    "Y":  "Yegua",
    "P1": "Potro 1 año",
    "P2": "Potro 2 años",
    "M2": "Macho 2 dientes",
    "C":  "Caballo",
}

# ─── NORMALIZACIÓN DE PROCEDENCIA (MUNICIPIOS) ────────────────────────────────
# Corrige variantes del mismo municipio detectadas en los PDFs.
# Clave = nombre tal cual aparece (en Title Case), Valor = nombre canónico.
NORMALIZAR_PROCEDENCIA: dict[str, str] = {
    # Variantes de "Entrada De Flaco" (zona de descarga de la Central Ganadera)
    "Entra De Flaco":           "Entrada De Flaco",
    "Entradad De Flaco":        "Entrada De Flaco",
    "Entrada De Falco":         "Entrada De Flaco",
    "Entrada De Gando Flaco":   "Entrada De Flaco",
    # Variantes de "Entrada De Feria"
    "Entra De Feria":           "Entrada De Feria",
    "Entrada D Eferia":         "Entrada De Feria",
    "Entrada De Ganado":        "Entrada De Feria",
    # Tildes y variantes ortográficas
    "Santa Bárbara":            "Santa Barbara",
    "Sabana Larga":             "Sabanalarga",
    "Caucasi":                  "Caucasia",
    # Nombres truncados o abreviados
    "San Pedro De Los":         "San Pedro De Los Milagros",
    "San Pedro":                "San Pedro De Los Milagros",
    "Santuario":                "El Santuario",
    "Santa Rosa":               "Santa Rosa De Osos",
    "Landazuri -":              "Landazuri",
    "Victoria- Caldas":         "Victoria",
}

# ─── MESES EN ESPAÑOL ─────────────────────────────────────────────────────────
_MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

_MESES_ABREV = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sept": 9, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

_PATRON_HORA = r"(?:\d{1,2}:\d{2}:\d{2}\s*(?:[AP]M|[ap]\.?\s*m\.?))"


def normalizar_procedencia(nombre: str) -> str:
    """Normaliza el nombre del municipio según el diccionario de correcciones."""
    return NORMALIZAR_PROCEDENCIA.get(nombre, nombre)


def limpiar_numero(texto: str) -> float | None:
    """Convierte texto de número colombiano (1.234.567) a float."""
    if not texto or str(texto).strip() in ("", "0", "-"):
        return None
    # Elimina puntos de miles y espacios, reemplaza coma decimal si la hubiera
    limpio = str(texto).replace(".", "").replace(",", ".").replace(" ", "")
    try:
        return float(limpio)
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# EXTRACCIÓN DE METADATA DESDE EL CONTENIDO DEL PDF
# ═══════════════════════════════════════════════════════════════════════════════

def extraer_metadata_del_pdf(texto_completo: str) -> dict:
    """
    Extrae fecha, tipo de subasta y número de boletín directamente
    del encabezado del PDF (primeras líneas).
    
    Formatos de encabezado encontrados:
    
    2024:  RESULTADOS DE SUBASTA TRADICIONAL
           BOLETÍN N° 01
           09 DE ENERO DEL 2024
           
    2025:  RESULTADOS DE SUBASTA EQUINA N°1
           BOLETÍN N° 25
           28 DE MAYO DEL 2025

    2026:  BOLETÍN
           RESULTADOS DE SUBASTA TRADICIONAL
           01
           ENE. 6 DEL 2026
    """
    # Escaneamos más líneas porque el formato 2026 distribuye el encabezado en más renglones
    lineas = [l.strip() for l in texto_completo.split("\n") if l.strip()][:10]
    
    fecha = None
    tipo_subasta = None
    num_boletin = None
    
    # ── Buscar tipo de subasta ──
    for linea in lineas:
        upper = linea.upper()
        if "RESULTADOS DE SUBASTA" in upper:
            after = upper.replace("RESULTADOS DE SUBASTA", "").strip()
            if "GYR" in after:
                tipo_subasta = "Especial GYR"
            elif "EQUINA" in after:
                tipo_subasta = "Equina"
            elif "MULAR" in after or "MULARES" in after:
                tipo_subasta = "Mulares"
            elif "ESPECIAL" in after:
                tipo_subasta = "Especial"
            elif "TRADICIONAL" in after or "COMERCIAL" in after:
                tipo_subasta = "Tradicional"
            elif after == "" or after.startswith("DE") or after.startswith("N"):
                tipo_subasta = "Tradicional"
            else:
                tipo_subasta = "Tradicional"
            break
    
    # ── Buscar número de boletín ──
    for linea in lineas:
        match_bol = re.search(r"BOLET[IÍ]N\s*N[°º]\s*(\d+)", linea, re.IGNORECASE)
        if match_bol:
            num_boletin = int(match_bol.group(1))
            break
    # Formato 2026: "BOLETÍN" sola en una línea, número en línea posterior
    if num_boletin is None:
        for i, linea in enumerate(lineas):
            if re.match(r"^BOLET[IÍ]N$", linea, re.IGNORECASE) and i + 2 < len(lineas):
                try:
                    num_boletin = int(lineas[i + 2])
                except ValueError:
                    pass
                break
    
    # ── Buscar fecha ──
    texto_header = " ".join(lineas)
    texto_header_compacto = re.sub(r'\b([A-ZÁÉÍÓÚÑ]{2,})\s+([A-ZÁÉÍÓÚÑ]{2,})\b', r'\1\2', texto_header)
    
    # Formato A: "DD DE MES DEL YYYY" (2023-2025)
    match_fecha = re.search(
        r"(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DEL?\s+(\d{4})",
        texto_header, re.IGNORECASE
    )
    if match_fecha:
        dia_str, mes_nombre, anio_str = match_fecha.groups()
        mes_num = _MESES_ES.get(mes_nombre.lower())
        if mes_num:
            try:
                fecha = datetime(int(anio_str), mes_num, int(dia_str)).date()
            except ValueError:
                pass
    
    # Formato B: "MES. D DEL YYYY" (2026) — ej: "ENE. 6 DEL 2026"
    if fecha is None:
        match_fecha2 = re.search(
            r"([A-ZÁÉÍÓÚÑ]{3,5})\.\s*(\d{1,2})\s+DEL?\s+(\d{4})",
            texto_header, re.IGNORECASE
        )
        if match_fecha2:
            mes_abrev, dia_str, anio_str = match_fecha2.groups()
            mes_num = _MESES_ABREV.get(mes_abrev.lower().rstrip("."))
            if mes_num:
                try:
                    fecha = datetime(int(anio_str), mes_num, int(dia_str)).date()
                except ValueError:
                    pass

    # Formato C: "MES DD DEL YYYY" sin punto — ej: "ENE 6 DEL 2026", "ENERO 9 DEL 2026"
    # Cubre variantes de 2026 y futuros PDFs donde el punto se omite
    if fecha is None:
        match_fecha3 = re.search(
            r"\b([A-ZÁÉÍÓÚÑ]{3,10})\s+(\d{1,2})\s+DEL?\s+(\d{4})\b",
            texto_header, re.IGNORECASE
        )
        if match_fecha3:
            mes_str, dia_str, anio_str = match_fecha3.groups()
            mes_str_clean = mes_str.lower()
            mes_num = _MESES_ES.get(mes_str_clean)
            if not mes_num:
                mes_num = _MESES_ABREV.get(mes_str_clean[:3] if len(mes_str_clean) >= 3 else mes_str_clean)
            if mes_num:
                try:
                    fecha = datetime(int(anio_str), mes_num, int(dia_str)).date()
                except ValueError:
                    pass

    # Fallback para encabezados con palabras partidas por OCR, sin destruir conectores como "DE"
    if fecha is None:
        match_fecha4 = re.search(
            r"(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DEL?\s+(\d{4})",
            texto_header_compacto, re.IGNORECASE
        )
        if match_fecha4:
            dia_str, mes_nombre, anio_str = match_fecha4.groups()
            mes_num = _MESES_ES.get(mes_nombre.lower())
            if mes_num:
                try:
                    fecha = datetime(int(anio_str), mes_num, int(dia_str)).date()
                except ValueError:
                    pass

    return {
        "fecha": fecha,
        "tipo_subasta": tipo_subasta or "Tradicional",
        "num_boletin": num_boletin,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FALLBACKS: extracción del nombre del archivo (si el PDF no tiene encabezado)
# ═══════════════════════════════════════════════════════════════════════════════

def extraer_fecha_de_nombre(nombre_archivo: str) -> date | None:
    """Fallback: extrae fecha del nombre del archivo si el encabezado del PDF falla."""
    # Format 1: 17_02_26_cg or 17_02_26 (DD_MM_YY optionally followed by _something)
    # Tolera sufijos como -1, -v2, -compressed después de la fecha
    match = re.search(r"[_\-](\d{2})_(\d{2})_(\d{2})(?:[_\-]|$|\.)", nombre_archivo)
    if match:
        dia, mes, anio_corto = match.groups()
        try:
            return datetime(2000 + int(anio_corto), int(mes), int(dia)).date()
        except ValueError:
            pass
    
    # Format 2: 17_de-mayo_-2024 or similar (DD_de-MES_-YYYY)
    match2 = re.search(r"[_\-](\d{1,2})[_]*de[-_]*([a-zñáéíóú]{3,})[-_]*(\d{4})", nombre_archivo, re.IGNORECASE)
    if match2:
        dia_str, mes_nombre, anio_str = match2.groups()
        mes_nombre = mes_nombre.lower().rstrip("-_")
        # Probar mes completo
        mes_num = _MESES_ES.get(mes_nombre)
        # Probar abreviatura
        if not mes_num:
            # Limpiar posibles puntos o fragmentos
            mes_abrev = mes_nombre[:3]
            mes_num = _MESES_ABREV.get(mes_abrev)
        
        if mes_num:
            try:
                return datetime(int(anio_str), mes_num, int(dia_str)).date()
            except ValueError:
                pass

    # Format 3: DD de MES (without year, assume current year)
    match_fecha_no_year = re.search(r"[_\-](\d{1,2})[_\s\-]*de[_\s\-]*([a-zñáéíóú]{3,})", nombre_archivo, re.IGNORECASE)
    # Format 4: DD_MM without year
    match_fecha_num_no_year = re.search(r"[_\-](\d{1,2})[_\-](\d{1,2})(?:_|$|\.)", nombre_archivo)

    if match_fecha_no_year:
        dia_str, mes_nombre = match_fecha_no_year.groups()
        mes_nombre = mes_nombre.lower().rstrip("-_ ")
        mes_num = _MESES_ES.get(mes_nombre)
        if not mes_num:
            mes_abrev = mes_nombre[:3] if len(mes_nombre) >= 3 else mes_nombre
            mes_num = _MESES_ABREV.get(mes_abrev)
            
        if mes_num:
            try:
                return datetime(datetime.now().year, mes_num, int(dia_str)).date()
            except ValueError:
                pass
    elif match_fecha_num_no_year:
        dia_str, mes_str = match_fecha_num_no_year.groups()
        try:
            return datetime(datetime.now().year, int(mes_str), int(dia_str)).date()
        except ValueError:
            pass

    return None


def extraer_numero_boletin(nombre_archivo: str) -> int | None:
    """Fallback: extrae el número de boletín del nombre del archivo."""
    match = re.match(r"^(\d+)_", nombre_archivo)
    return int(match.group(1)) if match else None


def extraer_tipo_subasta(nombre_archivo: str) -> str:
    """Fallback: detecta tipo de subasta del nombre del archivo."""
    nombre = nombre_archivo.lower()
    if "gyr" in nombre:
        return "Especial GYR"
    elif "equina" in nombre:
        return "Equina"
    elif "mular" in nombre or "mulares" in nombre:
        return "Mulares"
    elif "tradicional" in nombre or "comercial" in nombre:
        return "Tradicional"
    elif "especial" in nombre:
        return "Especial"
    return "Tradicional"


# ═══════════════════════════════════════════════════════════════════════════════
# PARSEO DE DATOS DE LOTES
# ═══════════════════════════════════════════════════════════════════════════════

def parsear_lineas_pdf(texto_pagina: str, nombre_archivo: str = "") -> list[dict]:
    """
    Extrae filas de datos de lotes del texto bruto del PDF.

    Estrategia en cascada:
    1. Regex ESTRICTO  (requiere campo de hora HH:MM:SS a/p. m.)
    2. Regex FALLBACK  (sin campo de hora — para PDFs con layout distinto)
    3. Diagnóstico     (imprime las 5 primeras líneas candidatas para debug)

    Formato esperado por el ESTRICTO:
      LOTE TIPO CANT P_TOTAL P_PROM PROCEDENCIA HH:MM:SS a.m. BASE_KG FINAL_KG TOTAL

    Ejemplo tradicional: 001 HV 1 384 384 SAN LUIS 08:22:02 a. m. 8.000 9.400 3.609.600
    Ejemplo equina:      001 M3 1 0   0   YARUMAL  11:20:13 a. m. 0     0     2.900.000
    """
    filas: list[dict] = []

    # ── 1. REGEX ESTRICTO (con campo hora) ────────────────────────────────────
    _PATRON_ESTRICTO = re.compile(
        r'^\s*(\d{1,3})\s+'                         # Lote
        r'([A-Z][A-Z0-9]?)\s+'                      # Tipo (HV, ML, R…)
        r'(\d+)\s+'                                  # Cantidad
        r'([\d\.]+)\s+'                              # P.Total (kg)
        r'([\d\.]+)\s+'                              # P.Prom (kg)
        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-\.]+?)\s+'     # Procedencia
        rf'({_PATRON_HORA})\s+'                      # Hora HH:MM:SS AM/PM o a. m./p. m.
        r'([\d\.]+)\s+'                              # $Base/kg
        r'([\d\.]+)\s+'                              # $Final/kg
        r'([\d\.]+)',                                 # $Total
        re.MULTILINE | re.IGNORECASE,
    )

    for m in _PATRON_ESTRICTO.finditer(texto_pagina):
        lote, tipo, cant, p_total, p_prom, procedencia, hora, base_kg, final_kg, promedio = m.groups()
        tipo = tipo.strip()
        if tipo not in TIPOS_ANIMAL:
            continue
        # Para subastas equinas/mulares, $Base y $Final son 0 (sin precio/kg).
        # El precio real por animal está en la última columna ($Promedio/$Total).
        precio_final = limpiar_numero(final_kg) or limpiar_numero(promedio)
        filas.append({
            "numero_lote":       lote.strip(),
            "tipo_codigo":       tipo,
            "cantidad_animales": int(cant),
            "peso_total_kg":     limpiar_numero(p_total),
            "peso_promedio_kg":  limpiar_numero(p_prom),
            "procedencia":       normalizar_procedencia(procedencia.strip().title()),
            "hora_subasta":      hora.strip(),
            "precio_base_kg":    limpiar_numero(base_kg),
            "precio_final_kg":   precio_final,
        })

    if filas:
        return filas

    # ── 2. REGEX FALLBACK (sin campo hora) ────────────────────────────────────
    # Cubre PDFs donde la columna de hora está ausente o en formato diferente.
    _PATRON_FALLBACK = re.compile(
        r'^\s*(\d{1,3})\s+'                         # Lote
        r'([A-Z][A-Z0-9]?)\s+'                      # Tipo
        r'(\d+)\s+'                                  # Cantidad
        r'([\d\.]+)\s+'                              # P.Total
        r'([\d\.]+)\s+'                              # P.Prom
        r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-\.]{2,30}?)\s+'# Procedencia (más corta)
        r'([\d\.]+)\s+'                              # $Base/kg o $Final/kg
        r'([\d\.]+)\s+'                              # $Final/kg o $Total
        r'([\d\.]+)',                                 # $Total
        re.MULTILINE | re.IGNORECASE,
    )

    candidatos_fallback = 0
    for m in _PATRON_FALLBACK.finditer(texto_pagina):
        lote, tipo, cant, p_total, p_prom, procedencia, base_kg, final_kg, promedio = m.groups()
        tipo = tipo.strip()
        if tipo not in TIPOS_ANIMAL:
            continue
        candidatos_fallback += 1
        precio_final = limpiar_numero(final_kg) or limpiar_numero(promedio)
        filas.append({
            "numero_lote":       lote.strip(),
            "tipo_codigo":       tipo,
            "cantidad_animales": int(cant),
            "peso_total_kg":     limpiar_numero(p_total),
            "peso_promedio_kg":  limpiar_numero(p_prom),
            "procedencia":       normalizar_procedencia(procedencia.strip().title()),
            "hora_subasta":      None,   # No disponible en este formato
            "precio_base_kg":    limpiar_numero(base_kg),
            "precio_final_kg":   precio_final,
        })

    if filas:
        archivo = nombre_archivo or "PDF"
        print(f"    ℹ️  {archivo}: regex fallback (sin hora) → {len(filas)} lotes")
        return filas

    # ── 3. DIAGNÓSTICO (ambos regex fallaron) ─────────────────────────────────
    # Imprime las primeras líneas que *parecen* candidatos (empiezan con número)
    # para que sea fácil detectar qué formato tiene el PDF problemático.
    if nombre_archivo:
        lineas_candidatas = [
            l for l in texto_pagina.splitlines()
            if re.match(r'^\s*\d{1,3}\s+[A-Z]', l.strip())
        ]
        if lineas_candidatas:
            ejemplo_hora = any(re.search(_PATRON_HORA, l, re.IGNORECASE) for l in lineas_candidatas[:20])
            if ejemplo_hora:
                print(f"    ℹ️  {nombre_archivo}: se detectaron horas en formato reciente; revisar parser si no hubo match")
            print(f"    🔍 {nombre_archivo}: {len(lineas_candidatas)} líneas candidatas "
                  f"no parseadas. Primeras 3:")
            for linea in lineas_candidatas[:3]:
                print(f"       > {linea.strip()[:120]}")
        else:
            print(f"    🔍 {nombre_archivo}: sin líneas candidatas (¿tabla en imagen/columnas?)")

    return filas




# ═══════════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO DE PDFs
# ═══════════════════════════════════════════════════════════════════════════════

def procesar_pdf(ruta_pdf: str, metadata: dict = None) -> pd.DataFrame:
    """
    Lee un PDF de subasta y retorna un DataFrame con todos los lotes.
    
    Extrae fecha, tipo de subasta y número de boletín directamente
    del contenido del PDF (más confiable que el nombre del archivo).
    Usa el nombre del archivo solo como fallback.
    """
    nombre_archivo = os.path.basename(ruta_pdf)
    todas_las_filas = []
    
    try:
        with pdfplumber.open(ruta_pdf) as pdf:
            texto_completo = ""
            for pagina in pdf.pages:
                texto = pagina.extract_text()
                if texto:
                    texto_completo += texto + "\n"
            
            # ── Extraer metadata del contenido del PDF (fuente de verdad) ──
            meta_pdf = extraer_metadata_del_pdf(texto_completo)
            
            # Usar metadata del PDF como fuente primaria; nombre del archivo como fallback
            fecha = meta_pdf["fecha"] or extraer_fecha_de_nombre(nombre_archivo)
            tipo_subasta = meta_pdf["tipo_subasta"] if meta_pdf["tipo_subasta"] != "Tradicional" else extraer_tipo_subasta(nombre_archivo)
            num_boletin = meta_pdf["num_boletin"] or extraer_numero_boletin(nombre_archivo)

            # Fallo explícito — nunca silencioso — si la fecha no pudo extraerse de ninguna fuente
            if fecha is None:
                print(f"  ⚠️ FECHA NO ENCONTRADA: {nombre_archivo} — revisar manualmente")
            
            # ── Extraer datos de lotes ──
            filas = parsear_lineas_pdf(texto_completo, nombre_archivo=nombre_archivo)
            
            for fila in filas:
                fila["fecha_subasta"] = fecha
                fila["tipo_subasta"] = tipo_subasta
                fila["numero_boletin"] = num_boletin
                fila["archivo_fuente"] = nombre_archivo
            
            todas_las_filas.extend(filas)
    
    except Exception as e:
        print(f"  ❌ Error procesando {nombre_archivo}: {e}")
        return pd.DataFrame()
    
    if not todas_las_filas:
        print(f"  ⚠️  Sin datos en: {nombre_archivo}")
        return pd.DataFrame()
    
    df = pd.DataFrame(todas_las_filas)
    print(f"  ✅ {nombre_archivo}: {len(df)} lotes extraídos")
    return df


def procesar_todos_los_pdfs(
    carpeta_pdfs: str = None,
    boletines_meta: list = None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Procesa todos los PDFs en una carpeta y retorna un DataFrame unificado
    junto con la lista de archivos que quedaron sin fecha.

    Parámetros:
    - carpeta_pdfs: carpeta donde están los PDFs descargados
    - boletines_meta: lista de dicts con metadata (de extract.py), opcional

    Retorna:
    - (df_final, pdfs_sin_fecha): DataFrame con todos los lotes y lista de
      nombres de archivo que no tienen fecha_subasta.
    """
    if carpeta_pdfs is None:
        carpeta_pdfs = os.path.join(_DIR_PROYECTO, "pdfs")
    if not os.path.exists(carpeta_pdfs):
        raise FileNotFoundError(f"La carpeta '{carpeta_pdfs}' no existe. Ejecuta extract.py primero.")
    
    # Crear índice de metadata por nombre de archivo
    meta_index = {}
    if boletines_meta:
        for b in boletines_meta:
            meta_index[b["nombre_archivo"]] = b
    
    archivos_pdf = sorted([
        f for f in os.listdir(carpeta_pdfs) if f.endswith(".pdf")
    ])
    
    print(f"\n📂 Procesando {len(archivos_pdf)} PDFs en '{carpeta_pdfs}/'...")
    
    dfs = []
    for nombre in archivos_pdf:
        ruta = os.path.join(carpeta_pdfs, nombre)
        meta = meta_index.get(nombre)
        df = procesar_pdf(ruta, meta)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        print("⚠️  No se pudo extraer datos de ningún PDF.")
        return pd.DataFrame(), []
    
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Limpieza final: eliminar registros sin precio (requerido siempre)
    df_final = df_final.dropna(subset=["precio_final_kg"])
    df_final = df_final[df_final["precio_final_kg"] > 0]

    # Subastas equinas/mulares: caballos y mulas se venden por cabeza, no por kg.
    # Su peso reportado es 0 y su precio es el total por animal (puede superar 500k COP).
    TIPOS_EQUINOS = {"Equina", "Mulares"}
    es_equino = df_final["tipo_subasta"].isin(TIPOS_EQUINOS)

    # Para bovinos: también requerir peso válido
    df_final = df_final[es_equino | (df_final["peso_total_kg"].notna() & (df_final["peso_total_kg"] > 0))]

    # Diagnóstico previo a filtros de calidad — ayuda a detectar el umbral correcto
    if "tipo_subasta" in df_final.columns:
        for tipo_s, grupo in df_final.groupby("tipo_subasta"):
            p_max = grupo["precio_final_kg"].max() if not grupo.empty else 0
            print(f"  📊 {tipo_s}: {len(grupo)} lotes, precio_final_kg máx = {p_max:,.0f}")

    # Filtros de calidad: eliminar errores obvios de parseo del PDF
    n_antes = len(df_final)
    # Bovinos: ningún lote pesa <10 kg ni supera 500k COP/kg
    bovino_invalido = (~es_equino) & (
        (df_final["peso_total_kg"] < 10) | (df_final["precio_final_kg"] > 500_000)
    )
    # Equinos: precio total por animal no puede superar 500 millones COP (error de parseo)
    equino_invalido = es_equino & (df_final["precio_final_kg"] > 500_000_000)
    df_final = df_final[~(bovino_invalido | equino_invalido)]
    n_filtrados = n_antes - len(df_final)
    if n_filtrados > 0:
        print(f"  ⚠️  {n_filtrados} registros eliminados por datos sospechosos (errores de parseo)")
    
    # Reporte de PDFs sin fecha — detallado por archivo para detectar fallos silenciosos
    pdfs_sin_fecha: list[str] = []
    if "fecha_subasta" in df_final.columns:
        pdfs_sin_fecha = (
            df_final[df_final["fecha_subasta"].isna()]["archivo_fuente"]
            .unique()
            .tolist()
        )
    if pdfs_sin_fecha:
        print(f"\n⚠️  {len(pdfs_sin_fecha)} PDF(s) sin fecha_subasta — revisar manualmente:")
        for nombre in pdfs_sin_fecha:
            n_lotes = (df_final["archivo_fuente"] == nombre).sum()
            print(f"    - {nombre} ({n_lotes} lotes afectados)")
    else:
        fechas_nulas = df_final["fecha_subasta"].isna().sum() if "fecha_subasta" in df_final.columns else 0
        if fechas_nulas > 0:
            print(f"  ⚠️  {fechas_nulas} registros sin fecha (no se pudo extraer del PDF ni del nombre)")

    print(f"\n✅ Total de lotes procesados: {len(df_final)}")
    fechas_validas = df_final['fecha_subasta'].dropna()
    if not fechas_validas.empty:
        print(f"📅 Rango de fechas: {fechas_validas.min()} → {fechas_validas.max()}")

    return df_final, pdfs_sin_fecha


# ─── EJECUCIÓN DIRECTA ────────────────────────────────────────────────────────
if __name__ == "__main__":
    df, _ = procesar_todos_los_pdfs()
    
    if not df.empty:
        # Vista previa
        print("\n📋 Primeras 5 filas:")
        print(df.head().to_string())
        
        print("\n📊 Estadísticas básicas:")
        print(f"  Precio final por kg - Promedio: ${df['precio_final_kg'].mean():,.0f} COP/kg")
        print(f"  Precio final por kg - Mínimo:   ${df['precio_final_kg'].min():,.0f} COP/kg")
        print(f"  Precio final por kg - Máximo:   ${df['precio_final_kg'].max():,.0f} COP/kg")
        
        # Guardar CSV provisional
        csv_path = os.path.join(_DIR_PROYECTO, "datos_subastas.csv")
        df.to_csv(csv_path, index=False)
        print(f"\n💾 Guardado como: {csv_path}")
