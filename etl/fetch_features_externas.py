"""
fetch_features_externas.py
==========================
Descarga features macroeconómicas y climáticas para el modelo de precios
ganaderos. Fuentes validadas y gratuitas:

  - CLIMA:  Open-Meteo archive API (sin key) → lluvia, temp_max, ET0
  - IPC:    Banco de la República de Colombia CSV público → variación mensual IPC alimentos
  - MAÍZ:   World Bank Pink Sheet CSV → precio maíz USD/ton (mensual)

Guarda los resultados en Supabase → tabla `features_externas` (upsert por mes).

Uso:
    python etl/fetch_features_externas.py            ← histórico completo (2023–hoy)
    python etl/fetch_features_externas.py --meses 6  ← últimos N meses

Autor: SubaDatos · 2026
"""

import os
import sys
import io
import argparse
from datetime import date
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
LAT_MED  = 6.25      # Medellín, Antioquia
LON_MED  = -75.56
FECHA_INICIO_HIST = date(2023, 1, 1)


# ─── SUPABASE ─────────────────────────────────────────────────────────────────
def conectar_supabase():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY no configuradas")
    return create_client(url, key)


# ─── FUENTE 1: CLIMA (Open-Meteo) ────────────────────────────────────────────
def fetch_clima(fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    """
    Descarga datos climáticos diarios de Open-Meteo para Medellín y agrega por mes.
    Variables: lluvia acumulada (mm), temperatura máxima promedio (°C),
    ET0 FAO (mm) como proxy de estrés hídrico / disponibilidad de pasto.

    Retorna columnas: fecha_mes, lluvia_acum_mm, temp_max_prom_c, et0_prom_mm
    """
    print("   [1/3] Open-Meteo (clima Medellin)...")
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude":  LAT_MED,
            "longitude": LON_MED,
            "start_date": str(fecha_inicio),
            "end_date":   str(fecha_fin),
            "daily": "precipitation_sum,temperature_2m_max,et0_fao_evapotranspiration",
            "timezone": "America/Bogota",
        },
        timeout=30
    )
    if r.status_code != 200:
        print(f"   WARN Open-Meteo {r.status_code}: {r.text[:150]}")
        return pd.DataFrame()

    d   = r.json().get("daily", {})
    df  = pd.DataFrame({
        "fecha":       d.get("time", []),
        "lluvia":      d.get("precipitation_sum", []),
        "temp_max":    d.get("temperature_2m_max", []),
        "et0":         d.get("et0_fao_evapotranspiration", []),
    })
    df["fecha"]    = pd.to_datetime(df["fecha"])
    df["fecha_mes"] = df["fecha"].dt.to_period("M").dt.to_timestamp()

    res = df.groupby("fecha_mes").agg(
        lluvia_acum_mm  =("lluvia",   "sum"),
        temp_max_prom_c =("temp_max", "mean"),
        et0_prom_mm     =("et0",      "mean"),
    ).reset_index()
    print(f"        OK: {len(res)} meses ({res['fecha_mes'].min().date()} - {res['fecha_mes'].max().date()})")
    return res


# ─── FUENTE 2: IPC (Banco de la República de Colombia) ───────────────────────
def fetch_ipc(fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    """
    Descarga el índice de precios al consumidor de alimentos desde el
    Banco de la República de Colombia, vía su API de series de tiempo.

    Serie: IPC variación mensual total (código: 1.1.BOG_CCCL_33_ACM_CPI_TOTAL)
    Formato: CSV

    Si falla, usa el endpoint de DANE IPC disponible en datos.gov.co.

    Retorna columnas: fecha_mes, ipc_var_mensual_pct
    """
    print("   [2/3] BanRep - IPC Colombia...")

    # BanRep series de tiempo API
    # Documentación: https://www.banrep.gov.co/es/estadisticas/serie-IPC
    url = "https://suameca.banrep.gov.co/estadisticas-economicas/rest/consultaEconomica/serie"
    series_ipc = "1.3_1.1.BOG_CCCL_33_ACM_CPI_TOTAL"  # IPC total mensual variacion

    params = {
        "codigoCatalogo": series_ipc,
        "frecuencia":     "MES",
        "fechaInicio":    fecha_inicio.strftime("%d/%m/%Y"),
        "fechaFin":       fecha_fin.strftime("%d/%m/%Y"),
        "exportar":       "true",
        "formato":        "json",
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            recs = r.json()
            if recs and isinstance(recs, list):
                df = pd.DataFrame(recs)
                # Buscar columnas de fecha y valor
                col_fecha = next((c for c in df.columns if "fecha" in c.lower()), None)
                col_val   = next((c for c in df.columns if "valor" in c.lower()), None)
                if col_fecha and col_val:
                    df["fecha_mes"] = pd.to_datetime(df[col_fecha], dayfirst=True, errors="coerce").dt.to_period("M").dt.to_timestamp()
                    df["ipc_var_mensual_pct"] = pd.to_numeric(df[col_val], errors="coerce")
                    res = df[["fecha_mes", "ipc_var_mensual_pct"]].dropna().drop_duplicates("fecha_mes")
                    print(f"        OK BanRep: {len(res)} meses")
                    return res
    except Exception as e:
        print(f"        WARN BanRep API: {e}")

    # Fallback: IPC general Colombia desde datos.gov.co (DANE serie encadenada)
    print("        Intentando fallback DANE...")
    try:
        r2 = requests.get(
            "https://www.datos.gov.co/resource/3fjj-z9qy.json",
            params={"$limit": 500, "$order": ":id ASC"},
            timeout=30
        )
        if r2.status_code == 200:
            recs2 = r2.json()
            if recs2:
                df2 = pd.DataFrame(recs2)
                print(f"        DANE cols: {list(df2.columns)[:8]}")
                # Intentar extraer fecha y valor
                col_f = next((c for c in df2.columns if any(x in c.lower() for x in ["fecha","periodo","mes","a_o"])), None)
                col_v = next((c for c in df2.columns if any(x in c.lower() for x in ["variaci","valor","ipc"])), None)
                if col_f and col_v:
                    df2["fecha_mes"] = pd.to_datetime(df2[col_f], errors="coerce").dt.to_period("M").dt.to_timestamp()
                    df2["ipc_var_mensual_pct"] = pd.to_numeric(df2[col_v], errors="coerce")
                    res2 = df2[["fecha_mes", "ipc_var_mensual_pct"]].dropna().drop_duplicates("fecha_mes")
                    print(f"        OK DANE fallback: {len(res2)} meses")
                    return res2
    except Exception as e2:
        print(f"        WARN DANE fallback: {e2}")

    print("        WARN: IPC no disponible, se omite este feature")
    return pd.DataFrame()


# ─── FUENTE 3: PRECIO MAÍZ (World Bank Pink Sheet) ───────────────────────────
def fetch_precio_maiz(fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    """
    Descarga el precio mensual del maíz amarillo desde el Pink Sheet del
    Banco Mundial en formato CSV (actualización mensual).

    URL: https://thedocs.worldbank.org/en/doc/[...]/CMO-Historical-Data-Monthly.csv

    Retorna columnas: fecha_mes, precio_maiz_usd_ton
    """
    print("   [3/3] World Bank Pink Sheet - precio maiz...")

    # URL directa del CSV mensual del Pink Sheet 2024 (actualizado mensualmente)
    url_csv = (
        "https://thedocs.worldbank.org/en/doc/"
        "5d903e848db1d1b83e0ec8f744e55570-0350012021/"
        "related/CMO-Historical-Data-Monthly.xlsx"
    )

    try:
        # Intentar descargar el Excel del Pink Sheet
        r = requests.get(url_csv, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            xl = pd.read_excel(io.BytesIO(r.content), sheet_name=None, header=None)
            # El Pink Sheet tiene una hoja "Monthly Prices"
            hoja = None
            for nombre in xl:
                if "monthly" in nombre.lower():
                    hoja = xl[nombre]; break
            if hoja is None:
                hoja = list(xl.values())[0]

            # Buscar la columna de maíz - suele llamarse "Maize" o "MAIZE"
            # Las primeras filas son headers, buscar la fila correcta
            max_row = min(10, len(hoja))
            maize_col = None
            header_row = None
            for i in range(max_row):
                row = hoja.iloc[i].astype(str).str.lower()
                if any("maize" in c or "corn" in c for c in row):
                    header_row = i
                    maize_col  = row[row.str.contains("maize|corn", na=False)].index[0]
                    break

            if maize_col is not None and header_row is not None:
                df = hoja.iloc[header_row+1:].copy()
                df.columns = hoja.iloc[header_row].values
                df = df[[hoja.iloc[header_row, 0], maize_col]].copy()
                df.columns = ["fecha_raw", "precio_maiz_usd_ton"]
                df["fecha_mes"] = pd.to_datetime(df["fecha_raw"], errors="coerce").dt.to_period("M").dt.to_timestamp()
                df["precio_maiz_usd_ton"] = pd.to_numeric(df["precio_maiz_usd_ton"], errors="coerce")
                res = df[["fecha_mes", "precio_maiz_usd_ton"]].dropna()
                res = res[(res["fecha_mes"] >= pd.Timestamp(fecha_inicio)) &
                          (res["fecha_mes"] <= pd.Timestamp(fecha_fin))]
                print(f"        OK Pink Sheet: {len(res)} meses de precio maiz")
                return res
    except Exception as e:
        print(f"        WARN Pink Sheet Excel: {e}")

    # Fallback: indicador anual del Banco Mundial
    print("        Intentando fallback WB indicator API...")
    try:
        r2 = requests.get(
            "https://api.worldbank.org/v2/en/indicator/AG.PRD.MAIZ.MT?format=json&mrv=5&per_page=20",
            timeout=30
        )
        if r2.status_code == 200:
            payload = r2.json()
            filas = []
            if isinstance(payload, list) and len(payload) > 1:
                for rec in (payload[1] or []):
                    if rec.get("value") and str(rec.get("date","")).isdigit():
                        for m in range(1, 13):
                            filas.append({
                                "fecha_mes": pd.Timestamp(f"{rec['date']}-{m:02d}-01"),
                                "precio_maiz_usd_ton": float(rec["value"]) / 1e6,  # convertir toneladas
                            })
            if filas:
                df3 = pd.DataFrame(filas)
                df3 = df3[(df3["fecha_mes"] >= pd.Timestamp(fecha_inicio)) &
                          (df3["fecha_mes"] <= pd.Timestamp(fecha_fin))]
                print(f"        OK WB API anual ({len(df3)} registros mensualizados)")
                return df3
    except Exception as e2:
        print(f"        WARN WB API fallback: {e2}")

    print("        WARN: precio maiz no disponible, se omite")
    return pd.DataFrame()


# ─── ENSAMBLADO ───────────────────────────────────────────────────────────────
def ensamblar_features(fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    """
    Llama a las 3 fuentes y hace LEFT JOIN por fecha_mes.
    Rellena con interpolación lineal los meses sin dato.
    """
    df_clima = fetch_clima(fecha_inicio, fecha_fin)
    df_ipc   = fetch_ipc(fecha_inicio, fecha_fin)
    df_maiz  = fetch_precio_maiz(fecha_inicio, fecha_fin)

    # Base: todos los meses del rango
    meses = pd.date_range(
        start=pd.Timestamp(fecha_inicio).to_period("M").to_timestamp(),
        end=pd.Timestamp(fecha_fin).to_period("M").to_timestamp(),
        freq="MS",
    )
    base = pd.DataFrame({"fecha_mes": meses})

    for df_ext in [df_clima, df_ipc, df_maiz]:
        if df_ext.empty:
            continue
        df_ext = df_ext.copy()
        df_ext["fecha_mes"] = pd.to_datetime(df_ext["fecha_mes"]).dt.to_period("M").dt.to_timestamp()
        base = base.merge(df_ext, on="fecha_mes", how="left")

    # Interpolar para meses sin dato
    nums = base.select_dtypes(include="number").columns.tolist()
    if nums:
        base[nums] = base[nums].interpolate(method="linear", limit_direction="both")

    base["fecha_mes"] = base["fecha_mes"].dt.date
    return base


# ─── SUPABASE UPSERT ──────────────────────────────────────────────────────────
def subir_a_supabase(df: pd.DataFrame, supabase) -> None:
    if df.empty:
        print("   WARN: sin datos para subir")
        return

    # Columnas definidas en la tabla
    cols_tabla = ["fecha_mes", "lluvia_acum_mm", "temp_max_prom_c", "et0_prom_mm",
                  "ipc_var_mensual_pct", "precio_maiz_usd_ton"]

    registros = []
    for _, row in df.iterrows():
        rec = {"fecha_mes": str(row["fecha_mes"])}
        for col in cols_tabla[1:]:
            val = row.get(col, None)
            if pd.notna(val) if val is not None else False:
                rec[col] = round(float(val), 4)
        registros.append(rec)

    total = 0
    for i in range(0, len(registros), 200):
        lote = registros[i:i+200]
        supabase.table("features_externas").upsert(lote, on_conflict="fecha_mes").execute()
        total += len(lote)

    print(f"   OK: {total} meses subidos a features_externas")


# ─── SQL ─────────────────────────────────────────────────────────────────────
SQL_CREATE_TABLE = """
-- EJECUTAR ESTO EN SUPABASE SQL EDITOR ANTES DE CORRER EL SCRIPT
-- Dashboard > SQL Editor > New query > pegar todo > Run

CREATE TABLE IF NOT EXISTS features_externas (
    fecha_mes            DATE        NOT NULL PRIMARY KEY,
    lluvia_acum_mm       FLOAT,
    temp_max_prom_c      FLOAT,
    et0_prom_mm          FLOAT,
    ipc_var_mensual_pct  FLOAT,
    precio_maiz_usd_ton  FLOAT,
    actualizado_en       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fe_fecha ON features_externas(fecha_mes);

-- Verificar:
SELECT * FROM features_externas ORDER BY fecha_mes DESC LIMIT 12;
"""


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main(meses: int | None = None) -> None:
    print("\n" + "=" * 60)
    print("FEATURES EXTERNAS - Central Ganadera de Medellin")
    print("=" * 60)

    hoy          = date.today()
    fecha_inicio = (hoy.replace(day=1) - relativedelta(months=meses - 1)) if meses else FECHA_INICIO_HIST
    fecha_fin    = hoy - relativedelta(days=1)

    # Guardar SQL en carpeta sql/
    sql_dir  = os.path.join(os.path.dirname(__file__), "..", "sql")
    sql_path = os.path.join(sql_dir, "create_features_externas.sql")
    os.makedirs(sql_dir, exist_ok=True)
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(SQL_CREATE_TABLE)
    print(f"\n   SQL guardado: {os.path.abspath(sql_path)}")
    print("   Asegurate de ejecutarlo en Supabase antes de continuar.")
    if not getattr(args, "no_interactive", False):
        input("\n   Presiona Enter cuando hayas creado la tabla en Supabase...")

    print(f"\n   Rango: {fecha_inicio} a {fecha_fin}")
    print("\n   Descargando features...")
    df = ensamblar_features(fecha_inicio, fecha_fin)

    print(f"\n   Preview (ultimas 4 filas):")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    print(df.tail(4).to_string(index=False))

    print(f"\n   Conectando a Supabase...")
    sb = conectar_supabase()
    subir_a_supabase(df, sb)

    print(f"\n   LISTO: {len(df)} meses de features externas disponibles")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--meses", type=int, default=None,
                        help="Ultimos N meses (default: historico desde 2023-01)")
    parser.add_argument("--no-interactive", action="store_true",
                        help="No esperar confirmacion del usuario")
    args = parser.parse_args()
    main(meses=args.meses)
