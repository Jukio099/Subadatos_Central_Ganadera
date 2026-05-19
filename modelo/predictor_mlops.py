from __future__ import annotations

import json
import math
import os
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path, PureWindowsPath
from typing import Any

import numpy as np
import pandas as pd


_DIR_MODELO = Path(__file__).resolve().parent
_ARTIFACTS_DIR = _DIR_MODELO / "ml_artifacts"

CENTRAL_FEATURE_COLS = [
    "peso_promedio_kg",
    "tipo_codigo_num",
    "cantidad_animales",
    "fecha_num",
    "hora_seg",
    "dia_semana",
    "mes",
    "semana_anio",
    "precio_mes_promedio",
]

CASANARE_FEATURE_COLS = [
    "peso_promedio_kg",
    "cantidad_animales",
    "sexo_codigo_enc",
    "mes",
    "anio",
]

MARKETS = {
    "central_antioquia": {
        "label": "Central Ganadera de Medellin, Antioquia",
        "short_label": "Central Ganadera",
    },
    "casanare_yopal": {
        "label": "Subasta General de Yopal, Casanare",
        "short_label": "Yopal/Casanare",
    },
}

DISCLAIMER = "Estimacion basada en historico, no recomendacion de compra/venta."


@dataclass
class PredictionResult:
    market_id: str
    market_label: str
    available: bool
    message: str
    precio_kg: float | None = None
    mae: float | None = None
    rmse: float | None = None
    r2: float | None = None
    precio_animal: float | None = None
    valor_lote: float | None = None
    rango_bajo: float | None = None
    rango_alto: float | None = None
    promedio_historico: float | None = None
    registros_historicos: int = 0
    diferencia_pct: float | None = None
    fechas_recientes: list[str] | None = None
    precios_recientes: list[float] | None = None
    fecha_entrenamiento: str | None = None
    registros_entrenamiento: int | None = None
    fuente: str | None = None
    modelo: str | None = None


def market_label(market_id: str) -> str:
    return MARKETS.get(market_id, {}).get("label", market_id)


def cargar_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def hora_a_seg(hora: str | None) -> int:
    if not hora:
        return 36_000
    texto = str(hora).strip().lower()
    try:
        if "a." in texto or "p." in texto:
            es_pm = "p." in texto
            hora_limpia = texto.replace("a. m.", "").replace("p. m.", "").strip()
            partes = hora_limpia.split(":")
            h_val = int(partes[0])
            m_val = int(partes[1])
            s_val = int(partes[2]) if len(partes) > 2 else 0
            if es_pm and h_val != 12:
                h_val += 12
            if not es_pm and h_val == 12:
                h_val = 0
            return h_val * 3600 + m_val * 60 + s_val

        partes = texto.split(":")
        return int(partes[0]) * 3600 + int(partes[1]) * 60 + (int(partes[2]) if len(partes) > 2 else 0)
    except (ValueError, IndexError):
        return 36_000


def resolver_artefacto(ruta: str | os.PathLike[str] | None, raiz: Path = _DIR_MODELO) -> Path:
    if not ruta:
        raise FileNotFoundError("Ruta de artefacto vacia")

    ruta_str = str(ruta)
    raw = Path(ruta_str)
    raw_windows = PureWindowsPath(ruta_str)
    candidatos = [raw]
    if raw.name:
        candidatos.append(raiz / raw.name)
    if raw_windows.name and raw_windows.name != raw.name:
        candidatos.append(raiz / raw_windows.name)
    if not raw.is_absolute():
        candidatos.append(raiz / raw)

    for candidato in candidatos:
        if candidato.exists():
            return candidato

    raise FileNotFoundError(f"No se encontro el artefacto {raw.name or raw}")


def _load_joblib(path: Path) -> Any:
    import joblib

    return joblib.load(path)


def _download_cached(url: str, filename: str, market_id: str) -> Path:
    cache_dir = Path(tempfile.gettempdir()) / "subadatos_models" / market_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest = cache_dir / filename
    if dest.exists() and dest.stat().st_size > 200:
        return dest

    tmp = dest.with_suffix(dest.suffix + ".tmp")
    urllib.request.urlretrieve(url, tmp)
    if tmp.stat().st_size <= 200:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"El artefacto descargado parece invalido: {filename}")
    tmp.replace(dest)
    return dest


@lru_cache(maxsize=8)
def _central_bundle(tipo_subasta: str) -> dict[str, Any]:
    meta = cargar_json(_DIR_MODELO / "modelo_metadata.json")
    modelos = {m["tipo"]: m for m in meta.get("modelos", [])}
    if tipo_subasta not in modelos:
        disponibles = ", ".join(modelos) or "ninguno"
        raise KeyError(f"No hay modelo Central para '{tipo_subasta}'. Disponibles: {disponibles}")

    info = modelos[tipo_subasta]
    artefactos = info.get("artefactos", {})
    return {
        "metadata": meta,
        "info": info,
        "modelo": _load_joblib(resolver_artefacto(artefactos.get("modelo"))),
        "scaler_X": _load_joblib(resolver_artefacto(artefactos.get("scaler_X"))),
        "encoder": _load_joblib(resolver_artefacto(artefactos.get("encoder"))),
    }


@lru_cache(maxsize=1)
def _casanare_bundle() -> dict[str, Any]:
    root = _ARTIFACTS_DIR / "casanare_yopal"
    metadata = cargar_json(root / "metadata.json")
    urls = metadata.get("artifact_urls", {})

    model_path = root / "model.pkl"
    encoders_path = root / "encoders.pkl"

    if not model_path.exists():
        model_path = _download_cached(urls["model"], "model.pkl", "casanare_yopal")
    if not encoders_path.exists():
        encoders_path = _download_cached(urls["encoders"], "encoders.pkl", "casanare_yopal")

    return {
        "metadata": metadata,
        "modelo": _load_joblib(model_path),
        "encoders": _load_joblib(encoders_path),
    }


def _asegurar_fecha(fecha_val: date | datetime | str | pd.Timestamp | None) -> date:
    if isinstance(fecha_val, datetime):
        return fecha_val.date()
    if isinstance(fecha_val, date):
        return fecha_val
    if fecha_val is None:
        return date.today()
    parsed = pd.to_datetime(fecha_val, errors="coerce")
    if pd.isna(parsed):
        return date.today()
    return parsed.date()


def _historico_categoria(
    df: pd.DataFrame | None,
    categoria: str,
    tipo_subasta: str | None = None,
    dias: int = 180,
) -> dict[str, Any]:
    if df is None or df.empty:
        return {"promedio": None, "count": 0, "fechas": [], "precios": []}

    hist = df.copy()
    hist["fecha_subasta"] = pd.to_datetime(hist["fecha_subasta"], errors="coerce")
    hist["precio_final_kg"] = pd.to_numeric(hist["precio_final_kg"], errors="coerce")
    hist = hist.dropna(subset=["fecha_subasta", "precio_final_kg"])

    col_categoria = "tipo_codigo" if "tipo_codigo" in hist.columns else "sexo_codigo"
    if col_categoria in hist.columns:
        hist = hist[hist[col_categoria].astype(str) == str(categoria)]
    if tipo_subasta and "tipo_subasta" in hist.columns:
        hist = hist[hist["tipo_subasta"].astype(str) == str(tipo_subasta)]

    if hist.empty:
        return {"promedio": None, "count": 0, "fechas": [], "precios": []}

    fecha_max = hist["fecha_subasta"].max()
    recientes = hist[hist["fecha_subasta"] >= fecha_max - pd.Timedelta(days=dias)].sort_values("fecha_subasta")
    return {
        "promedio": float(recientes["precio_final_kg"].mean()),
        "count": int(len(recientes)),
        "fechas": recientes["fecha_subasta"].dt.strftime("%Y-%m-%d").tolist(),
        "precios": recientes["precio_final_kg"].astype(float).tolist(),
    }


def _resultado_con_historico(
    *,
    market_id: str,
    precio_kg: float,
    error_aprox: float,
    peso_promedio_kg: float,
    cantidad_animales: int,
    historico: dict[str, Any],
    metadata: dict[str, Any],
    model_info: dict[str, Any] | None = None,
) -> PredictionResult:
    precio_animal = precio_kg * peso_promedio_kg
    valor_lote = precio_animal * cantidad_animales
    promedio = historico.get("promedio")
    diferencia_pct = None
    if promedio and promedio > 0:
        diferencia_pct = ((precio_kg - promedio) / promedio) * 100

    info = model_info or metadata
    return PredictionResult(
        market_id=market_id,
        market_label=market_label(market_id),
        available=True,
        message=DISCLAIMER,
        precio_kg=precio_kg,
        mae=info.get("mae") or metadata.get("mae") or error_aprox,
        rmse=info.get("rmse") or metadata.get("rmse"),
        r2=info.get("r2") or metadata.get("r2"),
        precio_animal=precio_animal,
        valor_lote=valor_lote,
        rango_bajo=max(0.0, precio_kg - error_aprox),
        rango_alto=precio_kg + error_aprox,
        promedio_historico=promedio,
        registros_historicos=int(historico.get("count", 0)),
        diferencia_pct=diferencia_pct,
        fechas_recientes=historico.get("fechas", []),
        precios_recientes=historico.get("precios", []),
        fecha_entrenamiento=metadata.get("fecha_entrenamiento"),
        registros_entrenamiento=info.get("registros") or metadata.get("registros_entrenamiento"),
        fuente=metadata.get("fuente"),
        modelo=metadata.get("motor") or metadata.get("model_type"),
    )


def predecir_lote(
    market_id: str,
    lote: dict[str, Any],
    historico_df: pd.DataFrame | None = None,
) -> PredictionResult:
    try:
        if market_id == "central_antioquia":
            return _predecir_central(lote, historico_df)
        if market_id == "casanare_yopal":
            return _predecir_casanare(lote, historico_df)
    except Exception as exc:
        return PredictionResult(
            market_id=market_id,
            market_label=market_label(market_id),
            available=False,
            message=f"No se pudo cargar el modelo: {exc}",
        )

    return PredictionResult(
        market_id=market_id,
        market_label=market_label(market_id),
        available=False,
        message="Mercado no soportado por el predictor.",
    )


def _predecir_central(lote: dict[str, Any], historico_df: pd.DataFrame | None) -> PredictionResult:
    tipo_subasta = str(lote.get("tipo_subasta") or "Tradicional")
    tipo_codigo = str(lote.get("tipo_codigo") or "").strip().upper()
    peso_prom = float(lote.get("peso_promedio_kg") or 0)
    cantidad = int(lote.get("cantidad_animales") or 1)
    fecha_sub = _asegurar_fecha(lote.get("fecha_subasta"))
    hora_seg = hora_a_seg(lote.get("hora_subasta"))

    bundle = _central_bundle(tipo_subasta)
    info = bundle["info"]
    encoder = bundle["encoder"]
    if tipo_codigo not in set(info.get("categorias", [])):
        raise ValueError(f"Categoria '{tipo_codigo}' no disponible para {tipo_subasta}")

    historico = _historico_categoria(historico_df, tipo_codigo, tipo_subasta=tipo_subasta)
    precio_mes_promedio = historico.get("promedio") or 8_000.0
    ts = pd.Timestamp(fecha_sub)
    cat_num = int(encoder.transform([tipo_codigo])[0])

    x = np.array(
        [[
            peso_prom,
            cat_num,
            float(cantidad),
            int(ts.timestamp()),
            hora_seg,
            ts.dayofweek,
            ts.month,
            int(ts.isocalendar().week),
            float(precio_mes_promedio),
        ]],
        dtype=float,
    )
    x_sc = bundle["scaler_X"].transform(x)
    precio_kg = float(bundle["modelo"].predict(x_sc)[0])
    mae = float(info.get("mae") or 0)

    return _resultado_con_historico(
        market_id="central_antioquia",
        precio_kg=precio_kg,
        error_aprox=mae,
        peso_promedio_kg=peso_prom,
        cantidad_animales=cantidad,
        historico=historico,
        metadata=bundle["metadata"],
        model_info=info,
    )


def _predecir_casanare(lote: dict[str, Any], historico_df: pd.DataFrame | None) -> PredictionResult:
    bundle = _casanare_bundle()
    metadata = bundle["metadata"]
    encoders = bundle["encoders"]

    tipo_codigo = str(lote.get("tipo_codigo") or "").strip().upper()
    peso_prom = float(lote.get("peso_promedio_kg") or 0)
    cantidad = int(lote.get("cantidad_animales") or 1)
    fecha_sub = _asegurar_fecha(lote.get("fecha_subasta"))

    le = encoders["sexo_codigo"]
    if tipo_codigo not in set(le.classes_):
        raise ValueError(f"Categoria '{tipo_codigo}' no disponible para Yopal/Casanare")

    sexo_enc = int(le.transform([tipo_codigo])[0])
    x = np.array([[peso_prom, cantidad, sexo_enc, fecha_sub.month, fecha_sub.year]], dtype=float)
    precio_kg = float(bundle["modelo"].predict(x)[0])
    rmse = float(metadata.get("rmse") or 0)
    historico = _historico_categoria(historico_df, tipo_codigo, tipo_subasta=None)

    return _resultado_con_historico(
        market_id="casanare_yopal",
        precio_kg=precio_kg,
        error_aprox=rmse,
        peso_promedio_kg=peso_prom,
        cantidad_animales=cantidad,
        historico=historico,
        metadata=metadata,
    )


def _derive_peso_promedio(df: pd.DataFrame) -> pd.DataFrame:
    if "peso_promedio_kg" in df.columns:
        return df
    if {"peso_total_kg", "cantidad_animales"}.issubset(df.columns):
        cantidad = pd.to_numeric(df["cantidad_animales"], errors="coerce").replace(0, np.nan)
        df = df.copy()
        df["peso_promedio_kg"] = pd.to_numeric(df["peso_total_kg"], errors="coerce") / cantidad
    return df


def calcular_resumen_ejecutivo(df: pd.DataFrame, feria: str) -> dict[str, Any]:
    if df.empty:
        return {"lectura": "No hay datos para construir el resumen ejecutivo."}

    work = _derive_peso_promedio(df.copy())
    work["fecha_subasta"] = pd.to_datetime(work["fecha_subasta"], errors="coerce")
    work["precio_final_kg"] = pd.to_numeric(work["precio_final_kg"], errors="coerce")
    work["cantidad_animales"] = pd.to_numeric(work["cantidad_animales"], errors="coerce").fillna(0)
    work = work.dropna(subset=["fecha_subasta", "precio_final_kg"])

    cat_top = work.groupby("tipo_codigo")["precio_final_kg"].mean().sort_values(ascending=False)
    municipio_top = work.groupby("procedencia")["precio_final_kg"].mean().sort_values(ascending=False)

    variacion_cat = None
    variacion_pct = None
    fecha_max = work["fecha_subasta"].max()
    actual = work[work["fecha_subasta"] > fecha_max - pd.Timedelta(days=30)]
    anterior = work[
        (work["fecha_subasta"] > fecha_max - pd.Timedelta(days=60))
        & (work["fecha_subasta"] <= fecha_max - pd.Timedelta(days=30))
    ]
    if not actual.empty and not anterior.empty:
        comp = (
            actual.groupby("tipo_codigo")["precio_final_kg"].mean().rename("actual").to_frame()
            .join(anterior.groupby("tipo_codigo")["precio_final_kg"].mean().rename("anterior"), how="inner")
        )
        comp = comp[comp["anterior"] > 0]
        if not comp.empty:
            comp["pct"] = (comp["actual"] - comp["anterior"]) / comp["anterior"] * 100
            variacion_cat = str(comp["pct"].abs().idxmax())
            variacion_pct = float(comp.loc[variacion_cat, "pct"])

    categoria = str(cat_top.index[0]) if not cat_top.empty else "N/D"
    municipio = str(municipio_top.index[0]) if not municipio_top.empty else "N/D"
    precio_categoria = float(cat_top.iloc[0]) if not cat_top.empty else None
    total_animales = int(work["cantidad_animales"].sum())
    total_lotes = int(len(work))

    movimiento = ""
    if variacion_cat and variacion_pct is not None:
        direccion = "subio" if variacion_pct >= 0 else "bajo"
        movimiento = f" {variacion_cat} {direccion} {abs(variacion_pct):.1f}% frente al periodo anterior."

    lectura = (
        f"En {feria}, la categoria mejor pagada en los filtros es {categoria}"
        f"{f' con promedio cercano a ${precio_categoria:,.0f}/kg' if precio_categoria else ''}. "
        f"El municipio destacado es {municipio}.{movimiento} "
        f"La muestra filtrada cubre {total_lotes:,} lotes y {total_animales:,} animales."
    )

    return {
        "categoria_top": categoria,
        "precio_categoria_top": precio_categoria,
        "categoria_variacion": variacion_cat,
        "variacion_pct": variacion_pct,
        "municipio_top": municipio,
        "total_animales": total_animales,
        "total_lotes": total_lotes,
        "lectura": lectura,
    }


def calcular_drift_basico(df: pd.DataFrame, dias_recientes: int = 90) -> list[dict[str, str]]:
    if df.empty:
        return [{"nivel": "info", "titulo": "Sin datos", "detalle": "No hay datos para monitorear drift."}]

    work = _derive_peso_promedio(df.copy())
    work["fecha_subasta"] = pd.to_datetime(work["fecha_subasta"], errors="coerce")
    work = work.dropna(subset=["fecha_subasta"])
    if work.empty:
        return [{"nivel": "info", "titulo": "Fechas no disponibles", "detalle": "No se pudo evaluar drift temporal."}]

    fecha_max = work["fecha_subasta"].max()
    corte = fecha_max - pd.Timedelta(days=dias_recientes)
    reciente = work[work["fecha_subasta"] >= corte]
    base = work[work["fecha_subasta"] < corte]

    alertas: list[dict[str, str]] = []
    if len(reciente) < 30 or len(base) < 30:
        alertas.append({
            "nivel": "info",
            "titulo": "Datos recientes limitados",
            "detalle": "La ventana reciente o el baseline tienen pocos registros; interpreta las alertas con cautela.",
        })

    for col, nombre in [
        ("peso_promedio_kg", "peso promedio"),
        ("cantidad_animales", "cantidad por lote"),
        ("precio_final_kg", "precio final/kg"),
    ]:
        if col not in work.columns or reciente.empty or base.empty:
            continue
        rec_mean = pd.to_numeric(reciente[col], errors="coerce").mean()
        base_mean = pd.to_numeric(base[col], errors="coerce").mean()
        if pd.notna(rec_mean) and pd.notna(base_mean) and base_mean > 0:
            cambio = (rec_mean - base_mean) / base_mean * 100
            if abs(cambio) >= 15:
                alertas.append({
                    "nivel": "warning",
                    "titulo": f"Cambio en {nombre}",
                    "detalle": f"La ventana reciente cambio {cambio:+.1f}% frente al historico base.",
                })

    if "tipo_codigo" in work.columns and not reciente.empty and not base.empty:
        rec_dist = reciente["tipo_codigo"].astype(str).value_counts(normalize=True)
        base_dist = base["tipo_codigo"].astype(str).value_counts(normalize=True)
        cats = rec_dist.index.union(base_dist.index)
        diffs = (rec_dist.reindex(cats, fill_value=0) - base_dist.reindex(cats, fill_value=0)).abs()
        if not diffs.empty and float(diffs.max()) >= 0.15:
            cat = str(diffs.idxmax())
            alertas.append({
                "nivel": "warning",
                "titulo": "Cambio en mezcla de categorias",
                "detalle": f"La participacion de {cat} cambio {float(diffs.max()) * 100:.1f} puntos porcentuales.",
            })

    if "edad" not in {c.lower() for c in work.columns}:
        alertas.append({
            "nivel": "info",
            "titulo": "Edad no disponible",
            "detalle": "No existe una variable de edad explicita; se monitorea como proxy con categoria y peso promedio.",
        })

    if not alertas:
        alertas.append({
            "nivel": "ok",
            "titulo": "Sin alertas fuertes",
            "detalle": "La ventana reciente se parece razonablemente al historico base en las variables monitoreadas.",
        })

    return alertas


def valor_finito(valor: float | None) -> bool:
    return valor is not None and not math.isnan(float(valor)) and math.isfinite(float(valor))
