import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modelo.predictor_mlops import (  # noqa: E402
    DISCLAIMER,
    calcular_drift_basico,
    calcular_resumen_ejecutivo,
    predecir_lote,
    valor_finito,
)
from shared.data_cleaning import (  # noqa: E402
    FERIA_CASANARE,
    FERIA_CENTRAL,
    normalizar_procedencia,
    normalizar_tipo_subasta,
)


st.set_page_config(
    page_title="Predictor MLOps - SubaDatos",
    page_icon="🐄",
    layout="wide",
)


MARKETS = {
    "Central Ganadera de Medellin, Antioquia": {
        "id": "central_antioquia",
        "feria": FERIA_CENTRAL,
        "tabla": "subastas",
        "columnas": "fecha_subasta,numero_boletin,tipo_subasta,tipo_codigo,cantidad_animales,peso_total_kg,precio_final_kg,procedencia",
        "order_col": "fecha_subasta",
    },
    "Subasta General de Yopal, Casanare": {
        "id": "casanare_yopal",
        "feria": FERIA_CASANARE,
        "tabla": "subastas_casanare",
        "columnas": "fecha_subasta,numero_pdf,tipo_subasta,sexo_codigo,cantidad_animales,peso_total_kg,precio_final_kg,procedencia",
        "order_col": "fecha_subasta",
    },
}


def _credenciales_supabase() -> tuple[str, str]:
    load_dotenv(ROOT / ".env")
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if url and key:
        return url, key
    try:
        return st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"]
    except Exception:
        st.error("Faltan SUPABASE_URL y SUPABASE_KEY en variables de entorno o Streamlit secrets.")
        st.stop()


def _normalizar_dashboard(df: pd.DataFrame, feria: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "sexo_codigo" in df.columns:
        df = df.rename(columns={"sexo_codigo": "tipo_codigo", "numero_pdf": "numero_boletin"})
    df["fecha_subasta"] = pd.to_datetime(df["fecha_subasta"], errors="coerce")
    for col in ["precio_final_kg", "peso_total_kg", "cantidad_animales"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    cantidad = df["cantidad_animales"].replace(0, pd.NA)
    df["peso_promedio_kg"] = df["peso_total_kg"] / cantidad
    df["tipo_subasta"] = df["tipo_subasta"].apply(lambda v: normalizar_tipo_subasta(v, feria))
    df["procedencia"] = df["procedencia"].apply(lambda v: normalizar_procedencia(v, feria))
    df["precio_total_cop"] = df["peso_total_kg"] * df["precio_final_kg"]
    return df.dropna(subset=["fecha_subasta", "precio_final_kg", "procedencia", "tipo_codigo"])


@st.cache_data(ttl=60 * 60 * 24)
def cargar_mercado(label: str) -> pd.DataFrame:
    cfg = MARKETS[label]
    url, key = _credenciales_supabase()
    client = create_client(url, key)
    filas = []
    inicio, fin = 0, 999
    while True:
        res = (
            client.table(cfg["tabla"])
            .select(cfg["columnas"])
            .gt("precio_final_kg", 0)
            .order(cfg["order_col"])
            .range(inicio, fin)
            .execute()
        )
        if not res.data:
            break
        filas.extend(res.data)
        if len(res.data) < 1000:
            break
        inicio += 1000
        fin += 1000
    return _normalizar_dashboard(pd.DataFrame(filas), cfg["feria"])


def formato_cop(valor: float | None, sufijo: str = "") -> str:
    if not valor_finito(valor):
        return "N/D"
    return f"${valor:,.0f}{sufijo}"


def mostrar_resumen(df: pd.DataFrame, label: str):
    resumen = calcular_resumen_ejecutivo(df, label)
    st.markdown("### Resumen ejecutivo")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            f"Categoria: {resumen.get('categoria_top', 'N/D')}",
            formato_cop(resumen.get("precio_categoria_top"), "/kg"),
        )
    with c2:
        variacion = resumen.get("variacion_pct")
        st.metric(
            "Mayor variacion",
            resumen.get("categoria_variacion") or "N/D",
            f"{variacion:+.1f}%" if variacion is not None else "Sin dato prev.",
        )
    with c3:
        st.metric("Municipio destacado", resumen.get("municipio_top", "N/D"))
    with c4:
        st.metric(
            "Muestra",
            f"{resumen.get('total_lotes', 0):,} lotes",
            f"{resumen.get('total_animales', 0):,} animales",
        )
    st.info(resumen.get("lectura", "No hay datos suficientes para el resumen."))


def grafica_resultado(resultado, fecha_pred):
    fig = go.Figure()
    if resultado.fechas_recientes and resultado.precios_recientes:
        fig.add_trace(
            go.Scatter(
                x=resultado.fechas_recientes,
                y=resultado.precios_recientes,
                mode="lines+markers",
                name="Historico reciente",
                line=dict(color="#16A34A", width=2.5),
            )
        )
    if resultado.precio_kg is not None:
        fig.add_trace(
            go.Scatter(
                x=[str(fecha_pred)],
                y=[resultado.precio_kg],
                mode="markers",
                name="Estimacion",
                marker=dict(size=16, symbol="star", color="#6B21A8", line=dict(width=2, color="white")),
            )
        )
    if resultado.rango_bajo is not None and resultado.rango_alto is not None:
        fig.add_hrect(y0=resultado.rango_bajo, y1=resultado.rango_alto, fillcolor="#6B21A8", opacity=0.10, line_width=0)
    fig.update_layout(
        title="Estimacion vs historico reciente",
        xaxis_title="Fecha",
        yaxis_title="Precio (COP/kg)",
        yaxis_tickformat="$,.0f",
        hovermode="x unified",
    )
    return fig


st.title("🐄 Predictor MLOps SubaDatos")
st.caption("Modelos livianos derivados de pipelines MLflow; Streamlit solo carga artefactos joblib y metadata.")

mercado_label = st.selectbox("Mercado", list(MARKETS.keys()))
market_id = MARKETS[mercado_label]["id"]

with st.spinner(f"Cargando historico de {mercado_label}..."):
    df = cargar_mercado(mercado_label)

if df.empty:
    st.warning("No se encontraron datos para este mercado.")
    st.stop()

mostrar_resumen(df, mercado_label)

st.download_button(
    "Descargar CSV del historico cargado",
    data=df.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"subadatos_{market_id}.csv",
    mime="text/csv",
    use_container_width=True,
)

st.divider()
st.markdown("### Estimar lote")

tipos_subasta = sorted(df["tipo_subasta"].dropna().unique().tolist()) or ["Tradicional"]
categorias = sorted(df["tipo_codigo"].dropna().astype(str).unique().tolist())

with st.form("predictor_mlops_page"):
    c1, c2, c3 = st.columns(3)
    with c1:
        tipo_subasta = st.selectbox(
            "Tipo de subasta",
            tipos_subasta,
            index=tipos_subasta.index("Tradicional") if "Tradicional" in tipos_subasta else 0,
            disabled=market_id == "casanare_yopal",
        )
        tipo_codigo = st.selectbox("Categoria animal", categorias)
    with c2:
        pesos_cat = pd.to_numeric(df[df["tipo_codigo"].astype(str) == tipo_codigo]["peso_promedio_kg"], errors="coerce")
        mediana = pesos_cat.median()
        peso_default = float(mediana) if pd.notna(mediana) and mediana > 0 else 280.0
        peso_promedio = st.number_input("Peso promedio por animal (kg)", 20.0, 900.0, round(peso_default, 1), step=5.0)
        cantidad = st.number_input("Cantidad de animales", 1, 500, 20, step=1)
    with c3:
        fecha_pred = st.date_input("Fecha estimada", value=pd.Timestamp.today().date())
        hora_pred = st.time_input(
            "Hora estimada",
            value=pd.Timestamp("2026-01-01 10:00").time(),
            disabled=market_id == "casanare_yopal",
        )
    calcular = st.form_submit_button("Calcular estimacion", use_container_width=True)

st.warning(DISCLAIMER)

if calcular:
    resultado = predecir_lote(
        market_id,
        {
            "tipo_subasta": tipo_subasta,
            "tipo_codigo": tipo_codigo,
            "peso_promedio_kg": peso_promedio,
            "cantidad_animales": int(cantidad),
            "fecha_subasta": fecha_pred,
            "hora_subasta": str(hora_pred),
        },
        historico_df=df,
    )
    if not resultado.available:
        st.error(resultado.message)
        if market_id == "central_antioquia":
            st.info("El modelo Central puede requerir recalibracion o revision de artefactos.")
        st.stop()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Precio estimado/kg", formato_cop(resultado.precio_kg, "/kg"))
    m2.metric("Rango aprox.", f"{formato_cop(resultado.rango_bajo)} - {formato_cop(resultado.rango_alto)}")
    m3.metric("Precio por animal", formato_cop(resultado.precio_animal))
    m4.metric("Valor lote", formato_cop(resultado.valor_lote))

    if resultado.promedio_historico:
        delta = f"{resultado.diferencia_pct:+.1f}%" if resultado.diferencia_pct is not None else None
        st.metric("Historico reciente categoria", formato_cop(resultado.promedio_historico, "/kg"), delta=delta)
    st.plotly_chart(grafica_resultado(resultado, fecha_pred), use_container_width=True)

    meta = []
    if resultado.modelo:
        meta.append(resultado.modelo)
    if resultado.fecha_entrenamiento:
        meta.append(f"entrenado: {resultado.fecha_entrenamiento[:10]}")
    if resultado.registros_entrenamiento:
        meta.append(f"{resultado.registros_entrenamiento:,} registros")
    if resultado.rmse:
        meta.append(f"RMSE aprox. ${resultado.rmse:,.0f}/kg")
    elif resultado.mae:
        meta.append(f"MAE aprox. ${resultado.mae:,.0f}/kg")
    if meta:
        st.caption(" · ".join(meta))

with st.expander("Calidad de datos y drift"):
    for alerta in calcular_drift_basico(df):
        texto = f"{alerta['titulo']}: {alerta['detalle']}"
        if alerta["nivel"] == "warning":
            st.warning(texto)
        elif alerta["nivel"] == "ok":
            st.success(texto)
        else:
            st.info(texto)
