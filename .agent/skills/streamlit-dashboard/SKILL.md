---
name: streamlit-dashboard
description: >
  Aplica este skill cuando el usuario trabaje en modelo/app.py o cualquier
  archivo Streamlit. Aplica cuando mencione pestañas, sidebar, filtros,
  KPIs, layout, componentes de la app, o despliegue en Streamlit Cloud.
  Usar SIEMPRE junto al skill plotly-dashboard cuando haya gráficas.
---

# Skill: Streamlit Dashboard — Mercado Ganadero

## Estructura de pestañas del proyecto
```
app.py
├── Pestaña 1: 📈 Precios        → serie de tiempo + KPIs
├── Pestaña 2: 🗺️ Municipios    → barras por procedencia + mapa
├── Pestaña 3: ⚖️ Volumen       → volumen semanal + estacionalidad
├── Pestaña 4: 🔍 Detalle       → tabla filtrable de registros
└── Pestaña 5: 🤖 Predictor     → modelo predictivo (Fase 6)
```

## Estructura base del app.py
```python
import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client
from dotenv import load_dotenv
import os

# ── CONFIGURACIÓN ────────────────────────────────────────────
st.set_page_config(
    page_title="Mercado Ganadero — Central Ganadera Medellín",
    page_icon="🐄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CARGA DE DATOS ────────────────────────────────────────────
@st.cache_data(ttl=3600)  # Cachear 1 hora
def cargar_datos() -> pd.DataFrame:
    """Carga todos los datos de Supabase y los retorna como DataFrame."""
    load_dotenv()
    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    resultado = supabase.table("subastas").select("*").execute()
    df = pd.DataFrame(resultado.data)
    df["fecha_subasta"] = pd.to_datetime(df["fecha_subasta"])
    df = df[df["precio_final_kg"] > 0]  # Filtrar lotes no vendidos
    return df

# ── SIDEBAR CON FILTROS GLOBALES ──────────────────────────────
def sidebar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    """Renderiza los filtros del sidebar y retorna el DataFrame filtrado."""
    st.sidebar.image("🐄", width=60)  # Reemplazar con logo si hay
    st.sidebar.title("Filtros")

    # Rango de fechas
    fecha_min = df["fecha_subasta"].min().date()
    fecha_max = df["fecha_subasta"].max().date()
    fechas = st.sidebar.date_input(
        "📅 Período",
        value=(fecha_min, fecha_max),
        min_value=fecha_min,
        max_value=fecha_max
    )

    # Tipo de subasta
    tipos_subasta = ["Todos"] + sorted(df["tipo_subasta"].unique().tolist())
    tipo_subasta = st.sidebar.selectbox("🏷️ Tipo de subasta", tipos_subasta)

    # Tipo de animal (multiselect)
    tipos_animal = sorted(df["tipo_codigo"].unique().tolist())
    tipos_sel = st.sidebar.multiselect(
        "🐄 Tipo de animal",
        tipos_animal,
        default=tipos_animal
    )

    # Municipio
    municipios = ["Todos"] + sorted(df["procedencia"].unique().tolist())
    municipio = st.sidebar.selectbox("📍 Municipio", municipios)

    # Aplicar filtros
    df_fil = df.copy()
    if len(fechas) == 2:
        df_fil = df_fil[
            (df_fil["fecha_subasta"].dt.date >= fechas[0]) &
            (df_fil["fecha_subasta"].dt.date <= fechas[1])
        ]
    if tipo_subasta != "Todos":
        df_fil = df_fil[df_fil["tipo_subasta"] == tipo_subasta]
    if tipos_sel:
        df_fil = df_fil[df_fil["tipo_codigo"].isin(tipos_sel)]
    if municipio != "Todos":
        df_fil = df_fil[df_fil["procedencia"] == municipio]

    st.sidebar.markdown("---")
    st.sidebar.metric("Registros filtrados", f"{len(df_fil):,}")

    return df_fil

# ── KPIs ──────────────────────────────────────────────────────
def mostrar_kpis(df: pd.DataFrame):
    """Muestra los 3 KPIs principales en columnas."""
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "💰 Precio prom. kg",
            f"${df['precio_final_kg'].mean():,.0f}",
            help="Precio promedio final por kg en COP"
        )
    with col2:
        st.metric(
            "📦 Lotes subastados",
            f"{len(df):,}",
            help="Total de lotes en el período seleccionado"
        )
    with col3:
        st.metric(
            "⚖️ Kg totales",
            f"{df['peso_total_kg'].sum():,.0f}",
            help="Kilogramos totales transados"
        )
    with col4:
        st.metric(
            "💵 Total transado",
            f"${df['precio_total_cop'].sum()/1_000_000:,.1f}M",
            help="Millones de COP transados en el período"
        )

# ── MAIN ──────────────────────────────────────────────────────
def main():
    # Header
    st.markdown(
        "<h1 style='color:#1B5E20; font-family:Google Sans,sans-serif;'>"
        "🐄 Mercado Ganadero — Central Ganadera Medellín</h1>",
        unsafe_allow_html=True
    )

    # Cargar datos
    with st.spinner("Cargando datos desde Supabase..."):
        df = cargar_datos()

    # Sidebar
    df_filtrado = sidebar_filtros(df)

    if df_filtrado.empty:
        st.warning("⚠️ No hay datos para los filtros seleccionados.")
        return

    # KPIs siempre visibles arriba
    mostrar_kpis(df_filtrado)
    st.divider()

    # Pestañas
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Precios",
        "🗺️ Municipios",
        "⚖️ Volumen",
        "🔍 Detalle",
        "🤖 Predictor"
    ])

    with tab1:
        st.subheader("Evolución histórica del precio por kg")
        # → usar grafica_serie_tiempo() del skill plotly-dashboard

    with tab2:
        st.subheader("Precio y volumen por municipio de procedencia")
        # → usar grafica_barras_municipio() del skill plotly-dashboard

    with tab3:
        st.subheader("Volumen transado y estacionalidad")
        # → usar grafica_volumen_semanal() y grafica_estacionalidad()

    with tab4:
        st.subheader("Detalle de registros")
        # → tabla filtrable con st.dataframe()

    with tab5:
        st.subheader("🤖 Predictor de precios")
        st.info("Disponible en la Fase 6 — después de entrenar el modelo.")

if __name__ == "__main__":
    main()
```

## Reglas obligatorias de Streamlit

### Performance
- SIEMPRE usar `@st.cache_data` para carga de datos desde Supabase
- SIEMPRE usar `@st.cache_resource` para cargar el modelo predictivo
- El TTL de cache debe ser 3600 (1 hora) para datos de subastas

### Gráficas
- SIEMPRE usar `st.plotly_chart(fig, use_container_width=True)`
- NUNCA usar `st.pyplot()` — solo Plotly
- Importar las funciones de gráficas desde el skill plotly-dashboard

### Estilo
- El header principal siempre con color `#1B5E20` y font `Google Sans`
- Los st.metric() usan los colores automáticos de Streamlit (no sobreescribir)
- st.divider() para separar secciones visualmente
- st.spinner() en toda operación que tome más de 1 segundo

### Filtros
- Los filtros globales SIEMPRE van en el sidebar
- Filtros específicos de una pestaña van dentro de esa pestaña
- SIEMPRE mostrar el conteo de registros filtrados en el sidebar

### Despliegue en Streamlit Cloud
- El archivo principal es `modelo/app.py`
- Las variables de entorno van en Secrets de Streamlit Cloud (no en .env)
- Formato en Streamlit Cloud Secrets:
  ```toml
  SUPABASE_URL = "https://xxxx.supabase.co"
  SUPABASE_KEY = "tu_key_aqui"
  ```
- Acceder con: `st.secrets["SUPABASE_URL"]` en producción
- Usar `os.environ.get()` con fallback a `st.secrets` para funcionar en ambos entornos:
  ```python
  url = os.environ.get("SUPABASE_URL") or st.secrets["SUPABASE_URL"]
  ```
