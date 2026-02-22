---
name: plotly-dashboard
description: >
  Aplica este skill cuando el usuario pida crear, modificar o revisar
  cualquier gráfica o visualización del proyecto. Aplica para cualquier
  archivo que use plotly, px, go, o st.plotly_chart. También aplica cuando
  el usuario mencione colores, gráficas, series de tiempo, barras, dispersión,
  heatmap o cualquier visualización de datos ganaderos.
---

# Skill: Plotly Dashboard — Mercado Ganadero

## Regla de oro
Siempre usar **Plotly Express (px)** como primera opción.
Solo usar **Graph Objects (go)** cuando px no tenga la funcionalidad necesaria.
Combinar ambos cuando se necesite: `fig = px.line(...); fig.add_trace(go.Scatter(...))`

## Paleta de colores del proyecto
```python
COLORES = {
    # Tipos de animal — FIJOS, siempre los mismos en todas las gráficas
    "HV": "#1B5E20",   # Hembra de vientre    → Verde oscuro
    "HL": "#388E3C",   # Hembra de levante    → Verde medio
    "MC": "#388E3C",   # Macho de ceba        → Verde medio
    "ML": "#7B1FA2",   # Macho de levante     → Morado
    "AT": "#0288D1",   # Añojo toro           → Azul
    "VH": "#FFB300",   # Vaca de horro        → Ámbar claro
    "T2": "#00838F",   # Toro 2 dientes       → Teal
    "R":  "#4E342E",   # Reproductor          → Café
    # UI
    "principal":   "#1B5E20",  # Verde bosque
    "secundario":  "#388E3C",  # Verde campo
    "acento":      "#F57F17",  # Ámbar — KPIs
    "alerta":      "#C62828",  # Rojo — precio bajo
    "fondo":       "#F5F5F5",  # Fondo general
    "texto":       "#212121",  # Texto principal
    "fondo_carta": "#FFFFFF",  # Fondo de gráficas
}

# Lista ordenada para gráficas con desglose por tipo_codigo
COLOR_SEQUENCE = [
    "#1B5E20", "#7B1FA2", "#0288D1", "#F57F17",
    "#388E3C", "#C62828", "#00838F", "#4E342E"
]
```

## Template base del proyecto
```python
import plotly.graph_objects as go
import plotly.express as px

TEMPLATE = go.layout.Template()
TEMPLATE.layout = go.Layout(
    font=dict(family="Google Sans, Roboto, sans-serif", color="#212121"),
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#F5F5F5",
    colorway=COLOR_SEQUENCE,
    title=dict(font=dict(size=16, color="#1B5E20", family="Google Sans")),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right", x=1,
        font=dict(size=11)
    ),
    margin=dict(l=40, r=20, t=60, b=40),
    hovermode="x unified",
)

# Aplicar a todas las figuras del proyecto
import plotly.io as pio
pio.templates["ganadero"] = TEMPLATE
pio.templates.default = "ganadero"
```

## Gráficas del proyecto — código listo

### 1. Serie de tiempo del precio por tipo de animal
```python
def grafica_serie_tiempo(df: pd.DataFrame) -> go.Figure:
    """Serie de tiempo del precio promedio por kg, desglosado por tipo de animal."""
    df_agrupado = (
        df[df["precio_final_kg"] > 0]
        .groupby(["fecha_subasta", "tipo_codigo"])["precio_final_kg"]
        .mean()
        .reset_index()
    )
    fig = px.line(
        df_agrupado,
        x="fecha_subasta",
        y="precio_final_kg",
        color="tipo_codigo",
        color_discrete_map=COLORES,
        title="Precio histórico por kg — por tipo de animal",
        labels={
            "fecha_subasta": "Fecha",
            "precio_final_kg": "Precio final (COP/kg)",
            "tipo_codigo": "Tipo"
        },
        template="ganadero",
    )
    fig.update_traces(line=dict(width=2.5))
    fig.update_xaxes(rangeslider_visible=True)
    return fig
```

### 2. Barras horizontales por municipio
```python
def grafica_barras_municipio(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """Precio promedio por municipio de procedencia."""
    df_mun = (
        df[df["precio_final_kg"] > 0]
        .groupby("procedencia")["precio_final_kg"]
        .mean()
        .nlargest(top_n)
        .reset_index()
        .sort_values("precio_final_kg")
    )
    fig = px.bar(
        df_mun,
        x="precio_final_kg",
        y="procedencia",
        orientation="h",
        title=f"Top {top_n} municipios por precio promedio/kg",
        labels={"precio_final_kg": "Precio promedio (COP/kg)", "procedencia": ""},
        color="precio_final_kg",
        color_continuous_scale=["#81C784", "#1B5E20"],
        template="ganadero",
    )
    fig.update_coloraxes(showscale=False)
    return fig
```

### 3. Volumen transado por semana
```python
def grafica_volumen_semanal(df: pd.DataFrame) -> go.Figure:
    """Millones de COP transados por semana."""
    df["semana"] = pd.to_datetime(df["fecha_subasta"]).dt.to_period("W").dt.start_time
    df_vol = (
        df.groupby("semana")["precio_total_cop"]
        .sum()
        .div(1_000_000)
        .reset_index()
    )
    fig = px.bar(
        df_vol,
        x="semana",
        y="precio_total_cop",
        title="Volumen transado por semana (millones COP)",
        labels={"semana": "Semana", "precio_total_cop": "Millones COP"},
        color_discrete_sequence=["#388E3C"],
        template="ganadero",
    )
    return fig
```

### 4. Dispersión peso vs precio
```python
def grafica_dispersion(df: pd.DataFrame) -> go.Figure:
    """Relación entre peso promedio y precio final por kg."""
    df_clean = df[(df["precio_final_kg"] > 0) & (df["peso_promedio_kg"] > 0)]
    fig = px.scatter(
        df_clean,
        x="peso_promedio_kg",
        y="precio_final_kg",
        color="tipo_codigo",
        color_discrete_map=COLORES,
        title="Peso vs Precio por kg",
        labels={
            "peso_promedio_kg": "Peso promedio (kg)",
            "precio_final_kg": "Precio final (COP/kg)",
            "tipo_codigo": "Tipo"
        },
        opacity=0.6,
        trendline="ols",
        template="ganadero",
    )
    return fig
```

### 5. Heatmap de estacionalidad
```python
def grafica_estacionalidad(df: pd.DataFrame) -> go.Figure:
    """Precio promedio por mes y tipo de animal — detecta estacionalidad."""
    df["mes"] = pd.to_datetime(df["fecha_subasta"]).dt.month
    df["mes_nombre"] = pd.to_datetime(df["fecha_subasta"]).dt.strftime("%b")
    df_heat = (
        df[df["precio_final_kg"] > 0]
        .groupby(["mes_nombre", "tipo_codigo"])["precio_final_kg"]
        .mean()
        .reset_index()
        .pivot(index="tipo_codigo", columns="mes_nombre", values="precio_final_kg")
    )
    fig = px.imshow(
        df_heat,
        color_continuous_scale=["#E8F5E9", "#1B5E20"],
        title="Estacionalidad — Precio promedio por mes y tipo",
        labels=dict(color="COP/kg"),
        text_auto=".0f",
        template="ganadero",
    )
    return fig
```

## Reglas de estilo obligatorias
- SIEMPRE filtrar `precio_final_kg > 0` antes de graficar
- SIEMPRE usar `use_container_width=True` en `st.plotly_chart()`
- NUNCA usar gráficas de torta (pie) con más de 3 categorías
- Los títulos de gráficas siempre en español
- El hover siempre muestra valores en formato COP: `hovertemplate="$%{y:,.0f} COP/kg"`
- Grosor de línea siempre 2.5px mínimo para buena visibilidad
- Usar `rangeslider_visible=True` en todas las series de tiempo

## Formato de números colombianos en hover
```python
# Para precios en COP
hovertemplate="<b>%{x}</b><br>Precio: $%{y:,.0f} COP/kg<extra></extra>"

# Para pesos en kg  
hovertemplate="<b>%{x}</b><br>Peso: %{y:,.0f} kg<extra></extra>"

# Para totales en millones
hovertemplate="<b>%{x}</b><br>Total: $%{y:.1f}M COP<extra></extra>"
```
