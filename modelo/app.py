import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client
from dotenv import load_dotenv
import os
from datetime import timedelta
import locale

# Intentar setear locale en español para nombres de meses (útil para la pestaña 3)
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except Exception:
    pass

# ── CONFIGURACIÓN ────────────────────────────────────────────
st.set_page_config(
    page_title="Mercado Ganadero — Central Ganadera Medellín",
    page_icon="🐄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── PALETA DE COLORES FIJOS ─────────────────────────────────
COLORES = {
    "HV": "#1B5E20",   
    "HL": "#388E3C",   
    "MC": "#388E3C",   
    "ML": "#7B1FA2",   
    "AT": "#0288D1",   
    "VH": "#FFB300",   
    "T2": "#00838F",   
    "R":  "#4E342E",   
}

COLOR_SEQUENCE = [
    "#1B5E20", "#7B1FA2", "#0288D1", "#F57F17",
    "#388E3C", "#C62828", "#00838F", "#4E342E"
]

# ── FUNCIÓN PARA ACTUALIZAR TEMPLATE PLOTLY SEGÚN TEMA ────────
import plotly.io as pio

def aplicar_tema_plotly(modo: str):
    """Registra la plantilla de Plotly según el modo de apariencia.

    Solo reconstruye el objeto Template cuando el modo cambia — evita recrearlo
    en cada render (lo que ocurría antes y costaba ~300ms por interacción).
    """
    clave = "ganadero_claro" if "Claro" in modo else "ganadero_oscuro"
    # Si ya está registrado como default, no hacer nada
    if pio.templates.default == clave:
        return

    color_fondo = "#FFFFFF" if "Claro" in modo else "#F5F5F5"
    color_texto = "#212121"

    tmpl = go.layout.Template()
    tmpl.layout = go.Layout(
        font=dict(family="Google Sans, Roboto, sans-serif", color=color_texto),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor=color_fondo,
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
    pio.templates[clave] = tmpl
    pio.templates.default = clave

# ── CARGA DE DATOS ────────────────────────────────────────────
@st.cache_data(ttl=60 * 60 * 24)  # 24 horas — datos se actualizan semanalmente
def cargar_datos() -> pd.DataFrame:
    """Carga todos los datos de Supabase y los retorna como DataFrame."""
    load_dotenv()
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        try:
            supabase_url = st.secrets["SUPABASE_URL"]
            supabase_key = st.secrets["SUPABASE_KEY"]
        except Exception:
            st.error("❌ Faltan credenciales de Supabase en el entorno o en los secrets.")
            st.stop()
            
    try:
        supabase = create_client(supabase_url, supabase_key)
        
        # Paginación (1000 a la vez)
        todas_las_filas = []
        rango_inicio = 0
        rango_fin = 999
        
        # Seleccionamos las columnas específicamente para no saturar memoria en despliegue
        columnas = "fecha_subasta,numero_boletin,tipo_subasta,tipo_codigo,cantidad_animales,peso_total_kg,precio_final_kg,procedencia"
        
        while True:
            # Filtramos directo desde la db para traer solo precio > 0
            respuesta = supabase.table("subastas").select(columnas).gt("precio_final_kg", 0).range(rango_inicio, rango_fin).execute()
            data = respuesta.data
            
            if not data:
                break
                
            todas_las_filas.extend(data)
            
            if len(data) < 1000:
                break
                
            rango_inicio += 1000
            rango_fin += 1000
            
        df = pd.DataFrame(todas_las_filas)
        if df.empty:
            return pd.DataFrame()
            
        df["fecha_subasta"] = pd.to_datetime(df["fecha_subasta"])
        
        # FIX BARRANCABERMEJA: Limpiar anomalías de 'Barrancabermeja' (Precios  > 20k COP error OCR)
        df = df[~((df["procedencia"].str.contains("Barrancabermeja", case=False, na=False)) & (df["precio_final_kg"] > 20000))]

        # FIX PROCEDENCIAS INTERNAS: Agrupar "Entra de Gordo", "Entrada de Feria", etc.
        # Todos los que empiecen por "Entra " o "Entrada "
        es_entrada = df["procedencia"].str.contains(r"^Entra\s+|^Entrada\s+|^Entran\s+", case=False, na=False, regex=True)
        df.loc[es_entrada, "procedencia"] = "Instalaciones Central Ganadera"

        if "precio_total_cop" not in df.columns and "peso_total_kg" in df.columns:
            df["precio_total_cop"] = df["peso_total_kg"] * df["precio_final_kg"]
            
        return df
    except Exception as e:
        st.error(f"Error conectando a Supabase: {e}")
        return pd.DataFrame()

# ── FUNCIONES DE GRÁFICAS (del SKILL) ─────────────────────────
def grafica_serie_tiempo(df: pd.DataFrame) -> go.Figure:
    df_agrupado = (
        df.groupby(["fecha_subasta", "tipo_codigo"])["precio_final_kg"]
        .mean()
        .reset_index()
    )
    fig = px.line(
        df_agrupado,
        x="fecha_subasta",
        y="precio_final_kg",
        color="tipo_codigo",
        color_discrete_map=COLORES,
        title="Precio histórico promedio por tipo de animal",
        labels={
            "fecha_subasta": "Fecha",
            "precio_final_kg": "Precio promedio (COP/kg)",
            "tipo_codigo": "Tipo"
        }
    )
    fig.update_traces(
        line=dict(width=2.5),
        hovertemplate="<b>%{x}</b><br>Precio: $%{y:,.0f} COP/kg<extra></extra>"
    )
    fig.update_xaxes(rangeslider_visible=True, title="Fecha")
    fig.update_yaxes(title="Precio promedio (COP/kg)")
    return fig

def grafica_barras_municipio(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    df_mun = (
        df.groupby("procedencia")["precio_final_kg"]
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
        title=f"Top {top_n} municipios por precio promedio",
        labels={"precio_final_kg": "Precio promedio (COP/kg)", "procedencia": "Municipio"},
        color="precio_final_kg",
        color_continuous_scale=["#81C784", "#1B5E20"]
    )
    fig.update_coloraxes(showscale=False)
    fig.update_traces(hovertemplate="<b>%{y}</b><br>Precio: $%{x:,.0f} COP/kg<extra></extra>")
    fig.update_xaxes(title="Precio promedio (COP/kg)")
    fig.update_yaxes(title="")
    return fig

def grafica_volumen_semanal(df: pd.DataFrame) -> go.Figure:
    # assign() evita df.copy() — no muta el df original y es más rápido
    df_temp = df.assign(
        semana=pd.to_datetime(df["fecha_subasta"]).dt.to_period("W").dt.start_time
    )
    df_vol = (
        df_temp.groupby("semana")["precio_total_cop"]
        .sum()
        .div(1_000_000)
        .reset_index()
    )
    fig = px.bar(
        df_vol,
        x="semana",
        y="precio_total_cop",
        title="Volumen transado por semana",
        labels={"semana": "Semana", "precio_total_cop": "Millones COP"},
        color_discrete_sequence=["#388E3C"]
    )
    fig.update_traces(hovertemplate="<b>%{x}</b><br>Total: $%{y:,.1f}M COP<extra></extra>")
    fig.update_xaxes(title="Semana")
    fig.update_yaxes(title="Millones COP")
    return fig

def grafica_estacionalidad(df: pd.DataFrame) -> go.Figure:
    # Mapeo manual para forzar meses en español por si el server (Streamlit Cloud) está en inglés
    meses_es = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun",
                7:"Jul", 8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}

    # assign() en cadena — evita df.copy() + mutación
    mes_num = pd.to_datetime(df["fecha_subasta"]).dt.month
    df_temp = df.assign(mes_num=mes_num, mes_nombre=mes_num.map(meses_es))
    
    df_heat = (
        df_temp.groupby(["mes_nombre", "tipo_codigo"])["precio_final_kg"]
        .mean()
        .reset_index()
        .pivot(index="tipo_codigo", columns="mes_nombre", values="precio_final_kg")
    )
    
    # Ordenar los meses lógicamente
    meses_orden = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    columnas_validas = [m for m in meses_orden if m in df_heat.columns]
    df_heat = df_heat[columnas_validas]
    
    fig = px.imshow(
        df_heat,
        # Paleta del proyecto: morado (precio bajo) → verde (precio alto)
        color_continuous_scale=["#7B1FA2", "#9C4DCC", "#B2DFDB", "#388E3C", "#1B5E20"],
        title="Estacionalidad — Precio promedio por mes y tipo",
        labels=dict(color="COP/kg", x="Mes", y="Tipo")
    )
    # Formato de texto del heatmap a COP/kg aproximado
    fig.update_traces(texttemplate="$%{z:,.0f}")
    return fig

@st.cache_data(show_spinner=False)
def _cargar_logo(logo_path: str):
    """Carga el logo una sola vez y lo cachea — evita I/O de disco en cada render."""
    try:
        from PIL import Image
        return Image.open(logo_path)
    except Exception:
        return None


# ── SIDEBAR CON FILTROS GLOBALES ──────────────────────────────
def sidebar_filtros(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "logo.webp")
    if os.path.exists(logo_path):
        col1, col2, col3 = st.sidebar.columns([1, 6, 1])
        img = _cargar_logo(logo_path)
        if img is not None:
            col2.image(img, use_column_width=True)
        else:
            col2.image(logo_path, use_column_width=True)
            
    st.sidebar.markdown(
        "<h2 style='color:#1B5E20; text-align:center;'>Filtros</h2>", 
        unsafe_allow_html=True
    )
    
    # Botón Modo Claro / Oscuro — "Claro" es el default (más profesional)
    modo_color = st.sidebar.radio("Apariencia", ["Claro", "Oscuro"])
    
    if df.empty:
        return df, modo_color

    # 1. Rango de fechas (Default = Último año hasta la fecha máxima)
    fecha_max = df["fecha_subasta"].max().date()
    # Si tenemos datos de 2026, restamos 365 días
    fecha_default_min = fecha_max - timedelta(days=365)
    
    # Obtenemos la mínima global por seguridad
    fecha_min = df["fecha_subasta"].min().date()

    if fecha_default_min < fecha_min:
        fecha_default_min = fecha_min

    fechas = st.sidebar.date_input(
        "📅 Rango de Fechas",
        value=(fecha_default_min, fecha_max),
        min_value=fecha_min,
        max_value=fecha_max
    )

    # 2. Tipo de subasta (Default = "Tradicional" primero)
    tipos_subasta = ["Tradicional", "Todos"] + sorted([t for t in df["tipo_subasta"].dropna().unique() if t != "Tradicional"])
    tipo_subasta = st.sidebar.selectbox("🏷️ Tipo de Subasta", tipos_subasta, index=0)

    # 3. Tipo de animal (MC y ML por defecto)
    tipos_animal = sorted(df["tipo_codigo"].dropna().unique().tolist())
    defaults = [t for t in ["MC", "ML"] if t in tipos_animal]
    if not defaults: 
        defaults = tipos_animal
        
    tipos_sel = st.sidebar.multiselect(
        "🐄 Tipo de Animal",
        tipos_animal,
        default=defaults
    )

    # 4. Municipio
    municipios = ["Todos"] + sorted(df["procedencia"].dropna().unique().tolist())
    municipio = st.sidebar.selectbox("📍 Municipio", municipios)

    # ── Aplicar filtros
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

    return df_fil, modo_color

# ── KPIs ──────────────────────────────────────────────────────
def mostrar_kpis(df: pd.DataFrame):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "💰 Precio prom. kg",
            f"${df['precio_final_kg'].mean():,.0f}" if not df.empty else "$0",
        )
    with col2:
        st.metric(
            "📦 Lotes subastados",
            f"{len(df):,}",
        )
    with col3:
        # Tendencia: precio promedio último mes vs mes anterior
        if not df.empty:
            fecha_max = df["fecha_subasta"].max()
            fecha_corte = fecha_max - pd.Timedelta(days=30)
            fecha_corte2 = fecha_max - pd.Timedelta(days=60)
            precio_mes_actual = df[df["fecha_subasta"] > fecha_corte]["precio_final_kg"].mean()
            precio_mes_ant = df[
                (df["fecha_subasta"] > fecha_corte2) & (df["fecha_subasta"] <= fecha_corte)
            ]["precio_final_kg"].mean()
            if pd.notna(precio_mes_actual) and pd.notna(precio_mes_ant) and precio_mes_ant > 0:
                delta_pct = ((precio_mes_actual - precio_mes_ant) / precio_mes_ant) * 100
                st.metric(
                    "📈 Tendencia 1 mes",
                    f"${precio_mes_actual:,.0f}/kg",
                    delta=f"{delta_pct:+.1f}%",
                )
            else:
                st.metric("📈 Tendencia 1 mes", f"${precio_mes_actual:,.0f}/kg" if pd.notna(precio_mes_actual) else "N/D")
        else:
            st.metric("📈 Tendencia 1 mes", "N/D")
    with col4:
        total_cop = df['precio_total_cop'].sum() / 1_000_000 if not df.empty and 'precio_total_cop' in df else 0
        st.metric(
            "💵 Total transado (Millones)",
            f"${total_cop:,.1f}M",
        )

# ── CAJA DE AYUDA (fondo morado paleta del proyecto) ──────────
def info_box(texto: str):
    """Renderiza una caja informativa con fondo morado suave (paleta del proyecto)."""
    st.markdown(
        f"""
        <div style="
            background-color:#EDE7F6;
            border-left:4px solid #7B1FA2;
            border-radius:6px;
            padding:12px 16px;
            margin:6px 0 12px 0;
            color:#212121;
            font-size:0.88rem;
            line-height:1.6;
        ">
        💡 {texto}
        <br><span style="font-size:0.80rem; color:#6A1B9A; margin-top:6px; display:block;">
        🔍 <em>Tip: pasa el cursor sobre la gráfica y haz clic en el ícono <strong>↗</strong>
        (esquina superior derecha) para verla en pantalla completa.</em>
        </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ── ÚLTIMA SUBASTA ────────────────────────────────────────────
def tab_ultima_subasta(df: pd.DataFrame):
    """Análisis detallado de la última sesión de subasta disponible."""
    if df.empty:
        st.warning("No hay datos para mostrar.")
        return

    fecha_ultima = df["fecha_subasta"].max()
    df_sub = df[df["fecha_subasta"] == fecha_ultima].copy()

    if df_sub.empty:
        st.warning("⚠️ La última subasta no tiene datos en el rango filtrado. Amplía el rango de fechas.")
        return

    st.markdown(
        f"<h3 style='color:#7B1FA2;'>📋 Subasta del {fecha_ultima.strftime('%d %b %Y')}</h3>",
        unsafe_allow_html=True
    )
    st.caption(f"{len(df_sub):,} lotes · {df_sub['cantidad_animales'].sum():,.0f} animales · Precio prom. ${df_sub['precio_final_kg'].mean():,.0f} COP/kg")
    st.divider()

    c1, c2 = st.columns(2)

    # Gráfica 1: Animales por categoría
    with c1:
        df_cat = df_sub.groupby("tipo_codigo")["cantidad_animales"].sum().reset_index().sort_values("cantidad_animales", ascending=True)
        fig1 = px.bar(
            df_cat, x="cantidad_animales", y="tipo_codigo", orientation="h",
            title="Animales vendidos por categoría",
            labels={"cantidad_animales": "Animales", "tipo_codigo": "Tipo"},
            color="tipo_codigo", color_discrete_map=COLORES,
        )
        fig1.update_coloraxes(showscale=False)
        fig1.update_traces(hovertemplate="<b>%{y}</b><br>Animales: %{x:,}<extra></extra>")
        st.plotly_chart(fig1, use_container_width=True)
        info_box("<strong>Animales por categoría:</strong> Total de cabezas subastadas por tipo en esta sesión. Muestra qué clase de ganado dominó la oferta del día.")

    # Gráfica 2: Precio por orden de lote
    with c2:
        df_lote = df_sub.sort_values("numero_boletin").reset_index(drop=True)
        df_lote["orden"] = range(1, len(df_lote) + 1)
        fig2 = px.line(
            df_lote, x="orden", y="precio_final_kg",
            color="tipo_codigo", color_discrete_map=COLORES,
            title="Evolución del precio durante la subasta",
            labels={"orden": "Orden del lote", "precio_final_kg": "Precio (COP/kg)", "tipo_codigo": "Tipo"},
        )
        fig2.update_traces(line=dict(width=2.5))
        fig2.update_traces(hovertemplate="<b>Lote %{x}</b><br>Precio: $%{y:,.0f} COP/kg<extra></extra>")
        st.plotly_chart(fig2, use_container_width=True)
        info_box("<strong>Precio por orden de lote:</strong> Muestra si el precio subió o bajó a medida que avanzó la subasta. Una pendiente positiva indica que el mercado 'se calentó'; negativa, que la oferta superó la demanda hacia el final.")

    c3, c4 = st.columns(2)

    # Gráfica 3: Precio vs cantidad de animales
    with c3:
        fig3 = px.scatter(
            df_sub, x="cantidad_animales", y="precio_final_kg",
            color="tipo_codigo", color_discrete_map=COLORES,
            title="Precio vs cantidad de animales por lote",
            labels={"cantidad_animales": "Animales en el lote", "precio_final_kg": "Precio (COP/kg)", "tipo_codigo": "Tipo"},
            opacity=0.8, trendline="ols",
        )
        fig3.update_traces(hovertemplate="<b>%{x} animales</b><br>Precio: $%{y:,.0f} COP/kg<extra></extra>")
        st.plotly_chart(fig3, use_container_width=True)
        info_box("<strong>Precio vs cantidad:</strong> ¿Los lotes grandes se pagan mejor? Si la línea de tendencia sube, lotes más numerosos cotizan más alto (posible efecto de economía de escala). Si baja, hay descuento por volumen.")

    # Gráfica 4: Precio vs peso promedio por animal
    with c4:
        # assign() para no mutar df_sub que se comparte entre gráficas de esta tab
        df_sub = df_sub.assign(
            peso_promedio=df_sub["peso_total_kg"] / df_sub["cantidad_animales"].replace(0, pd.NA)
        ).dropna(subset=["peso_promedio"])
        fig4 = px.scatter(
            df_sub, x="peso_promedio", y="precio_final_kg",
            color="tipo_codigo", color_discrete_map=COLORES,
            title="Precio vs peso promedio por animal",
            labels={"peso_promedio": "Peso prom. por animal (kg)", "precio_final_kg": "Precio (COP/kg)", "tipo_codigo": "Tipo"},
            opacity=0.8, trendline="ols",
        )
        fig4.update_traces(hovertemplate="<b>%{x:.0f} kg/animal</b><br>Precio: $%{y:,.0f} COP/kg<extra></extra>")
        st.plotly_chart(fig4, use_container_width=True)
        info_box("<strong>Precio vs peso promedio:</strong> ¿Los animales más pesados valen más por kilo? Una correlación positiva indica que el mercado premia el gordeo. Compara por tipo para ver diferencias entre categorías.")


# ── TENDENCIAS ────────────────────────────────────────────────
def calcular_tendencia(df: pd.DataFrame, dias: int) -> tuple[float, float, float]:
    """Precio promedio en los últimos `dias` días vs los `dias` anteriores.
    Retorna (precio_periodo, precio_anterior, pct_cambio)."""
    if df.empty:
        return 0.0, 0.0, 0.0
    fecha_max = df["fecha_subasta"].max()
    fecha_inicio = fecha_max - pd.Timedelta(days=dias)
    fecha_inicio2 = fecha_max - pd.Timedelta(days=dias * 2)
    precio_actual = df[df["fecha_subasta"] > fecha_inicio]["precio_final_kg"].mean()
    precio_ant = df[
        (df["fecha_subasta"] > fecha_inicio2) & (df["fecha_subasta"] <= fecha_inicio)
    ]["precio_final_kg"].mean()
    if pd.isna(precio_actual): precio_actual = 0.0
    if pd.isna(precio_ant): precio_ant = 0.0
    pct = ((precio_actual - precio_ant) / precio_ant * 100) if precio_ant > 0 else 0.0
    return precio_actual, precio_ant, pct


def tab_tendencias(df: pd.DataFrame):
    """Evolución del precio en ventanas de 7, 30, 90 y 180 días."""
    if df.empty:
        st.warning("No hay datos para mostrar.")
        return

    ventanas = [
        (7,  "7 días"),
        (30, "1 mes"),
        (90, "3 meses"),
        (180, "6 meses"),
    ]

    # Métricas delta en fila
    st.markdown("<h4 style='color:#7B1FA2; margin-bottom:4px;'>Variación del precio promedio</h4>", unsafe_allow_html=True)
    cols = st.columns(4)
    for col, (dias, etiqueta) in zip(cols, ventanas):
        precio_act, precio_ant, pct = calcular_tendencia(df, dias)
        with col:
            if precio_ant > 0:
                st.metric(
                    f"📅 Últ. {etiqueta}",
                    f"${precio_act:,.0f}/kg",
                    delta=f"{pct:+.1f}%",
                )
            else:
                st.metric(f"📅 Últ. {etiqueta}", f"${precio_act:,.0f}/kg", delta="Sin dato prev.")

    st.divider()

    # Gráfica de líneas: precio diario para cada ventana de tiempo
    fecha_max = df["fecha_subasta"].max()
    frames = []
    for dias, etiqueta in ventanas:
        fecha_inicio = fecha_max - pd.Timedelta(days=dias)
        df_v = (
            df[df["fecha_subasta"] > fecha_inicio]
            .groupby("fecha_subasta")["precio_final_kg"]
            .mean()
            .reset_index()
        )
        df_v["ventana"] = etiqueta
        frames.append(df_v)

    df_plot = pd.concat(frames, ignore_index=True)
    fig = px.line(
        df_plot, x="fecha_subasta", y="precio_final_kg", color="ventana",
        title="Evolución del precio promedio por ventana de tiempo",
        labels={"fecha_subasta": "Fecha", "precio_final_kg": "Precio prom. (COP/kg)", "ventana": "Período"},
        color_discrete_sequence=["#7B1FA2", "#0288D1", "#388E3C", "#1B5E20"],
    )
    fig.update_traces(line=dict(width=2.5))
    fig.update_traces(hovertemplate="<b>%{x}</b><br>$%{y:,.0f} COP/kg<extra></extra>")
    fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig, use_container_width=True)
    info_box(
        "<strong>Tendencias de precio:</strong> Cada línea muestra el precio promedio diario para una ventana de tiempo. "
        "Comparar las 4 ventanas permite ver si la tendencia reciente (7 días) es una corrección puntual o si sigue el "
        "movimiento estructural de los últimos 6 meses. Aplica los filtros de <em>Tipo de Animal</em> en el sidebar para "
        "ver la tendencia de una categoría específica."
    )


# ── CONTACTO / FEEDBACK ───────────────────────────────────────
def tab_contacto():
    """Formulario de contacto y feedback — guarda en Supabase tabla `feedback`."""

    CALIFICACIONES = {
        "\u2b50 1 \u2014 Necesita mucha mejora": 1,
        "\u2b50\u2b50 2 \u2014 Regular": 2,
        "\u2b50\u2b50\u2b50 3 \u2014 Bueno": 3,
        "\u2b50\u2b50\u2b50\u2b50 4 \u2014 Muy bueno": 4,
        "\u2b50\u2b50\u2b50\u2b50\u2b50 5 \u2014 Excelente": 5,
    }
    TIPOS_USUARIO = [
        "Ganadero / Productor",
        "Comerciante / Comisionista",
        "Analista / Investigador",
        "Veterinario / Profesional del sector",
        "Estudiante",
        "Otro",
    ]

    col_form, col_info = st.columns([3, 2])

    with col_form:
        with st.form("form_feedback", clear_on_submit=True):
            nombre    = st.text_input("\U0001f464 Nombre (opcional)")
            email     = st.text_input("\U0001f4e7 Correo electr\u00f3nico (opcional)")
            tipo      = st.selectbox("\U0001f3f7\ufe0f \u00bfC\u00f3mo describes tu perfil?", TIPOS_USUARIO)
            cal_label = st.selectbox("\u2b50 Calificaci\u00f3n del dashboard", list(CALIFICACIONES.keys()), index=3)
            mensaje   = st.text_area(
                "\U0001f4ac Comentario o sugerencia",
                placeholder="\u00bfQu\u00e9 funciona bien? \u00bfQu\u00e9 mejorar\u00edas? \u00bfQu\u00e9 dato te falta?",
                height=130,
            )
            enviado = st.form_submit_button("Enviar feedback \u2192", use_container_width=True)

        if enviado:
            if not mensaje.strip():
                st.warning("Por favor escribe un comentario antes de enviar.")
            else:
                try:
                    load_dotenv()
                    supa_url = os.environ.get("SUPABASE_URL") or st.secrets.get("SUPABASE_URL", "")
                    supa_key = os.environ.get("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY", "")
                    supabase_fb = create_client(supa_url, supa_key)
                    supabase_fb.table("feedback").insert({
                        "nombre":       nombre.strip() or None,
                        "email":        email.strip()  or None,
                        "tipo_usuario": tipo,
                        "calificacion": CALIFICACIONES[cal_label],
                        "mensaje":      mensaje.strip(),
                    }).execute()
                    st.success("\u2705 \u00a1Gracias por tu feedback! Lo tendremos muy en cuenta.")
                    st.balloons()
                except Exception as e:
                    st.error(f"\u274c No se pudo guardar el feedback: {e}")

    with col_info:
        st.markdown(
            """
            <div style="background:#EDE7F6; border-left:4px solid #7B1FA2; border-radius:6px;
                        padding:16px; margin-top:8px; font-size:0.88rem; color:#212121; line-height:1.7;">
            <strong>\u00bfPor qu\u00e9 compartir tu correo?</strong><br><br>
            Si nos dejas tu email te avisaremos de:<br>
            \U0001f4ca Nuevos an\u00e1lisis de mercado<br>
            \U0001f514 Alertas de precio por categor\u00eda<br>
            \U0001f6e0\ufe0f Nuevas funciones del dashboard<br>
            \U0001f4bc Servicios de inteligencia ganadera<br><br>
            <em>No compartimos tu informaci\u00f3n con terceros.</em>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ── MAIN ──────────────────────────────────────────────────────
def main():
    # Cargar datos
    with st.spinner("Conectando con Supabase y descargando históricos..."):
        df = cargar_datos()

    if df.empty:
        st.warning("⚠️ No se encontraron datos en Supabase o no hay conexión.")
        return

    # Sidebar
    df_filtrado, modo_color = sidebar_filtros(df)

    # 1. Aplicar la plantilla Plotly basado en la selección del radio
    aplicar_tema_plotly(modo_color)
    
    # Header
    st.markdown(
        "<h1 style='color:#1B5E20; font-family:Google Sans,sans-serif; margin-bottom: 0;'>"
        "🐄 Mercado Ganadero — Central Ganadera Medellín</h1>",
        unsafe_allow_html=True
    )
    
    color_subtitulo = "#212121" if modo_color == "Claro" else "#E0E0E0"
    st.markdown(f"<p style='color:{color_subtitulo};'>Inteligencia de precios y volumen de subastas en Antioquia</p>", unsafe_allow_html=True)

    if df_filtrado.empty:
        st.warning("⚠️ No hay datos para los filtros seleccionados. Intenta ampliar el rango.")
        return

    # KPIs siempre visibles arriba
    mostrar_kpis(df_filtrado)
    st.divider()

    # CSS: tabs grandes + layout responsive para móvil
    st.markdown(
        """
        <style>
        /* ── TABS: tamaño legible en desktop ── */
        [data-baseweb="tab"] button p {
            font-size: 1rem !important;
            font-weight: 600 !important;
        }
        [data-baseweb="tab"] button {
            padding-top: 10px !important;
            padding-bottom: 10px !important;
        }

        /* ── RESPONSIVE MÓVIL (≤768px) ── */
        @media (max-width: 768px) {

            /* Columnas de Streamlit colapsan a ancho completo */
            [data-testid="column"] {
                width: 100% !important;
                flex: 1 1 100% !important;
                min-width: 100% !important;
            }

            /* Tabs: texto más pequeño para que quepan */
            [data-baseweb="tab"] button p {
                font-size: 0.85rem !important;
                font-weight: 600 !important;
            }
            [data-baseweb="tab"] button {
                padding: 6px 8px !important;
            }

            /* Sidebar más angosto en móvil */
            [data-testid="stSidebar"] {
                min-width: 260px !important;
                max-width: 260px !important;
            }

            /* Gráficas: altura mínima para que no queden aplastadas */
            .js-plotly-plot {
                min-height: 280px;
            }

            /* Métricas KPI: centradas */
            [data-testid="metric-container"] {
                text-align: center !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📈 Precios",
        "🏷️ Última Subasta",
        "📊 Tendencias",
        "⚖️ Volumen",
        "🗺️ Municipios",
        "🔍 Detalle",
        "🤖 Predictor"
    ])

    # ── Tab 1: Precios
    with tab1:
        st.plotly_chart(grafica_serie_tiempo(df_filtrado), use_container_width=True)
        info_box("<strong>Serie de precios:</strong> Muestra el precio promedio pactado (COP/kg) al que se cerraron los lotes para cada tipo de animal a lo largo del tiempo, ignorando el peso de cada lote. Útil para ubicar tendencias y comparar categorías.")

    # ── Tab 2: Última Subasta (NUEVA)
    with tab2:
        tab_ultima_subasta(df_filtrado)

    # ── Tab 3: Tendencias (NUEVA)
    with tab3:
        tab_tendencias(df_filtrado)

    # ── Tab 4: Volumen
    with tab4:
        col_vol1, col_vol2 = st.columns(2)
        with col_vol1:
            st.plotly_chart(grafica_volumen_semanal(df_filtrado), use_container_width=True)
            info_box("<strong>Volumen semanal:</strong> Total de dinero (millones COP) movilizado cada semana en subastas. Barras altas indican temporadas de alta actividad; caídas abruptas pueden señalar factores externos como épocas de lluvia, festivos o menor oferta.")
        with col_vol2:
            st.plotly_chart(grafica_estacionalidad(df_filtrado), use_container_width=True)
            info_box("<strong>Mapa de estacionalidad:</strong> Cada celda muestra el precio promedio COP/kg para ese tipo de animal ese mes. <span style='color:#7B1FA2; font-weight:600;'>■ Morado = precio bajo</span> · <span style='color:#1B5E20; font-weight:600;'>■ Verde = precio alto.</span> Identifica en qué meses cotiza mejor cada categoría.")

    # ── Tab 5: Municipios
    with tab5:
        colA, colB = st.columns([2, 1])
        with colA:
            st.plotly_chart(grafica_barras_municipio(df_filtrado), use_container_width=True)
        with colB:
            info_box("<strong>Precio por municipio:</strong> Ranking de los municipios con mayor precio promedio por kg. Los municipios con precios más altos suelen aportar animales de ceba (MC) con mayor peso y mejor genética. Cruza con el filtro de <em>Tipo de Animal</em> en el sidebar para análisis más específico.")

    # ── Tab 6: Detalle
    with tab6:
        st.subheader("Base de datos filtrada (Lotes individuales)")
        mostrar_df = df_filtrado[[
            "fecha_subasta", "numero_boletin", "tipo_subasta", "tipo_codigo",
            "cantidad_animales", "peso_total_kg", "precio_final_kg", "procedencia"
        ]].sort_values("fecha_subasta", ascending=False)
        st.dataframe(
            mostrar_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha_subasta": st.column_config.DateColumn("Fecha"),
                "tipo_codigo": "Animal",
                "cantidad_animales": st.column_config.NumberColumn("Cant.", format="%d"),
                "peso_total_kg": st.column_config.NumberColumn("Peso Total (kg)", format="%.1f"),
                "precio_final_kg": st.column_config.NumberColumn("Precio Final (COP/kg)", format="$%d"),
                "procedencia": "Municipio"
            }
        )
        info_box("<strong>Tabla de lotes:</strong> Cada fila es un lote subastado individualmente. Usa los filtros del sidebar para reducir el conjunto. Haz clic en el encabezado de cualquier columna para ordenar. Descarga la tabla con el botón de la esquina superior derecha.")

    # ── Tab 7: Predictor
    with tab7:
        st.header("🤖 Predictor de Precios (Próximamente)")
        st.info("Esta sección corresponde a la Fase 6 del proyecto. Aquí se integrará el modelo de Machine Learning (ej. Random Forest o LSTM) para predecir precios basándose en peso, procedencia y estacionalidad.")

    # ── Sección de feedback — al pie de la página principal ──────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style="background: linear-gradient(135deg,#7B1FA2,#4A148C);
                    border-radius:12px; padding:20px 28px; color:white;
                    text-align:center; margin:12px 0 4px 0;">
          <h3 style="margin:0 0 4px 0; color:white;">📬 ¿Cómo podemos mejorar?</h3>
          <p style="margin:0; opacity:0.88; font-size:0.95rem;">
            Tu opinión es clave. Déjanos un mensaje y si quieres, tu correo para mantenerte al tanto
            de nuevas funciones y análisis de mercado.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    tab_contacto()

if __name__ == "__main__":
    main()
