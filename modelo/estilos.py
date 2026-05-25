"""
estilos.py — Maquillaje global para el dashboard SubaDatos (Streamlit)

Uso:
    from modelo.estilos import aplicar_estilos_globales

    st.set_page_config(...)        # como ya lo tienes
    aplicar_estilos_globales()     # <-- añade esta línea justo después

Inyecta un único bloque de CSS que:
  - Oculta el "cromo" de Streamlit (menú, toolbar/Deploy, footer, barra superior).
  - Carga la fuente Inter (cercana a Google Sans) desde Google Fonts.
  - Da más aire al contenido y limita el ancho para un look de app real.
  - Convierte los st.metric en tarjetas con borde, sombra y acento de marca.
  - Reestiliza el sidebar, las pestañas, los botones y los inputs.
  - Suaviza tablas, expanders, alertas y dividers.

Colores de marca (de tu .streamlit/config.toml):
  Verde primario   #1B5E20
  Verde secundario #388E3C
  Morado acento    #7B1FA2
  Fondo            #FFFFFF
  Fondo secundario #F5F5F5
  Texto            #212121
"""

import streamlit as st

# Paleta centralizada — cámbiala aquí y se propaga a todo el CSS.
VERDE_PRIMARIO = "#1B5E20"
VERDE_SECUNDARIO = "#388E3C"
MORADO_ACENTO = "#7B1FA2"
FONDO = "#FFFFFF"
FONDO_SECUNDARIO = "#F7F8FA"
TEXTO = "#1A1F1B"
GRIS_BORDE = "#E5E8EB"


def aplicar_estilos_globales() -> None:
    """Inyecta el CSS global. Llamar una sola vez, tras st.set_page_config()."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        /* ───────────────────────────────────────────────────────────
           1. TIPOGRAFÍA GLOBAL
           ─────────────────────────────────────────────────────────── */
        html, body, [class*="css"], [class*="st-"] {{
            font-family: 'Inter', -apple-system, 'Segoe UI', Roboto, sans-serif !important;
        }}
        .stApp {{
            background: {FONDO};
            color: {TEXTO};
        }}

        /* ───────────────────────────────────────────────────────────
           2. OCULTAR EL "CROMO" DE STREAMLIT
           (menú hamburguesa, toolbar/Deploy, footer, barra de color)
           ─────────────────────────────────────────────────────────── */
        #MainMenu {{ visibility: hidden; }}
        footer {{ visibility: hidden; }}
        [data-testid="stToolbar"] {{ display: none !important; }}
        [data-testid="stDecoration"] {{ display: none !important; }}
        /* Header transparente: mantiene el botón de abrir sidebar en móvil */
        [data-testid="stHeader"] {{
            background: transparent !important;
            height: 0 !important;
        }}

        /* ───────────────────────────────────────────────────────────
           3. CONTENEDOR PRINCIPAL — más aire, ancho controlado
           ─────────────────────────────────────────────────────────── */
        .block-container {{
            padding-top: 2.2rem !important;
            padding-bottom: 3rem !important;
            max-width: 1240px !important;
        }}

        /* Títulos */
        h1 {{
            font-weight: 800 !important;
            letter-spacing: -0.02em !important;
            color: {VERDE_PRIMARIO} !important;
        }}
        h2, h3, h4 {{
            font-weight: 700 !important;
            letter-spacing: -0.01em !important;
            color: {TEXTO} !important;
        }}

        /* ───────────────────────────────────────────────────────────
           4. KPIs (st.metric) COMO TARJETAS
           ─────────────────────────────────────────────────────────── */
        [data-testid="stMetric"],
        [data-testid="metric-container"] {{
            background: {FONDO};
            border: 1px solid {GRIS_BORDE};
            border-radius: 14px;
            padding: 16px 18px !important;
            box-shadow: 0 1px 2px rgba(16,24,40,0.04),
                        0 1px 3px rgba(16,24,40,0.06);
            transition: box-shadow .18s ease, transform .18s ease;
            position: relative;
            overflow: hidden;
        }}
        [data-testid="stMetric"]::before,
        [data-testid="metric-container"]::before {{
            content: "";
            position: absolute;
            left: 0; top: 0; bottom: 0;
            width: 4px;
            background: {VERDE_PRIMARIO};
        }}
        [data-testid="stMetric"]:hover,
        [data-testid="metric-container"]:hover {{
            box-shadow: 0 4px 10px rgba(16,24,40,0.10);
            transform: translateY(-1px);
        }}
        [data-testid="stMetricLabel"] p {{
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            color: #667085 !important;
        }}
        [data-testid="stMetricValue"] {{
            font-size: 1.7rem !important;
            font-weight: 800 !important;
            color: {TEXTO} !important;
        }}

        /* ───────────────────────────────────────────────────────────
           5. SIDEBAR
           ─────────────────────────────────────────────────────────── */
        [data-testid="stSidebar"] {{
            background: {FONDO_SECUNDARIO} !important;
            border-right: 1px solid {GRIS_BORDE};
        }}
        [data-testid="stSidebar"] .block-container {{
            padding-top: 1.4rem !important;
        }}

        /* ───────────────────────────────────────────────────────────
           6. PESTAÑAS (tabs) — estilo moderno con subrayado
           ─────────────────────────────────────────────────────────── */
        [data-baseweb="tab-list"] {{
            gap: 4px;
            border-bottom: 1px solid {GRIS_BORDE};
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
            flex-wrap: nowrap !important;
            scrollbar-width: none !important;
        }}
        [data-baseweb="tab-list"]::-webkit-scrollbar {{ display: none; }}
        [data-baseweb="tab"] {{
            border-radius: 10px 10px 0 0 !important;
        }}
        [data-baseweb="tab"] button p {{
            font-size: 0.95rem !important;
            font-weight: 600 !important;
            color: #667085 !important;
        }}
        [data-baseweb="tab"] button {{
            padding: 10px 16px !important;
        }}
        [data-baseweb="tab"][aria-selected="true"] button p {{
            color: {VERDE_PRIMARIO} !important;
        }}
        [data-baseweb="tab"]:hover button p {{
            color: {VERDE_SECUNDARIO} !important;
        }}
        /* Barra inferior activa */
        [data-baseweb="tab-highlight"] {{
            background-color: {VERDE_PRIMARIO} !important;
            height: 3px !important;
            border-radius: 3px 3px 0 0 !important;
        }}

        /* ───────────────────────────────────────────────────────────
           7. BOTONES
           ─────────────────────────────────────────────────────────── */
        .stButton > button,
        [data-testid="stFormSubmitButton"] > button {{
            background: {VERDE_PRIMARIO} !important;
            color: #FFFFFF !important;
            border: none !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
            padding: 0.55rem 1.1rem !important;
            box-shadow: 0 1px 2px rgba(16,24,40,0.08);
            transition: background .15s ease, transform .15s ease;
        }}
        .stButton > button:hover,
        [data-testid="stFormSubmitButton"] > button:hover {{
            background: #14491A !important;
            transform: translateY(-1px);
        }}
        .stDownloadButton > button {{
            border-radius: 10px !important;
            border: 1px solid {VERDE_PRIMARIO} !important;
            color: {VERDE_PRIMARIO} !important;
            font-weight: 600 !important;
        }}

        /* ───────────────────────────────────────────────────────────
           8. INPUTS, SELECTS, RADIOS, MULTISELECT
           ─────────────────────────────────────────────────────────── */
        [data-baseweb="select"] > div,
        [data-baseweb="input"] > div,
        .stTextInput input, .stNumberInput input {{
            border-radius: 10px !important;
            border-color: {GRIS_BORDE} !important;
        }}
        [data-baseweb="select"] > div:focus-within,
        [data-baseweb="input"] > div:focus-within {{
            border-color: {VERDE_PRIMARIO} !important;
            box-shadow: 0 0 0 3px rgba(27,94,32,0.12) !important;
        }}
        /* Chips del multiselect en verde de marca */
        [data-baseweb="tag"] {{
            background-color: {VERDE_SECUNDARIO} !important;
            border-radius: 8px !important;
        }}
        /* Slider y radios en color de marca */
        [data-testid="stSlider"] [role="slider"] {{
            background-color: {VERDE_PRIMARIO} !important;
        }}

        /* ───────────────────────────────────────────────────────────
           9. EXPANDERS, TABLAS, ALERTAS, DIVIDERS
           ─────────────────────────────────────────────────────────── */
        [data-testid="stExpander"] {{
            border: 1px solid {GRIS_BORDE} !important;
            border-radius: 12px !important;
            overflow: hidden;
        }}
        [data-testid="stDataFrame"], [data-testid="stTable"] {{
            border: 1px solid {GRIS_BORDE} !important;
            border-radius: 12px !important;
            overflow: hidden;
        }}
        [data-testid="stAlert"] {{
            border-radius: 12px !important;
        }}
        hr {{ border-color: {GRIS_BORDE} !important; }}

        /* Gráficas Plotly: tarjeta sutil alrededor */
        .js-plotly-plot {{
            border-radius: 12px;
        }}

        /* ───────────────────────────────────────────────────────────
           10. RESPONSIVE MÓVIL (≤768px)
           ─────────────────────────────────────────────────────────── */
        @media (max-width: 768px) {{
            .block-container {{ padding-top: 1.2rem !important; }}
            [data-testid="column"] {{
                width: 100% !important;
                flex: 1 1 100% !important;
                min-width: 100% !important;
            }}
            [data-baseweb="tab"] button p {{ font-size: 0.82rem !important; }}
            [data-baseweb="tab"] button {{ padding: 7px 11px !important; white-space: nowrap !important; }}
            [data-testid="stMetricValue"] {{ font-size: 1.35rem !important; }}
            h1 {{ font-size: 1.5rem !important; }}
            .js-plotly-plot {{ min-height: 300px !important; }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
