"""
entrenar_modelo.py
==================
Entrenamiento del modelo de predicción de precios para la Central Ganadera
de Medellín. Lee los datos directamente desde Supabase (tabla subastas).

Estrategia: un modelo por tipo_subasta (Tradicional, Especial GYR, etc.)
para capturar las dinámicas de precio distintas de cada mercado.

Motor: scikit-learn GradientBoostingRegressor
Fuente: Supabase → tabla subastas (precio_final_kg > 0)

Uso:
    python modelo/entrenar_modelo.py
    python modelo/entrenar_modelo.py --test 0.15

Autor: SubaDatos
Fecha: 2026
"""

import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import plotly.graph_objects as go

load_dotenv()

# ─── RUTAS DE ARTEFACTOS ──────────────────────────────────────────────────────
_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))

# Los artefactos se guardan con sufijo por tipo de subasta:
#   modelo_Tradicional.pkl, scaler_X_Tradicional.pkl, etc.
def ruta_artefacto(nombre: str, tipo_subasta: str) -> str:
    """Retorna la ruta absoluta del artefacto para el tipo de subasta dado."""
    sufijo = tipo_subasta.replace(" ", "_").replace("/", "_")
    return os.path.join(_DIR_SCRIPT, f"{nombre}_{sufijo}.pkl")

METADATA_PATH = os.path.join(_DIR_SCRIPT, "modelo_metadata.json")
GRAFICA_PATH  = os.path.join(_DIR_SCRIPT, "prediccion_3meses.html")

# ─── PALETA DE COLORES ────────────────────────────────────────────────────────
COLORES = {
    "HV": "#1B5E20", "HL": "#388E3C", "MC": "#388E3C",
    "ML": "#7B1FA2", "AT": "#0288D1", "VH": "#FFB300",
    "T2": "#00838F", "R":  "#4E342E",
}
COLOR_SEQUENCE = ["#1B5E20","#7B1FA2","#0288D1","#F57F17",
                  "#388E3C","#C62828","#00838F","#4E342E"]

# Tipos de subasta que entrenamos (EXCLUYE 'Mulares' — distinto mercado,
# distintos precios, contamina el modelo de vacuno)
TIPOS_SUBASTA = {
    "Tradicional":  {"precio_max": 20_000},
    "Especial GYR": {"precio_max": 55_000},
}

# Peso mínimo razonable por categoría (kg)
# Animales más livianos suelen ser razas o edades distintas con dinámica diferente
PESO_MINIMO_CAT = {
    "HL": 60,   # HL adulto ≥ 60 kg
    "HV": 80,   # HV adulto ≥ 80 kg
    "ML": 60,   # ML adulto ≥ 60 kg
    "MC": 80,   # MC adulto ≥ 80 kg
    "AT": 100,  # Añojo torete ≥ 100 kg
    "VH": 100,  # Vaca horrada ≥ 100 kg
    "T2": 150,  # Toro ≥ 150 kg
    "R":  60,   # Razas pequeñas (menor umbral)
}


# ─── FUENTE DE DATOS ──────────────────────────────────────────────────────────

def conectar_supabase() -> Client:
    """Crea y retorna el cliente de Supabase usando variables de entorno."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("❌ Variables SUPABASE_URL y SUPABASE_KEY no encontradas en .env")
    return create_client(url, key)


def cargar_datos_supabase(supabase: Client) -> pd.DataFrame:
    """
    Descarga todos los registros de subastas (precio_final_kg > 0) desde Supabase.
    Pagina en bloques de 1000 filas.
    """
    print("🔌 Conectando a Supabase...")
    todos = []
    pagina = 0

    while True:
        try:
            r = (supabase.table("subastas")
                 .select("*")
                 .gt("precio_final_kg", 0)
                 .range(pagina * 1000, (pagina + 1) * 1000 - 1)
                 .execute())
        except Exception as e:
            raise RuntimeError(f"❌ Error al consultar Supabase: {e}")

        if not r.data:
            break
        todos.extend(r.data)
        print(f"   Página {pagina + 1}: {len(r.data)} registros")
        pagina += 1

    df = pd.DataFrame(todos)
    print(f"✅ Total descargado: {len(df)} registros")
    return df


def cargar_features_externas(supabase: Client) -> pd.DataFrame:
    """
    Descarga la tabla features_externas de Supabase (climate + IPC + maiz).
    Si la tabla no existe o está vacía, retorna DataFrame vacío sin romper el flujo.

    Retorna DataFrame con columnas: fecha_mes (dtype date), lluvia_acum_mm,
    temp_max_prom_c, et0_prom_mm, ipc_var_mensual_pct, precio_maiz_usd_ton
    """
    try:
        r = supabase.table("features_externas").select("*").execute()
        if not r.data:
            print("   INFO: tabla features_externas vacía o no existe.")
            print("   --> Ejecuta: python etl/fetch_features_externas.py")
            return pd.DataFrame()
        df = pd.DataFrame(r.data)
        df["fecha_mes"] = pd.to_datetime(df["fecha_mes"]).dt.to_period("M").dt.to_timestamp()
        print(f"   Features externas: {len(df)} meses cargados")
        return df
    except Exception as e:
        print(f"   WARN features_externas: {e}")
        return pd.DataFrame()


# ─── PREPARACIÓN DE DATOS (por tipo_subasta) ──────────────────────────────────

def hora_a_seg(h: str) -> int | None:
    """
    Convierte hora a segundos del día.
    Soporta formato 24h ('09:14:35') y 12h español ('01:29:25 p. m.').
    """
    try:
        texto = str(h).strip().lower()
        if "a." in texto or "p." in texto:
            es_pm = "p." in texto
            hora_limpia = texto.replace("a. m.", "").replace("p. m.", "").strip()
            partes = hora_limpia.split(":")
            h_val = int(partes[0])
            m_val = int(partes[1])
            s_val = int(partes[2]) if len(partes) > 2 else 0
            if es_pm and h_val != 12:
                h_val += 12
            elif not es_pm and h_val == 12:
                h_val = 0
            return h_val * 3600 + m_val * 60 + s_val
        else:
            partes = texto.split(":")
            return int(partes[0]) * 3600 + int(partes[1]) * 60 + (int(partes[2]) if len(partes) > 2 else 0)
    except (ValueError, IndexError):
        return None


def preparar_subset(
    df: pd.DataFrame,
    tipo: str,
    precio_max: int,
) -> tuple[pd.DataFrame, LabelEncoder]:
    """
    Filtra y prepara un subset del DataFrame para un tipo_subasta concreto.
    """
    sub = df[df["tipo_subasta"] == tipo].copy()
    if sub.empty:
        return sub, LabelEncoder()

    sub = sub.dropna(subset=["tipo_codigo", "peso_promedio_kg", "precio_final_kg",
                              "hora_subasta", "fecha_subasta"])
    sub = sub[(sub["precio_final_kg"] > 2_000) & (sub["precio_final_kg"] < precio_max)]
    sub = sub[sub["peso_promedio_kg"] > 0]

    sub["cantidad_animales"] = pd.to_numeric(sub.get("cantidad_animales", np.nan), errors="coerce")
    mediana_cant = sub.groupby("tipo_codigo")["cantidad_animales"].transform("median")
    sub["cantidad_animales"] = sub["cantidad_animales"].fillna(mediana_cant)
    sub["cantidad_animales"] = sub["cantidad_animales"].fillna(sub["cantidad_animales"].median())
    sub["cantidad_animales"] = sub["cantidad_animales"].fillna(10).astype(float)

    cats_validas = sorted(sub["tipo_codigo"].value_counts()[lambda x: x >= 5].index.tolist())
    sub = sub[sub["tipo_codigo"].isin(cats_validas)]

    partes = []
    for cat in cats_validas:
        s = sub[sub["tipo_codigo"] == cat].copy()
        media, std = s["precio_final_kg"].mean(), s["precio_final_kg"].std()
        if pd.notna(std) and std > 0:
            s = s[(s["precio_final_kg"] > media - 3 * std) &
                  (s["precio_final_kg"] < media + 3 * std)]
        partes.append(s)
    sub = pd.concat(partes).reset_index(drop=True)

    for cat, peso_min in PESO_MINIMO_CAT.items():
        mask = (sub["tipo_codigo"] == cat) & (sub["peso_promedio_kg"] < peso_min)
        sub = sub[~mask]
    sub = sub.reset_index(drop=True)


    sub["fecha_dt"]    = pd.to_datetime(sub["fecha_subasta"])
    sub["fecha_num"]   = sub["fecha_dt"].astype(np.int64) // 10 ** 9
    sub["dia_semana"]  = sub["fecha_dt"].dt.dayofweek
    sub["mes"]         = sub["fecha_dt"].dt.month
    sub["semana_anio"] = sub["fecha_dt"].dt.isocalendar().week.astype(int)

    sub["hora_seg"] = sub["hora_subasta"].apply(hora_a_seg)
    sub = sub.dropna(subset=["hora_seg"])
    sub["hora_seg"] = sub["hora_seg"].astype(int)

    sub["mes_key"]            = sub["fecha_dt"].dt.to_period("M")
    sub["precio_mes_promedio"] = (
        sub.groupby(["tipo_codigo", "mes_key"])["precio_final_kg"]
        .transform("mean")
    )

    encoder = LabelEncoder()
    encoder.fit(cats_validas)
    sub["tipo_codigo_num"] = encoder.transform(sub["tipo_codigo"])

    return sub, encoder


# ─── FEATURES DEL MODELO ─────────────────────────────────────────────────────
# 9 features base para el modelo (se ha demostrado que son las más robustas)
FEATURE_COLS = [
    "peso_promedio_kg", "tipo_codigo_num",
    "cantidad_animales", "fecha_num", "hora_seg",
    "dia_semana", "mes", "semana_anio",
    "precio_mes_promedio",   # tendencia mensual por categoría
]


# ─── ENTRENAMIENTO DE UN MODELO ───────────────────────────────────────────────

def entrenar_modelo_tipo(
    df_tipo: pd.DataFrame,
    encoder: LabelEncoder,
    tipo: str,
    test_size: float,
) -> dict:
    """
    Entrena un GradientBoostingRegressor para un tipo_subasta específico.

    Parámetros:
        df_tipo: DataFrame ya preparado (solo filas del tipo en cuestión)
        encoder: LabelEncoder ajustado sobre las categorías de este tipo
        tipo: nombre del tipo de subasta (para logs y guardado de artefactos)
        test_size: fracción de datos para test

    Retorna:
        dict con métricas y rutas de artefactos guardados
    """
    sufijo = tipo.replace(" ", "_").replace("/", "_")
    print(f"\n{'─' * 60}")
    print(f"🏷  Tipo: {tipo}  |  {len(df_tipo)} registros")

    X = df_tipo[FEATURE_COLS].values.astype(float)
    y = df_tipo["precio_final_kg"].values.astype(float)

    if len(X) < 50:
        print(f"   ⚠️  Muy pocos registros ({len(X)}). Saltando este tipo.")
        return {}

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    print(f"   Split: {len(X_train)} train / {len(X_test)} test")

    scaler_X = StandardScaler()
    X_train_sc = scaler_X.fit_transform(X_train)
    X_test_sc  = scaler_X.transform(X_test)

    scaler_y = StandardScaler()
    scaler_y.fit(y_train.reshape(-1, 1))

    print(f"   🏋️  Entrenando GBR...")
    modelo = GradientBoostingRegressor(
        n_estimators=300, max_depth=5, learning_rate=0.08,
        subsample=0.8, min_samples_split=10, min_samples_leaf=5,
        random_state=42, validation_fraction=0.1, n_iter_no_change=25,
        verbose=0,
    )
    modelo.fit(X_train_sc, y_train)

    y_pred = modelo.predict(X_test_sc)
    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mape = np.mean(np.abs((y_pred - y_test) / y_test)) * 100

    print(f"   MAE:  ${mae:,.0f} COP/kg")
    print(f"   RMSE: ${rmse:,.0f} COP/kg")
    print(f"   MAPE: {mape:.2f}%")

    # Cross-validation
    try:
        cv = cross_val_score(modelo, X_train_sc, y_train, cv=5,
                             scoring="neg_mean_absolute_error")
        cv_mean = float(-cv.mean())
        cv_std  = float(cv.std())
        print(f"   CV MAE: ${cv_mean:,.0f} ± ${cv_std:,.0f}")
    except Exception as e:
        print(f"   ⚠️  CV no disponible: {e}")
        cv_mean, cv_std = float(mae), 0.0

    # Feature importance
    print(f"   🎯 Importancia de features:")
    imps = modelo.feature_importances_
    for nom, imp in sorted(zip(FEATURE_COLS, imps), key=lambda x: -x[1]):
        barra = "█" * int(imp * 40)
        print(f"      {nom:<20} {imp:.3f} {barra}")

    # Ejemplos
    print(f"   🔍 Ejemplos (primeros 8):")
    print(f"      {'Real/kg':>13}  {'Pred/kg':>13}  {'Precio/animal':>15}  {'Valor lote':>15}  {'Err%':>6}")
    peso_t  = X_test[:, 0]
    cant_t  = X_test[:, 2]
    for i in range(min(8, len(y_test))):
        err = abs(y_pred[i] - y_test[i]) / y_test[i] * 100
        p_animal = y_pred[i] * peso_t[i]
        v_lote   = p_animal * cant_t[i]
        print(f"      ${y_test[i]:>11,.0f}  ${y_pred[i]:>11,.0f}  ${p_animal:>13,.0f}  ${v_lote:>13,.0f}  {err:>5.1f}%")

    # Guardar artefactos
    m_path  = os.path.join(_DIR_SCRIPT, f"modelo_{sufijo}.pkl")
    sx_path = os.path.join(_DIR_SCRIPT, f"scaler_X_{sufijo}.pkl")
    sy_path = os.path.join(_DIR_SCRIPT, f"scaler_y_{sufijo}.pkl")
    enc_path= os.path.join(_DIR_SCRIPT, f"encoder_tipo_{sufijo}.pkl")

    joblib.dump(modelo,   m_path);  print(f"   ✅ {m_path}")
    joblib.dump(scaler_X, sx_path); print(f"   ✅ {sx_path}")
    joblib.dump(scaler_y, sy_path); print(f"   ✅ {sy_path}")
    joblib.dump(encoder,  enc_path);print(f"   ✅ {enc_path}")

    return {
        "tipo":             tipo,
        "registros":        len(df_tipo),
        "categorias":       list(encoder.classes_),
        "mae":              float(mae),
        "rmse":             float(rmse),
        "mape":             float(mape),
        "cv_mae_mean":      cv_mean,
        "cv_mae_std":       cv_std,
        "n_estimadores":    int(modelo.n_estimators_),
        "feature_cols":     FEATURE_COLS,
        "feature_importances": {n: float(v) for n, v in zip(FEATURE_COLS, imps)},
        "artefactos": {
            "modelo":   m_path,
            "scaler_X": sx_path,
            "scaler_y": sy_path,
            "encoder":  enc_path,
        },
    }


# ─── GRÁFICA PREDICTIVA 3 MESES (una categoría a la vez) ─────────────────────

def graficar_prediccion_3meses(
    modelos_info: list[dict],
    df_completo: pd.DataFrame,
) -> go.Figure:
    """
    Genera gráfica interactiva Plotly con histórico (6m) + predicción (3m).

    Muestra UNA categoría a la vez. Un dropdown en la esquina superior izquierda
    permite cambiar entre categorías sin recargar — elimina el ruido visual
    de mostrar 8 líneas simultáneas.

    La categoría inicial es HV (la más frecuente en Tradicional).

    Parámetros:
        modelos_info: lista de dicts retornados por entrenar_modelo_tipo()
        df_completo: DataFrame completo preparado

    Retorna:
        Figura Plotly
    """
    # Usar modelo Tradicional
    info = next((m for m in modelos_info if "Tradicional" in m.get("tipo", "")), None)
    if info is None:
        info = modelos_info[0]

    tipo_graf = info["tipo"]
    modelo    = joblib.load(info["artefactos"]["modelo"])
    scaler_X  = joblib.load(info["artefactos"]["scaler_X"])
    encoder   = joblib.load(info["artefactos"]["encoder"])
    mae       = info["mae"]

    hoy       = datetime.now().date()
    fecha_fin = hoy + relativedelta(months=3)
    fechas    = pd.date_range(start=hoy, end=fecha_fin, freq="D")

    # Histórico últimos 6 meses
    df_tipo = df_completo[df_completo["tipo_subasta"] == tipo_graf].copy()
    df_tipo["fecha_dt"] = pd.to_datetime(df_tipo["fecha_subasta"])
    inicio_hist = pd.Timestamp(hoy) - relativedelta(months=6)
    df_hist = (
        df_tipo[df_tipo["fecha_dt"] >= inicio_hist]
        .groupby(["fecha_dt", "tipo_codigo"])["precio_final_kg"]
        .mean().reset_index()
    )

    cats = list(encoder.classes_)
    # Categoría inicial: HV si existe, si no la primera
    cat_inicial = "HV" if "HV" in cats else cats[0]

    # Pre-calcular precio_mes_promedio para la predicción:
    # usamos el promedio del último mes conocido por categoría como proxy
    precio_mes_default = {}
    for cat in cats:
        s = df_tipo[df_tipo["tipo_codigo"] == cat]
        if not s.empty:
            precio_mes_default[cat] = float(s["precio_final_kg"].tail(30).mean())
        else:
            precio_mes_default[cat] = float(df_tipo["precio_final_kg"].mean())

    # Construir todas las trazas (histórico + predicción + banda)
    # pero solo mostrar la categoría inicial; el resto se activa con el dropdown
    all_traces      = []  # lista de (cat, [traces])
    trace_visibility= {}  # cat -> lista de índices de traza
    trace_idx       = 0

    fig = go.Figure()

    for cat in cats:
        color = COLORES.get(cat, COLOR_SEQUENCE[cats.index(cat) % len(COLOR_SEQUENCE)])
        es_inicial = (cat == cat_inicial)
        trazas_cat = []

        # ─ Histórico ─
        h = df_hist[df_hist["tipo_codigo"] == cat].sort_values("fecha_dt")
        if not h.empty:
            fig.add_trace(go.Scatter(
                x=h["fecha_dt"], y=h["precio_final_kg"],
                name=f"Histórico ({cat})",
                visible=es_inicial,
                line=dict(color=color, width=2.5),
                mode="lines+markers",
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{cat} — Histórico</b><br>"
                    "%{x|%d %b %Y}<br>"
                    "$%{y:,.0f} COP/kg<extra></extra>"
                ),
            ))
            trazas_cat.append(trace_idx); trace_idx += 1

        # ─ Predicción ─
        s        = df_tipo[df_tipo["tipo_codigo"] == cat]
        peso_med = float(s["peso_promedio_kg"].median()) if not s.empty else 200.0
        cant_med = float(s["cantidad_animales"].median()) if not s.empty else 20.0
        hora_med = float(s["hora_seg"].median()) if "hora_seg" in s.columns and not s.empty else 36000.0
        cat_num  = int(encoder.transform([cat])[0])
        precio_mes = precio_mes_default[cat]

        filas = []
        for f in fechas:
            fn = int(pd.Timestamp(f).timestamp())
            filas.append([peso_med, cat_num, cant_med, fn, hora_med,
                          f.dayofweek, f.month, f.isocalendar()[1], precio_mes])

        X_fut    = np.array(filas, dtype=float)
        y_fut    = modelo.predict(scaler_X.transform(X_fut))
        p_animal = y_fut * peso_med
        v_lote   = p_animal * cant_med

        fig.add_trace(go.Scatter(
            x=fechas, y=y_fut,
            name=f"Predicción ({cat})",
            visible=es_inicial,
            line=dict(color=color, width=2.5, dash="dash"),
            mode="lines",
            customdata=np.stack([p_animal, [peso_med]*len(y_fut),
                                 v_lote, [cant_med]*len(y_fut)], axis=-1),
            hovertemplate=(
                f"<b>{cat} — Predicción</b><br>%{{x|%d %b %Y}}<br>"
                "Precio/kg:     <b>$%{y:,.0f} COP/kg</b><br>"
                "Precio/animal (≈%{customdata[1]:.0f} kg): $%{customdata[0]:,.0f} COP<br>"
                "Valor lote (≈%{customdata[3]:.0f} animales): $%{customdata[2]:,.0f} COP"
                "<extra></extra>"
            ),
        ))
        trazas_cat.append(trace_idx); trace_idx += 1

        # ─ Banda ± MAE ─
        rgb = _hex_to_rgb(color)
        fig.add_trace(go.Scatter(
            x=list(fechas) + list(fechas[::-1]),
            y=list(y_fut + mae) + list((y_fut - mae)[::-1]),
            fill="toself", fillcolor=f"rgba({rgb},0.12)",
            line=dict(color="rgba(0,0,0,0)"),
            name=f"± MAE ({cat})",
            visible=es_inicial,
            showlegend=False,
            hoverinfo="skip",
        ))
        trazas_cat.append(trace_idx); trace_idx += 1

        trace_visibility[cat] = trazas_cat

    # Construir el dropdown
    n_total = trace_idx
    buttons = []
    for cat in cats:
        vis = [False] * n_total
        for i in trace_visibility[cat]:
            vis[i] = True
        precio_ref = precio_mes_default.get(cat, 0)
        buttons.append(dict(
            label=f"{cat}  (ref: ${precio_ref:,.0f}/kg)",
            method="update",
            args=[
                {"visible": vis},
                {"title": {
                    "text": (
                        f"Predicción de precios — {cat} ({tipo_graf})<br>"
                        f"<sup>Próximos 3 meses · MAE ±${mae:,.0f} COP/kg · "
                        f"Precio ref: ${precio_ref:,.0f}/kg</sup>"
                    ),
                    "font": {"size": 15, "color": "#1B5E20",
                             "family": "Google Sans, Roboto, sans-serif"},
                }},
            ],
        ))

    # Línea "Hoy" (compatible con plotly 5.18 — sin add_vline)
    hoy_str = str(hoy)
    fig.add_shape(type="line", x0=hoy_str, x1=hoy_str, y0=0, y1=1,
                  xref="x", yref="paper",
                  line=dict(color="#C62828", width=1.5, dash="dot"))
    fig.add_annotation(x=hoy_str, y=1.02, xref="x", yref="paper",
                       text="Hoy", showarrow=False,
                       font=dict(color="#C62828", size=11),
                       bgcolor="white", bordercolor="#C62828", borderwidth=1)

    precio_ref_inicial = precio_mes_default.get(cat_inicial, 0)
    fig.update_layout(
        title=dict(
            text=(
                f"Predicción de precios — {cat_inicial} ({tipo_graf})<br>"
                f"<sup>Próximos 3 meses · MAE ±${mae:,.0f} COP/kg · "
                f"Precio ref: ${precio_ref_inicial:,.0f}/kg</sup>"
            ),
            font=dict(size=15, color="#1B5E20", family="Google Sans, Roboto, sans-serif"),
        ),
        updatemenus=[dict(
            type="dropdown",
            direction="down",
            x=0.0, y=1.18,
            xanchor="left",
            buttons=buttons,
            bgcolor="#FFFFFF",
            bordercolor="#1B5E20",
            font=dict(size=12, color="#212121"),
            showactive=True,
            active=cats.index(cat_inicial),
        )],
        annotations=[dict(
            text="Categoría:", x=0.0, y=1.24,
            xref="paper", yref="paper",
            showarrow=False, xanchor="left",
            font=dict(size=12, color="#555"),
        )],
        xaxis=dict(
            title="Fecha", rangeslider=dict(visible=True),
            rangeselector=dict(buttons=[
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all", label="Todo"),
            ]),
        ),
        yaxis=dict(title="Precio final (COP/kg)", tickformat="$,.0f"),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, font=dict(size=11),
        ),
        hovermode="x unified",
        paper_bgcolor="#FFFFFF", plot_bgcolor="#F5F5F5",
        font=dict(family="Google Sans, Roboto, sans-serif", color="#212121"),
        margin=dict(l=40, r=20, t=110, b=40),
    )
    return fig



def _hex_to_rgb(hex_color: str) -> str:
    """Convierte '#RRGGBB' a 'R,G,B' para usar en rgba()."""
    h = hex_color.lstrip("#")
    return f"{int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)}"


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def entrenar(test_size: float = 0.2) -> None:
    """
    Pipeline completo: descarga → entrenamiento por tipo → métricas → gráfica.

    Parámetros:
        test_size: fracción de datos para el conjunto de test (default 0.2)
    """
    print(f"\n{'═' * 60}")
    print("🐄  ENTRENAMIENTO DEL MODELO — Central Ganadera de Medellín")
    print("    Motor: GradientBoostingRegressor por tipo de subasta")
    print(f"{'═' * 60}")

    # 1. Datos desde Supabase
    print("\n📥 PASO 1: Descargando datos desde Supabase...")
    try:
        sb  = conectar_supabase()
        df  = cargar_datos_supabase(sb)
    except RuntimeError as e:
        print(e); sys.exit(1)

    if df.empty:
        print("❌ No se obtuvieron datos de Supabase. Verifica la tabla subastas.")
        sys.exit(1)

    print(f"\n   Distribución por tipo_subasta:")
    for t, cnt in df["tipo_subasta"].value_counts().items():
        precio_med = df[df["tipo_subasta"] == t]["precio_final_kg"].mean()
        print(f"     {t}: {cnt} registros  |  precio medio: ${precio_med:,.0f}/kg")


    # 2. Entrenar un modelo por tipo
    print("\n🔄 PASO 2: Entrenando modelos por tipo de subasta...")
    resultados   = []
    df_preparados = {}   # acumular los df limpios para la gráfica


    for tipo, cfg in TIPOS_SUBASTA.items():
        df_sub, encoder = preparar_subset(df, tipo, cfg["precio_max"])
        if df_sub.empty:
            print(f"\n⚠️  Sin datos para {tipo}. Saltando.")
            continue
        df_preparados[tipo] = df_sub
        res = entrenar_modelo_tipo(df_sub, encoder, tipo, test_size)
        if res:
            resultados.append(res)

    if not resultados:
        print("❌ No se pudo entrenar ningún modelo.")
        sys.exit(1)

    # 3. Guardar metadata consolidada
    print("\n💾 PASO 3: Guardando metadata...")
    metadata = {
        "fecha_entrenamiento": datetime.now().isoformat(),
        "fuente":              "Supabase → tabla subastas",
        "motor":               "GradientBoostingRegressor (scikit-learn) — por tipo",
        "feature_cols":        FEATURE_COLS,
        "modelos":             resultados,
    }
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"   ✅ {METADATA_PATH}")

    # 4. Gráfica 3 meses (usando el primer df preparado como referencia histórica)
    print("\n📊 PASO 4: Generando gráfica de predicción 3 meses...")
    df_ref = next(iter(df_preparados.values()))
    # Asegurar que df_ref tenga hora_seg para la gráfica
    if "hora_seg" not in df_ref.columns:
        df_ref["hora_seg"] = df_ref["hora_subasta"].apply(hora_a_seg)
    try:
        fig = graficar_prediccion_3meses(resultados, df_ref)
        fig.write_html(GRAFICA_PATH, include_plotlyjs="cdn")
        print(f"   ✅ {GRAFICA_PATH}")
        print("   💡 Abriendo en el navegador...")
        fig.show()
    except Exception as e:
        print(f"   ⚠️  Gráfica no disponible: {e}")

    # 5. Resumen final
    print(f"\n{'═' * 60}")
    print("✅ ¡ENTRENAMIENTO COMPLETADO! — Resumen por tipo")
    print(f"{'═' * 60}")
    for r in resultados:
        print(f"  🏷  {r['tipo']}")
        print(f"     Registros: {r['registros']}  |  Árboles: {r['n_estimadores']}")
        print(f"     MAE:  ${r['mae']:,.0f} COP/kg  |  MAPE: {r['mape']:.1f}%")
        print(f"     Categorías: {r['categorias']}")
    print(f"{'═' * 60}\n")


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="🐄 Entrenar modelos de precios — Central Ganadera de Medellín"
    )
    parser.add_argument("--test", type=float, default=0.2,
                        help="Proporción de datos para test (default: 0.2)")
    args = parser.parse_args()
    entrenar(test_size=args.test)
