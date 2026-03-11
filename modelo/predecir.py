"""
predecir.py
===========
Interfaz CLI para predecir el precio de un lote en la Central Ganadera
de Medellín. El usuario ingresa las características del lote que desea
comprar o vender y recibe:
  - Precio estimado por kg (COP/kg)
  - Precio estimado por animal
  - Valor estimado del lote completo
  - Gráfica comparativa vs. histórico de esa categoría

Uso:
    python modelo/predecir.py

Autor: SubaDatos
Fecha: 2026
"""

import os
import sys
import json
from datetime import datetime, date

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import joblib
import plotly.graph_objects as go

load_dotenv()

# ─── RUTAS ────────────────────────────────────────────────────────────────────
_DIR_SCRIPT = os.path.dirname(os.path.abspath(__file__))
METADATA_PATH = os.path.join(_DIR_SCRIPT, "modelo_metadata.json")

# ─── PALETA ───────────────────────────────────────────────────────────────────
COLORES = {
    "HV": "#1B5E20", "HL": "#388E3C", "MC": "#2E7D32",
    "ML": "#7B1FA2", "AT": "#0288D1", "VH": "#FFB300",
    "T2": "#00838F", "R":  "#4E342E",
}

FEATURE_COLS = [
    "peso_promedio_kg", "tipo_codigo_num",
    "cantidad_animales", "fecha_num", "hora_seg",
    "dia_semana", "mes", "semana_anio",
    "precio_mes_promedio",
]


# ─── CARGA DE METADATOS ───────────────────────────────────────────────────────

def cargar_metadata() -> dict:
    """
    Lee el archivo modelo_metadata.json con la información de los modelos
    entrenados (tipos disponibles, categorías, MAE por tipo, rutas de artefactos).

    Retorna:
        dict con la metadata completa.

    Lanza:
        SystemExit si el archivo no existe (no se ha entrenado todavía).
    """
    if not os.path.exists(METADATA_PATH):
        print("❌ No se encontró modelo_metadata.json.")
        print("   Ejecuta primero:  python modelo/entrenar_modelo.py")
        sys.exit(1)
    with open(METADATA_PATH, encoding="utf-8") as f:
        return json.load(f)


# ─── CONVERSIÓN DE HORA ───────────────────────────────────────────────────────

def hora_a_seg(h: str) -> int:
    """
    Convierte 'HH:MM' o 'HH:MM:SS' a segundos del día.
    En la predicción siempre usamos formato 24h ingresado por el usuario.
    """
    try:
        partes = str(h).strip().split(":")
        return int(partes[0]) * 3600 + int(partes[1]) * 60 + (int(partes[2]) if len(partes) > 2 else 0)
    except (ValueError, IndexError):
        return 36000  # 10:00 AM por defecto


# ─── DATOS HISTÓRICOS DESDE SUPABASE ──────────────────────────────────────────

def obtener_historico_categoria(tipo_subasta: str, tipo_codigo: str) -> dict:
    """
    Descarga las estadísticas históricas de una categoría específica.
    Retorna un dict con: mean, std, min, max, count, y últimos 90 días de precios.

    Si no hay conexión, retorna dict vacío (no bloquea la predicción).
    """
    try:
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            return {}
        sb = create_client(url, key)
        r  = (sb.table("subastas")
               .select("precio_final_kg,fecha_subasta")
               .eq("tipo_subasta", tipo_subasta)
               .eq("tipo_codigo", tipo_codigo)
               .gt("precio_final_kg", 0)
               .execute())
        if not r.data:
            return {}
        df = pd.DataFrame(r.data)
        df["fecha_dt"] = pd.to_datetime(df["fecha_subasta"])

        # Últimos 90 días para la gráfica
        hace_90 = pd.Timestamp.now() - pd.Timedelta(days=90)
        df_rec  = df[df["fecha_dt"] >= hace_90].sort_values("fecha_dt")

        return {
            "mean":     float(df["precio_final_kg"].mean()),
            "std":      float(df["precio_final_kg"].std()),
            "min":      float(df["precio_final_kg"].min()),
            "max":      float(df["precio_final_kg"].max()),
            "count":    len(df),
            "fechas_recientes":  df_rec["fecha_dt"].dt.strftime("%Y-%m-%d").tolist(),
            "precios_recientes": df_rec["precio_final_kg"].tolist(),
        }
    except Exception:
        return {}


# ─── INPUT DEL USUARIO ────────────────────────────────────────────────────────

def pedir_input(prompt: str, opciones: list[str] | None = None,
                tipo=str, default=None) -> object:
    """
    Pide un valor al usuario por consola con validación.

    Parámetros:
        prompt:  texto a mostrar
        opciones: lista de valores válidos (None = cualquier valor del tipo)
        tipo:    int / float / str
        default: valor por defecto si el usuario presiona Enter

    Retorna:
        valor validado del tipo indicado
    """
    while True:
        raw = input(f"  ► {prompt}").strip()
        if raw == "" and default is not None:
            return default
        try:
            val = tipo(raw)
        except (ValueError, TypeError):
            print(f"    ⚠️  Valor inválido. Esperaba: {tipo.__name__}")
            continue
        if opciones and val not in opciones:
            print(f"    ⚠️  Opciones válidas: {opciones}")
            continue
        return val


def recopilar_datos_lote(tipos_disponibles: list[str]) -> dict:
    """
    Solicita al usuario los datos del lote a predecir.

    Retorna:
        dict con: tipo_subasta, tipo_codigo, peso_promedio_kg,
                  cantidad_animales, fecha_subasta, hora_subasta
    """
    print("\n" + "─" * 55)
    print("📋 DATOS DEL LOTE")
    print("─" * 55)

    tipo_subasta = pedir_input(
        f"Tipo de subasta {tipos_disponibles}: ",
        opciones=tipos_disponibles, tipo=str
    )

    # Obtener categorías disponibles para este tipo desde la metadata
    return {"tipo_subasta": tipo_subasta}


# ─── GRÁFICA COMPARATIVA ──────────────────────────────────────────────────────

def graficar_prediccion(
    precio_pred: float,
    mae: float,
    tipo_codigo: str,
    tipo_subasta: str,
    historico: dict,
    fecha_pred: date,
) -> None:
    """
    Genera y abre una gráfica Plotly comparando el precio predicho
    vs. el histórico de la categoría en la misma tipo_subasta.

    Parámetros:
        precio_pred: precio predicho en COP/kg
        mae: error esperado del modelo en COP/kg
        tipo_codigo: código de categoría (ej. 'HV')
        tipo_subasta: nombre del tipo de subasta (ej. 'Tradicional')
        historico: dict retornado por obtener_historico_categoria()
        fecha_pred: fecha de la subasta predicha
    """
    color = COLORES.get(tipo_codigo, "#1B5E20")
    fig = go.Figure()

    # Histórico reciente
    if historico.get("fechas_recientes"):
        fig.add_trace(go.Scatter(
            x=historico["fechas_recientes"],
            y=historico["precios_recientes"],
            name=f"{tipo_codigo} — histórico 90d",
            line=dict(color=color, width=2.5),
            mode="lines+markers",
            marker=dict(size=5),
            hovertemplate="<b>%{x}</b><br>$%{y:,.0f} COP/kg<extra></extra>",
        ))

    # Punto de predicción
    fig.add_trace(go.Scatter(
        x=[str(fecha_pred)],
        y=[precio_pred],
        name="Tu lote (predicción)",
        mode="markers",
        marker=dict(size=18, symbol="star", color="#C62828", line=dict(width=2, color="white")),
        error_y=dict(type="constant", value=mae, visible=True, color="#C62828", thickness=2),
        hovertemplate=(
            f"<b>Predicción {tipo_codigo}</b><br>"
            "%{x}<br>"
            "<b>$%{y:,.0f} COP/kg</b><br>"
            f"±${mae:,.0f} COP/kg<extra></extra>"
        ),
    ))

    # Línea promedio histórico
    if historico.get("mean"):
        fig.add_hline(
            y=historico["mean"],
            line_dash="dot", line_color="#888888", line_width=1.5,
            annotation_text=f"Promedio histórico: ${historico['mean']:,.0f}",
            annotation_position="bottom right",
        )

    fig.update_layout(
        title=dict(
            text=(f"Precio predicho vs. histórico — {tipo_codigo} ({tipo_subasta})<br>"
                  f"<sup>Predicción: <b>${precio_pred:,.0f} COP/kg</b>  ±${mae:,.0f}</sup>"),
            font=dict(size=15, color="#1B5E20", family="Google Sans, Roboto, sans-serif"),
        ),
        xaxis=dict(title="Fecha", rangeslider=dict(visible=True)),
        yaxis=dict(title="Precio (COP/kg)", tickformat="$,.0f"),
        paper_bgcolor="#FFFFFF", plot_bgcolor="#F5F5F5",
        font=dict(family="Google Sans, Roboto, sans-serif", color="#212121"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1),
        margin=dict(l=40, r=20, t=80, b=40),
        hovermode="x unified",
    )
    fig.show()


# ─── PREDICCIÓN PRINCIPAL ─────────────────────────────────────────────────────

def predecir() -> None:
    """
    Interfaz interactiva completa:
      1. Carga la metadata de los modelos entrenados
      2. Solicita datos del lote al usuario
      3. Carga los artefactos del modelo correcto por tipo_subasta
      4. Genera la predicción con precio/kg, precio/animal y valor del lote
      5. Muestra comparativa vs. histórico y abre gráfica
    """
    print("\n" + "═" * 55)
    print("🐄  PREDICTOR DE PRECIOS — Central Ganadera de Medellín")
    print("═" * 55)

    # 1. Cargar metadata
    meta = cargar_metadata()
    modelos = {m["tipo"]: m for m in meta.get("modelos", [])}
    tipos   = list(modelos.keys())

    if not tipos:
        print("❌ No hay modelos entrenados. Ejecuta: python modelo/entrenar_modelo.py")
        sys.exit(1)

    print(f"\n  Modelos disponibles: {tipos}")
    print(f"  Fecha de entrenamiento: {meta.get('fecha_entrenamiento', 'N/A')[:10]}")

    # 2. Recopilar datos del lote
    print("\n" + "─" * 55)
    print("📋 DATOS DEL LOTE  (Enter = valor por defecto)")
    print("─" * 55)

    tipo_subasta = pedir_input(
        f"Tipo de subasta {tipos}: ",
        opciones=tipos, tipo=str
    )

    info_modelo   = modelos[tipo_subasta]
    cats_validas  = info_modelo["categorias"]
    mae_modelo    = info_modelo["mae"]
    artefactos    = info_modelo["artefactos"]

    tipo_codigo = pedir_input(
        f"Tipo de animal {cats_validas}: ",
        opciones=cats_validas, tipo=str
    )
    peso_prom = pedir_input(
        "Peso promedio del animal (kg) [ej. 280]: ",
        tipo=float, default=250.0
    )
    cantidad  = pedir_input(
        "Cantidad de animales en el lote [ej. 35]: ",
        tipo=int, default=20
    )

    # Fecha
    hoy_str   = date.today().strftime("%Y-%m-%d")
    fecha_str = pedir_input(
        f"Fecha de subasta YYYY-MM-DD [default: {hoy_str}]: ",
        tipo=str, default=hoy_str
    )
    try:
        fecha_sub = datetime.strptime(fecha_str, "%Y-%m-%d").date()
    except ValueError:
        print("  ⚠️  Fecha inválida, usando hoy.")
        fecha_sub = date.today()

    hora_str   = pedir_input(
        "Hora de subasta HH:MM [default: 10:00]: ",
        tipo=str, default="10:00"
    )
    hora_seg   = hora_a_seg(hora_str)

    # Obtener el contexto histórico ANTES de predecir para usarlo como feature
    print("\n  🔌 Consultando histórico en Supabase...")
    historico = obtener_historico_categoria(tipo_subasta, tipo_codigo)
    precio_mes_promedio = historico.get("mean", 8000.0) # Fallback seguro si no hay datos

    # 3. Construir vector de features
    ts        = pd.Timestamp(fecha_sub)
    fecha_num = int(ts.timestamp())
    dia_sem   = ts.dayofweek
    mes       = ts.month
    sem_anio  = ts.isocalendar()[1]

    modelo   = joblib.load(artefactos["modelo"])
    scaler_X = joblib.load(artefactos["scaler_X"])
    encoder  = joblib.load(artefactos["encoder"])

    cat_num  = int(encoder.transform([tipo_codigo])[0])
    X        = np.array([[peso_prom, cat_num, float(cantidad),
                          fecha_num, hora_seg, dia_sem, mes, sem_anio,
                          precio_mes_promedio]])
    X_sc     = scaler_X.transform(X)

    # 4. Predicción
    precio_kg    = float(modelo.predict(X_sc)[0])
    precio_animal= precio_kg * peso_prom
    valor_lote   = precio_animal * cantidad

    # 5. Output
    print("\n" + "═" * 55)
    print("💰 ESTIMACIÓN DE PRECIO DEL LOTE")
    print("═" * 55)
    print(f"  Tipo subasta:        {tipo_subasta}")
    print(f"  Categoría animal:    {tipo_codigo}")
    print(f"  Peso prom. animal:   {peso_prom:,.0f} kg")
    print(f"  Cantidad de animales:{cantidad}")
    print(f"  Fecha subasta:       {fecha_sub}")
    print("─" * 55)
    print(f"  💵 Precio/kg predicho:    ${precio_kg:>12,.0f} COP/kg")
    print(f"     ± error esperado:       ${mae_modelo:>12,.0f} COP/kg")
    print(f"  🐄 Precio/animal:          ${precio_animal:>12,.0f} COP")
    print(f"  📦 Valor del lote ({cantidad} animales): ${valor_lote:>12,.0f} COP")

    if historico:
        h_mean = historico["mean"]
        h_std  = historico["std"]
        diff   = (precio_kg - h_mean) / h_mean * 100
        signo  = "↑" if diff > 0 else "↓"
        print("─" * 55)
        print(f"  📊 Histórico {tipo_codigo} ({tipo_subasta}):")
        print(f"     Promedio:   ${h_mean:>12,.0f} COP/kg")
        print(f"     Std:        ${h_std:>12,.0f} COP/kg")
        print(f"     Rango:      ${historico['min']:,.0f} – ${historico['max']:,.0f}")
        print(f"     Registros:  {historico['count']}")
        print(f"  📈 Tu precio vs promedio: {diff:+.1f}% {signo}")

    print("═" * 55)

    # 6. Gráfica
    ver_grafica = input("\n  ¿Ver gráfica comparativa? (s/n) [s]: ").strip().lower()
    if ver_grafica in ("", "s", "si", "sí", "y", "yes"):
        graficar_prediccion(
            precio_pred=precio_kg,
            mae=mae_modelo,
            tipo_codigo=tipo_codigo,
            tipo_subasta=tipo_subasta,
            historico=historico,
            fecha_pred=fecha_sub,
        )

    # 7. ¿Predecir otro lote?
    otro = input("\n  ¿Predecir otro lote? (s/n) [n]: ").strip().lower()
    if otro in ("s", "si", "sí", "y", "yes"):
        predecir()


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        predecir()
    except KeyboardInterrupt:
        print("\n\n  Hasta luego. 🐄")
