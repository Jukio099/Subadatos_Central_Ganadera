# ─── Script de Entrenamiento del Modelo de Predicción ──────────────────────
# Usa scikit-learn (GradientBoosting) en vez de Keras — más ligero y práctico
# Uso: python entrenar_modelo.py [--csv datos_lotes.csv]
# ────────────────────────────────────────────────────────────────────────────

import os
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

# ─── Configuración ──────────────────────────────────────────────────────────
MODELO_PATH = "modelo_ganado.pkl"
SCALER_X_PATH = "scaler_X.pkl"
SCALER_Y_PATH = "scaler_y.pkl"
LABEL_ENCODER_PATH = "encoder_sexo.pkl"
METADATA_PATH = "modelo_metadata.json"

# Modelo viejo (Keras) — para backup
MODELO_KERAS_PATH = "modelo_ganado_numerado.h5"

# Todas las categorías detectadas en SubaCasanare
CATEGORIAS_SEXO = ['HL', 'HV', 'MC', 'MG', 'ML', 'NC', 'NG', 'VC', 'VG', 'VP']


# ─── Preparación de datos ──────────────────────────────────────────────────

def cargar_y_preparar_datos(csv_path):
    """Carga el CSV de lotes y prepara las features para el modelo."""
    print(f"📂 Cargando datos de '{csv_path}'...")
    df = pd.read_csv(csv_path)
    print(f"   {len(df)} registros cargados")

    # Filtrar filas con datos incompletos
    columnas_requeridas = ['Sexo', 'Cantidad', 'Peso_Promedio', 'Hora_Entrada',
                           'Fecha', 'Precio_Final']
    df = df.dropna(subset=columnas_requeridas)

    # Filtrar precios y pesos inválidos
    df = df[df['Precio_Final'] > 0]
    df = df[df['Peso_Promedio'] > 0]
    df = df[df['Cantidad'] > 0]

    # Filtrar categorías válidas
    df = df[df['Sexo'].isin(CATEGORIAS_SEXO)]

    # ── Corrección de Escala de Precios (Normalización) ──
    # Algunos precios están "Por Animal" (> $50.000) y otros "Por Kilo" (< $20.000)
    # Normalizamos todo a "Precio Por Kilo"
    def normalizar_precio(row):
        precio = row['Precio_Final']
        peso = row['Peso_Promedio']
        # Si el precio es mayor a 50,000, asumimos que es por ANIMAL y lo convertimos a por KILO
        if precio > 50000:
            return precio / peso if peso > 0 else 0
        return precio

    df['Precio_Kilo'] = df.apply(normalizar_precio, axis=1)

    # Filtrar precios por kilo inválidos (rango razonable: 2k - 20k)
    df = df[(df['Precio_Kilo'] > 2000) & (df['Precio_Kilo'] < 25000)]

    # Eliminar outliers solo sobre Precio_Kilo
    df_limpio = []
    for sexo in df['Sexo'].unique():
        subset = df[df['Sexo'] == sexo]
        mean_p = subset['Precio_Kilo'].mean()
        std_p = subset['Precio_Kilo'].std()
        if std_p > 0:
            subset = subset[
                (subset['Precio_Kilo'] > mean_p - 3 * std_p) &
                (subset['Precio_Kilo'] < mean_p + 3 * std_p)
            ]
        df_limpio.append(subset)
    df = pd.concat(df_limpio)

    print(f"   {len(df)} registros válidos después de limpieza y normalización")
    print(f"   Precio Kilo Promedio: ${df['Precio_Kilo'].mean():,.0f}")
    
    print(f"\n   Distribución por categoría:")
    for sexo, count in df['Sexo'].value_counts().items():
        print(f"     {sexo}: {count} registros")

    # ── Convertir fecha ──
    df['Fecha_dt'] = pd.to_datetime(df['Fecha'])
    df['Fecha_num'] = df['Fecha_dt'].astype(np.int64) // 10**9

    # Features temporales adicionales
    df['Dia_semana'] = df['Fecha_dt'].dt.dayofweek     # 0=Lunes, 6=Domingo
    df['Mes'] = df['Fecha_dt'].dt.month
    df['Semana_anio'] = df['Fecha_dt'].dt.isocalendar().week.astype(int)

    # ── Convertir hora a segundos ──
    def hora_a_seg(h):
        try:
            parts = str(h).split(':')
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        except (ValueError, IndexError):
            return None

    df['Hora_seg'] = df['Hora_Entrada'].apply(hora_a_seg)
    df = df.dropna(subset=['Hora_seg'])

    # ── Codificar sexo ──
    encoder = LabelEncoder()
    encoder.fit(CATEGORIAS_SEXO)
    df['Sexo_num'] = encoder.transform(df['Sexo'])

    print(f"\n   Mapeo de Sexo:")
    for i, cat in enumerate(encoder.classes_):
        count = len(df[df['Sexo'] == cat])
        if count > 0:
            print(f"     {cat} → {i}  ({count} registros)")

    return df, encoder


def construir_features(df):
    """Construye matrices X (features) e y (target)."""
    feature_cols = ['Peso_Promedio', 'Sexo_num', 'Cantidad', 'Fecha_num',
                    'Hora_seg', 'Dia_semana', 'Mes']
    X = df[feature_cols].values
    y = df['Precio_Kilo'].values
    return X, y, feature_cols


# ─── Entrenamiento ─────────────────────────────────────────────────────────

def entrenar(csv_path, test_size=0.2):
    """Pipeline completo de entrenamiento con GradientBoosting."""
    print(f"\n{'═' * 60}")
    print(f"🐂 ENTRENAMIENTO DEL MODELO — SubaDatos")
    print(f"   Motor: scikit-learn GradientBoostingRegressor")
    print(f"{'═' * 60}")

    # 1. Cargar datos
    df, encoder = cargar_y_preparar_datos(csv_path)
    X, y, feature_cols = construir_features(df)

    print(f"\n📐 Features: {feature_cols}")
    print(f"   X: {X.shape}  |  y: {y.shape}")

    # 2. Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    print(f"\n📊 Split: {len(X_train)} train / {len(X_test)} test")

    # 3. Escalar features (no el target — GradientBoosting no lo necesita)
    scaler_X = StandardScaler()
    X_train_scaled = scaler_X.fit_transform(X_train)
    X_test_scaled = scaler_X.transform(X_test)

    # Scaler para y (por compatibilidad con la interfaz)
    scaler_y = StandardScaler()
    scaler_y.fit(y_train.reshape(-1, 1))

    # 4. Modelo GradientBoosting
    print(f"\n🏋️ Entrenando GradientBoostingRegressor...")
    model = GradientBoostingRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        validation_fraction=0.1,
        n_iter_no_change=20,       # Early stopping
        verbose=1,
    )
    model.fit(X_train_scaled, y_train)

    # 5. Evaluar
    y_pred = model.predict(X_test_scaled)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mape = np.mean(np.abs((y_pred - y_test) / y_test)) * 100

    print(f"\n{'─' * 50}")
    print(f"📈 Evaluación en test set:")
    print(f"   MAE:  ${mae:,.0f}")
    print(f"   RMSE: ${rmse:,.0f}")
    print(f"   MAPE: {mape:.2f}%")

    # Cross-validation
    print(f"\n� Cross-validation (5 folds):")
    cv_scores = cross_val_score(model, X_train_scaled, y_train,
                                 cv=5, scoring='neg_mean_absolute_error')
    print(f"   MAE por fold: {[-s for s in cv_scores]}")
    print(f"   MAE promedio: ${-cv_scores.mean():,.0f} (±${cv_scores.std():,.0f})")

    # Feature importance
    print(f"\n🎯 Importancia de features:")
    importances = model.feature_importances_
    for name, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1]):
        bar = '█' * int(imp * 50)
        print(f"   {name:<16} {imp:.3f} {bar}")

    # Ejemplos
    print(f"\n🔍 Ejemplos de predicción:")
    print(f"   {'Real':>12}  {'Predicho':>12}  {'Error %':>8}")
    for i in range(min(10, len(y_test))):
        err = abs(y_pred[i] - y_test[i]) / y_test[i] * 100
        print(f"   ${y_test[i]:>10,.0f}  ${y_pred[i]:>10,.0f}  {err:>6.1f}%")

    # 6. Guardar
    print(f"\n💾 Guardando archivos...")

    # Backup del modelo Keras anterior si existe
    if os.path.exists(MODELO_KERAS_PATH):
        backup_path = MODELO_KERAS_PATH.replace('.h5', '_backup.h5')
        if not os.path.exists(backup_path):
            os.rename(MODELO_KERAS_PATH, backup_path)
            print(f"   📦 Backup Keras: {backup_path}")

    joblib.dump(model, MODELO_PATH)
    print(f"   ✅ Modelo: {MODELO_PATH}")

    joblib.dump(scaler_X, SCALER_X_PATH)
    print(f"   ✅ Scaler X: {SCALER_X_PATH}")

    joblib.dump(scaler_y, SCALER_Y_PATH)
    print(f"   ✅ Scaler Y: {SCALER_Y_PATH}")

    joblib.dump(encoder, LABEL_ENCODER_PATH)
    print(f"   ✅ Encoder: {LABEL_ENCODER_PATH}")

    # Metadata
    metadata = {
        'fecha_entrenamiento': datetime.now().isoformat(),
        'motor': 'GradientBoostingRegressor (scikit-learn)',
        'registros_totales': len(df),
        'registros_train': len(X_train),
        'registros_test': len(X_test),
        'feature_columns': feature_cols,
        'mae': float(mae),
        'rmse': float(rmse),
        'mape': float(mape),
        'cv_mae_mean': float(-cv_scores.mean()),
        'categorias': list(encoder.classes_),
        'feature_importances': {name: float(imp) for name, imp in zip(feature_cols, importances)},
    }

    with open(METADATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Metadata: {METADATA_PATH}")

    print(f"\n{'═' * 60}")
    print(f"✅ ¡ENTRENAMIENTO COMPLETADO!")
    print(f"   Error promedio (MAPE): {mape:.1f}%")
    print(f"   Modelo: sklearn GradientBoosting ({model.n_estimators_} árboles)")
    print(f"{'═' * 60}\n")

    return model, scaler_X, scaler_y, encoder, metadata


# ─── CLI ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='🐂 Entrenamiento del modelo de predicción de precios'
    )
    parser.add_argument('--csv', type=str, default='datos_lotes.csv',
                        help='Ruta al CSV de datos (default: datos_lotes.csv)')
    parser.add_argument('--test', type=float, default=0.2,
                        help='Proporción de datos para test (default: 0.2)')

    args = parser.parse_args()
    entrenar(args.csv, test_size=args.test)
