"""
_test_precio_total.py
Script para probar la hipótesis del usuario: predecir el PRECIO TOTAL del animal
en lugar del precio por kg, para mitigar el efecto de animales muy livianos que
tienen un precio/kg altísimo debido al valor base del animal vivo.
"""
import os
import sys
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_percentage_error, mean_absolute_error

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

print("Cargando datos...")
todos = []
pagina = 0
while True:
    r = sb.table("subastas").select("*").gt("precio_final_kg", 0).range(pagina*1000, (pagina+1)*1000-1).execute()
    if not r.data: break
    todos.extend(r.data)
    pagina += 1

df = pd.DataFrame(todos)
df["peso_promedio_kg"] = pd.to_numeric(df["peso_promedio_kg"], errors="coerce")
df["precio_final_kg"] = pd.to_numeric(df["precio_final_kg"], errors="coerce")

# Quedarnos solo con Tradicional (la masa principal)
df = df[df["tipo_subasta"] == "Tradicional"].copy()
df = df.dropna(subset=["peso_promedio_kg", "precio_final_kg", "fecha_subasta", "tipo_codigo"])

# FEATURE INGENIERÍA ----------------------------------------------------
# 1. Calcular variables base
df["fecha_dt"] = pd.to_datetime(df["fecha_subasta"])
df["fecha_num"] = df["fecha_dt"].astype(np.int64) // 10**9
df["mes"] = df["fecha_dt"].dt.month
df["dia_semana"] = df["fecha_dt"].dt.dayofweek

# 2. Factor Inflacion Acumulada (~8% anual desde 2023)
mes_base = pd.Timestamp("2023-01-01")
df["inflacion_acum"] = (1 + 0.08/12) ** ((df["fecha_dt"] - mes_base).dt.days / 30)

# 3. TARGET: PRUEBA A -> Predecir Precio por KG (Lo que hacemos hoy)
df["target_kg"] = df["precio_final_kg"] / df["inflacion_acum"]

# 4. TARGET: PRUEBA B -> Predecir Valor Total del animal
df["valor_total"] = df["precio_final_kg"] * df["peso_promedio_kg"]
df["target_total"] = df["valor_total"] / df["inflacion_acum"]

# Filtrar outliers extremos para tener un test justo
df = df[(df["precio_final_kg"] > 2000) & (df["precio_final_kg"] < 25000)]
df = df[df["peso_promedio_kg"] > 50]

encoder = LabelEncoder()
df["tipo_codigo_num"] = encoder.fit_transform(df["tipo_codigo"])

FEATURES = ["peso_promedio_kg", "tipo_codigo_num", "fecha_num", "mes", "dia_semana"]

X = df[FEATURES]
y_kg = df["target_kg"]
y_total = df["target_total"]

X_train, X_test, y_kg_train, y_kg_test, y_total_train, y_total_test, w_train, w_test = train_test_split(
    X, y_kg, y_total, df["peso_promedio_kg"], test_size=0.2, random_state=42
)

inf_test = df.loc[X_test.index, "inflacion_acum"]

print("\n--- ENTRENANDO MODELO A (Predecir Precio/KG directamente) ---")
m_kg = GradientBoostingRegressor(n_estimators=100, random_state=42)
m_kg.fit(X_train, y_kg_train)
pred_kg_deflactada = m_kg.predict(X_test)
pred_kg_real = pred_kg_deflactada * inf_test
mape_a = mean_absolute_percentage_error(y_kg_test * inf_test, pred_kg_real)
mae_a = mean_absolute_error(y_kg_test * inf_test, pred_kg_real)
print(f"  MAPE: {mape_a*100:.2f}%")
print(f"  MAE:  ${mae_a:,.0f} COP/kg")

print("\n--- ENTRENANDO MODELO B (Predecir Valor Total -> dividir por peso) ---")
m_total = GradientBoostingRegressor(n_estimators=100, random_state=42)
m_total.fit(X_train, y_total_train)
pred_total_deflactada = m_total.predict(X_test)
pred_total_real = pred_total_deflactada * inf_test
# Para comparar peras con peras, dividimos el total predicho entre el peso
pred_kg_desde_total = pred_total_real / w_test
mape_b = mean_absolute_percentage_error(y_kg_test * inf_test, pred_kg_desde_total)
mae_b = mean_absolute_error(y_kg_test * inf_test, pred_kg_desde_total)
print(f"  MAPE: {mape_b*100:.2f}%")
print(f"  MAE:  ${mae_b:,.0f} COP/kg")

print("\nConclusión:")
if mape_b < mape_a:
    print(f"✅ El usuario TENÍA RAZÓN. Predecir el valor total baja el MAPE en {mape_a*100 - mape_b*100:.2f} puntos porcentuales.")
else:
    print(f"❌ La hipótesis es interesante, pero directo al KG sigue siendo {mape_b*100 - mape_a*100:.2f} p.p. mejor.")
