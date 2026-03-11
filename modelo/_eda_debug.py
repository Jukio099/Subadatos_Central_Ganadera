import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import pandas as pd, numpy as np

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

todos = []
for p in range(0, 7):
    r = (sb.table("subastas")
         .select("precio_final_kg,peso_promedio_kg,tipo_codigo,fecha_subasta,cantidad_animales")
         .eq("tipo_subasta", "Tradicional")
         .gt("precio_final_kg", 0)
         .range(p * 1000, (p + 1) * 1000 - 1)
         .execute())
    if not r.data:
        break
    todos.extend(r.data)

df = pd.DataFrame(todos)
print(f"Muestra Tradicional: {len(df)} registros")
print()

print("=== Coeficiente de variación (CV) por categoría ===")
for cat, g in df.groupby("tipo_codigo"):
    cv = g["precio_final_kg"].std() / g["precio_final_kg"].mean() * 100
    print(f"  {cat}: mean=${g['precio_final_kg'].mean():,.0f}  std=${g['precio_final_kg'].std():,.0f}  CV={cv:.1f}%  n={len(g)}")

print()
print("=== Correlación peso vs precio ===")
print(df[["peso_promedio_kg", "precio_final_kg"]].corr())

print()
print("=== Distribución de precio_final_kg HL ===")
hl = df[df["tipo_codigo"] == "HL"]
print(hl["precio_final_kg"].describe())

print()
print("=== ¿Cuántos registros tienen peso < 50 kg? (posibles errores de entrada) ===")
print(df[df["peso_promedio_kg"] < 50][["tipo_codigo", "peso_promedio_kg", "precio_final_kg"]].head(10))

print()
print("=== Evolución mensual del precio/kg (HV) ===")
df["fecha_dt"] = pd.to_datetime(df["fecha_subasta"])
df["mes_key"]  = df["fecha_dt"].dt.to_period("M")
hv = df[df["tipo_codigo"] == "HV"]
print(hv.groupby("mes_key")["precio_final_kg"].agg(["mean", "std", "count"]).tail(12))
