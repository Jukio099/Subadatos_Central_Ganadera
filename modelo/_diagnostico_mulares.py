"""
_diagnostico_mulares.py
=======================
Diagnóstico rápido para detectar si hay equinos (mulares, caballos, burros)
mezclados con vacuno en la tabla subastas, y ver el impacto en el MAPE.
"""
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Cargar todos los datos
print("Cargando datos...")
todos = []
pagina = 0
while True:
    r = (sb.table("subastas").select("tipo_subasta,tipo_codigo,peso_promedio_kg,precio_final_kg,fecha_subasta")
         .gt("precio_final_kg", 0).range(pagina*1000, (pagina+1)*1000-1).execute())
    if not r.data: break
    todos.extend(r.data)
    pagina += 1

df = pd.DataFrame(todos)
print(f"Total: {len(df)} registros\n")

print("=" * 55)
print("TIPOS DE SUBASTA distintos:")
print("=" * 55)
for t, cnt in df["tipo_subasta"].value_counts().items():
    pm = df[df["tipo_subasta"]==t]["precio_final_kg"]
    print(f"  '{t}': {cnt} reg | precio med=${pm.mean():,.0f} std=${pm.std():,.0f}")

print()
print("=" * 55)
print("CODIGOS DE CATEGORIA distintos (todos los tipos):")
print("=" * 55)
for cat, cnt in df["tipo_codigo"].value_counts().items():
    g = df[df["tipo_codigo"]==cat]
    pm = g["precio_final_kg"]
    pp = g["peso_promedio_kg"].astype(float)
    tipos = g["tipo_subasta"].unique().tolist()
    print(f"  '{cat}': {cnt} reg | precio=${pm.mean():,.0f} | peso={pp.mean():.0f}kg | tipos={tipos}")

print()
print("=" * 55)
print("POSIBLES EQUINOS (precio > $15.000/kg o peso < 30kg):")
print("=" * 55)
# Equinos suelen tener precios más altos por kg o pesos muy bajos
sospechosos = df[(df["precio_final_kg"] > 15000) |
                 (pd.to_numeric(df["peso_promedio_kg"], errors="coerce") < 30)]
print(f"  Registros sospechosos: {len(sospechosos)}")
if not sospechosos.empty:
    print(sospechosos[["tipo_subasta","tipo_codigo","peso_promedio_kg","precio_final_kg"]].head(20).to_string(index=False))

print()
print("=" * 55)
print("MAPE por tipo_codigo (tradicional) — para ver cuales son problematicos:")
print("=" * 55)
trad = df[df["tipo_subasta"] == "Tradicional"].copy()
trad["precio_final_kg"] = pd.to_numeric(trad["precio_final_kg"], errors="coerce")

# Calcular varianza por categoria
for cat, g in trad.groupby("tipo_codigo"):
    if len(g) < 10: continue
    mean = g["precio_final_kg"].mean()
    std  = g["precio_final_kg"].std()
    cv   = std/mean*100 if mean > 0 else 0
    peso = pd.to_numeric(g["peso_promedio_kg"], errors="coerce").mean()
    print(f"  '{cat}': n={len(g)} | media=${mean:,.0f} | std=${std:,.0f} | CV={cv:.1f}% | peso_med={peso:.0f}kg")
