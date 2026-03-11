"""Test rápido de las 3 APIs externas — v2 con variables correctas."""
import requests, json
from datetime import date
from dateutil.relativedelta import relativedelta

hoy = date.today()
hace_3m = date(hoy.year, hoy.month, 1) - relativedelta(months=3)
ayer = hoy - relativedelta(days=1)

print("=" * 55)
print("TEST 1: Open-Meteo (clima Medellín) — variables corregidas")
print("=" * 55)
# soil_moisture_0_to_7cm solo disponible como variable horaria, no diaria
# Usamos et0_fao_evapotranspiration como proxy de estrés hídrico (sí es diaria)
r = requests.get(
    "https://archive-api.open-meteo.com/v1/archive",
    params={
        "latitude": 6.25, "longitude": -75.56,
        "start_date": str(hace_3m), "end_date": str(ayer),
        "daily": "precipitation_sum,temperature_2m_max,et0_fao_evapotranspiration",
        "timezone": "America/Bogota",
    },
    timeout=30
)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    dias = d.get("daily", {}).get("time", [])
    print(f"  Días recibidos: {len(dias)}")
    if dias:
        i = -1
        print(f"  Muestra último día ({dias[i]}): "
              f"lluvia={d['daily']['precipitation_sum'][i]}mm  "
              f"temp_max={d['daily']['temperature_2m_max'][i]}°C  "
              f"ET0={d['daily']['et0_fao_evapotranspiration'][i]}mm")
else:
    print(f"  Error: {r.text[:300]}")

print()
print("=" * 55)
print("TEST 2: datos.gov.co — IPC alimentos Colombia")
print("=" * 55)
r2 = requests.get(
    "https://www.datos.gov.co/resource/pcyb-3xhf.json",
    params={"$limit": 5},
    timeout=30
)
print(f"Status: {r2.status_code}")
if r2.status_code == 200:
    recs = r2.json()
    print(f"  Registros muestra: {len(recs)}")
    if recs:
        print(f"  Columnas disponibles: {list(recs[0].keys())}")
        print(f"  Fila muestra: {json.dumps(recs[0], ensure_ascii=False)[:300]}")
else:
    print(f"  Error ({r2.status_code}): {r2.text[:200]}")
    # Intentar endpoint alternativo
    print("  Probando endpoint alternativo...")
    r2b = requests.get(
        "https://www.datos.gov.co/resource/3fjj-z9qy.json",
        params={"$limit": 5}, timeout=30
    )
    print(f"  Alt status: {r2b.status_code}")
    if r2b.status_code == 200:
        recs2 = r2b.json()
        if recs2:
            print(f"  Alt columnas: {list(recs2[0].keys())}")
            print(f"  Alt fila: {json.dumps(recs2[0], ensure_ascii=False)[:300]}")

print()
print("=" * 55)
print("TEST 3: World Bank — Precio maíz PMAIZMT.GBL")
print("=" * 55)
r3 = requests.get(
    "https://api.worldbank.org/v2/en/indicator/PMAIZMT.GBL?format=json&mrv=5&per_page=10",
    timeout=30
)
print(f"Status: {r3.status_code}")
if r3.status_code == 200:
    payload = r3.json()
    if isinstance(payload, list) and len(payload) > 1 and payload[1]:
        recs = [x for x in payload[1] if x.get("value")]
        print(f"  Registros con valor: {len(recs)}")
        for rec in recs[:4]:
            print(f"    {rec['date']}: ${rec['value']:,.1f} USD/ton")
    else:
        print(f"  Respuesta: {str(payload)[:300]}")
else:
    print(f"  Error: {r3.text[:200]}")

print()
print("✅ Tests v2 terminados")
