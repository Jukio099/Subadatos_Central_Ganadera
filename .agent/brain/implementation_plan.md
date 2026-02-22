# Dashboard Expansión — Plan de Implementación

Agregar dos pestañas nuevas, reemplazar el KPI de "Kg totales" por una métrica de tendencia con delta, y mantener coherencia visual con la paleta del proyecto.

---

## 1. KPI col3 — Reemplazar "Kg totales" por "Tendencia 1 mes"

#### [MODIFY] [app.py](file:///c:/Users/ASUS/Documents/Subadatos_ETL_Streamlit/modelo/app.py)

Dentro de `mostrar_kpis(df)`, col3 actualmente muestra suma de kg. Reemplazar por:
- Precio promedio del **último mes** del rango filtrado vs. el **mes anterior**
- `st.metric(..., delta="±X%")` → Streamlit pinta verde si sube, rojo si baja

---

## 2. Nueva tab 2 — "🏷️ Última Subasta"

Filtra el `df` al **último `fecha_subasta`** disponible en los datos filtrados.

| Gráfica | Tipo | Pregunta que responde |
|---------|------|-----------------------|
| Animales por categoría | Barras (paleta COLORES) | ¿Qué tipos se vendieron y cuántos? |
| Precio vs orden de lote | Línea (`numero_boletin`) | ¿Cómo evolucionó el precio durante la subasta? |
| Precio vs cantidad de animales | Scatter | ¿Los lotes grandes se pagan mejor o peor? |
| Precio vs peso promedio/animal | Scatter | ¿Los animales más pesados cotizan más? |

> [!NOTE]
> Si el filtro de fechas del sidebar excluye la última fecha, se muestra `st.warning`.

---

## 3. Nueva tab 3 — "📊 Tendencias"

4 métricas delta en fila + gráfica de línea de precio por día para cada ventana:

| Ventana | Lógica |
|---------|--------|
| 7 días | Precio hoy vs precio de hace 7 días |
| 30 días | Precio hoy vs precio de hace 30 días |
| 90 días | Precio hoy vs precio de hace 90 días |
| 180 días | Precio hoy vs precio de hace 180 días |

Función auxiliar: `calcular_tendencia(df, dias) → (precio_inicio, precio_fin, pct)`

La gráfica muestra las 4 series en un solo `px.line` para comparar visualmente la velocidad de cambio.

---

## 4. Nueva estructura de pestañas

```
tab1  📈 Precios          ← sin cambios
tab2  🏷️ Última Subasta   ← NUEVA
tab3  📊 Tendencias       ← NUEVA
tab4  🗺️ Municipios       ← antes tab2
tab5  ⚖️ Volumen          ← antes tab3
tab6  🔍 Detalle          ← antes tab4
tab7  🤖 Predictor        ← antes tab5
```

---

## Verificación
- Revisar KPI col3: delta % sin NaN si hay poco historial
- Pestaña "Última Subasta": 4 gráficas de la fecha más reciente
- Pestaña "Tendencias": 4 métricas + línea, respeta filtro de tipo
- Pestañas anteriores: sin regresiones
