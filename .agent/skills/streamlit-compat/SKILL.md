---
description: Guía de compatibilidad de Streamlit para el proyecto (use_column_width y carga de imágenes).
---

# Streamlit Compatibilidad y Buenas Prácticas

Este skill se aplica cuando se trabaje con la UI de Streamlit, especialmente al insertar imágenes, gráficos o columnas en `app.py`.

El entorno actual del usuario emplea una **versión antigua de Streamlit** (aparentemente 1.5.x o similar) que no soporta todas las funcionalidades modernas. Por tanto, DEBES adherirte a las siguientes reglas:

1. **Atributo de ancho de contenido (`use_column_width` vs `use_container_width`):**
   - **NUNCA** utilices `use_container_width=True` en componentes de Streamlit (como `st.image()`). Esta versión lanzará un error `TypeError: unexpected keyword argument 'use_container_width'`.
   - **SIEMPRE** utiliza `use_column_width=True` en su lugar.

2. **Carga de Imágenes (`st.image`):**
   - En esta versión, pasar directamente una cadena de texto (ruta local) a `st.image()` puede fallar estrepitosamente y mostrar el ícono de imagen rota (`<img>` tag vacío o error en el frontend).
   - **SIEMPRE** utiliza `PIL.Image.open()` para abrir imágenes locales antes de pasarlas a Streamlit.
   - Ejemplo correcto:
     ```python
     from PIL import Image
     img = Image.open("ruta/a/la/imagen.png")
     st.image(img, use_column_width=True)
     ```

3. **Columnas de Diseño:**
   - Para ajustar el tamaño relativo de las imágenes, usa `st.columns` proporcionando proporciones (ej. `[1, 6, 1]`) y pasa la imagen a la columna central. Acompaña SIEMPRE con `use_column_width=True` dentro de esa columna:
     ```python
     col1, col2, col3 = st.columns([1, 6, 1])
     col2.image(img, use_column_width=True)
     ```
