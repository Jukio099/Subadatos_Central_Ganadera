"""Carga shared/data_cleaning.py por ruta, sin `import shared`.

En Streamlit Cloud el nombre `shared` choca con otro módulo y el
ImportError real llega redactado.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_PATH = Path(__file__).resolve().parent.parent / "shared" / "data_cleaning.py"
_NAME = "subadatos_shared_data_cleaning"


def _load():
    existente = sys.modules.get(_NAME)
    if existente is not None:
        return existente
    if not _PATH.is_file():
        raise ImportError(f"No esta el archivo de limpieza: {_PATH}")
    spec = importlib.util.spec_from_file_location(_NAME, _PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo crear el loader de {_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_NAME] = module
    spec.loader.exec_module(module)
    return module


_mod = _load()

FERIA_CENTRAL = _mod.FERIA_CENTRAL
FERIA_CASANARE = _mod.FERIA_CASANARE
FERIA_SUBASTAR = _mod.FERIA_SUBASTAR
limpiar_texto = _mod.limpiar_texto
normalizar_tipo_subasta = _mod.normalizar_tipo_subasta
normalizar_procedencia = _mod.normalizar_procedencia
es_precio_kg_valido = _mod.es_precio_kg_valido
filtrar_lotes_comerciales = _mod.filtrar_lotes_comerciales
