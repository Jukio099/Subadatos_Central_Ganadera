import unittest

import pandas as pd

from modelo.predictor_mlops import (
    calcular_drift_basico,
    calcular_resumen_ejecutivo,
    resolver_artefacto,
)


class PredictorMlopsTests(unittest.TestCase):
    def test_resolver_artefacto_usa_basename_local(self):
        path = resolver_artefacto(r"C:\otro\equipo\modelo_Tradicional.pkl")
        self.assertTrue(str(path).endswith("modelo_Tradicional.pkl"))

    def test_resumen_ejecutivo_con_datos_minimos(self):
        df = pd.DataFrame(
            {
                "fecha_subasta": pd.to_datetime(["2026-01-01", "2026-01-15", "2026-02-01"]),
                "tipo_codigo": ["MC", "ML", "MC"],
                "precio_final_kg": [9000, 8000, 9500],
                "procedencia": ["Yopal", "Yopal", "Pore"],
                "cantidad_animales": [10, 5, 8],
                "peso_total_kg": [3500, 1500, 2800],
            }
        )
        resumen = calcular_resumen_ejecutivo(df, "Yopal/Casanare")
        self.assertEqual(resumen["categoria_top"], "MC")
        self.assertEqual(resumen["total_animales"], 23)
        self.assertIn("Yopal/Casanare", resumen["lectura"])

    def test_drift_sin_columna_edad_usa_proxy(self):
        df = pd.DataFrame(
            {
                "fecha_subasta": pd.date_range("2026-01-01", periods=120, freq="D"),
                "tipo_codigo": ["MC"] * 100 + ["ML"] * 20,
                "precio_final_kg": [9000] * 120,
                "cantidad_animales": [10] * 120,
                "peso_total_kg": [3500] * 120,
            }
        )
        alertas = calcular_drift_basico(df)
        detalles = " ".join(a["detalle"] for a in alertas)
        self.assertIn("edad explicita", detalles)


if __name__ == "__main__":
    unittest.main()
