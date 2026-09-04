import unittest

from billing.plans import PRECIOS_COP, amount_in_cents
from billing.wompi_reference import armar_referencia, parsear_referencia


class PreciosYReferenciaTests(unittest.TestCase):
    def test_precios_oficiales(self):
        self.assertEqual(PRECIOS_COP["dashboard"], 79900)
        self.assertEqual(PRECIOS_COP["pro"], 149900)
        self.assertEqual(amount_in_cents("pro"), 14_990_000)
        self.assertEqual(amount_in_cents("dashboard"), 7_990_000)

    def test_referencia_redonda(self):
        user_id = "11111111-1111-1111-1111-111111111111"
        raw = armar_referencia(user_id, "dashboard")
        parsed = parsear_referencia(raw)
        self.assertEqual(parsed["plan"], "dashboard")
        self.assertEqual(parsed["user_id"], user_id)
        self.assertIn(":", raw)
        self.assertGreater(len(raw.split(":")), 2)

    def test_referencias_distintas_no_colisionan(self):
        user_id = "11111111-1111-1111-1111-111111111111"
        a = armar_referencia(user_id, "pro")
        b = armar_referencia(user_id, "pro")
        self.assertNotEqual(a, b)
        self.assertEqual(parsear_referencia(a)["plan"], "pro")
        self.assertEqual(parsear_referencia(b)["user_id"], user_id)

    def test_plan_desconocido_rompe(self):
        with self.assertRaises(ValueError):
            amount_in_cents("empresarial")
        with self.assertRaises(ValueError):
            armar_referencia("11111111-1111-1111-1111-111111111111", "gold")


if __name__ == "__main__":
    unittest.main()
