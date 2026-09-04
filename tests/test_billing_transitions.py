from datetime import datetime, timedelta, timezone
import unittest

from billing.plans import PRECIOS_COP, amount_in_cents
from billing.transitions import (
    aplicar_pago_aprobado,
    aplicar_renovacion_fallida,
    aplicar_renovacion_ok,
    expirar_si_vence,
    marcar_baja_a_dashboard,
    marcar_cancelacion,
)
from billing.wompi_reference import armar_referencia, parsear_referencia

UTC = timezone.utc
AHORA = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _trial(**kwargs):
    fila = {
        "status": "trial",
        "plan": "pro",
        "pending_plan": None,
        "trial_ends_at": AHORA + timedelta(days=2),
        "access_ends_at": AHORA + timedelta(days=2),
        "wompi_transaction_id": None,
        "wompi_payment_source_id": None,
        "cancel_at_period_end": False,
    }
    fila.update(kwargs)
    return fila


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


class TransicionesTests(unittest.TestCase):
    def test_primer_pago_dashboard(self):
        out = aplicar_pago_aprobado(
            _trial(),
            plan="dashboard",
            transaction_id="tx_1",
            payment_source_id="src_1",
            ahora=AHORA,
        )
        self.assertEqual(out["status"], "active")
        self.assertEqual(out["plan"], "dashboard")
        self.assertEqual(out["access_ends_at"], AHORA + timedelta(days=30))
        self.assertFalse(out["cancel_at_period_end"])
        self.assertIsNone(out["pending_plan"])
        self.assertEqual(out["wompi_transaction_id"], "tx_1")
        self.assertEqual(out["wompi_payment_source_id"], "src_1")

    def test_primer_pago_pro(self):
        out = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_2",
            payment_source_id="src_2",
            ahora=AHORA,
        )
        self.assertEqual(out["status"], "active")
        self.assertEqual(out["plan"], "pro")
        self.assertEqual(out["access_ends_at"], AHORA + timedelta(days=30))

    def test_pago_duplicado_es_idempotente(self):
        primera = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_dup",
            payment_source_id="src_dup",
            ahora=AHORA,
        )
        segunda = aplicar_pago_aprobado(
            primera,
            plan="dashboard",
            transaction_id="tx_dup",
            payment_source_id="src_otra",
            ahora=AHORA + timedelta(days=1),
        )
        self.assertEqual(segunda, primera)

    def test_pago_sin_fila_rompe(self):
        with self.assertRaises(ValueError):
            aplicar_pago_aprobado(
                None,
                plan="pro",
                transaction_id="tx_x",
                payment_source_id="src_x",
                ahora=AHORA,
            )

    def test_renovacion_ok_suma_30_dias_y_aplica_pending(self):
        activa = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_old",
            payment_source_id="src_1",
            ahora=AHORA,
        )
        activa["pending_plan"] = "dashboard"
        out = aplicar_renovacion_ok(activa, transaction_id="tx_new", ahora=AHORA)
        self.assertEqual(out["access_ends_at"], AHORA + timedelta(days=60))
        self.assertEqual(out["plan"], "dashboard")
        self.assertIsNone(out["pending_plan"])
        self.assertEqual(out["status"], "active")
        self.assertEqual(out["wompi_transaction_id"], "tx_new")

    def test_renovacion_no_corre_si_cancelo(self):
        activa = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_old",
            payment_source_id="src_1",
            ahora=AHORA,
        )
        cancelada = marcar_cancelacion(activa)
        with self.assertRaises(ValueError):
            aplicar_renovacion_ok(cancelada, transaction_id="tx_new", ahora=AHORA)

    def test_renovacion_fallida_gracia(self):
        activa = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_old",
            payment_source_id="src_1",
            ahora=AHORA,
        )
        # access_ends_at está a +30 días; max(eso, ahora+3) = +30
        out = aplicar_renovacion_fallida(activa, AHORA)
        self.assertEqual(out["status"], "past_due")
        self.assertEqual(out["access_ends_at"], AHORA + timedelta(days=30))

        casi_vencida = dict(activa)
        casi_vencida["access_ends_at"] = AHORA + timedelta(hours=1)
        out2 = aplicar_renovacion_fallida(casi_vencida, AHORA)
        self.assertEqual(out2["access_ends_at"], AHORA + timedelta(days=3))

    def test_expira_si_gracia_acabo(self):
        fila = _trial(
            status="past_due",
            plan="pro",
            access_ends_at=AHORA - timedelta(seconds=1),
        )
        out = expirar_si_vence(fila, AHORA)
        self.assertEqual(out["status"], "expired")

    def test_cancelar_solo_marca_flag(self):
        activa = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_old",
            payment_source_id="src_1",
            ahora=AHORA,
        )
        fin = activa["access_ends_at"]
        out = marcar_cancelacion(activa)
        self.assertTrue(out["cancel_at_period_end"])
        self.assertEqual(out["access_ends_at"], fin)
        self.assertEqual(out["status"], "active")

    def test_bajar_solo_si_es_pro(self):
        pro = aplicar_pago_aprobado(
            _trial(),
            plan="pro",
            transaction_id="tx_p",
            payment_source_id="src_p",
            ahora=AHORA,
        )
        out = marcar_baja_a_dashboard(pro)
        self.assertEqual(out["pending_plan"], "dashboard")
        self.assertEqual(out["plan"], "pro")

        dash = aplicar_pago_aprobado(
            _trial(),
            plan="dashboard",
            transaction_id="tx_d",
            payment_source_id="src_d",
            ahora=AHORA,
        )
        with self.assertRaises(ValueError):
            marcar_baja_a_dashboard(dash)


if __name__ == "__main__":
    unittest.main()

