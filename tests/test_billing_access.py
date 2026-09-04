from datetime import datetime, timedelta, timezone
import unittest

from billing.access import decidir_acceso, fila_desde_registro
from billing.session_query import sesion_desde_query


UTC = timezone.utc
AHORA = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _fila(**kwargs):
    base = {
        "status": "trial",
        "plan": "pro",
        "trial_ends_at": AHORA + timedelta(days=3),
        "access_ends_at": AHORA + timedelta(days=3),
        "cancel_at_period_end": False,
        "pending_plan": None,
    }
    base.update(kwargs)
    return base


class DecidirAccesoTests(unittest.TestCase):
    def test_sin_fila(self):
        acceso = decidir_acceso(None, AHORA)
        self.assertFalse(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_trial_vigente(self):
        acceso = decidir_acceso(_fila(status="trial"), AHORA)
        self.assertTrue(acceso.dashboard)
        self.assertTrue(acceso.predictor)

    def test_trial_vencido(self):
        acceso = decidir_acceso(
            _fila(
                status="trial",
                trial_ends_at=AHORA - timedelta(seconds=1),
                access_ends_at=AHORA - timedelta(seconds=1),
            ),
            AHORA,
        )
        self.assertFalse(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_active_dashboard_vigente(self):
        acceso = decidir_acceso(
            _fila(
                status="active",
                plan="dashboard",
                access_ends_at=AHORA + timedelta(days=10),
            ),
            AHORA,
        )
        self.assertTrue(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_active_pro_vigente(self):
        acceso = decidir_acceso(
            _fila(status="active", plan="pro", access_ends_at=AHORA + timedelta(days=10)),
            AHORA,
        )
        self.assertTrue(acceso.dashboard)
        self.assertTrue(acceso.predictor)

    def test_active_vencido(self):
        acceso = decidir_acceso(
            _fila(status="active", plan="pro", access_ends_at=AHORA - timedelta(seconds=1)),
            AHORA,
        )
        self.assertFalse(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_canceled_pro_vigente(self):
        acceso = decidir_acceso(
            _fila(
                status="canceled",
                plan="pro",
                access_ends_at=AHORA + timedelta(days=5),
                cancel_at_period_end=True,
            ),
            AHORA,
        )
        self.assertTrue(acceso.dashboard)
        self.assertTrue(acceso.predictor)

    def test_past_due_dashboard_en_gracia(self):
        acceso = decidir_acceso(
            _fila(
                status="past_due",
                plan="dashboard",
                access_ends_at=AHORA + timedelta(days=2),
            ),
            AHORA,
        )
        self.assertTrue(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_past_due_fuera_de_gracia(self):
        acceso = decidir_acceso(
            _fila(
                status="past_due",
                plan="pro",
                access_ends_at=AHORA - timedelta(seconds=1),
            ),
            AHORA,
        )
        self.assertFalse(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_expired(self):
        acceso = decidir_acceso(
            _fila(status="expired", plan="pro", access_ends_at=AHORA - timedelta(days=1)),
            AHORA,
        )
        self.assertFalse(acceso.dashboard)
        self.assertFalse(acceso.predictor)

    def test_fila_desde_registro_iso(self):
        fila = fila_desde_registro(
            {
                "status": "trial",
                "plan": "pro",
                "trial_ends_at": "2026-09-11T12:00:00Z",
                "access_ends_at": "2026-09-11T12:00:00+00:00",
            }
        )
        self.assertEqual(fila["trial_ends_at"].tzinfo, timezone.utc)
        acceso = decidir_acceso(fila, AHORA)
        self.assertTrue(acceso.dashboard)
        self.assertTrue(acceso.predictor)


class SesionQueryTests(unittest.TestCase):
    def test_token_hash(self):
        got = sesion_desde_query({"token_hash": ["abc"], "type": ["email"]})
        self.assertEqual(got["kind"], "otp")
        self.assertEqual(got["token_hash"], "abc")

    def test_access_token_plano(self):
        got = sesion_desde_query({"access_token": "tok", "refresh_token": "ref"})
        self.assertEqual(got["kind"], "token")
        self.assertEqual(got["refresh_token"], "ref")

    def test_vacio(self):
        self.assertIsNone(sesion_desde_query({}))
        self.assertIsNone(sesion_desde_query(None))


if __name__ == "__main__":
    unittest.main()

