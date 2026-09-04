"""Precios oficiales de los dos planes. Fuente: spec 2026-09-04."""

from __future__ import annotations

PLAN_DASHBOARD = "dashboard"
PLAN_PRO = "pro"
PLANES = (PLAN_DASHBOARD, PLAN_PRO)

PRECIOS_COP = {
    PLAN_DASHBOARD: 79_900,
    PLAN_PRO: 149_900,
}


def validar_plan(plan: str) -> str:
    if plan not in PRECIOS_COP:
        raise ValueError(f"Plan desconocido: {plan}")
    return plan


def amount_in_cents(plan: str) -> int:
    return PRECIOS_COP[validar_plan(plan)] * 100
