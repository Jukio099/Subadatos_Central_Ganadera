"""Transiciones puras de suscripción. Los RPC SQL deben copiar esta semántica."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from billing.plans import validar_plan

DIAS_PERIODO = 30
DIAS_GRACIA = 3


def _copia(fila: dict[str, Any] | None) -> dict[str, Any]:
    if fila is None:
        raise ValueError("no hay suscripción")
    return deepcopy(fila)


def aplicar_pago_aprobado(
    fila: dict[str, Any] | None,
    *,
    plan: str,
    transaction_id: str,
    payment_source_id: str,
    ahora: datetime,
) -> dict[str, Any]:
    validar_plan(plan)
    if not transaction_id or not payment_source_id:
        raise ValueError("faltan ids de Wompi")
    if ahora.tzinfo is None:
        raise ValueError("ahora debe llevar timezone")
    if fila is None:
        raise ValueError("no hay suscripción")
    if fila.get("wompi_transaction_id") == transaction_id:
        return deepcopy(fila)

    out = _copia(fila)
    out["status"] = "active"
    out["plan"] = plan
    out["pending_plan"] = None
    out["access_ends_at"] = ahora + timedelta(days=DIAS_PERIODO)
    out["wompi_transaction_id"] = transaction_id
    out["wompi_payment_source_id"] = payment_source_id
    out["cancel_at_period_end"] = False
    return out


def aplicar_renovacion_ok(
    fila: dict[str, Any] | None,
    *,
    transaction_id: str,
    ahora: datetime,
) -> dict[str, Any]:
    if ahora.tzinfo is None:
        raise ValueError("ahora debe llevar timezone")
    out = _copia(fila)
    if out.get("cancel_at_period_end"):
        raise ValueError("suscripción cancelada; no se renueva")
    if not transaction_id:
        raise ValueError("faltan ids de Wompi")
    if out.get("wompi_transaction_id") == transaction_id:
        return out

    pending = out.get("pending_plan")
    if pending:
        out["plan"] = validar_plan(pending)
    out["pending_plan"] = None
    out["status"] = "active"
    out["access_ends_at"] = out["access_ends_at"] + timedelta(days=DIAS_PERIODO)
    out["wompi_transaction_id"] = transaction_id
    return out


def aplicar_renovacion_fallida(
    fila: dict[str, Any] | None,
    ahora: datetime,
) -> dict[str, Any]:
    if ahora.tzinfo is None:
        raise ValueError("ahora debe llevar timezone")
    out = _copia(fila)
    gracia = ahora + timedelta(days=DIAS_GRACIA)
    actual = out["access_ends_at"]
    out["status"] = "past_due"
    out["access_ends_at"] = max(actual, gracia)
    return out


def marcar_cancelacion(fila: dict[str, Any] | None) -> dict[str, Any]:
    out = _copia(fila)
    if out.get("status") != "active":
        raise ValueError("solo se cancela un plan active")
    out["cancel_at_period_end"] = True
    return out


def marcar_baja_a_dashboard(fila: dict[str, Any] | None) -> dict[str, Any]:
    out = _copia(fila)
    if out.get("plan") != "pro":
        raise ValueError("solo se baja desde Pro")
    out["pending_plan"] = "dashboard"
    return out


def expirar_si_vence(fila: dict[str, Any] | None, ahora: datetime) -> dict[str, Any]:
    if ahora.tzinfo is None:
        raise ValueError("ahora debe llevar timezone")
    out = _copia(fila)
    if out.get("status") == "past_due" and ahora > out["access_ends_at"]:
        out["status"] = "expired"
    elif out.get("status") == "active" and out.get("cancel_at_period_end") and ahora > out["access_ends_at"]:
        out["status"] = "canceled"
    return out
