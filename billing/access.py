"""Regla pura de acceso. Sin I/O."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


_VIGENTES = frozenset({"active", "canceled", "past_due"})


@dataclass(frozen=True)
class Acceso:
    dashboard: bool
    predictor: bool
    motivo: str


def _como_dt(valor: Any) -> datetime:
    if isinstance(valor, datetime):
        if valor.tzinfo is None:
            raise ValueError("las fechas de suscripción deben llevar timezone")
        return valor
    raise TypeError(f"fecha inválida: {valor!r}")


def fila_desde_registro(data: dict | None) -> dict | None:
    """Pasa timestamps ISO de Supabase a datetime timezone-aware."""
    if data is None:
        return None
    fila = dict(data)
    for clave in ("trial_ends_at", "access_ends_at"):
        valor = fila.get(clave)
        if isinstance(valor, str):
            fila[clave] = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    return fila


def decidir_acceso(fila: dict | None, ahora: datetime) -> Acceso:
    if ahora.tzinfo is None:
        raise ValueError("ahora debe llevar timezone")
    if fila is None:
        return Acceso(False, False, "sin_fila")

    status = fila.get("status")
    plan = fila.get("plan")

    if status == "trial":
        vigente = ahora <= _como_dt(fila["trial_ends_at"])
        if not vigente:
            return Acceso(False, False, "trial_vencido")
        return Acceso(True, True, "trial")

    if status in _VIGENTES:
        vigente = ahora <= _como_dt(fila["access_ends_at"])
        if not vigente:
            return Acceso(False, False, f"{status}_vencido")
        predictor = plan == "pro"
        motivo = "pro" if predictor else "dashboard"
        return Acceso(True, predictor, motivo)

    return Acceso(False, False, status or "desconocido")
