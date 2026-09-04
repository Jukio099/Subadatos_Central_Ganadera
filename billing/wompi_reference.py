"""Referencia Wompi: user_id:plan:nonce (el nonce evita colisiones)."""

from __future__ import annotations

import uuid

from billing.plans import validar_plan

_PARTES = 3


def armar_referencia(user_id: str, plan: str, nonce: str | None = None) -> str:
    validar_plan(plan)
    try:
        user = str(uuid.UUID(user_id))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"user_id inválido: {user_id}") from exc
    token = nonce if nonce is not None else uuid.uuid4().hex[:12]
    if not token or ":" in token:
        raise ValueError("nonce inválido")
    return f"{user}:{plan}:{token}"


def parsear_referencia(raw: str) -> dict[str, str]:
    if not raw or raw.count(":") < 2:
        raise ValueError(f"referencia inválida: {raw}")
    user_id, plan, _nonce = raw.split(":", _PARTES - 1)
    validar_plan(plan)
    try:
        user = str(uuid.UUID(user_id))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"user_id inválido en referencia: {user_id}") from exc
    return {"user_id": user, "plan": plan}
