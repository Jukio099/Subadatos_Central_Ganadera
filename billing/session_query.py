"""Parsea query params del magic link (sin Streamlit)."""

from __future__ import annotations

from typing import Any


def _primero(valor: Any) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, (list, tuple)):
        if not valor:
            return None
        valor = valor[0]
    texto = str(valor).strip()
    return texto or None


def sesion_desde_query(params: dict | None) -> dict | None:
    if not params:
        return None
    token_hash = _primero(params.get("token_hash"))
    tipo = _primero(params.get("type")) or "email"
    if token_hash:
        return {"kind": "otp", "token_hash": token_hash, "type": tipo}

    access_token = _primero(params.get("access_token"))
    refresh_token = _primero(params.get("refresh_token"))
    if access_token:
        return {
            "kind": "token",
            "access_token": access_token,
            "refresh_token": refresh_token,
        }
    return None
