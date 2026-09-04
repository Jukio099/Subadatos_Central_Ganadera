"""Gate de login y planes. Streamlit solo pinta; la regla vive en billing/."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from billing.access import Acceso, decidir_acceso, fila_desde_registro
from billing.plans import PLAN_DASHBOARD, PLAN_PRO, PRECIOS_COP
from billing.session_query import sesion_desde_query


def _fmt_cop(monto: int) -> str:
    return f"${monto:,}".replace(",", ".")


def _secret_o_env(nombre: str) -> str | None:
    valor = os.environ.get(nombre)
    if valor:
        return valor
    try:
        return st.secrets.get(nombre)
    except Exception:
        return None


def precio_cop(plan: str) -> int:
    clave = (
        "PLAN_DASHBOARD_PRICE_COP" if plan == PLAN_DASHBOARD else "PLAN_PRO_PRICE_COP"
    )
    crudo = _secret_o_env(clave)
    if crudo:
        return int(crudo)
    return PRECIOS_COP[plan]


def redirect_auth() -> str:
    return _secret_o_env("STREAMLIT_APP_URL") or "http://localhost:8501"


def _credenciales() -> tuple[str, str]:
    load_dotenv(ROOT / ".env")
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if url and key:
        return url, key
    try:
        return st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"]
    except Exception:
        st.error("Faltan SUPABASE_URL y SUPABASE_KEY.")
        st.stop()


def _params_query() -> dict:
    if hasattr(st, "query_params"):
        return dict(st.query_params)
    return st.experimental_get_query_params()


def _limpiar_query() -> None:
    if hasattr(st, "query_params"):
        try:
            st.query_params.clear()
            return
        except Exception:
            pass
    try:
        st.experimental_set_query_params()
    except Exception:
        pass


def _cliente_anon():
    url, key = _credenciales()
    return create_client(url, key)


def _cliente_usuario():
    cliente = _cliente_anon()
    access = st.session_state.get("auth_access_token")
    refresh = st.session_state.get("auth_refresh_token")
    if access and refresh:
        cliente.auth.set_session(access, refresh)
    return cliente


def _guardar_sesion(session) -> None:
    if session is None:
        return
    st.session_state["auth_access_token"] = session.access_token
    st.session_state["auth_refresh_token"] = session.refresh_token
    user = getattr(session, "user", None)
    if user is not None:
        st.session_state["auth_email"] = user.email
        st.session_state["auth_user_id"] = str(user.id)


def _consumir_magic_link(cliente) -> None:
    parsed = sesion_desde_query(_params_query())
    if not parsed:
        return
    try:
        if parsed["kind"] == "otp":
            resp = cliente.auth.verify_otp(
                {
                    "token_hash": parsed["token_hash"],
                    "type": parsed.get("type") or "email",
                }
            )
            _guardar_sesion(resp.session)
        else:
            cliente.auth.set_session(parsed["access_token"], parsed.get("refresh_token") or "")
            user = cliente.auth.get_user()
            st.session_state["auth_access_token"] = parsed["access_token"]
            st.session_state["auth_refresh_token"] = parsed.get("refresh_token")
            if user and user.user:
                st.session_state["auth_email"] = user.user.email
                st.session_state["auth_user_id"] = str(user.user.id)
    except Exception as exc:
        st.error(f"No se pudo validar el enlace: {exc}")
    _limpiar_query()


def _mostrar_login(cliente) -> None:
    st.markdown("### Entra a SubaDatos")
    st.caption("Te enviamos un enlace al correo. La prueba de 7 días incluye predictores (plan Pro).")
    email = st.text_input("Correo", key="login_email")
    enviar = st.button("Enviar enlace")
    reenviar = st.button("Reenviar enlace")
    if enviar or reenviar:
        if not email or "@" not in email:
            st.warning("Escribe un correo válido.")
            return
        try:
            cliente.auth.sign_in_with_otp(
                {
                    "email": email.strip(),
                    "options": {"email_redirect_to": redirect_auth()},
                }
            )
            st.success("Revisa tu correo y abre el enlace.")
        except Exception as exc:
            st.error(f"No se pudo enviar el correo: {exc}")


def _fila_rpc(data):
    if data is None:
        return None
    if isinstance(data, list):
        return data[0] if data else None
    return data


def _leer_suscripcion(cliente) -> dict | None:
    try:
        creado = cliente.rpc("start_trial").execute()
        fila = _fila_rpc(creado.data)
        if fila:
            return fila_desde_registro(fila)
    except Exception as exc:
        st.error(
            "No se pudo crear o leer la suscripción. "
            f"¿Aplicaste sql/create_suscripciones.sql? Detalle: {exc}"
        )
        st.stop()
    try:
        uid = st.session_state.get("auth_user_id")
        resp = (
            cliente.table("suscripciones")
            .select("*")
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return fila_desde_registro(rows[0]) if rows else None
    except Exception as exc:
        st.error(f"No se pudo leer suscripciones: {exc}")
        st.stop()


def mostrar_candado_pro() -> None:
    st.header("Predictores · plan Pro")
    st.info(
        f"Los predictores van en el plan Pro ({_fmt_cop(precio_cop(PLAN_PRO))} / mes). "
        "El plan Dashboard solo abre precios, volumen y detalle."
    )
    st.caption("El botón de pago Wompi se conecta en la siguiente entrega.")


def _mostrar_paywall() -> None:
    st.markdown("### Elige un plan")
    st.caption("La prueba de 7 días terminó. Elige cómo seguir. Pro es el plan recomendado.")
    dash = precio_cop(PLAN_DASHBOARD)
    pro = precio_cop(PLAN_PRO)
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Dashboard")
        st.markdown(f"**{_fmt_cop(dash)} / mes**")
        st.write("Precios, municipios, volumen y detalle.")
        st.button("Pagar Dashboard", key="pay_dash", disabled=True)
    with col2:
        st.subheader("Pro · Recomendado")
        st.markdown(f"**{_fmt_cop(pro)} / mes**")
        st.write("Todo el dashboard más predictores.")
        st.button("Pagar Pro", key="pay_pro", disabled=True)
    if st.button("Ya pagué"):
        st.rerun() if hasattr(st, "rerun") else st.experimental_rerun()
    st.caption("Pago con Wompi (tarjeta o Nequi). El checkout real queda para la siguiente entrega.")


def _barra_cuenta(acceso: Acceso, fila: dict) -> None:
    email = st.session_state.get("auth_email") or fila.get("email") or ""
    st.sidebar.caption(email)
    if acceso.motivo == "trial":
        fin = fila.get("trial_ends_at")
        extra = f" hasta {fin.date()}" if isinstance(fin, datetime) else ""
        st.sidebar.info(f"Quedan días de prueba · Plan Pro{extra}")
    elif acceso.predictor:
        fin = fila.get("access_ends_at")
        extra = f" hasta {fin.date()}" if isinstance(fin, datetime) else ""
        st.sidebar.success(f"Plan Pro{extra}")
    elif acceso.dashboard:
        fin = fila.get("access_ends_at")
        extra = f" hasta {fin.date()}" if isinstance(fin, datetime) else ""
        st.sidebar.success(f"Plan Dashboard{extra}")
    if st.sidebar.button("Cerrar sesión"):
        for clave in list(st.session_state.keys()):
            if str(clave).startswith("auth_"):
                del st.session_state[clave]
        if hasattr(st, "rerun"):
            st.rerun()
        st.experimental_rerun()


def exigir_acceso_dashboard() -> Acceso:
    cliente = _cliente_anon()
    _consumir_magic_link(cliente)

    if not st.session_state.get("auth_access_token"):
        _mostrar_login(cliente)
        st.stop()

    usuario = _cliente_usuario()
    fila = _leer_suscripcion(usuario)
    acceso = decidir_acceso(fila, datetime.now(timezone.utc))

    if not acceso.dashboard:
        if st.session_state.get("auth_email"):
            st.sidebar.caption(st.session_state["auth_email"])
            if st.sidebar.button("Cerrar sesión", key="logout_paywall"):
                for clave in list(st.session_state.keys()):
                    if str(clave).startswith("auth_"):
                        del st.session_state[clave]
                if hasattr(st, "rerun"):
                    st.rerun()
                st.experimental_rerun()
        _mostrar_paywall()
        st.stop()

    _barra_cuenta(acceso, fila or {})
    return acceso
