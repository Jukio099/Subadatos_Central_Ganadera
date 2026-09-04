# Acceso, trial y dos planes Wompi — plan de implementación

> **Para el agente:** implementar este plan tarea por tarea. Spec: `docs/superpowers/specs/2026-09-04-acceso-pagos-design.md`. No improvisar precios, estados ni webhooks en Streamlit.

**Goal:** Cerrar el dashboard si no hay login + trial/pago; dos planes (Dashboard $79.900, Pro $149.900); predictores solo en trial/Pro; cobro mensual Wompi fuera de Streamlit.

**Architecture:** Lógica de negocio en Python puro (`billing/`). Streamlit solo pinta y llama RPCs. SQL en Supabase (tabla + RLS + RPCs). Edge Functions delgadas: firman Wompi y llaman los mismos RPCs. Keepalive ya existe.

**Tech stack:** Python 3.11, `unittest` (como `tests/test_predictor_mlops.py`), Streamlit 1.31, supabase 2.15.1, SQL en `sql/`, Edge Functions Deno.

**Fuera de este plan:** ETL, tablas de subastas, mudar hosting, Stripe, Resend (solo checklist manual), CTA en `www.subadatos.com` (otro repo).

---

## Archivos

| Path | Rol |
|---|---|
| `billing/__init__.py` | Paquete |
| `billing/access.py` | `decidir_acceso(fila, ahora) -> Acceso` |
| `billing/plans.py` | ids y montos 79900 / 149900 |
| `billing/transitions.py` | primer pago, renew, fallo, cancelar, bajar |
| `billing/wompi_reference.py` | armar/parsear `user_id:plan:nonce` |
| `tests/test_billing_access.py` | regla de acceso |
| `tests/test_billing_transitions.py` | webhook / renew / planes |
| `sql/create_suscripciones.sql` | tabla, RLS, `start_trial`, `apply_wompi_payment`, `set_cancel`, `set_pending_plan`, `renew_due_subscriptions` |
| `supabase/functions/wompi-checkout/index.ts` | crea transacción, devuelve URL |
| `supabase/functions/wompi-webhook/index.ts` | verifica firma → `apply_wompi_payment` |
| `supabase/functions/wompi-renew/index.ts` | cron → `renew_due_subscriptions` |
| `modelo/acceso.py` | sesión magic link + lee `suscripciones` + llama RPCs |
| `modelo/app.py` | gate al inicio de `main()`; candado en tab Predictor |
| `modelo/pages/1_Predictor_MLOps.py` | candado Pro al inicio |
| `.github/workflows/keepalive.yml` | sin cambio de lógica; falta la var de repo |

No tocar `etl/`, `publicar_precios_landing.py`, ni el esquema de `subastas`.

---

### Task 1: Precios y referencia Wompi

**Files:**
- Create: `billing/__init__.py`, `billing/plans.py`, `billing/wompi_reference.py`
- Test: `tests/test_billing_transitions.py` (casos de referencia; el resto de transiciones en Task 3)

**Step 1: Test que falle**

En `tests/test_billing_transitions.py`:

```python
from billing.plans import PRECIOS_COP, amount_in_cents
from billing.wompi_reference import armar_referencia, parsear_referencia

def test_precios_oficiales():
    assert PRECIOS_COP["dashboard"] == 79900
    assert PRECIOS_COP["pro"] == 149900
    assert amount_in_cents("pro") == 14_990_000

def test_referencia_redonda():
    raw = armar_referencia("11111111-1111-1111-1111-111111111111", "dashboard")
    parsed = parsear_referencia(raw)
    assert parsed["plan"] == "dashboard"
    assert parsed["user_id"] == "11111111-1111-1111-1111-111111111111"
```

`armar_referencia` debe incluir un nonce para que dos checkouts del mismo usuario no colisionen. `parsear_referencia` ignora el nonce.

**Step 2:** `python -m unittest tests.test_billing_transitions -q` → FAIL (no existe el módulo).

**Step 3:** Implementar `billing/plans.py` y `billing/wompi_reference.py`. Plan desconocido → `ValueError`.

**Step 4:** Mismos tests → PASS.

**Step 5:** Commit `feat: precios y referencia Wompi de los dos planes`

---

### Task 2: Regla de acceso (TDD)

**Files:**
- Create: `billing/access.py`
- Test: `tests/test_billing_access.py`

**Step 1: Tests**

`decidir_acceso(fila: dict | None, ahora: datetime) -> Acceso` con `dashboard: bool`, `predictor: bool`, `motivo: str`.

Casos mínimos (uno por test):

1. `fila is None` → ambos False
2. `trial` y `ahora <= trial_ends_at` → ambos True
3. `trial` vencido → ambos False
4. `active` + `plan=dashboard` + `ahora <= access_ends_at` → dashboard True, predictor False
5. `active` + `plan=pro` vigente → ambos True
6. `active` vencido → ambos False
7. `canceled` + `plan=pro` vigente → ambos True
8. `past_due` dentro de `access_ends_at` + `plan=dashboard` → dashboard True, predictor False
9. `past_due` fuera de `access_ends_at` → ambos False
10. `expired` → ambos False

Usar datetimes timezone-aware (UTC).

**Step 2:** Correr tests → FAIL.

**Step 3:** Implementar exactamente la regla del spec (sección “Regla de acceso”). No hablar con Supabase.

**Step 4:** PASS.

**Step 5:** Commit `feat: regla de acceso dashboard vs predictor`

---

### Task 3: Transiciones de suscripción (TDD)

**Files:**
- Create: `billing/transitions.py`
- Modify: `tests/test_billing_transitions.py`

Estas funciones son la fuente de verdad. Los RPC SQL de la Task 4 deben hacer lo mismo.

Funciones (puras: reciben fila + evento, devuelven fila nueva o error):

- `aplicar_pago_aprobado(fila, *, plan, transaction_id, payment_source_id, ahora)`
- `aplicar_renovacion_ok(fila, *, transaction_id, ahora)`
- `aplicar_renovacion_fallida(fila, ahora)`
- `marcar_cancelacion(fila)`
- `marcar_baja_a_dashboard(fila)`
- `expirar_si_vence(fila, ahora)`

**Step 1: Tests**

- Primer pago `dashboard` / `pro`: `status=active`, `plan` correcto, `access_ends_at = ahora + 30 días`, `cancel_at_period_end=False`, `pending_plan=None`
- Mismo `wompi_transaction_id` otra vez: fila igual (idempotente)
- Pago no se aplica sobre una fila `None` (el checkout exige usuario con trial o expired; si no hay fila → error)
- Renew ok: +30 días; si `pending_plan=dashboard`, `plan` pasa a `dashboard` y pending se limpia
- Renew no corre si `cancel_at_period_end`
- Renew fallo: `past_due`, `access_ends_at = max(actual, ahora+3 días)`
- Tras fallo, `ahora > access_ends_at` → `expired`
- Cancelar: solo flag; no cambia `access_ends_at`
- Bajar: `pending_plan=dashboard` solo si `plan=pro`

**Step 2:** FAIL.

**Step 3:** Implementar. Duraciones fijas: trial no se toca aquí; pago +30 días; gracia +3 días.

**Step 4:** PASS.

**Step 5:** Commit `feat: transiciones de pago, renovación y cambio de plan`

---

### Task 4: SQL en Supabase

**Files:**
- Create: `sql/create_suscripciones.sql`

**Step 1:** Escribir el SQL completo, ejecutable en el SQL Editor:

- Tabla `suscripciones` según el spec (check de `status` y `plan`)
- Unique `user_id`
- Unique parcial `wompi_transaction_id` (`WHERE wompi_transaction_id IS NOT NULL`)
- RLS: `SELECT` propio (`user_id = auth.uid()`). Sin INSERT/UPDATE directos para `authenticated`
- `start_trial()` SECURITY DEFINER: si no hay fila para `auth.uid()`, inserta `status=trial`, `plan=pro`, `trial_ends_at=now()+7 days`, `access_ends_at=trial_ends_at`. Si ya hay fila, la devuelve. `SET search_path = public`.
- `apply_wompi_payment(p_user_id, p_plan, p_transaction_id, p_payment_source_id)` service_role only: carga fila, aplica la misma semántica que `aplicar_pago_aprobado`
- `set_cancel_at_period_end()` authenticated: solo su fila, solo si `status=active`
- `set_pending_plan_dashboard()` authenticated: solo su fila, solo si `plan=pro` y acceso vigente
- `renew_due_subscriptions()` service_role: selecciona `active`, no cancelados, con fuente, `access_ends_at` en 48 h; el **cargo HTTP a Wompi lo hace la Edge Function**, este RPC solo actualiza estado cuando la función le pasa éxito/fallo. Para no mezclar HTTP en SQL, partir en:
  - `list_renewals_due()` → set de filas
  - `apply_renewal_result(p_user_id, p_ok, p_transaction_id)`

**Step 2:** No hay runner SQL en CI. Revisión: cada RPC debe poder mapearse 1:1 a una función de `billing/transitions.py`. Si algo no mapea, corregir el SQL.

**Step 3:** El usuario (o el agente con acceso) ejecuta el archivo en Supabase SQL Editor. No commitear secrets.

**Step 4:** Commit `feat: tabla suscripciones, RLS y RPCs de billing`

---

### Task 5: Gate Streamlit (login + candado del dashboard)

**Files:**
- Create: `modelo/acceso.py`
- Modify: `modelo/app.py` (`main` línea ~931)

**Step 1:** Extraer helpers testeables en `modelo/acceso.py` (o reexportar `decidir_acceso`). Tests nuevos solo si hay parsing de query params del magic link (token en URL). Si Streamlit recibe `?code=` / hash de Supabase, encapsular `sesion_desde_query(params) -> token | None` y testear el parser.

**Step 2:** En `main()`, **antes** del `selectbox` de feria y **antes** de `cargar_datos`:

1. `st.set_page_config` ya corrió (está a nivel módulo; no mover).
2. Resolver sesión (magic link: `sign_in_with_otp` + verificar token de la URL; guardar JWT en `st.session_state`).
3. Si no hay sesión: UI de correo + “Enviar enlace” / “Reenviar”. `st.stop()`.
4. Si hay sesión: `start_trial()` una vez; leer `suscripciones`; `decidir_acceso`.
5. Si `dashboard` es False: UI de dos planes (Pro con badge “Recomendado”). Botones de checkout se cablean en Task 7; aquí pueden ser placeholders `st.info`. `st.stop()`.
6. Si `dashboard` es True: sidebar con email, aviso de trial/plan, y el `main()` actual.

No cargar `subastas` si el gate corta.

Cliente: el `SUPABASE_KEY` actual sigue para datos. Un segundo cliente con el JWT del usuario para `suscripciones` y RPCs. No meter service role en Streamlit.

**Step 3:** Probar a mano `streamlit run modelo/app.py`: sin login no hay spinner de Supabase de históricos.

**Step 4:** Commit `feat: gate de login y trial en el dashboard Streamlit`

---

### Task 6: Candado de predictores

**Files:**
- Modify: `modelo/app.py` tab 7 (~1133)
- Modify: `modelo/pages/1_Predictor_MLOps.py` (después de `set_page_config`)

**Step 1:** Helper `mostrar_candado_pro()` en `modelo/acceso.py`: texto + precio Pro + (luego) botón upgrade.

**Step 2:** Tab Predictor: si no `acceso.predictor`, solo el candado. No dejar el “Próximamente” como único contenido si hay Pro: si `predictor` es True, dejar el contenido actual (aunque siga diciendo Próximamente) o el embed que ya exista. No reescribir el modelo.

**Step 3:** En `1_Predictor_MLOps.py`, repetir resolución de sesión + `decidir_acceso`. Si no hay dashboard, `st.stop()` con login/paywall. Si hay dashboard pero no predictor, candado y `st.stop()`. No llamar `predecir_lote` sin permiso.

**Step 4:** Commit `feat: bloquear predictores al plan Dashboard`

---

### Task 7: Edge Function checkout + webhook

**Files:**
- Create: `supabase/functions/wompi-checkout/index.ts`
- Create: `supabase/functions/wompi-webhook/index.ts`
- Modify: `modelo/acceso.py` / `modelo/app.py` (botones reales)

**Step 1:** `wompi-checkout`

- Header `Authorization: Bearer <jwt usuario>`
- Body `{ plan: "dashboard" | "pro" }`
- Verifica JWT con Supabase
- Crea transacción Wompi (llave privada, solo aquí) con `amount_in_cents` de `plans`, `reference` de `armar_referencia`, `redirect_url` = URL Streamlit
- Responde `{ url }`
- Streamlit: `st.link_button` o `js` redirect a esa URL

**Step 2:** `wompi-webhook`

- Verifica `WOMPI_EVENTS_SECRET` (checksum Wompi; si la firma no cuadra → 401)
- Si evento no es transacción aprobada → 200 vacío
- Parsea `reference` → `apply_wompi_payment(...)` con service role
- Idempotente gracias a `wompi_transaction_id`

**Step 3:** Paywall y “Ya pagué”: reléé la fila (no inventar status en session).

**Step 4:** Secrets solo en Supabase / Streamlit Cloud según el spec. Añadir `.env.example` **sin** valores reales: nombres `PLAN_DASHBOARD_PRICE_COP`, `PLAN_PRO_PRICE_COP`.

**Step 5:** Commit `feat: checkout y webhook Wompi`

Despliegue: `supabase functions deploy wompi-checkout wompi-webhook` (manual; requiere CLI y proyecto). Documentar la URL del webhook en Wompi Dashboard (sandbox primero).

---

### Task 8: Renovación, cancelar, subir/bajar

**Files:**
- Create: `supabase/functions/wompi-renew/index.ts`
- Modify: `modelo/app.py` sidebar (cuenta)

**Step 1:** `wompi-renew`

- Auth: header secreto `RENEW_CRON_SECRET` (no público)
- Llama `list_renewals_due()`, por cada fila cobra Wompi con `payment_source_id` y `amount_in_cents` de `pending_plan ?? plan`, `recurrent: true` en tarjeta
- Llama `apply_renewal_result`
- Programar: Supabase scheduled function diario 05:00 America/Bogota, o GitHub Action `curl` a la función. Preferir schedule de Supabase para no depender de Actions.

**Step 2:** UI autenticada, acceso vigente:

- `Cancelar plan` → `set_cancel_at_period_end`
- Si `plan=pro`: `Cambiar a Dashboard` → `set_pending_plan_dashboard`
- Si `plan=dashboard`: `Mejorar a Pro` → mismo checkout `pro` (Task 7)

**Step 3:** Tests Python ya cubren la semántica. Aquí solo cablear.

**Step 4:** Commit `feat: renovación automática y cambio de plan`

---

### Task 9: Keepalive

**Files:**
- Modify: `.github/workflows/keepalive.yml` (mensaje más claro si falta la var)
- Modify: `docs/session_log.md` — anotar que hay que setear `STREAMLIT_APP_URL`

**Step 1:** En GitHub → Settings → Variables → `STREAMLIT_APP_URL` = URL real `https://<app>.streamlit.app` (sin path raro). El agente no puede adivinarla; si está en Streamlit Cloud, copiarla.

**Step 2:** `workflow_dispatch` del keepalive y comprobar un HTTP 200/302.

**Step 3:** Commit solo si el YAML o el session_log cambian: `chore: documentar STREAMLIT_APP_URL del keepalive`

---

### Task 10: Checklist manual (no código)

Hacer, no commitear secretos:

- [ ] SQL `create_suscripciones.sql` aplicado
- [ ] Auth: magic link on; Redirect URL = app Streamlit (local y cloud)
- [ ] Functions desplegadas; webhook URL en Wompi sandbox
- [ ] Secrets Streamlit: `PLAN_DASHBOARD_PRICE_COP`, `PLAN_PRO_PRICE_COP`
- [ ] Secrets Supabase functions: Wompi + precios + `RENEW_CRON_SECRET`
- [ ] Probar: correo → trial Pro → ver predictor
- [ ] Pagar sandbox Dashboard → predictor bloqueado
- [ ] Upgrade sandbox Pro → predictor abre
- [ ] Cancelar / bajar (pending)
- [ ] SMTP Resend antes de usuarios reales
- [ ] CTA “Probar 7 días” en `www.subadatos.com` (otro repo)

---

## Orden y parada

Tasks 1–3 son locales y no necesitan credenciales. 4 y 7–9 sí. Si falta cuenta Wompi o permiso de Supabase, entregar 1–3 + SQL + gate con checkout en `st.info("pendiente URL")` y no fingir un pago.

Tras cada task: tests de billing en verde (`python -m unittest tests.test_billing_access tests.test_billing_transitions tests.test_predictor_mlops -q`) y commit.

El plan detallado de código termina aquí. Implementar solo cuando el usuario lo pida.
