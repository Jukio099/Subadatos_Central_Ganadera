# Acceso por correo, prueba gratis y pago mensual — diseño

Fecha: 2026-09-04  
Estado: aprobado por el usuario (2026-09-04)  
Producto: dashboard Streamlit SubaDatos (`modelo/app.py`)

## Problema

La app en Streamlit Cloud es pública, se puede dormir por inactividad y no hay forma de saber quién entra ni de cobrar. Se necesita: que no se sienta caída, login con correo, 7 días de prueba y dos planes mensuales automáticos en Colombia.

## Decisiones cerradas

- Hosting: se queda en Streamlit Cloud. No se muda a Railway/Render en esta versión.
- Keepalive: se activa el workflow que ya existe (`.github/workflows/keepalive.yml`) con la URL real en `STREAMLIT_APP_URL`.
- Login: magic link de Supabase Auth (el usuario pone el correo, le llega un enlace).
- Mercado de cobro: Colombia. Proveedor: Wompi (tarjeta y Nequi para el débito automático).
- Precios: **dos planes**. Dashboard **$79.900 COP/mes**. Dashboard + predictores **$149.900 COP/mes** (recomendado en la UI). Los montos viven en secrets, no hardcodeados salvo para mostrarlos.
- Prueba: 7 días desde el primer login, **con acceso Pro** (dashboard + predictores). Un trial por correo; no se regenera.
- Acceso: portada comercial en `www.subadatos.com` (ya existe). Sin sesión o sin pago/trial no hay datos. El plan Dashboard no abre predictores. No hay plan gratis ni pestañas sueltas de cortesía.
- Cobro: mensual automático. Wompi no factura solo: se tokeniza tarjeta o Nequi y un job en Supabase cobra cada mes.
- Streamlit no recibe webhooks ni cobra. Solo lee si el usuario puede entrar.

## Experiencia de usuario

1. `www.subadatos.com` sigue siendo la vitrina (precios públicos vía `publicar_precios_landing.py`). El CTA “Probar 7 días gratis” apunta a la app Streamlit.
2. En Streamlit, sin sesión: pantalla de login (correo) y un resumen corto de qué es el dashboard. No se reconstruye la landing comercial dentro de Streamlit. No se cargan `subastas` ni filtros.
3. El usuario recibe el magic link, entra, y al primer login se crea su fila en `suscripciones` con `status = trial`, `plan = pro`, `trial_ends_at = ahora + 7 días`.
4. Con trial vigente ve **todo** (dashboard + predictores), para que pruebe lo que se recomienda comprar. Aviso: “Quedan N días de prueba · Plan Pro”.
5. Con pago `dashboard` vigente: pestañas de precios, municipios, volumen, detalle, etc. Los predictores se ven bloqueados (mensaje + CTA a Pro).
6. Con pago `pro` vigente: todo, igual que el trial. Aviso: “Plan Pro hasta YYYY-MM-DD”.
7. Si el trial venció y no hay pago: no ve datos. Dos tarjetas de plan; la de **$149.900** lleva la etiqueta “Recomendado”.
8. Tras el pago aprobado: `status = active`, `plan` = el elegido, `access_ends_at = ahora + 30 días`.
9. Puede cancelar: no se vuelve a cobrar; el acceso de ese plan sigue hasta `access_ends_at`.
10. Puede **subir** a Pro en cualquier momento (paga $149.900, período nuevo de 30 días; no hay reembolso del Dashboard). Puede **bajar** a Dashboard: sigue en Pro hasta `access_ends_at`; el siguiente cobro es $79.900.
11. “Ya pagué” relee Supabase por si el webhook tardó.

## Arquitectura

Cuatro piezas. Cada una hace una sola cosa.

```
www.subadatos.com          Streamlit Cloud              Supabase
(vitrina pública)          modelo/app.py                Auth + tabla + functions
        |                      |                              |
        |  CTA probar/pagar    |  lee sesión + suscripción    |
        +--------------------->| <----------------------------+
                               |                              ^
                               |                              |
                               |                         Wompi (checkout + cargos)
                               |                              |
                               +---- no cobra, no webhook ----+
```

1. **Streamlit Cloud** — gate de acceso y dashboard. Pregunta a Supabase: ¿hay sesión y acceso vigente? Si no → login o paywall. Si sí → dashboard; los predictores solo si el plan es `pro` o el trial sigue vivo.
2. **Supabase** — identidad (magic link), tabla `suscripciones`, Edge Function `wompi-checkout` (crea el pago y devuelve la URL de Wompi), Edge Function `wompi-webhook` (Wompi avisa un pago), Edge Function `wompi-renew` (cron diario que cobra a quien toca renovar).
3. **Wompi** — primer checkout por redirect (no se embebe el widget JS en Streamlit) y cargos siguientes con `payment_source_id` (CARD o NEQUI). PSE no se ofrece en el plan automático. La referencia de cada transacción incluye `user_id` y `plan` (`dashboard` o `pro`) para que el webhook acredite el plan correcto.
4. **Keepalive** — ping HTTP cada 25 minutos a la URL de Streamlit. Costo $0.

El ETL, `subastas`, `subastas_casanare` y `publicar_precios_landing.py` no cambian.

### Por qué el webhook no va a Streamlit

Streamlit Cloud se duerme. Un webhook de Wompi a la app se pierde. La función de Supabase está siempre disponible y usa la service role para escribir `suscripciones`.

## Modelo de datos

Tabla nueva `suscripciones`. No se altera el esquema de subastas.

| Columna | Tipo | Regla |
|---|---|---|
| `id` | uuid pk | Default `gen_random_uuid()` |
| `user_id` | uuid unique not null | `auth.users.id` |
| `email` | text not null | Copia para soporte; la identidad canónica es `user_id` |
| `status` | text not null | `trial` \| `active` \| `past_due` \| `expired` \| `canceled` |
| `plan` | text not null | `dashboard` \| `pro`. En trial siempre `pro` |
| `pending_plan` | text null | Si baja de Pro a Dashboard: `dashboard` (se aplica en la renovación) |
| `trial_ends_at` | timestamptz not null | Primer login + 7 días |
| `access_ends_at` | timestamptz not null | En trial coincide con `trial_ends_at`. En pago, fin del período |
| `wompi_payment_source_id` | text null | Fuente tokenizada (tarjeta o Nequi) |
| `wompi_transaction_id` | text null unique | Último pago aprobado; idempotencia |
| `cancel_at_period_end` | boolean not null default false | El usuario pidió no renovar |
| `created_at` | timestamptz | Default `now()` |
| `updated_at` | timestamptz | Default `now()` |

Índices: único en `user_id`; único en `wompi_transaction_id` donde no sea null.

RLS:

- El usuario autenticado solo hace `SELECT` de su fila (`user_id = auth.uid()`).
- El primer trial lo crea un RPC `start_trial()` `SECURITY DEFINER`: inserta una fila solo si no existe para ese `user_id`. Streamlit no escribe la tabla a mano.
- El webhook, el checkout y el cron usan service role. Streamlit usa dos clientes: el actual (`SUPABASE_KEY`) para leer subastas como hoy, y la sesión del usuario (JWT) solo para `suscripciones` / `start_trial`. La service role de Wompi no se guarda en Streamlit Cloud.

## Qué incluye cada plan

| | Dashboard · $79.900 | Pro · $149.900 (recomendado) |
|---|---|---|
| Pestañas de precios, municipios, volumen, detalle y el resto del `app.py` que no sea predictor | Sí | Sí |
| Pestaña **🤖 Predictor** en `modelo/app.py` | No | Sí |
| Página **Predictor MLOps** (`modelo/pages/1_Predictor_MLOps.py`) | No | Sí |
| Trial 7 días | — | Es el plan del trial |

Si alguien en Dashboard abre un predictor: no se calcula nada. Se muestra un candado y el CTA a Pro ($149.900).

## Regla de acceso

Una sola función pura, fácil de testear. Entrada: fila o `None` + `ahora`. Salida: `{dashboard: bool, predictor: bool}` + motivo.

`acceso_vigente` si existe fila y:

- `status = trial` y `ahora <= trial_ends_at`, o
- `status` en (`active`, `canceled`, `past_due`) y `ahora <= access_ends_at`.

Entonces:

- `dashboard = acceso_vigente`
- `predictor = acceso_vigente` y (`status = trial` o `plan = pro`)

No entra al dashboard si no hay sesión, no hay fila, el trial venció sin pago, `past_due` ya superó `access_ends_at`, o `expired`.

`canceled` con período vigente sigue con el `plan` que tenía: ya pagó el mes.

Un trial por `user_id`/correo. Si la fila existe, no se vuelve a poner `trial`.

## Cobro mensual automático

Montos (secrets):

| Plan | COP | `amount_in_cents` Wompi |
|---|---|---|
| `dashboard` | 79900 | 7990000 |
| `pro` | 149900 | 14990000 |

### Alta

1. El usuario, ya logueado y sin acceso, elige un plan. La UI marca Pro como recomendado. Streamlit llama `wompi-checkout` (JWT + `plan`).
2. La función cobra el monto de ese plan e incluye `user_id` y `plan` en `reference`.
3. Redirect a Wompi. Paga con tarjeta o Nequi (tokens de aceptación). Queda `payment_source`.
4. Webhook `transaction.updated` aprobado → `wompi-webhook`:
   - Firma inválida: 401, no escribe.
   - Si `wompi_transaction_id` ya existe: no-op.
   - Si no: `status = active`, `plan` = el de la referencia, `access_ends_at = ahora + 30 días`, guarda fuente e id de transacción, `cancel_at_period_end = false`, `pending_plan = null`.
5. Pago rechazado: no se abre el dashboard.

### Renovación

`wompi-renew` diario:

- Filas `status = active`, `cancel_at_period_end = false`, con fuente, `access_ends_at` en 48 h.
- Cobra el precio de `pending_plan` si existe, si no el de `plan`.
- Éxito: `access_ends_at += 30 días`; si había `pending_plan`, `plan = pending_plan` y `pending_plan = null`; `status = active`.
- Fallo: `status = past_due` y `access_ends_at = max(access_ends_at, ahora + 3 días)`. Aviso al correo.
- Reintento diario mientras `past_due` y `ahora <= access_ends_at`.
- Si `ahora > access_ends_at`: `status = expired`. Se cierra dashboard y predictores.

### Subir, bajar, cancelar

- **Subir a Pro** (solo `plan = dashboard` y acceso vigente): `wompi-checkout` de `pro`. Al aprobar: `plan = pro`, `pending_plan = null`, `access_ends_at = ahora + 30 días`. No se devuelve el mes de Dashboard.
- **Bajar a Dashboard** (solo `plan = pro`): `pending_plan = dashboard`. Sigue viendo predictores hasta `access_ends_at`. El próximo cobro es $79.900.
- **Cancelar** (`status = active`): `cancel_at_period_end = true`. El cron no cobra. Al vencer: `status = canceled`. `expired` es solo fallo de cobro. Puede volver a contratar con `wompi-checkout`.

No hay reembolso prorrateado en v1.

## Correo (magic link)

- Desarrollo: SMTP por defecto de Supabase (~2 emails/hora). Suficiente para pruebas.
- Producción: SMTP propio (Resend, 3.000/mes en free). Sin esto el login no escala.
- Botón “Reenviar enlace” si no llega o el link venció (expiración ~1 hora).
- Redirect del magic link: URL de la app Streamlit, listada en Supabase Auth → Redirect URLs.

El cliente `supabase==2.15.1` alcanza para `sign_in_with_otp` y verificar el enlace. Streamlit se queda en `1.31.0`. No se usa `st.login()` nativo.

## Secretos y configuración

Streamlit Cloud secrets (además de `SUPABASE_URL` / `SUPABASE_KEY` actuales):

- `PLAN_DASHBOARD_PRICE_COP` = `79900`
- `PLAN_PRO_PRICE_COP` = `149900`
- no guardar la llave privada ni el event secret de Wompi en Streamlit

Supabase Edge Functions:

- `WOMPI_PRIV_KEY`, `WOMPI_PUB_KEY`, `WOMPI_EVENTS_SECRET`
- `PLAN_DASHBOARD_PRICE_COP=79900`
- `PLAN_PRO_PRICE_COP=149900`

GitHub: variable de repo `STREAMLIT_APP_URL` = URL real `https://….streamlit.app`.

## Errores

| Caso | Comportamiento |
|---|---|
| No llega el correo | Reenviar. En prod, SMTP Resend |
| Magic link vencido | Pedir uno nuevo |
| Pagó y sigue bloqueado | “Ya pagué” relee la fila |
| Webhook duplicado | Único por `wompi_transaction_id` |
| Pago rechazado / renovación fallida | No abrir; `past_due` o se queda como estaba |
| Streamlit dormido durante el pago | El webhook igual escribe en Supabase |
| Alguien pide otra fila | RLS + unique `user_id` |
| Cron no corre un día | El cron es diario y mira una ventana de 48 h; el siguiente día cobra |

## Pruebas

Automatizadas (sin red a Wompi):

- Regla de acceso: sin fila; trial vigente (dashboard+predictor); trial vencido; `dashboard` vigente (solo dashboard); `pro` vigente (ambos); active vencido; canceled vigente según su `plan`; past_due dentro/fuera de gracia; expired.
- Webhook: firma inválida se rechaza; pago `dashboard` / `pro` deja el `plan` correcto; el mismo `wompi_transaction_id` no alarga el período; pago no aprobado no cambia status.
- Renew: no cobra si `cancel_at_period_end`; cobra precio de `pending_plan` si existe; éxito suma 30 días y aplica el pending; fallo pasa a `past_due`; al vencer gracia pasa a `expired`.

Manuales (sandbox Wompi + correo de prueba):

- Magic link ida y vuelta.
- Primer pago tarjeta y Nequi, plan Dashboard y plan Pro.
- Dashboard no abre predictores; Pro sí.
- Subir a Pro y bajar a Dashboard (pending).
- “Ya pagué”.
- Cancelar y comprobar que no hay segundo cobro (simular cron).
- Keepalive: el workflow deja de hacer no-op cuando existe `STREAMLIT_APP_URL`.

## Fuera de alcance (v1)

- Un tercer plan o precio por feria.
- Plan gratis / pestañas sueltas sin pagar.
- Stripe, Lemon Squeezy, Mercado Pago.
- PSE en el plan automático.
- Cuentas de equipo / varios asientos.
- Mudanza de hosting.
- Reembolsos y prorrateo (incluye al subir de Dashboard a Pro).
- Cambiar el ETL o las tablas de subastas.
- `st.login()` de Streamlit / Auth0 / Google como único login.

## Costo operativo

- Fijo: $0/mes (Streamlit Community Cloud + Supabase Free + GitHub Actions keepalive).
- Variable: comisión Wompi por cobro (~2,9% + $900 COP tarjeta, o ~$900 Nequi).
- Dashboard $79.900: neto aprox. $76.700 (tarjeta) o $79.000 (Nequi).
- Pro $149.900: neto aprox. $144.650 (tarjeta) o $149.000 (Nequi).
- Límite a vigilar: SMTP de Supabase (2 emails/hora) hasta poner Resend. Auth Free aguanta 50.000 MAU.

## Orden de implementación (cuando se apruebe el spec)

1. Activar keepalive (`STREAMLIT_APP_URL`).
2. Tabla `suscripciones` + RLS + función de acceso (dashboard vs predictor).
3. Gate en Streamlit (magic link + candado + candado de predictores).
4. `wompi-checkout` (dos montos) + webhook + primer pago.
5. Cron de renovación + cancelar + “Ya pagué”.
6. SMTP de producción (Resend) y CTA en `www.subadatos.com`.

Plan de implementación: `docs/superpowers/plans/2026-09-04-acceso-pagos.md`.
