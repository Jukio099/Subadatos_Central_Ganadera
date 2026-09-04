# Acceso por correo, prueba gratis y pago mensual — diseño

Fecha: 2026-09-04  
Estado: pendiente de revisión del usuario  
Producto: dashboard Streamlit SubaDatos (`modelo/app.py`)

## Problema

La app en Streamlit Cloud es pública, se puede dormir por inactividad y no hay forma de saber quién entra ni de cobrar. Se necesita: que no se sienta caída, login con correo, 7 días de prueba y un plan mensual automático en Colombia.

## Decisiones cerradas

- Hosting: se queda en Streamlit Cloud. No se muda a Railway/Render en esta versión.
- Keepalive: se activa el workflow que ya existe (`.github/workflows/keepalive.yml`) con la URL real en `STREAMLIT_APP_URL`.
- Login: magic link de Supabase Auth (el usuario pone el correo, le llega un enlace).
- Mercado de cobro: Colombia. Proveedor: Wompi (tarjeta y Nequi para el débito automático).
- Precio: **$79.900 COP al mes**, un solo plan. El monto vive en el secret `PLAN_PRICE_COP`, no hardcodeado en la UI más que para mostrarlo.
- Prueba: 7 días desde el primer login. Un trial por correo; no se regenera.
- Acceso: portada comercial en `www.subadatos.com` (ya existe). El dashboard completo solo con trial vigente o pago activo. No hay pestañas freemium.
- Cobro: mensual automático. Wompi no factura solo: se tokeniza tarjeta o Nequi y un job en Supabase cobra cada mes.
- Streamlit no recibe webhooks ni cobra. Solo lee si el usuario puede entrar.

## Experiencia de usuario

1. `www.subadatos.com` sigue siendo la vitrina (precios públicos vía `publicar_precios_landing.py`). El CTA “Probar 7 días gratis” apunta a la app Streamlit.
2. En Streamlit, sin sesión: pantalla de login (correo) y un resumen corto de qué es el dashboard. No se reconstruye la landing comercial dentro de Streamlit. No se cargan `subastas` ni filtros.
3. El usuario recibe el magic link, entra, y al primer login se crea su fila en `suscripciones` con `status = trial` y `trial_ends_at = ahora + 7 días`.
4. Con trial vigente o pago activo ve el dashboard actual, sin cambios de pestañas. Aviso discreto: “Quedan N días de prueba” o “Plan activo hasta YYYY-MM-DD”.
5. Si el trial venció y no hay pago: no ve datos. Botón para suscribirse con Wompi ($79.900/mes, tarjeta o Nequi).
6. Tras el primer pago aprobado: `status = active` y `access_ends_at = ahora + 30 días`. Al recargar, entra.
7. Puede cancelar en la app: no se vuelve a cobrar; el acceso sigue hasta `access_ends_at`.
8. “Ya pagué” relee Supabase por si el webhook tardó.

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

1. **Streamlit Cloud** — gate de acceso y dashboard. Pregunta a Supabase: ¿hay sesión y (trial vigente o acceso pagado vigente)? Sí → `app.py` actual. No → login o paywall.
2. **Supabase** — identidad (magic link), tabla `suscripciones`, Edge Function `wompi-checkout` (crea el pago y devuelve la URL de Wompi), Edge Function `wompi-webhook` (Wompi avisa un pago), Edge Function `wompi-renew` (cron diario que cobra a quien toca renovar).
3. **Wompi** — primer checkout por redirect (no se embebe el widget JS en Streamlit) y cargos siguientes con `payment_source_id` (CARD o NEQUI). PSE no se ofrece en el plan automático: no tokeniza para débito recurrente. La referencia de cada transacción incluye `user_id` para que el webhook sepa a quién acreditar.
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

## Regla de acceso

Una sola función pura, fácil de testear. Entrada: fila o `None` + `ahora`. Salida: `allow` o `deny` + motivo.

Entra si existe fila y:

- `status = trial` y `ahora <= trial_ends_at`, o
- `status` en (`active`, `canceled`, `past_due`) y `ahora <= access_ends_at`.

No entra si no hay sesión, no hay fila, el trial venció sin pago, `past_due` ya superó `access_ends_at` (fin de gracia), o `expired`.

`canceled` con período vigente sigue entrando: ya pagó el mes.

Un trial por `user_id`/correo. Si la fila existe, no se vuelve a poner `trial`.

## Cobro mensual automático

Monto: **79900 COP**. Secret `PLAN_PRICE_COP=79900`. Wompi trabaja en centavos: `7990000` (`amount_in_cents`).

### Alta

1. El usuario, ya logueado y sin acceso, pulsa pagar. Streamlit llama `wompi-checkout` (con el JWT). Esa función crea la transacción en Wompi por $79.900 e incluye `user_id` en `reference`.
2. Streamlit redirige a la URL de pago de Wompi. El usuario acepta los contratos (tokens de aceptación) y paga con tarjeta o Nequi.
3. Wompi tokeniza y deja una `payment_source`. Al volver a la app, el usuario puede pulsar “Ya pagué” si el webhook aún no llegó.
4. Webhook `transaction.updated` aprobado → `wompi-webhook`:
   - Firma inválida: 401, no escribe.
   - Si `wompi_transaction_id` ya existe: no-op.
   - Si no: `status = active`, `access_ends_at = ahora + 30 días`, guarda `wompi_payment_source_id` y `wompi_transaction_id`, `cancel_at_period_end = false`.
5. Pago rechazado: no se abre el dashboard. `status` no pasa a `active`.

### Renovación

`wompi-renew` corre todos los días (pg_cron o cron de Edge Functions):

- Selecciona filas con `status = active`, `cancel_at_period_end = false`, `wompi_payment_source_id` no null, y `access_ends_at` en las próximas 48 horas.
- Cobra `PLAN_PRICE_COP` con esa fuente (`recurrent: true` en tarjeta Visa/Mastercard).
- Éxito: `access_ends_at += 30 días`, actualiza `wompi_transaction_id`, `status = active`.
- Fallo: `status = past_due` y `access_ends_at = max(access_ends_at, ahora + 3 días)`. Aviso al correo.
- Reintento diario mientras `status = past_due` y `ahora <= access_ends_at`.
- Si sigue fallando cuando `ahora > access_ends_at`: `status = expired`. El dashboard se cierra.

### Cancelación

Botón “Cancelar plan” (solo si `status = active`): pone `cancel_at_period_end = true`. El cron no cobra. Al vencer `access_ends_at`, `status = canceled`. `expired` queda solo para fallo de cobro. El usuario puede volver a suscribirse: mismo `wompi-checkout`, nueva fuente si hace falta.

No hay reembolso prorrateado en v1.

## Correo (magic link)

- Desarrollo: SMTP por defecto de Supabase (~2 emails/hora). Suficiente para pruebas.
- Producción: SMTP propio (Resend, 3.000/mes en free). Sin esto el login no escala.
- Botón “Reenviar enlace” si no llega o el link venció (expiración ~1 hora).
- Redirect del magic link: URL de la app Streamlit, listada en Supabase Auth → Redirect URLs.

El cliente `supabase==2.15.1` alcanza para `sign_in_with_otp` y verificar el enlace. Streamlit se queda en `1.31.0`. No se usa `st.login()` nativo.

## Secretos y configuración

Streamlit Cloud secrets (además de `SUPABASE_URL` / `SUPABASE_KEY` actuales):

- `PLAN_PRICE_COP` = `79900` (solo para mostrarlo)
- no guardar la llave privada ni el event secret de Wompi en Streamlit

Supabase Edge Functions:

- `WOMPI_PRIV_KEY`, `WOMPI_PUB_KEY`, `WOMPI_EVENTS_SECRET`
- `PLAN_PRICE_COP=79900`

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

- Regla de acceso: sin fila; trial vigente; trial vencido; active vigente; active vencido; canceled vigente; past_due dentro de gracia; past_due fuera; expired.
- Webhook: firma inválida se rechaza; primer pago crea `active`; el mismo `wompi_transaction_id` no alarga el período otra vez; pago no aprobado no cambia status.
- Renew: no cobra si `cancel_at_period_end`; éxito suma 30 días; fallo pasa a `past_due`; al vencer gracia pasa a `expired`.

Manuales (sandbox Wompi + correo de prueba):

- Magic link ida y vuelta.
- Primer pago tarjeta y Nequi.
- “Ya pagué”.
- Cancelar y comprobar que no hay segundo cobro (simular cron).
- Keepalive: el workflow deja de hacer no-op cuando existe `STREAMLIT_APP_URL`.

## Fuera de alcance (v1)

- Pestañas gratis vs de pago.
- Stripe, Lemon Squeezy, Mercado Pago.
- PSE en el plan automático.
- Cuentas de equipo / varios asientos.
- Mudanza de hosting.
- Reembolsos y prorrateo.
- Cambiar el ETL o las tablas de subastas.
- `st.login()` de Streamlit / Auth0 / Google como único login.
- Precio distinto por feria o por predictor.

## Costo operativo

- Fijo: $0/mes (Streamlit Community Cloud + Supabase Free + GitHub Actions keepalive).
- Variable: comisión Wompi por cobro (~2,9% + $900 COP tarjeta, o ~$900 Nequi).
- A $79.900/mes, neto aprox. $76.700 (tarjeta) o $79.000 (Nequi) por usuario.
- Límite a vigilar: SMTP de Supabase (2 emails/hora) hasta poner Resend. Auth Free aguanta 50.000 MAU.

## Orden de implementación (cuando se apruebe el spec)

1. Activar keepalive (`STREAMLIT_APP_URL`).
2. Tabla `suscripciones` + RLS + función de acceso.
3. Gate en Streamlit (magic link + candado).
4. `wompi-checkout` + webhook + primer pago.
5. Cron de renovación + cancelar + “Ya pagué”.
6. SMTP de producción (Resend) y CTA en `www.subadatos.com`.

El plan detallado de código se escribe después de aprobar este documento, no antes.
