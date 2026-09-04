-- Tabla de acceso de pago. Pegar en Supabase → SQL Editor → Run.
-- Semántica alineada con billing/transitions.py (no cambiar una sin la otra).

create table if not exists public.suscripciones (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null unique references auth.users (id) on delete cascade,
    email text not null,
    status text not null check (status in ('trial', 'active', 'past_due', 'expired', 'canceled')),
    plan text not null check (plan in ('dashboard', 'pro')),
    pending_plan text null check (pending_plan is null or pending_plan in ('dashboard', 'pro')),
    trial_ends_at timestamptz not null,
    access_ends_at timestamptz not null,
    wompi_payment_source_id text null,
    wompi_transaction_id text null,
    cancel_at_period_end boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists suscripciones_wompi_tx_uidx
    on public.suscripciones (wompi_transaction_id)
    where wompi_transaction_id is not null;

create or replace function public.set_suscripciones_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_suscripciones_updated_at on public.suscripciones;
create trigger trg_suscripciones_updated_at
    before update on public.suscripciones
    for each row execute function public.set_suscripciones_updated_at();

alter table public.suscripciones enable row level security;

drop policy if exists suscripciones_select_own on public.suscripciones;
create policy suscripciones_select_own
    on public.suscripciones
    for select
    to authenticated
    using (user_id = auth.uid());

revoke all on table public.suscripciones from anon, authenticated;
grant select on table public.suscripciones to authenticated;

-- ── start_trial: primer login, plan Pro, 7 días ─────────────
create or replace function public.start_trial()
returns public.suscripciones
language plpgsql
security definer
set search_path = public
as $$
declare
    fila public.suscripciones;
    uid uuid := auth.uid();
    mail text;
begin
    if uid is null then
        raise exception 'no autenticado';
    end if;

    select s.* into fila from public.suscripciones s where s.user_id = uid;
    if found then
        return fila;
    end if;

    select u.email into mail from auth.users u where u.id = uid;
    mail := coalesce(mail, '');

    insert into public.suscripciones (
        user_id, email, status, plan, pending_plan,
        trial_ends_at, access_ends_at
    ) values (
        uid, mail, 'trial', 'pro', null,
        now() + interval '7 days', now() + interval '7 days'
    )
    returning * into fila;

    return fila;
end;
$$;

-- ── apply_wompi_payment: = billing.aplicar_pago_aprobado ─────
create or replace function public.apply_wompi_payment(
    p_user_id uuid,
    p_plan text,
    p_transaction_id text,
    p_payment_source_id text
)
returns public.suscripciones
language plpgsql
security definer
set search_path = public
as $$
declare
    fila public.suscripciones;
begin
    if p_plan not in ('dashboard', 'pro') then
        raise exception 'plan desconocido: %', p_plan;
    end if;
    if p_transaction_id is null or p_payment_source_id is null then
        raise exception 'faltan ids de Wompi';
    end if;

    select s.* into fila from public.suscripciones s where s.user_id = p_user_id;
    if not found then
        raise exception 'no hay suscripción';
    end if;

    if fila.wompi_transaction_id is not distinct from p_transaction_id then
        return fila;
    end if;

    update public.suscripciones
    set
        status = 'active',
        plan = p_plan,
        pending_plan = null,
        access_ends_at = now() + interval '30 days',
        wompi_transaction_id = p_transaction_id,
        wompi_payment_source_id = p_payment_source_id,
        cancel_at_period_end = false
    where user_id = p_user_id
    returning * into fila;

    return fila;
end;
$$;

-- ── cancelar / bajar (usuario autenticado, solo su fila) ─────
create or replace function public.set_cancel_at_period_end()
returns public.suscripciones
language plpgsql
security definer
set search_path = public
as $$
declare
    fila public.suscripciones;
    uid uuid := auth.uid();
begin
    if uid is null then
        raise exception 'no autenticado';
    end if;

    select s.* into fila from public.suscripciones s where s.user_id = uid;
    if not found then
        raise exception 'no hay suscripción';
    end if;
    if fila.status <> 'active' then
        raise exception 'solo se cancela un plan active';
    end if;

    update public.suscripciones
    set cancel_at_period_end = true
    where user_id = uid
    returning * into fila;

    return fila;
end;
$$;

create or replace function public.set_pending_plan_dashboard()
returns public.suscripciones
language plpgsql
security definer
set search_path = public
as $$
declare
    fila public.suscripciones;
    uid uuid := auth.uid();
begin
    if uid is null then
        raise exception 'no autenticado';
    end if;

    select s.* into fila from public.suscripciones s where s.user_id = uid;
    if not found then
        raise exception 'no hay suscripción';
    end if;
    if fila.plan <> 'pro' then
        raise exception 'solo se baja desde Pro';
    end if;

    update public.suscripciones
    set pending_plan = 'dashboard'
    where user_id = uid
    returning * into fila;

    return fila;
end;
$$;

-- ── renovación: listar + aplicar resultado (service_role) ────
create or replace function public.list_renewals_due()
returns setof public.suscripciones
language sql
security definer
set search_path = public
as $$
    select s.*
    from public.suscripciones s
    where s.wompi_payment_source_id is not null
      and s.cancel_at_period_end = false
      and (
            (s.status = 'active' and s.access_ends_at <= now() + interval '48 hours')
         or (s.status = 'past_due' and now() <= s.access_ends_at)
      );
$$;

create or replace function public.apply_renewal_result(
    p_user_id uuid,
    p_ok boolean,
    p_transaction_id text
)
returns public.suscripciones
language plpgsql
security definer
set search_path = public
as $$
declare
    fila public.suscripciones;
begin
    select s.* into fila from public.suscripciones s where s.user_id = p_user_id;
    if not found then
        raise exception 'no hay suscripción';
    end if;

    if p_ok then
        if fila.cancel_at_period_end then
            raise exception 'suscripción cancelada; no se renueva';
        end if;
        if fila.wompi_transaction_id is not distinct from p_transaction_id then
            return fila;
        end if;
        if p_transaction_id is null then
            raise exception 'faltan ids de Wompi';
        end if;

        update public.suscripciones
        set
            plan = coalesce(pending_plan, plan),
            pending_plan = null,
            status = 'active',
            access_ends_at = access_ends_at + interval '30 days',
            wompi_transaction_id = p_transaction_id
        where user_id = p_user_id
        returning * into fila;
        return fila;
    end if;

    update public.suscripciones
    set
        status = 'past_due',
        access_ends_at = greatest(access_ends_at, now() + interval '3 days')
    where user_id = p_user_id
    returning * into fila;

    if now() > fila.access_ends_at then
        update public.suscripciones
        set status = 'expired'
        where user_id = p_user_id
        returning * into fila;
    end if;

    return fila;
end;
$$;

revoke all on function public.start_trial() from public, anon;
grant execute on function public.start_trial() to authenticated;

revoke all on function public.set_cancel_at_period_end() from public, anon;
grant execute on function public.set_cancel_at_period_end() to authenticated;

revoke all on function public.set_pending_plan_dashboard() from public, anon;
grant execute on function public.set_pending_plan_dashboard() to authenticated;

revoke all on function public.apply_wompi_payment(uuid, text, text, text) from public, anon, authenticated;
revoke all on function public.list_renewals_due() from public, anon, authenticated;
revoke all on function public.apply_renewal_result(uuid, boolean, text) from public, anon, authenticated;
-- service_role se salta RLS y ejecuta por defecto; no hace falta grant extra.
