"""
monitor_frescura.py
===================
Vigila que las tablas de subastas en Supabase tengan datos recientes.
Si alguna fuente supera su umbral de antigüedad, envía una alerta por
Telegram (usa las credenciales del bot definidas en /home/julio/ganadero/.env).

Pensado para cron diario. Solo envía mensaje cuando hay un problema,
así que en operación normal es silencioso.

Contexto: en julio-agosto de 2026 la ingesta de Central Ganadera estuvo
6 semanas congelada sin que nadie lo notara (GitHub Actions se desactivó
por inactividad y el cron local no tenía venv). Este monitor existe para
que eso no vuelva a pasar desapercibido.
"""

import os
import sys
from datetime import date, datetime

import requests
from dotenv import load_dotenv
from supabase import create_client

_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"))                # SUPABASE_URL / SUPABASE_KEY
load_dotenv(os.path.join("/home/julio/ganadero", ".env"))  # TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

# (tabla, columna de fecha, umbral en días, nombre, filtro opcional {col: valor})
FUENTES = [
    ("subastas", "fecha_subasta", 10, "Central Ganadera (Medellín)", None),
    ("subastas_casanare", "fecha_subasta", 10, "SubaCasanare", None),
    ("subastar_precios_resumen", "fecha_evento", 10, "Subastar", {"fuente": "subastar"}),
    ("subastar_precios_resumen", "fecha_evento", 10, "Cogasucre (Sincelejo)", {"fuente": "cogasucre"}),
    ("subastar_precios_resumen", "fecha_evento", 10, "Cencogán", {"fuente": "cencogan"}),
    ("subastar_precios_resumen", "fecha_evento", 10, "Asosubastas", {"fuente": "asosubastas"}),
    # Sugaberrío publica con retraso en su web; umbral más holgado
    ("subastar_precios_resumen", "fecha_evento", 45, "Sugaberrío", {"fuente": "sugaberrio"}),
]


def enviar_telegram(mensaje: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("GANADERO_TELEGRAM_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("⚠️  Sin credenciales de Telegram; alerta solo en log.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"},
            timeout=15,
        ).raise_for_status()
        print("📨 Alerta enviada por Telegram.")
    except Exception as e:
        print(f"❌ No se pudo enviar la alerta por Telegram: {e}")


def main() -> int:
    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    hoy = date.today()
    problemas: list[str] = []

    print(f"🔎 Chequeo de frescura — {datetime.utcnow():%Y-%m-%d %H:%M UTC}")
    for tabla, col_fecha, umbral, nombre, filtro in FUENTES:
        try:
            query = supabase.table(tabla).select(col_fecha)
            if filtro:
                for col, valor in filtro.items():
                    query = query.eq(col, valor)
            res = query.order(col_fecha, desc=True).limit(1).execute()
            if not res.data:
                problemas.append(f"• <b>{nombre}</b>: la tabla <code>{tabla}</code> está vacía.")
                print(f"  ❌ {nombre}: tabla vacía")
                continue
            ultima = date.fromisoformat(str(res.data[0][col_fecha])[:10])
            dias = (hoy - ultima).days
            estado = "✅" if dias <= umbral else "❌"
            print(f"  {estado} {nombre}: último dato {ultima} ({dias} días)")
            if dias > umbral:
                problemas.append(
                    f"• <b>{nombre}</b>: último dato del <b>{ultima}</b> "
                    f"(hace {dias} días, umbral {umbral})."
                )
        except Exception as e:
            problemas.append(f"• <b>{nombre}</b>: error consultando <code>{tabla}</code>: {e}")
            print(f"  ❌ {nombre}: error {e}")

    if problemas:
        enviar_telegram(
            "🚨 <b>SubaDatos — datos desactualizados</b>\n\n"
            + "\n".join(problemas)
            + "\n\nRevisar cron del VPS (<code>logs/etl_cron.log</code>) "
            "y GitHub Actions (pueden desactivarse tras 60 días sin commits)."
        )
        return 1

    print("✅ Todas las fuentes están al día.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
