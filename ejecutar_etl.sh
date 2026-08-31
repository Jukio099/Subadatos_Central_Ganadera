#!/usr/bin/env bash
# Ejecuta el ETL de Central Ganadera con logging robusto.
# Diseñado para cron: registra inicio/fin con timestamp y código de salida,
# y rota el log para que no crezca sin límite.
set -u

DIR="/home/julio/Subadatos_Central_Ganadera"
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/etl_cron.log"
PYTHON="$DIR/venv/bin/python"

mkdir -p "$LOG_DIR"

# Rotación simple: si el log supera 1 MB, conservar solo la mitad final
if [ -f "$LOG" ] && [ "$(stat -c%s "$LOG")" -gt 1048576 ]; then
    tail -c 524288 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

{
    echo ""
    echo "════════════════════════════════════════════════════"
    echo "▶ ETL Central Ganadera — inicio $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

    if [ ! -x "$PYTHON" ]; then
        echo "✗ ERROR CRÍTICO: no existe el intérprete $PYTHON"
        echo "  Recrear con: python3.11 -m venv $DIR/venv && $DIR/venv/bin/pip install -r $DIR/requirements.txt"
        exit 1
    fi

    cd "$DIR" || exit 1
    "$PYTHON" etl/main.py --solo-nuevos
    CODIGO=$?

    echo "▶ Ingesta de subastas nuevas (Cogasucre / Cencogán / Asosubastas / Sugaberrío)..."
    "$PYTHON" etl/fuentes_nuevas.py || echo "⚠️  Falló la ingesta de fuentes nuevas (no bloquea el ETL)"

    # Refrescar el JSON público de precios que consume la landing
    echo "▶ Publicando precios para la landing..."
    "$PYTHON" publicar_precios_landing.py || echo "⚠️  Falló la publicación del JSON de la landing (no bloquea el ETL)"

    echo "■ ETL Central Ganadera — fin $(date -u '+%Y-%m-%d %H:%M:%S UTC') (exit=$CODIGO)"
    exit $CODIGO
} >> "$LOG" 2>&1
