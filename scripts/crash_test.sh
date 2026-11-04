#!/usr/bin/env bash
# Caida a mitad de corrida: se mata el procesador con kill -9 a la mitad del
# archivo, se vuelve a correr sin borrar nada, y el resultado tiene que ser
# identico al de una corrida limpia (lat-2m).
#
#   scripts/crash_test.sh        (requiere Postgres arriba y lat-2m ya corrida)
#
# Prueba que el upsert monotono por revision no solo tolera corridas repetidas,
# tambien corridas parciales: lo que quedo a medias se sobrescribe con versiones
# de revision mayor o igual, nunca al reves.
set -euo pipefail

cd "$(dirname "$0")/.."
RUN=crash-2m
LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

psqlc() { docker exec e-go05-postgres psql -U go05 -d go05 -tAc "$1"; }

# Hash del contenido de una corrida, sin el run_id (para comparar dos corridas).
content() {
  psqlc "
    SELECT (SELECT coalesce(md5(string_agg(window_start::text||country||events||gmv_usd||revision||status||published_at::text, '|'
                       ORDER BY window_start, country)), '-') || ' ' || count(*) FROM window_results WHERE run_id = '$1')
      || ' / ' ||
           (SELECT coalesce(md5(string_agg(event_id||reason||amount_usd||processed_at::text, '|' ORDER BY event_id)), '-') || ' ' || count(*)
              FROM rejected_events WHERE run_id = '$1')"
}

[ -x ./processor ] || go build -o processor ./cmd/processor
if [ "$(psqlc "SELECT count(*) FROM run_metrics WHERE run_id = 'lat-2m'")" != "1" ]; then
  ./processor --lateness 2m --reset-run 2>/dev/null
fi

# 1. Arranca y se mata con kill -9 al pasar la linea 1,000,000 (mitad del archivo).
./processor --lateness 2m --run-id "$RUN" --reset-run >/dev/null 2>"$LOG" &
pid=$!
until grep -q "1000k lines" "$LOG"; do
  if ! kill -0 "$pid" 2>/dev/null; then echo "FALLA: el procesador termino antes de matarlo" >&2; exit 1; fi
  sleep 0.05
done
kill -9 "$pid"
wait "$pid" 2>/dev/null || true
echo "kill -9 en: $(grep -o '[0-9]*k lines.*' "$LOG" | tail -1)"

partial=$(content "$RUN")
metrics=$(psqlc "SELECT count(*) FROM run_metrics WHERE run_id = '$RUN'")
echo "tras la caida (parcial):  $partial   run_metrics=$metrics"

# 2. Se vuelve a correr sin --reset-run: encuentra las filas a medias.
./processor --lateness 2m --run-id "$RUN" >/dev/null 2>&1
recovered=$(content "$RUN")
reference=$(content lat-2m)
echo "tras volver a correr:     $recovered"
echo "corrida limpia (lat-2m):  $reference"

# 3. Se borra la corrida de prueba para que no aparezca en compare.py.
psqlc "DELETE FROM window_results WHERE run_id = '$RUN';
       DELETE FROM rejected_events WHERE run_id = '$RUN';
       DELETE FROM run_metrics WHERE run_id = '$RUN';" >/dev/null

if [ "$partial" = "$reference" ]; then
  echo "FALLA: la corrida parcial ya era igual a la completa; el kill llego tarde" >&2
  exit 1
fi
if [ "$recovered" != "$reference" ]; then
  echo "FALLA: tras la caida el resultado no es igual al de una corrida limpia" >&2
  exit 1
fi
echo "OK: tras kill -9 y volver a correr, identico a una corrida limpia"
