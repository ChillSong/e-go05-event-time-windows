#!/usr/bin/env bash
# Corrida completa desde cero, con un solo comando:
#
#   scripts/run_all.sh
#
# 1. entorno Python (uv, Python 3.13, numpy 2.4.4 + duckdb)
# 2. dataset (generate_events_go05.py) si no existe
# 3. verdad batch en DuckDB
# 4. Postgres (docker compose)
# 5. build del procesador
# 6. las tres configuraciones + control 4 h + experimento ingenuo
# 7. comparacion ventana por ventana contra la verdad
# 8. idempotencia: repetir una corrida deja el mismo hash
# 9. caida a mitad de corrida (scripts/crash_test.sh)
#
# Requisitos: Go 1.26+, Docker, uv. Termina con codigo != 0 si algo no cuadra.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=.venv/bin/python
DATA=data/eventos_tardios.jsonl

step() { printf '\n==> %s\n' "$*"; }

step "1/9 entorno Python"
if [ ! -x "$PY" ]; then
  uv venv .venv --python 3.13
fi
uv pip install --quiet --python "$PY" numpy==2.4.4 duckdb==2.0.0

step "2/9 dataset"
if [ ! -s "$DATA" ]; then
  mkdir -p data
  (cd data && "../$PY" ../generate_events_go05.py)
else
  echo "ya existe $DATA (borralo para regenerarlo)"
fi

step "3/9 verdad batch (DuckDB)"
"$PY" verdad/truth.py

step "4/9 Postgres"
docker compose up -d --wait

step "5/9 build"
go build -o processor ./cmd/processor

step "6/9 configuraciones"
for lat in 10s 2m 30m; do
  ./processor --lateness "$lat" --reset-run 2>/dev/null
done
./processor --lateness 4h --run-id control-4h --reset-run 2>/dev/null
./processor --lateness 2m --naive-watermark --run-id naive-2m --reset-run 2>/dev/null

step "7/9 comparacion contra la verdad"
"$PY" verdad/compare.py

step "8/9 idempotencia"
hash() {
  docker exec e-go05-postgres psql -U go05 -d go05 -tAc "
    SELECT (SELECT md5(string_agg(run_id||window_start::text||country||events||gmv_usd||revision||status||published_at::text, '|'
                       ORDER BY run_id, window_start, country)) || ' ' || count(*) FROM window_results)
      || ' / ' ||
           (SELECT md5(string_agg(run_id||event_id||reason||amount_usd, '|' ORDER BY run_id, event_id)) || ' ' || count(*)
              FROM rejected_events)"
}
before=$(hash)
./processor --lateness 2m >/dev/null 2>&1   # misma corrida otra vez, sin borrar nada
after=$(hash)
echo "antes:   $before"
echo "despues: $after"
if [ "$before" != "$after" ]; then
  echo "FALLA: repetir la corrida cambio las tablas" >&2
  exit 1
fi
echo "OK: mismo hash, mismas filas"

step "9/9 caida a mitad de corrida"
scripts/crash_test.sh

printf '\nTODO OK\n'
