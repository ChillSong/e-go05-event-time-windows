# E-Go05 — Ventanas por tiempo de evento y datos tardios

Procesador en Go que agrupa el GMV del Hot Sale por pais en ventanas de 5 minutos
`[inicio, fin)` segun **tiempo de evento**, con un watermark que los relojes rotos no
pueden adelantar, correcciones de ventanas ya publicadas dentro de una lateness
permitida, y descarte explicito de lo demasiado tardio. Escribe a Postgres con upsert
idempotente y se compara ventana por ventana contra una verdad batch independiente en
DuckDB.

Sin broker: el archivo esta en orden de llegada y `arrival_time` es el reloj de
procesamiento, inyectado como interfaz.

## Resultado

| Lateness | GMV descartado | Error por ventana (medio / max) | Demora final (p50 / p95) | Ventanas abiertas max |
|---|---|---|---|---|
| 10 s | 3.00% | 2.99% / 3.95% | 10.2 s / 10.4 s | 2 |
| 2 min | 1.84% | 1.84% / 2.71% | 2.0 min / 2.0 min | 2 |
| 30 min | 0.29% | 0.29% / 0.67% | 30.0 min / 31.0 min | 8 |
| control 4 h | 0.00% | **0.000% / 0.000%** (360/360 celdas exactas) | 240 min | 50 |

Cuarentena: 1,821 relojes rotos (USD 115,306.98) en todas las corridas. Analisis
completo y recomendacion en `decisions.md`.

## Arquitectura

```
data/eventos_tardios.jsonl  (orden de llegada, 2M lineas)
        │  bufio.Scanner: una linea a la vez
        ▼
  clock.Manual.Set(arrival_time)          <- reloj de procesamiento inyectado
        │
        ▼
  window.Processor.Process(evento)
    1. event_time > ahora + 1 min ?  ──si──> cuarentena (future_event_time)
    2. ventana = floor(event_time, 5 min)        [inicio, fin)
    3. fin + lateness <= W ?          ──si──> descartado (too_late), monto conservado
    4. suma a la celda (ventana, pais); si ya fue emitida -> correccion
    5. W = max( min(maxEventAceptado, ahora), ahora - 1 min )   monotono
    6. W >= fin          -> version provisional (revision 1)
       correcciones      -> nueva revision, a lo sumo 1 vez por segundo
       W >= fin+lateness -> version final, estado liberado
        │
        ▼
  store (Postgres)
    window_results   upsert por (run_id, window_start, country), solo si revision >= la guardada
    rejected_events  cuarentena y tardios, con motivo y monto
    run_metrics      metricas de la corrida
        │
        ▼
  verdad/compare.py  (DuckDB)  ── ventana por ventana ──  verdad/truth.py (DuckDB, batch)
```

## Requisitos

- Go 1.26+
- Docker (Postgres)
- [uv](https://docs.astral.sh/uv/) (crea un entorno Python 3.13 con `numpy==2.4.4` para
  el generador y `duckdb` para la verdad y la comparacion)

## Comandos (verificados desde cero)

### 1. Entorno Python y dataset

numpy 2.4.4 requiere Python >= 3.11 (el `python3` del sistema en macOS es 3.9), asi
que el entorno se crea con `uv`:

```bash
uv venv .venv --python 3.13
uv pip install --python .venv/bin/python numpy==2.4.4 duckdb
mkdir -p data && cd data && ../.venv/bin/python ../generate_events_go05.py && cd ..
# ~3 s -> data/eventos_tardios.jsonl (351 MB). Imprime los totales por tramo y por pais.
```

### 2. Verdad batch (independiente del procesador)

```bash
.venv/bin/python verdad/truth.py
# -> verdad/truth_windows.csv (360 celdas) y verdad/completeness.csv
```

### 3. Postgres

```bash
docker compose up -d          # Postgres 16 en 127.0.0.1:5436 (solo localhost)
```

### 4. Correr las configuraciones

```bash
go build -o processor ./cmd/processor
./processor --lateness 10s     # run_id lat-10s
./processor --lateness 2m      # run_id lat-2m
./processor --lateness 30m     # run_id lat-30m
./processor --lateness 4h --run-id control-4h   # control: lateness > atraso maximo
```

Cada corrida tarda ~3 s.

### 5. Comparar contra la verdad

```bash
.venv/bin/python verdad/compare.py
# tabla de la seccion 6 + verdad/comparison.csv
```

### 6. Verificar idempotencia

```bash
hash() { docker exec e-go05-postgres psql -U go05 -d go05 -tAc \
  "SELECT md5(string_agg(run_id||window_start::text||country||events||gmv_usd||revision||status||published_at::text, '|' ORDER BY run_id, window_start, country)) || ' ' || count(*) FROM window_results"; }
hash; ./processor --lateness 2m >/dev/null 2>&1; hash    # mismo hash, mismas filas
```

### Experimento: el fallo del watermark ingenuo

```bash
./processor --lateness 2m --naive-watermark --run-id naive-2m
# W = max(event_time) sin guardas: descarta 99.99% del GMV como "tardio"
```

## Flags del procesador

| Flag | Default | Descripcion |
|---|---|---|
| `--lateness` | `2m` | Lateness permitida despues de que W pasa el fin de la ventana |
| `--clock-tolerance` | `1m` | `event_time > arrival_time + esto` = reloj roto, a cuarentena |
| `--idle` | `1m` | W nunca va mas de esto detras del reloj de procesamiento |
| `--publish-every` | `1s` | Agrupa correcciones: publica a lo sumo cada tanto (tiempo de procesamiento) |
| `--run-id` | `lat-<lateness>` | Identificador de la corrida |
| `--reset-run` | `false` | Borra las filas de la corrida antes de procesar |
| `--naive-watermark` | `false` | Experimento: watermark ingenuo, para medir el fallo |
| `--dsn` | `postgres://go05:go05@localhost:5436/go05` | Postgres |
| `--file` | `data/eventos_tardios.jsonl` | Log de entrada |

## Tests

```bash
go test -race ./...
```

`tests/` tiene 14 tests deterministas con reloj manual inyectado; ninguno llama
`time.Now()`. Cubren: borde de ventana `[inicio, fin)`, emision al pasar el fin,
correccion dentro de la lateness, descarte fuera de ella (y el borde exacto
`W == fin + lateness`), reloj roto en cuarentena sin mover W, el tope de W por el reloj,
liberacion de estado, secuencia de revisiones provisional → correcciones → final, regla
de inactividad, fin de entrada, monotonia del reloj y el fallo del watermark ingenuo.
`cmd/processor` agrega tests del parseo exacto de montos a centavos.

## Estructura

```
cmd/processor/        Entrada: lectura por streaming, reloj de llegada, metricas
internal/window/      Ventanas, watermark, lateness, correcciones, cuarentena
internal/clock/       Interfaz Clock: Real (time.Now) y Manual
internal/store/       Sink Postgres: upsert por revision, rechazados, metricas
tests/                Tests con reloj inyectable
verdad/truth.py       Verdad batch en DuckDB + curva de completitud
verdad/compare.py     Comparacion ventana por ventana contra cada configuracion
docker-compose.yml    Postgres 16 (127.0.0.1:5436)
generate_events_go05.py  Generador del dataset (material del curso)
decisions.md          Watermark, tabla de configuraciones, recomendacion, relojes rotos, cola larga
```
