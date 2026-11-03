"""
Verdad batch de referencia (E-Go05).

Calcula el GMV y el conteo por ventana de 5 minutos [inicio, fin) y pais con
DuckDB, leyendo el archivo COMPLETO de una vez y agrupando por event_time. No
comparte codigo con el procesador Go: otro lenguaje, otro motor, y sin watermark
ni lateness — es lo que "deberia" haber pasado si se esperara a todos los
rezagados para siempre.

Reloj sano = event_time <= arrival_time. En este dataset un evento real nunca
llega antes de ocurrir, asi que esa condicion separa exactamente los relojes rotos
(que traen event_time 10 min a 2 h en el futuro). Es una regla distinta a la del
procesador (tolerancia de 1 min) a proposito: si ambas aislan el mismo conjunto, es
evidencia de que la tolerancia elegida no deja pasar ni rechaza de mas.

Tambien calcula la curva de completitud: que fraccion del GMV real de cada celda
(ventana x pais) ya habia llegado cuando el reloj de procesamiento marcaba
fin_de_ventana + x. Es lo que responde "cuanto confiar en el numero antes de que
la ventana sea final", medido sobre los datos crudos y sin el procesador.

Uso:   .venv/bin/python verdad/truth.py [data/eventos_tardios.jsonl]
Salida: verdad/truth_windows.csv  (window_start, country, events, gmv_usd)
        verdad/completeness.csv   (offset, gmv_pct_total, gmv_pct_peor_celda)
"""

import sys
from pathlib import Path

import duckdb

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/eventos_tardios.jsonl"
OUT = Path(__file__).parent / "truth_windows.csv"
COMPLETENESS = Path(__file__).parent / "completeness.csv"

con = duckdb.connect()
con.execute(f"""
CREATE TABLE ev AS
SELECT
    country,
    CAST(amount_usd AS DECIMAL(18,2))                     AS amount,
    epoch_ms(CAST(replace(event_time,   'Z', '') AS TIMESTAMP)) AS event_ms,
    epoch_ms(CAST(replace(arrival_time, 'Z', '') AS TIMESTAMP)) AS arrival_ms
FROM read_json('{SRC}', format = 'newline_delimited', columns = {{
    'event_id': 'VARCHAR', 'event_type': 'VARCHAR', 'country': 'VARCHAR',
    'amount_usd': 'DOUBLE', 'event_time': 'VARCHAR', 'arrival_time': 'VARCHAR'
}})
""")

# Ventana [inicio, fin): inicio = floor(event_ms / 300000) * 300000.
con.execute(f"""
COPY (
    SELECT
        strftime(make_timestamp((event_ms // 300000) * 300000 * 1000), '%Y-%m-%dT%H:%M:%SZ') AS window_start,
        country,
        count(*)            AS events,
        sum(amount)         AS gmv_usd
    FROM ev
    WHERE event_ms <= arrival_ms
    GROUP BY 1, 2
    ORDER BY 1, 2
) TO '{OUT}' (HEADER)
""")

tot = con.execute("""
SELECT
    count(*) FILTER (WHERE event_ms <= arrival_ms)                    AS sanos,
    count(*) FILTER (WHERE event_ms >  arrival_ms)                    AS rotos,
    sum(amount) FILTER (WHERE event_ms <= arrival_ms)                 AS gmv_sano,
    sum(amount) FILTER (WHERE event_ms >  arrival_ms)                 AS gmv_roto,
    min((event_ms - arrival_ms) / 60000.0) FILTER (WHERE event_ms > arrival_ms) AS min_adelanto_min,
    max((event_ms - arrival_ms) / 1000.0)  FILTER (WHERE event_ms <= arrival_ms) AS max_adelanto_sano_s
FROM ev
""").fetchone()
cells, windows = con.execute(f"SELECT count(*), count(DISTINCT window_start) FROM read_csv('{OUT}')").fetchone()

print(f"Verdad escrita en {OUT}")
print(f"  eventos con reloj sano: {tot[0]:,}   GMV USD {tot[2]:,.2f}")
print(f"  relojes rotos:          {tot[1]:,}   GMV USD {tot[3]:,.2f}")
print(f"  adelanto minimo de un reloj roto: {tot[4]:.2f} min  (ningun reloj sano va adelantado: max {tot[5]:.3f} s)")
print(f"  ventanas: {windows}  celdas (ventana x pais): {cells}")
for c, n, g in con.execute("SELECT country, count(*), sum(amount) FROM ev WHERE event_ms <= arrival_ms GROUP BY 1 ORDER BY 3 DESC").fetchall():
    print(f"  {c}: {n:,} eventos, USD {g:,.2f}")

# Curva de completitud: % del GMV de la celda con arrival < fin + offset.
offsets = [("al cierre (+0 s)", 0), ("+10 s", 10_000), ("+30 s", 30_000), ("+2 min", 120_000),
           ("+5 min", 300_000), ("+10 min", 600_000), ("+30 min", 1_800_000), ("+1 h", 3_600_000),
           ("+3 h", 10_800_000)]
con.execute("CREATE TABLE off(label VARCHAR, ms BIGINT, ord INT)")
con.executemany("INSERT INTO off VALUES (?, ?, ?)", [(l, m, i) for i, (l, m) in enumerate(offsets)])
con.execute("""
CREATE TABLE cell AS
SELECT (event_ms // 300000) * 300000 + 300000 AS window_end, country, amount, arrival_ms
FROM ev WHERE event_ms <= arrival_ms
""")
curve = con.execute("""
WITH per_cell AS (
    SELECT o.label, o.ord, c.window_end, c.country,
           sum(c.amount) FILTER (WHERE c.arrival_ms < c.window_end + o.ms) AS arrived,
           sum(c.amount) AS total
    FROM cell c CROSS JOIN off o
    GROUP BY 1, 2, 3, 4
)
SELECT label, ord,
       sum(coalesce(arrived, 0)) / sum(total) * 100        AS pct_total,
       min(coalesce(arrived, 0) / total) * 100             AS pct_worst_cell
FROM per_cell GROUP BY 1, 2 ORDER BY ord
""").fetchall()
with open(COMPLETENESS, "w") as fh:
    fh.write("offset,gmv_pct_total,gmv_pct_peor_celda\n")
    for label, _, tot, worst in curve:
        fh.write(f"{label},{tot:.3f},{worst:.3f}\n")
print("\nCompletitud del GMV por celda segun cuanto despues del fin de ventana se mira:")
print(f"  {'momento':<18}{'GMV total llegado':>20}{'peor celda':>14}")
for label, _, tot, worst in curve:
    print(f"  {label:<18}{tot:>19.2f}%{worst:>13.2f}%")
