"""
Comparacion ventana por ventana: resultados del procesador (Postgres) vs. verdad batch.

Para cada configuracion (run_id en run_metrics) une las ventanas finales con la
verdad por (window_start, country) con un FULL OUTER JOIN — una celda que falte de
cualquier lado cuenta como error del 100% — y calcula las cuatro metricas de la
seccion 6 del enunciado:

  1. eventos y GMV descartados por tardios (cantidad y % del GMV total real)
  2. error porcentual medio y maximo del GMV por ventana finalizada
  3. demora de finalizacion p50 / p95 (tiempo de procesamiento entre fin y version final)
  4. estado maximo en memoria (ventanas abiertas a la vez)

Ademas verifica la identidad contable que prueba que no se pierde ni un centavo:

  GMV verdad = GMV en ventanas finales + GMV descartado por tardio

Uso:   .venv/bin/python verdad/compare.py
Salida: tabla en pantalla + verdad/comparison.csv
"""

import csv
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
TRUTH = HERE / "truth_windows.csv"
OUT = HERE / "comparison.csv"
PG = "host=127.0.0.1 port=5436 dbname=go05 user=go05 password=go05"

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")
con.execute("SET TimeZone = 'UTC'")

con.execute(f"""
CREATE TABLE truth AS
SELECT CAST(window_start AS TIMESTAMPTZ) AS window_start, country,
       events, CAST(gmv_usd AS DECIMAL(18,2)) AS gmv_usd
FROM read_csv('{TRUTH}', types = {{'window_start': 'VARCHAR', 'gmv_usd': 'VARCHAR'}})
""")
truth_total, truth_cells = con.execute("SELECT sum(gmv_usd), count(*) FROM truth").fetchone()

runs = con.execute("""
SELECT run_id, allowed_lateness_s, late_events, late_gmv_usd, quarantined, quarantined_gmv_usd,
       finalize_p50_s, finalize_p95_s, finalize_max_s, max_open_windows, max_open_cells,
       corrections, publications, finalized_windows
FROM pg.public.run_metrics WHERE run_id NOT LIKE 'naive%' ORDER BY allowed_lateness_s
""").fetchall()
naive = con.execute("""
SELECT run_id, allowed_lateness_s, lines, late_events, late_gmv_usd, finalize_p50_s
FROM pg.public.run_metrics WHERE run_id LIKE 'naive%' ORDER BY run_id
""").fetchall()

rows = []
for (run, lat_s, late_n, late_gmv, q_n, q_gmv, p50, p95, pmax, maxw, maxc,
     corr, pubs, fin) in runs:
    cmp = con.execute("""
    WITH res AS (
        SELECT window_start, country, gmv_usd, status, revision
        FROM pg.public.window_results WHERE run_id = ?
    ), j AS (
        SELECT coalesce(t.window_start, r.window_start) AS ws,
               coalesce(t.country, r.country)           AS country,
               coalesce(t.gmv_usd, 0)                   AS truth_gmv,
               coalesce(r.gmv_usd, 0)                   AS got_gmv,
               r.status, r.revision
        FROM truth t FULL OUTER JOIN res r
          ON t.window_start = r.window_start AND t.country = r.country
    )
    SELECT
        count(*)                                                              AS cells,
        count(*) FILTER (WHERE status = 'final')                              AS final_cells,
        avg(CASE WHEN truth_gmv = 0 THEN 100 ELSE abs(got_gmv - truth_gmv) / truth_gmv * 100 END) AS err_mean,
        max(CASE WHEN truth_gmv = 0 THEN 100 ELSE abs(got_gmv - truth_gmv) / truth_gmv * 100 END) AS err_max,
        count(*) FILTER (WHERE got_gmv = truth_gmv)                           AS exact_cells,
        sum(got_gmv)                                                          AS got_total,
        avg(revision)                                                         AS avg_rev,
        max(revision)                                                         AS max_rev
    FROM j
    """, [run]).fetchone()
    cells, final_cells, err_mean, err_max, exact, got_total, avg_rev, max_rev = cmp

    rej_late = con.execute("""
        SELECT count(*), coalesce(sum(amount_usd), 0) FROM pg.public.rejected_events
        WHERE run_id = ? AND reason = 'too_late'""", [run]).fetchone()

    identity_ok = (got_total + late_gmv == truth_total) and (rej_late[1] == late_gmv)
    rows.append({
        "run_id": run,
        "lateness_s": lat_s,
        "late_events": late_n,
        "late_gmv_usd": float(late_gmv),
        "late_gmv_pct": float(late_gmv) / float(truth_total) * 100,
        "err_mean_pct": float(err_mean),
        "err_max_pct": float(err_max),
        "exact_cells": exact,
        "cells": cells,
        "final_cells": final_cells,
        "finalize_p50_s": p50,
        "finalize_p95_s": p95,
        "finalize_max_s": pmax,
        "max_open_windows": maxw,
        "max_open_cells": maxc,
        "corrections": corr,
        "avg_revision": float(avg_rev),
        "max_revision": max_rev,
        "quarantined": q_n,
        "quarantined_gmv_usd": float(q_gmv),
        "identity_ok": identity_ok,
    })

if rows:
    with open(OUT, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        # 6 decimales: el promedio en punto flotante de DuckDB puede variar en el
        # ultimo digito segun el orden de suma; redondeado, el CSV es estable.
        w.writerows({k: round(v, 6) if isinstance(v, float) else v for k, v in r.items()} for r in rows)


def fmt_s(s):
    return f"{s:.1f} s" if s < 120 else f"{s / 60:.1f} min"


print(f"Verdad: {truth_cells} celdas (72 ventanas x 5 paises), GMV USD {truth_total:,.2f}\n")
hdr = f"{'config':<9}{'descartado por tardio':>32}{'error ventana (medio / max)':>32}{'demora final p50 / p95':>26}{'ventanas abiertas max':>24}"
print(hdr)
print("-" * len(hdr))
for r in rows:
    late = f"{r['late_events']:,} ev / USD {r['late_gmv_usd']:,.0f} ({r['late_gmv_pct']:.2f}%)"
    err = f"{r['err_mean_pct']:.3f}% / {r['err_max_pct']:.3f}%"
    dly = f"{fmt_s(r['finalize_p50_s'])} / {fmt_s(r['finalize_p95_s'])}"
    print(f"{r['run_id']:<9}{late:>32}{err:>32}{dly:>26}{r['max_open_windows']:>12} ({r['max_open_cells']} celdas)")
print()
for r in rows:
    print(f"{r['run_id']}: celdas {r['cells']} (finales {r['final_cells']}), exactas {r['exact_cells']}, "
          f"correcciones {r['corrections']:,}, revision prom {r['avg_revision']:.1f} max {r['max_revision']}, "
          f"cuarentena {r['quarantined']} (USD {r['quarantined_gmv_usd']:,.2f}), "
          f"identidad contable {'OK' if r['identity_ok'] else 'FALLA'}")
for run, lat_s, lines, late_n, late_gmv, p50 in naive:
    print(f"\nEXPERIMENTO {run} (watermark = max event_time visto, sin guardas, lateness {fmt_s(lat_s)}):")
    print(f"  descartados como tardios: {late_n:,} de {lines:,} eventos ({late_n / lines:.2%}), "
          f"USD {late_gmv:,.2f}; demora de finalizacion p50 {p50 / 60:.1f} min (negativa: ventanas cerradas antes de terminar)")
print(f"\nEscrito: {OUT}")
