"""
E-Go05 — Generador del log de eventos con llegadas tardias
============================================================

Simula 6 horas de confirmaciones de pago de un Hot Sale en un marketplace
LATAM. El archivo esta en ORDEN DE LLEGADA (arrival_time), no en orden de
ocurrencia (event_time). Eso es lo que hace dificil el ejercicio: la mayoria
de los eventos llega casi al instante, pero una minoria llega minutos u horas
despues de haber ocurrido (apps moviles sin conexion, reintentos de gateway).

Ademas, ~0.1% de los eventos trae un event_time en el FUTURO respecto a su
arrival_time (relojes de dispositivo mal configurados). El archivo no te dice
cuales son.

Determinismo: todo sale de un generador con semilla fija, incluidos los ids,
asi que tu archivo es IDENTICO byte a byte al de tus companeros siempre que
uses la misma version de numpy.

Requisito: numpy==2.4.4  (pip install numpy==2.4.4 --break-system-packages)
Uso: python3 generate_events_go05.py
Salida: eventos_tardios.jsonl (~2,000,000 lineas, ~370 MB, tarda ~15 s)
"""

import os
import numpy as np

NUMPY_VERSION_REQUIRED = "2.4.4"
assert np.__version__ == NUMPY_VERSION_REQUIRED, (
    f"numpy {np.__version__} detectado, se requiere {NUMPY_VERSION_REQUIRED} "
    "para que el archivo sea identico al de tus companeros."
)

SEED = 20261102
N = int(os.environ.get("GO05_N", 2_000_000))   # solo para pruebas internas
OUT = os.environ.get("GO05_OUT", "eventos_tardios.jsonl")
HORIZON_S = 6 * 3600
START = np.datetime64("2026-11-14T10:00:00", "ms")  # UTC

COUNTRIES = np.array(["MX", "AR", "CO", "CL", "PE"])
COUNTRY_P = [0.35, 0.20, 0.20, 0.15, 0.10]

rng = np.random.default_rng(seed=SEED)

# 1) Cuando ocurre realmente cada evento
event_s = rng.uniform(0, HORIZON_S, N)

# 2) Cuanto tarda en llegar (mezcla de cuatro poblaciones)
u = rng.random(N)
lateness = np.empty(N)
b1 = u < 0.92
b2 = (u >= 0.92) & (u < 0.98)
b3 = (u >= 0.98) & (u < 0.997)
b4 = u >= 0.997
lateness[b1] = rng.exponential(2.0, b1.sum())          # casi instantaneo
lateness[b2] = rng.uniform(10, 120, b2.sum())          # 10 s a 2 min
lateness[b3] = rng.uniform(120, 1800, b3.sum())        # 2 a 30 min
lateness[b4] = rng.uniform(1800, 10800, b4.sum())      # 30 min a 3 h
arrival_s = event_s + lateness

# 3) Relojes rotos: el event_time REPORTADO queda en el futuro
bad_clock = rng.random(N) < 0.001
future_offset = rng.uniform(600, 7200, N)
reported_s = np.where(bad_clock, arrival_s + future_offset, event_s)

# 4) Resto de campos
country_idx = rng.choice(len(COUNTRIES), size=N, p=COUNTRY_P)
amount = np.clip(rng.lognormal(mean=3.8, sigma=0.8, size=N), 8, 900).round(2)
ids = rng.integers(0, 2**63 - 1, size=N, dtype=np.int64)

# 5) Orden de llegada
order = np.argsort(arrival_s, kind="stable")

event_ms = np.rint(reported_s[order] * 1000).astype(np.int64)
arrival_ms = np.rint(arrival_s[order] * 1000).astype(np.int64)
event_str = (START + event_ms.astype("timedelta64[ms]")).astype(str)
arrival_str = (START + arrival_ms.astype("timedelta64[ms]")).astype(str)
country_o = COUNTRIES[country_idx[order]]
amount_o = amount[order]
ids_o = ids[order]

with open(OUT, "w") as f:
    for i in range(N):
        f.write(
            f'{{"event_id":"ev_{ids_o[i]:016x}","event_type":"payment_confirmed",'
            f'"country":"{country_o[i]}","amount_usd":{amount_o[i]:.2f},'
            f'"event_time":"{event_str[i]}Z","arrival_time":"{arrival_str[i]}Z"}}\n'
        )

# 6) Resumen para que verifiques tu propio trabajo
valid = ~bad_clock[order]
lat_o = lateness[order]
print(f"Listo: {OUT}")
print(f"Eventos totales: {N:,}")
print(f"Relojes rotos (event_time en el futuro): {int((~valid).sum()):,}")
print("Llegadas tardias entre los eventos con reloj sano:")
for label, lo in [("> 10 s", 10), ("> 2 min", 120), ("> 30 min", 1800)]:
    n_late = int(((lat_o > lo) & valid).sum())
    print(f"  {label}: {n_late:,} ({n_late / valid.sum():.2%})")
print("GMV y conteo por pais (solo eventos con reloj sano):")
for k, c in enumerate(COUNTRIES):
    m = valid & (country_o == c)
    print(f"  {c}: {int(m.sum()):,} eventos, USD {amount_o[m].sum():,.2f}")
print(f"GMV total (reloj sano): USD {amount_o[valid].sum():,.2f}")
