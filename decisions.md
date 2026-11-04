# Decisiones tecnicas — E-Go05: Ventanas por tiempo de evento y datos tardios

Procesador que agrupa GMV por pais en ventanas de 5 minutos `[inicio, fin)` segun
**tiempo de evento**, decide cuando una ventana es final con un watermark que un
reloj roto no puede adelantar, corrige ventanas ya publicadas mientras dure la
lateness permitida y descarta de forma explicita lo que llega demasiado tarde.
Todo numero de este documento sale de una corrida real sobre los 2,000,000 de
eventos, comparada ventana por ventana contra una verdad batch independiente
(DuckDB, en `verdad/`).

**Dos relojes, dos trabajos:**
- `event_time` (lo que dice el dispositivo) decide **en que ventana** cae un evento.
- El tiempo de procesamiento (`arrival_time`, inyectado como `Clock`) decide
  **cuando** el sistema cree que una ventana esta completa.

## Resumen: las cinco preguntas

| Pregunta (seccion 7 del enunciado) | Respuesta corta | Detalle |
|---|---|---|
| Como calculo el watermark y por que un reloj roto no lo adelanta | `W = max(min(maxEventAceptado, ahora), ahora − 1 min)`, monotono. Un reloj roto tiene que pasar dos guardas: la cuarentena **antes** del watermark y el tope `min(..., ahora)`. Sin ellas, un solo reloj roto hace descartar el **99.99%** del GMV. | §1 |
| Tabla de las tres configuraciones | 10 s / 2 min / 30 min dejan fuera **3.00% / 1.84% / 0.29%** del GMV, con error por ventana de 2.99% / 1.84% / 0.29% y demora p50 de 10.2 s / 2.0 min / 30.0 min. El control de 4 h coincide **al centavo** en las 360 celdas. | §2 |
| Que recomiendo y cuanto confiar antes del final | **30 min**: recupera USD 1.9 M frente a 2 min por 6 ventanas mas en memoria. El numero se ve desde el cierre (~4% por debajo), a los 2 min esta dentro del ~2%, y solo se corrige hacia arriba. | §3 |
| Umbral de reloj roto, cuantos y por que cuarentena | `event_time > arrival_time + 1 min`: **1,821 eventos, USD 115,306.98** (0.094%). Cuarentena porque el pago es real: finanzas lo va a conciliar. | §4 |
| Que cambiaria con 30% de cola larga | Ninguna lateness alcanza. Desviaria lo tardio a correcciones con reexpresion batch, separaria el tablero operativo de la cifra contable, estimaria con la curva de completitud y sacaria el estado de memoria. | §5 |

Todo se reproduce desde cero con `scripts/run_all.sh` (~1 min), que ademas verifica
idempotencia y una caida a mitad de corrida (anexos).

---

## 1. Como calculo el watermark y por que un reloj roto no lo puede adelantar

```
W = max( min(maxEventAceptado, ahora), ahora - idle )        idle = 1 min
```

W es monotono: nunca retrocede. Cada termino tiene una razon:

| Termino | Que hace | Por que |
|---|---|---|
| `maxEventAceptado` | El mayor `event_time` **de los eventos aceptados** | El tiempo de evento es el que manda sobre las ventanas |
| `min(..., ahora)` | W nunca supera al reloj de procesamiento | Segunda guarda, independiente de la cuarentena |
| `ahora - idle` | Si el tiempo de evento se estanca, el de procesamiento sigue moviendo W | Sin esto, las ultimas ventanas nunca se finalizan (ver abajo) |

**Un reloj roto tiene que pasar dos guardas, y no pasa ninguna:**
1. **Cuarentena antes del watermark.** Si `event_time > ahora + 1 min`, el evento va
   a cuarentena y **nunca** entra a `maxEventAceptado`.
2. **Tope del reloj de procesamiento.** Aunque un evento adelantado entrara (por
   ejemplo, uno de +30 s, dentro de la tolerancia), `min(..., ahora)` le impide
   llevar W mas alla del presente.

### Evidencia: que pasa sin las guardas

Agregue un modo experimental (`--naive-watermark`, solo para medir el fallo) donde
`W = max(event_time)` de **todos** los eventos, como advierte el enunciado. Misma
lateness de 2 min:

| | Watermark con guardas | Watermark ingenuo |
|---|---|---|
| Eventos descartados como "tardios" | 36,995 (1.84% del GMV) | **1,999,753 (99.99%)** |
| GMV descartado | USD 2,263,747 | **USD 123,119,231** |
| Demora de finalizacion p50 | +2.0 min | **−112 min** |

El primer reloj roto llega en los primeros segundos, lanza W hasta 2 h al futuro y
cierra todas las ventanas reales antes de que existan (por eso la demora sale
negativa). El Hot Sale entero se descarta como "tardio" y aparecen 20 ventanas
falsas despues de las 16:00. Hay un test que fija este fallo
(`TestNaiveWatermarkIsBrokenByOneBadClock`), junto a los que prueban que con las
guardas un reloj roto no mueve W.

### Evidencia: el procesador asigna cada evento a la ventana correcta

Una corrida de control con lateness de **4 h** (mayor que el atraso maximo del
dataset, 3 h) no descarta nada. Si la agrupacion por tiempo de evento, los bordes
`[inicio, fin)` y la cuarentena son correctos, las 360 celdas (72 ventanas × 5
paises) tienen que coincidir **al centavo** con la verdad:

```
control-4h: celdas 360 (finales 360), exactas 360, error medio 0.000%, max 0.000%
```

Asi que todo el error de las otras configuraciones viene de la politica de
lateness, no de un error de asignacion.

### Por que la regla de inactividad (idle)

Los eventos ocurren entre las 10:00 y las 16:00, pero los rezagados siguen
**llegando hasta las ~19:00**. Sin el termino `ahora - idle`, W se queda pegado en
16:00 y la ultima ventana solo se finaliza al terminar el archivo, casi 3 h tarde.
Ademas, de facto, recibiria lateness infinita mientras las demas recibieron 30 min:
una politica inconsistente. Con `idle = 1 min`, las ultimas ventanas cierran con la
misma regla que las demas mas 1 minuto. Por eso el p95 de 30 min sale en 31.0 min:
las 6 ultimas ventanas de 72 (8.3%) cierran por inactividad.

1 minuto es inofensivo durante el flujo normal: llegan ~92 eventos por segundo y el
evento mas fresco tiene ~0 s de atraso, asi que `maxEventAceptado` siempre va por
delante de `ahora - 1 min`. El termino solo actua cuando el tiempo de evento deja
de avanzar.

### Emision

Una ventana se emite (version provisional) en cuanto `W >= fin`, sin margen extra
de desorden. Como W va a ~0.2 s detras del reloj de procesamiento, la primera
version sale casi al instante del cierre. Los eventos de los ultimos segundos de la
ventana, y todos los tardios, entran como correcciones. Elegi esto porque con un
solo parametro (la lateness) el intercambio queda limpio: la emision es inmediata y
la lateness solo decide **cuando se congela** el numero y **cuanto se pierde**.

---

## 2. Las tres configuraciones con las cuatro metricas

Verdad: 360 celdas, GMV real USD 123,018,607.68. Comparacion ventana por ventana con
`FULL OUTER JOIN` (una celda faltante cuenta como error del 100%).

| Lateness | Eventos y GMV descartados por tardios | Error por ventana vs verdad (medio / max) | Demora de finalizacion (p50 / p95) | Estado max en memoria |
|---|---|---|---|---|
| **10 s** | 60,172 ev · USD 3,694,069 · **3.00%** | 2.99% / 3.95% | **10.2 s** / 10.4 s | 2 ventanas (10 celdas) |
| **2 min** | 36,995 ev · USD 2,263,747 · **1.84%** | 1.84% / 2.71% | **2.0 min** / 2.0 min | 2 ventanas (10 celdas) |
| **30 min** | 5,901 ev · USD 355,734 · **0.29%** | 0.29% / 0.67% | **30.0 min** / 31.0 min | 8 ventanas (36 celdas) |
| *control 4 h* | *0 · USD 0 · 0.00%* | *0.000% / 0.000%* | *240 min / 241.5 min* | *50 ventanas (246 celdas)* |

**El intercambio, en una linea:** cada orden de magnitud de espera recupera dinero
(3.00% → 1.84% → 0.29% fuera de la cuenta) a cambio de demora (10 s → 2 min → 30 min)
y estado (2 → 2 → 8 ventanas; 50 con 4 h).

Detalles que respaldan la tabla:
- **Identidad contable verificada en las tres:** `GMV verdad = GMV en ventanas
  finales + GMV descartado`. No se pierde un centavo sin dejar rastro; cada evento
  descartado esta en `rejected_events` con su monto y su motivo.
- **Cruce independiente.** La curva de completitud (seccion 3), calculada desde los
  datos crudos sin el procesador, dice que a +10 s faltaba el 3.02% del GMV, a
  +2 min el 1.84% y a +30 min el 0.29%. Coincide con lo que descarto cada
  configuracion.
- **Por que 10 s descarta solo 3.00% si el 8.59% de los eventos llega con mas de
  10 s de atraso:** la lateness se cuenta desde el **fin de la ventana**, no desde el
  evento. Un pago de las 10:00:30 que llega 60 s tarde (10:01:30) cae en una ventana
  que todavia ni se emitio, asi que entra sin problema. Solo se pierde lo que llega
  despues de `fin + lateness`.
- **Correcciones:** 14,848 (10 s), 38,025 (2 min) y 69,119 (30 min) eventos
  corrigieron ventanas ya publicadas. Revision promedio por celda: 10.6 / 54.4 / 137.9.
  Las correcciones se agrupan y se publican a lo sumo una vez por segundo de tiempo
  de procesamiento.
- **Memoria acotada:** el archivo se lee linea por linea. Heap maximo entre ~5 y
  ~18 MB en todas las corridas, incluida la de 4 h (muestreado cada 200k lineas; varia
  segun cuando corre el GC), para un archivo de 351 MB.

---

## 3. Que configuracion recomiendo y cuanto confiar en el numero antes de que sea final

### Recomendacion: lateness de 30 minutos

El punto clave para operaciones: **la lateness no retrasa el numero que ven.** En las
tres configuraciones la primera version de cada ventana se publica ~0.2 s despues de
su cierre y se sigue corrigiendo en vivo (sube de revision). La lateness solo decide
**cuando se congela** y **cuanto dinero queda fuera para siempre**:

| | 2 min | 30 min |
|---|---|---|
| Primera version visible | al cierre | al cierre |
| Numero congelado (final) | +2 min | +30 min |
| GMV perdido para siempre | USD 2.26 M (1.84%) | **USD 0.36 M (0.29%)** |
| Costo | 2 ventanas en memoria, 19,570 versiones publicadas | 8 ventanas en memoria, 49,658 versiones publicadas |

Pasar de 2 a 30 minutos recupera **USD 1.9 M** del Hot Sale a cambio de 6 ventanas
mas en memoria, que es trivial. 10 segundos solo tendria sentido si alguien necesitara
un numero **congelado** en segundos, y el tablero no lo necesita: tiene la version
provisional desde el cierre.

### Cuanto confiar en el numero antes de que la ventana sea final

Medido sobre los datos crudos (`verdad/completeness.csv`): que fraccion del GMV real de
cada celda ventana × pais ya habia llegado en cada momento.

| Momento despues del cierre | GMV ya llegado (total) | Peor celda |
|---|---|---|
| al cierre (+0 s) | 96.17% | 95.13% |
| +10 s | 96.98% | 96.05% |
| +2 min | 98.16% | 97.29% |
| +10 min | 98.65% | 97.99% |
| **+30 min (final)** | **99.71%** | **99.33%** |
| +3 h | 100.00% | 100.00% |

**Lo que le diria al equipo de operaciones:**
1. **Al cierre de una ventana, el numero esta ~4% por debajo del real** (hasta ~5% en el
   peor pais). Siempre se corrige **hacia arriba**: los tardios solo suman.
2. **A los 2 minutos ya esta dentro del ~2%** (~2.7% en el peor pais). Es un buen
   momento para decidir refuerzos de logistica y soporte.
3. **No comparen paises cuya diferencia sea menor al ~5% al cierre**: esa diferencia
   puede ser solo que los pagos de un pais llegaron mas lento.
4. El tablero muestra `revision` y `status` (`provisional` / `final`) por celda. Un
   numero `final` ya no cambia; uno `provisional` todavia puede subir.

---

## 4. Umbral de reloj roto: 1 minuto

**Regla:** `event_time > arrival_time + 1 min` → cuarentena con motivo `future_event_time`.

| | Valor |
|---|---|
| Eventos en cuarentena | **1,821** (identicos en las 4 corridas y a los que reporta el generador) |
| GMV en cuarentena | **USD 115,306.98** (0.094% del GMV total) |
| Reloj roto menos adelantado del dataset | +10.03 min |
| Reloj sano mas adelantado del dataset | 0.000 s |

**Por que 1 minuto.** Cualquier valor entre 0 y 10 min separa exactamente ambos grupos
en este dataset. Lo verifique en la verdad con una regla distinta (`event_time <=
arrival_time`) y aisla los mismos 1,821. Dentro de ese rango:
- **0 no**: en un sistema real los relojes de los telefonos tienen segundos de desfase.
  Mandar a cuarentena todo evento con +200 ms de adelanto seria castigar ruido normal
  de NTP.
- **10 min no**: queda pegado al reloj roto mas cercano (+10.03 min), sin margen.
- **1 min** tolera el desfase normal y deja un margen de 10x antes del primer reloj
  roto. Ademas, un evento aceptado con +59 s de adelanto **no puede** mover W mas alla
  del presente, por el tope `min(..., ahora)`.

**Por que cuarentena y no descarte silencioso.** Ese dinero es **real**: el pago
ocurrio, lo que esta mal es la hora que reporto el dispositivo.
- USD 115k de un Hot Sale que finanzas va a conciliar contra el procesador de pagos.
  Si se descarta en silencio, la conciliacion no cuadra y nadie sabe por que.
- La cuarentena (`rejected_events`, con `reason`, `event_id`, monto, la hora que
  reporto el dispositivo y `processed_at`) permite reasignarlo despues, por ejemplo
  usando `arrival_time` como mejor estimacion de la hora real.
- Es una senal operativa: 1,821 dispositivos con el reloj mal es un problema de flota
  que alguien deberia mirar.

Igual que el E06 de la fase Python: lo que no entra al agregado se cuenta, se guarda y
se explica.

---

## 5. Que cambiaria si la cola larga fuera de 30% y no de 0.3%

Con este diseno, una cola de 30% con atrasos de 30 min a 3 h significaria que la
configuracion de 30 min **pierde ~30% del GMV**, y que cualquier lateness que la cubra
(≥ 3 h) congela los numeros 3 horas tarde. El control de 4 h ya lo muestra: el estado
pasa de 8 a **50 ventanas abiertas (246 celdas)**, y crece linealmente con la lateness.
No hay un valor de lateness que lo resuelva; habria que cambiar el diseno:

1. **No descartar: desviar.** Hoy lo que llega despues de `fin + lateness` se descarta
   (aunque queda registrado). Con 30% de cola, eso deberia ir a una **salida lateral
   de correcciones tardias**, y un proceso batch periodico (como `verdad/`) las aplicaria
   a las ventanas finales como **reexpresiones** (`revision` + 1, `status = restated`).
   La tabla ya tiene la clave y la revision para soportarlo; `rejected_events` ya
   guarda los montos.
2. **Separar el tablero operativo de la cifra contable.** Para decidir refuerzos en
   minutos, el tablero usaria lateness corta y mostraria explicitamente que esta
   incompleto. La cifra que se reporta como "GMV del Hot Sale" saldria del batch
   nocturno, que si ve todo. Hoy una sola tabla hace los dos trabajos porque la cola es
   0.3%; con 30% ya no alcanzaria.
3. **Estimar en vez de esperar (nowcasting).** Con la curva de completitud historica,
   el tablero puede mostrar `observado / completitud_esperada(t)`. Si al cierre
   historicamente llega 70%, un observado de USD 35k se presenta como ~USD 50k
   estimado, con su banda de error. Es la unica forma de dar un numero util a los
   5 minutos cuando el 30% llega horas despues.
4. **Watermark por percentil, no por maximo.** Con 30% de tardios, `max(event_time)` es
   demasiado optimista. Un watermark heuristico tipo `ahora − p70(atraso)` declara
   abiertamente que espera tardios y los manda al camino de correcciones.
5. **Estado fuera de memoria.** Horas de ventanas abiertas por pais (y mas
   dimensiones en un sistema real) ya no caben en un mapa en memoria. Moveria el estado
   a un store persistente con TTL (Postgres o un key-value embebido); la tabla de
   resultados ya funciona asi, porque el upsert por revision es idempotente.

---

## Anexo: idempotencia y escritura a Postgres

- **Clave** `(run_id, window_start, country)`. Cada version nueva hace upsert y sube
  `revision`.
- El upsert solo aplica si `revision` entrante **>=** la guardada
  (`WHERE window_results.revision <= EXCLUDED.revision`): una corrida repetida o
  atrasada nunca puede devolver una ventana final a una version anterior.
- `rejected_events` usa `ON CONFLICT (run_id, event_id) DO NOTHING`.
- **Verificado:** hash de ambas tablas antes y despues de repetir las tres corridas:

```
ANTES:   window_results 7fc62fe9411a3c4b426a4b26624f26c7 (1080 filas)  rejected_events d92125e92cf670064297b5abf3185c1b (108531 filas)
DESPUES: window_results 7fc62fe9411a3c4b426a4b26624f26c7 (1080 filas)  rejected_events d92125e92cf670064297b5abf3185c1b (108531 filas)
```

Mismas filas, mismos valores, mismas revisiones.

**Caida a mitad de corrida** (`scripts/crash_test.sh`). Repetir una corrida completa
no prueba que una corrida **a medias** se recupere, asi que lo probe aparte: el
procesador se mata con `kill -9` al pasar la linea 1,000,000, se vuelve a correr sin
borrar nada, y el resultado se compara contra la corrida limpia `lat-2m` (hash del
contenido sin el `run_id`):

```
kill -9 en: 1000k lines  W=13:00:37  open windows=2
tras la caida (parcial):  8f3537fb09fa65a30569278c143cf2da 175 / 1913992f8692f69008dad1f2701fafa8 15000   run_metrics=0
tras volver a correr:     caf6eafdcbe5a3fdb582ebf9bea4f511 360 / 85d6b5d6a5904e9acaa7e0cd1311bfea 38816
corrida limpia (lat-2m):  caf6eafdcbe5a3fdb582ebf9bea4f511 360 / 85d6b5d6a5904e9acaa7e0cd1311bfea 38816
```

La caida dejo 175 celdas a medio corregir, 15,000 rechazos y ninguna metrica. Al volver
a correr, cada celda recibe de nuevo sus versiones desde la revision 1. El upsert solo
aplica las de revision mayor o igual a la guardada, asi que lo que quedo a medias se
sobrescribe y nunca al reves. El resultado es identico a una corrida limpia.

## Anexo: el reloj inyectable

`internal/clock` define `Clock { Now() time.Time }` con dos implementaciones: `Real`
(`time.Now`, para una entrada en vivo) y `Manual` (solo avanza cuando se le pide, y nunca
hacia atras). El procesador nunca llama `time.Now()`. En la reproduccion del archivo,
un `Manual` se mueve al `arrival_time` de cada linea; en `tests/`, cada test mueve el
reloj a mano. Ningun test llama `time.Now()`, y todos pasan con `go test -race ./...`.

## Anexo: casos de la rubrica y el test que cubre cada uno

| Caso que pide la rubrica | Test (`tests/window_test.go`) |
|---|---|
| Borde de ventana `[inicio, fin)` | `TestWindowBoundaryIsHalfOpen` |
| Correccion dentro de la lateness | `TestLateEventWithinLatenessCorrectsWindow` |
| Descarte fuera de la lateness (contado, con motivo) | `TestEventBeyondLatenessIsDroppedAndCounted` |
| Borde exacto `W == fin + lateness` | `TestLatenessBoundaryIsExact` |
| Reloj roto en cuarentena, sin efecto sobre el watermark | `TestBrokenClockQuarantinedAndWatermarkUntouched` |
| Evento adelantado dentro de la tolerancia no lleva W mas alla del reloj | `TestEventWithinToleranceCannotPushWatermarkAheadOfClock` |
| Estado liberado al finalizar | `TestFinalizedWindowsReleaseState` |
| Emision al pasar el fin | `TestWindowEmittedWhenWatermarkPassesEnd` |
| Secuencia provisional → correcciones → final | `TestRevisionSequenceProvisionalCorrectionFinal` |
| Regla de inactividad | `TestIdleRuleClosesWindowsWhenEventTimeStalls` |
| Fin de entrada finaliza todo | `TestCloseFinalizesEverything` |
| Reloj inyectable: `Manual` nunca retrocede, `Real` cumple la interfaz | `TestManualClockNeverRunsBackwards`, `TestRealClockImplementsInterface` |
| El watermark ingenuo se rompe con un reloj roto | `TestNaiveWatermarkIsBrokenByOneBadClock` |
| Montos exactos a centavos (`cmd/processor`) | `TestParseCentsExact` |
