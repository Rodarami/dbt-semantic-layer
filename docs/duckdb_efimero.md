# DuckDB efímero: por qué existe y qué resuelve

El gateway semántico usa DuckDB en dos lugares. Este documento explica el segundo, el **DuckDB efímero**: por qué
se creó, qué problema resuelve, cómo funciona y en qué se diferencia del DuckDB del catálogo.

1. [El problema: Power BI DirectQuery no manda consultas simples](#1-el-problema-power-bi-directquery-no-manda-consultas-simples)
2. [Qué se intentó antes y por qué no alcanzaba](#2-qué-se-intentó-antes-y-por-qué-no-alcanzaba)
3. [La solución: núcleo en Athena, resto en DuckDB](#3-la-solución-núcleo-en-athena-resto-en-duckdb)
4. [Ejemplo paso a paso](#4-ejemplo-paso-a-paso)
5. [Diferencias con el DuckDB del catálogo](#5-diferencias-con-el-duckdb-del-catálogo)
6. [Por qué DuckDB y por qué efímero](#6-por-qué-duckdb-y-por-qué-efímero)
7. [Seguridad](#7-seguridad)
8. [Qué no hace y limitaciones](#8-qué-no-hace-y-limitaciones)
9. [Dónde está en el código y pruebas](#9-dónde-está-en-el-código-y-pruebas)

---

## 1. El problema: Power BI DirectQuery no manda consultas simples

El gateway traduce SQL a consultas de MetricFlow. MetricFlow entiende consultas **semánticas**: métricas,
dimensiones, filtros, orden y límite. Nada más.

Un cliente como DBeaver manda exactamente eso:

```sql
SELECT employee__position, dotacion_total FROM semantic.employee_headcount
```

Power BI en **DirectQuery** no. Arma su propio SQL y envuelve la consulta semántica en varias capas con cálculos
suyos. Para un visual de tabla con una fecha, manda:

```sql
select "_"."employee__process_date" as "c8"
from (select "rows"."employee__process_date" as "employee__process_date",
             case when "rows"."o0" is not null then cast("rows"."o0" as timestamp)
                  else timestamp '1899-12-28 00:00:00' end as "t0_0",      -- truco para ordenar fechas
             case when "rows"."o0" is null then 0 else 1 end as "t1_0"     -- y dejar los nulos al final
      from (select "employee__process_date" as "employee__process_date",
                   "employee__process_date" as "o0"
            from "semantic"."employee_headcount" "$Table"                  -- ← la parte semántica
            group by "employee__process_date", "o0") "rows") "_"
order by "_"."t0_0", "_"."t1_0"
limit 501
```

MetricFlow no sabe compilar `CASE`, `cast` ni columnas calculadas por el cliente. El gateway rechazaba la consulta:

```
Expresión no soportada: CASE WHEN "employee__process_date" IS NOT NULL THEN CAST(...) ...
```

y el visual mostraba error en Power BI. El gráfico de barras sí funcionaba, porque su envoltorio solo usa `SUM`,
`GROUP BY` y renombres, que el traductor ya sabía aplanar.

> No era un problema de datos ni de la tabla `metricflow_time_spine`: Athena nunca llegaba a recibir la consulta.

## 2. Qué se intentó antes y por qué no alcanzaba

| Opción | Qué hace | Por qué no alcanza |
| --- | --- | --- |
| **Aplanar** las subconsultas (`_aplanar`, ya existía) | Funde las capas que solo renombran, filtran, ordenan, limitan o suman en una sola consulta semántica | Sirve para medidas y para el modo Importar, pero no puede traducir `CASE` ni `cast`: no tienen equivalente en MetricFlow |
| **Traducir cada expresión** de Power BI a MetricFlow | Enseñarle al traductor cada `CASE`, `cast`, bandera de nulos… | Power BI genera muchas variantes y cambian entre versiones: sería una lista infinita de casos y siempre quedaría alguno afuera |
| **Mandar todo a Athena** sin pasar por MetricFlow | Ejecutar el SQL de Power BI directamente | Se pierde la capa semántica: las métricas, ratios y joins definidos en dbt no se aplican. Además `semantic.<modelo>` no existe en Athena |
| **Rechazar la consulta** (comportamiento anterior) | Error claro en el log | Power BI no permite cambiar su SQL: el visual queda inutilizable |

Hacía falta una forma de que **MetricFlow calcule la métrica** y que **otro motor SQL complete** lo que Power BI
agrega alrededor.

## 3. La solución: núcleo en Athena, resto en DuckDB

El gateway parte la consulta en dos:

```
                         consulta de Power BI
                 ┌──────────────────────────────────┐
                 │ SELECT … CASE … cast … ORDER BY  │  ← resto
                 │   FROM ( SELECT … FROM (         │
                 │     ┌───────────────────────────┐│
                 │     │ SELECT fecha, …           ││  ← núcleo: la única parte
                 │     │ FROM semantic.<modelo>    ││     que lee la tabla semántica
                 │     └───────────────────────────┘│
                 │   ) ) LIMIT 501                  │
                 └──────────────────────────────────┘

  núcleo ──► MetricFlow ──► SQL de Athena ──► Athena ──► filas agregadas (pocas)
                                                              │
  resto  ──────────────────────────────► DuckDB efímero ◄─────┘ (tabla __nucleo_semantico)
                                                │
                                                ▼
                                          Power BI
```

1. **Núcleo:** la única subconsulta que lee una tabla `semantic.<modelo>`. Va a MetricFlow y a Athena como cualquier
   consulta semántica. Athena hace el trabajo pesado: lee los datos de Glue/S3 y calcula las métricas.
2. **Resto:** todo lo que Power BI puso alrededor. En el SQL, el núcleo se reemplaza por
   `SELECT * FROM __nucleo_semantico`.
3. **DuckDB efímero:** el gateway crea una base DuckDB vacía en memoria, carga en la tabla `__nucleo_semantico` las
   filas que devolvió Athena y ejecuta ahí el resto. El resultado va a Power BI y la base se cierra.

En una frase: **Athena calcula, DuckDB le da la forma que pidió Power BI.**

El gateway prueba en este orden, y solo llega al DuckDB efímero si los anteriores no sirven:

| Nivel | Cuándo | Quién responde |
| --- | --- | --- |
| 1. Directo | La consulta ya es semántica (DBeaver, Metabase SQL, psql) | MetricFlow → Athena |
| 2. Aplanado | Envoltorios con renombres, filtros, orden, límite o `SUM/MAX/MIN/AVG` (Importar, medidas) | MetricFlow → Athena |
| 3. Núcleo + resto | Envoltorios con `CASE`, `cast`, `COUNT`, filtros sobre métricas… (DirectQuery) | MetricFlow → Athena, y **DuckDB efímero** |

## 4. Ejemplo paso a paso

Visual de tabla con `employee__process_date` en DirectQuery:

**1. Llega la consulta de Power BI** (sección 1).

**2. El traductor no puede traducirla entera** (por el `CASE`) y busca el núcleo:

```sql
SELECT "employee__process_date", "employee__process_date" AS "o0"
FROM "semantic"."employee_headcount" GROUP BY "employee__process_date", "o0"
```

**3. MetricFlow lo compila y Athena lo ejecuta.** Athena recorre las tablas reales (miles o millones de registros) y
devuelve el resultado agrupado. Con una sola carga en los datos, es **una fila**:

| employee__process_date | o0 |
| --- | --- |
| 2026-10-16 | 2026-10-16 |

**4. Se crea el DuckDB efímero** con esa fila como tabla:

```
__nucleo_semantico
┌────────────────────────┬────────────┐
│ employee__process_date │ o0         │
├────────────────────────┼────────────┤
│ 2026-10-16             │ 2026-10-16 │
└────────────────────────┴────────────┘
```

Es todo su contenido: una tabla con las filas **que Athena ya calculó**, no los datos de origen.

**5. Se ejecuta el resto** de la consulta de Power BI sobre esa tabla:

```sql
SELECT "_"."employee__process_date" AS "c8"
FROM (SELECT ..., CASE WHEN "rows"."o0" IS NOT NULL THEN CAST(...) ... END AS "t0_0", ...
      FROM (SELECT * FROM __nucleo_semantico) AS "rows") AS "_"
ORDER BY "_"."t0_0", "_"."t1_0" LIMIT 501
```

**6. Power BI recibe** `2026-10-16` y dibuja el visual. **El DuckDB se cierra** y la tabla desaparece.

En la consola del gateway (`--log-sql`) se ve como:

```
Consulta compilada (núcleo; el resto se aplica en DuckDB) · motor=athena ...
Ejecutada en athena · query_id=... · núcleo 1 filas → 1 filas · 2.3 s · 1.2 MB escaneados
```

## 5. Diferencias con el DuckDB del catálogo

| | DuckDB del catálogo | DuckDB efímero |
| --- | --- | --- |
| **Para qué sirve** | Hacerse pasar por PostgreSQL: responde qué esquemas, tablas, columnas y tipos existen | Completar las consultas de DirectQuery que traen cálculos de Power BI |
| **Qué datos tiene** | Solo metadatos: `information_schema`, `pg_catalog`, tablas `semantic.<modelo>` vacías con sus tipos, `semantic.metrics` / `dimensions` | Una tabla `__nucleo_semantico` con el resultado que Athena devolvió para esa consulta |
| **Cuándo se crea** | Una vez, al arrancar el gateway, desde el `semantic_manifest.json` | En cada consulta que lo necesita |
| **Cuánto dura** | Mientras el gateway esté corriendo | Lo que dura la consulta (milisegundos) |
| **Quién lo usa** | Todas las conexiones, compartido y de solo lectura | Solo esa consulta; cada una tiene el suyo |
| **Ejemplos** | DBeaver al listar tablas, el navegador de Power BI, Metabase al sincronizar | El visual de fecha de Power BI en DirectQuery |
| **¿Habla con Athena?** | Nunca | No: recibe lo que Athena ya calculó |
| **Código** | `catalogo_pg.py` (`install_catalog`), creado en `build_server` | `server.py` (`_resto_en_duckdb`) |

Al abrir un informe en DirectQuery participan así:

1. **Conectar y navegar:** Power BI pregunta por tablas y columnas → responde el **catálogo**.
2. **Gráfico de barras:** consulta simple o aplanable → MetricFlow → Athena. No participa ningún DuckDB.
3. **Visual de fecha:** núcleo → MetricFlow → Athena; el **efímero** aplica `CASE`, orden y límite.

## 6. Por qué DuckDB y por qué efímero

**Por qué DuckDB:**

- Entiende SQL completo, compatible con lo que genera Power BI (`CASE`, `cast`, `timestamp '…'`, `ORDER BY` de
  nulos, subconsultas).
- Ya era una dependencia del gateway (el catálogo): no suma librerías ni servicios.
- Lee tablas Arrow sin copiarlas: el resultado de Athena (PyArrow) se carga directo.
- Procesa las pocas filas del núcleo en milisegundos, en memoria, sin servidor.

**Por qué una base nueva por consulta y no la del catálogo:**

- **Aislamiento:** el catálogo lo comparten todas las conexiones. Si los resultados se cargaran ahí, dos usuarios de
  Power BI podrían pisarse o ver datos del otro.
- **Nada que limpiar:** la base se cierra al terminar; no quedan tablas ni datos en memoria entre consultas.
- **El catálogo queda intacto y de solo lectura**, como lo exige su blindaje.
- **Costo casi nulo:** crear un DuckDB vacío en memoria es instantáneo.

## 7. Seguridad

El resto lo escribe Power BI, pero un usuario podría escribir a mano un SQL con la misma forma. Por eso el DuckDB
efímero tiene el mismo blindaje que el catálogo:

| Control | Qué impide |
| --- | --- |
| `enable_external_access = false` | Leer o escribir archivos (`read_csv('/etc/passwd')`, `COPY … TO`), usar la red o cargar extensiones |
| `lock_configuration = true` | Volver a activar lo anterior con `SET` |
| `memory_limit` | Consumir toda la memoria de la máquina |
| `solo_lectura` | Ejecutar algo que no sea **una** sentencia `SELECT` (`COPY`, `ATTACH`, varias sentencias) |
| `separar_nucleo` | Que el resto lea otra tabla que no sea `__nucleo_semantico`, o que haya dos núcleos |

El blindaje se aplica **después** de cargar la tabla del núcleo y **antes** de ejecutar el resto. Las pruebas
comprueban que `read_csv`, `read_text` y `COPY` quedan bloqueados.

## 8. Qué no hace y limitaciones

- **No calcula métricas.** Las métricas, ratios y conteos distintos los calcula siempre MetricFlow en Athena. DuckDB
  solo trabaja sobre ese resultado.
- **No ve los datos de origen**, solo las filas agregadas del núcleo.
- **Solo con ejecución** (`dbname=athena` y `--ejecutar`). En `EXPLAIN` o con motores sin ejecutor, el gateway
  devuelve el SQL del núcleo.
- **Un solo núcleo:** si un visual mezcla dos tablas semánticas (o una semántica con otra tabla), Power BI genera un
  `JOIN` y la consulta se rechaza.
- **Filtros en capas externas:** si Power BI pone un filtro afuera del núcleo, se aplica en DuckDB y no en Athena. El
  resultado es correcto, pero Athena calcula todas las filas del núcleo. Los filtros de segmentadores suelen ir en la
  capa interna y sí llegan a MetricFlow.
- **Los totales no pasan por DuckDB:** Power BI pide el total de un visual como `SUM` sobre la consulta agrupada.
  Si eso se hiciera en DuckDB, sumaría los ratios o conteos distintos de cada fila (un ratio daría 70,6). El
  traductor lo reconoce antes y pide a MetricFlow la métrica sin esas dimensiones (`test_total_de_power_bi`).
- **Recuento sobre una dimensión** (`COUNT`) se calcula sobre las filas del núcleo (combinaciones distintas), no
  sobre los registros originales.
- **Tamaño:** pensado para resultados agregados (pocas filas). Con `--max-filas` se limita lo que Athena devuelve.

## 9. Dónde está en el código y pruebas

| Pieza | Archivo | Qué hace |
| --- | --- | --- |
| `Translator.separar_nucleo(sql)` | `semantic_gateway/translator.py` | Busca la única subconsulta que lee una tabla semántica y devuelve `(núcleo, resto)`; rechaza si hay dos núcleos o el resto lee otra tabla |
| `NUCLEO = "__nucleo_semantico"` | `semantic_gateway/translator.py` | Nombre de la tabla del núcleo dentro del DuckDB efímero |
| `ConexionSemantica._semantica` | `semantic_gateway/server.py` | Intenta traducir la consulta entera; si no puede, llama a `_nucleo_y_resto` |
| `ConexionSemantica._nucleo_y_resto` | `semantic_gateway/server.py` | Compila y ejecuta el núcleo, ajusta tipos y aplica el resto. En `describe` calcula la forma del resultado con un núcleo vacío, sin ir a Athena |
| `_resto_en_duckdb(sql_resto, nucleo)` | `semantic_gateway/server.py` | Crea el DuckDB efímero, carga la tabla, aplica `blindar` y `solo_lectura`, ejecuta y cierra |

Pruebas:

- `tests/test_translator.py` → `test_nucleo_y_resto`: separa el SQL real de Power BI, comprueba que el resto no lee
  la tabla semántica y que se rechazan dos núcleos o una tabla ajena.
- `tests/test_gateway.py` → `probar_ejecucion`: envía el SQL de Power BI por el protocolo Postgres, comprueba que a
  Athena (simulada) llega solo el núcleo, sin `CASE`, y que el cliente recibe la columna `c8` con la fecha.

Ver también: [power_bi_directquery.md](power_bi_directquery.md) (uso en Power BI) y
[gateway_semantico.md](gateway_semantico.md) (referencia completa del gateway).
