# Gateway semántico

Guía completa del gateway: qué publica, cómo está armado, cómo se instala, cómo se ejecuta y cómo se consume desde
psql, Jupyter, DBeaver, Metabase y Power BI.

El gateway sirve **cualquier `semantic_manifest.json`** de dbt: publica una tabla `semantic.<modelo>` por cada modelo
semántico con métricas, sin cambiar código. Los ejemplos de esta guía usan el manifest de `dbt_glue_lab` (tablas
`semantic.events` y `semantic.items`); con el de IT People la tabla es `semantic.employee_headcount`. Consultas listas
para cada manifest: [consultas_dbt_glue_lab.md](consultas_dbt_glue_lab.md) y
[consultas_it_people.md](consultas_it_people.md).

Todos los comandos se ejecutan desde la raíz del repositorio, `MetricFlow/` (en el Mac, `~/DBT-GLUE/MetricFlow`),
salvo `dbt parse`, que se ejecuta dentro de cada proyecto dbt (`DBT/dbt_glue_lab/`).

Actualizado el 10 de octubre de 2026.

> El recorrido detallado de una consulta por cada pieza, con diagramas, y la cobertura del ADR están en
> [arquitectura_gateway.md](arquitectura_gateway.md).

## Índice

1. [Qué es y qué hace hoy](#1-qué-es-y-qué-hace-hoy)
2. [Arquitectura](#2-arquitectura)
3. [Qué publica: tablas y catálogo](#3-qué-publica-tablas-y-catálogo)
4. [De dónde salen las tablas: la capa semántica en dbt](#4-de-dónde-salen-las-tablas-la-capa-semántica-en-dbt)
5. [Código: paquete `semantic_gateway`](#5-código-paquete-semantic_gateway)
6. [Recorrido de una consulta](#6-recorrido-de-una-consulta)
7. [Instalación](#7-instalación)
8. [Generar y validar el manifest](#8-generar-y-validar-el-manifest)
9. [Ejecutar las pruebas](#9-ejecutar-las-pruebas)
10. [Levantar el gateway](#10-levantar-el-gateway) (10.9 [Seguridad](#109-seguridad))
11. [Conectarse](#11-conectarse)
12. [Consultar: contrato SQL y ejemplos con respuesta](#12-consultar-contrato-sql-y-ejemplos-con-respuesta)
13. [Agregar métricas o modelos semánticos](#13-agregar-métricas-o-modelos-semánticos)
14. [Compatibilidad con clientes BI (JDBC)](#14-compatibilidad-con-clientes-bi-jdbc)
15. [Errores frecuentes](#15-errores-frecuentes)
16. [Limitaciones](#16-limitaciones)
17. [Correspondencia con el ADR y próximos pasos](#17-correspondencia-con-el-adr-y-próximos-pasos)
18. [Chuleta de comandos](#18-chuleta-de-comandos)
19. [Referencia de funciones](#19-referencia-de-funciones)

---

## 1. Qué es y qué hace hoy

El gateway es un servidor que **habla el protocolo de PostgreSQL** sin ser un PostgreSQL. Cualquier herramienta que
se conecte a Postgres (psql, psycopg, DBeaver, Metabase, Power BI) puede conectarse a él y consultar las métricas de
la capa semántica como si fueran columnas de una tabla.

| | Estado actual |
| --- | --- |
| Qué publica | Una tabla virtual `semantic.<modelo>` por cada modelo semántico con métricas del manifest (con `dbt_glue_lab`: `semantic.events` y `semantic.items`; con IT People: `semantic.employee_headcount`) |
| Qué hace con una consulta semántica | La traduce y la **compila** con MetricFlow al dialecto pedido. Sin `--ejecutar`, devuelve el SQL en una columna `sql`. Con `--ejecutar`, las conexiones `dbname=athena` reciben las **filas** de Athena y el SQL queda en el log (sección 10.6) |
| Qué no hace | No ejecuta en motores distintos de Athena: para ellos devuelve el SQL |
| Motores (dialectos) | athena, bigquery, databricks, duckdb, postgres, redshift, snowflake, trino |
| Cómo se elige el motor | En cada conexión, con el nombre de la base de datos (`dbname=snowflake`) |
| Fuente de verdad | Un `semantic_manifest.json` de `semantic_manifests/` (uno por proyecto dbt), generado por `dbt parse`. Se lee una vez al arrancar |
| Clientes probados | psql/psycopg, Jupyter, DBeaver, Metabase (conexión, sincronización de esquema y SQL nativo) y Power BI en modo Importar y DirectQuery (catálogo de Npgsql; envoltorios de subconsultas, `CASE` y `cast`) |
| Seguridad de la conexión | Usuario y clave fijos; TLS opcional (`--tls-cert`/`--tls-key`) u obligatorio (`--tls-obligatorio`) (sección 10.7) |

---

## 2. Arquitectura

```mermaid
flowchart TB
    C["Clientes Postgres<br/>psql · Jupyter · DBeaver · Metabase · Power BI"]
    subgraph GW["semantic_gateway/ (un proceso Python)"]
        S["server.py · riffq (núcleo Rust, pgwire)<br/>protocolo Postgres, TLS, auth, motor por conexión (dbname)"]
        R["catalogo_pg.reescribir<br/>adapta consultas de catálogo"]
        D[("DuckDB en memoria<br/>catálogo: tablas vacías + metrics / dimensions / metric_dimensions")]
        T["translator.py · sqlglot<br/>SQL del cliente → SemanticQuery"]
        E["engine.py · MetricFlow<br/>compila; no se conecta a ningún motor"]
        X["executor.py · PyAthena<br/>solo con --ejecutar"]
    end
    M["semantic_manifests/&lt;proyecto&gt;/semantic_manifest.json<br/>(dbt parse)"]
    A[("Athena<br/>Iceberg en S3 · Glue")]

    C -- "Postgres wire · usuario bi · dbname=motor · TLS" --> S
    S -- "metadatos (information_schema, pg_catalog, SET, sondas)" --> R --> D
    S -- "lee semantic.&lt;modelo&gt;" --> T --> E
    E -- "sin --ejecutar: SQL en la columna sql" --> S
    E -- "con --ejecutar" --> X -- "SQL" --> A
    X -- "filas (el SQL queda en el log)" --> S
    M -- "se carga al arrancar" --> E
    E -- "catálogo (tablas, métricas, dimensiones)" --> D
```

Hay dos rutas:

- **Metadatos.** Cuando un cliente pregunta qué tablas o columnas existen, o envía comandos de sesión, responde
  DuckDB en memoria. Ahí viven las tablas virtuales vacías con sus tipos y las tablas de catálogo con filas reales.
- **Consultas semánticas.** Cuando la consulta lee una tabla `semantic.<modelo>`, no se lee ninguna tabla. El
  traductor convierte el SQL en una consulta semántica y MetricFlow la compila. Con `--ejecutar`, `executor.py` corre
  ese SQL en Athena y el gateway devuelve las filas; sin esa opción, devuelve el SQL.

Nada se persiste. El catálogo se reconstruye desde el manifest cada vez que el gateway arranca.

Quién responde cada tipo de consulta (riffq resuelve el protocolo; Python decide la ruta):

![Quién responde cada consulta](img/gateway_rutas_riffq.png)

DuckDB sigue siendo necesario: el catálogo que trae riffq no cubre las consultas de DBeaver y Metabase (prueba en
[comparativa_protocolo.md, sección 5](comparativa_protocolo.md#5-prueba-se-puede-usar-el-catálogo-de-riffq-y-prescindir-de-duckdb)).

---

## 3. Qué publica: tablas y catálogo

Todo vive en el esquema `semantic`.

### 3.1 Tablas virtuales (una por modelo semántico)

Cada modelo semántico que tenga métricas se publica como una tabla `semantic.<modelo>`, con el nombre del modelo.
Contiene sus métricas y **solo** las dimensiones que se pueden cruzar con todas ellas. Por eso cualquier combinación de
columnas de una misma tabla es consultable. Las tablas no guardan filas: en el catálogo están vacías y sirven como
carta de lo que se puede pedir; al consultarlas con `--ejecutar`, el gateway calcula las filas en ese momento con
MetricFlow y Athena. Detalle de cada métrica de IT People:
[it_people_capa_semantica.md](../semantic_manifests/it-people/it_people_capa_semantica.md), sección 3. Uso de estas
tablas en Power BI: [power_bi_directquery.md](power_bi_directquery.md), sección 5. Para ver qué tablas publicó el
gateway con el manifest que se cargó:

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic';
```

**Ejemplo con el manifest `dbt_glue_lab`** (con IT People hay una sola tabla, `semantic.employee_headcount`, con 7
métricas y 9 dimensiones):

**`semantic.events`** (18 columnas): métricas de eventos, sobre `silver_event`.

| Tipo | Columnas |
| --- | --- |
| Métricas (`DOUBLE`) | `event_count`, `unique_users`, `item_views`, `profile_views`, `item_views_per_user`, `item_views_share`, `engagement_events` |
| Dimensiones (`VARCHAR`) | `event__event_name`, `event__platform`, `event__parameter_name`, `event__event_year` |
| Dimensiones (`DATE`) | `event__event_date`, `metric_time`, `metric_time__day`, `metric_time__week`, `metric_time__month`, `metric_time__quarter`, `metric_time__year` |

**`semantic.items`** (15 columnas): métricas de items, sobre `silver_item`.

| Tipo | Columnas |
| --- | --- |
| Métricas (`DOUBLE`) | `item_count`, `total_item_price`, `avg_item_price`, `max_item_price` |
| Dimensiones (`VARCHAR`) | `item__item_name`, `item__category`, `item__adjective`, `item__modifier` |
| Dimensiones (`DATE`) | `item__created_at_date`, `metric_time`, `metric_time__day`, `metric_time__week`, `metric_time__month`, `metric_time__quarter`, `metric_time__year` |

`metric_time` es la dimensión de tiempo común: en `events` corresponde a `event_date` y en `items` a
`created_at_date`. El modelo `top_items` no tiene métricas, así que no genera tabla.

### 3.2 Tablas de catálogo (con filas reales)

Existen siempre, con cualquier manifest. Las filas son las del manifest de `dbt_glue_lab`:

| Tabla | Columnas | Filas | Para qué |
| --- | --- | --- | --- |
| `semantic.metrics` | `metric`, `label`, `type`, `description`, `table_name` | 11 | Qué métricas hay, su tipo (simple, ratio, derived) y en qué tabla están |
| `semantic.dimensions` | `dimension`, `kind`, `data_type`, `description` | 16 | Qué dimensiones hay, si son de tiempo o categóricas, y su descripción del YAML |
| `semantic.metric_dimensions` | `metric`, `dimension` | 121 | Qué dimensiones admite cada métrica |

Ejemplos:

```sql
SELECT metric, label, type, table_name FROM semantic.metrics ORDER BY table_name, metric;
SELECT dimension, kind, description FROM semantic.dimensions;
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'total_item_price';
```

### 3.3 Tipos de columna

Sin conexión al warehouse no se conocen los tipos físicos. Se publican así: dimensiones de tiempo como `DATE`, el
resto de dimensiones como `VARCHAR` (por eso `event__event_year` aparece como texto) y las métricas como `DOUBLE`.

---

## 4. De dónde salen las tablas: la capa semántica en dbt

El gateway no lee YAML ni tablas: lee un único archivo, el `semantic_manifest.json` que se le indica con
`--manifest`. Sirve el de cualquier proyecto dbt (1.12 o superior, spec nueva): basta con dejarlo en
`semantic_manifests/<proyecto>/`. Ese archivo lo genera `dbt parse` en el proyecto dbt. Como ejemplo, estos son los
YAML del proyecto `dbt_glue_lab` (`DBT/dbt_glue_lab/target/`):

| Pieza | Archivo | Contenido |
| --- | --- | --- |
| Modelo semántico `events` | `models/silver/schema.yml` (modelo `silver_event`) | Entidades `event` y `user`, tiempo `event_date`, dimensiones y las métricas simples `event_count`, `unique_users`, `item_views`, `profile_views` |
| Modelo semántico `items` | `models/silver/schema.yml` (modelo `silver_item`) | Entidad `item`, tiempo `created_at_date`, dimensiones y las métricas simples `item_count`, `total_item_price`, `avg_item_price`, `max_item_price` |
| Modelo semántico `top_items` | `models/gold/schema.yml` | Dimensiones de `gold_top_item`; sin métricas |
| Métricas ratio y derivadas | `models/semantic/metrics.yml` | `item_views_per_user`, `item_views_share`, `engagement_events` |
| Time spine | `models/semantic/metricflow_time_spine.sql` + `time_spine.yml` | Calendario diario 2010–2030 que MetricFlow necesita para `metric_time` |

Regla de la spec: las métricas simples van dentro del modelo (`metrics:` con `type: simple`, `agg` y `expr`); las de
tipo ratio, derived, cumulative o conversion van en un bloque `metrics:` de nivel superior.

**Cómo se arma cada tabla virtual** (código en `engine.py`, método `_tables`):

1. Para cada métrica, MetricFlow informa de qué modelo(s) semántico(s) sale
   (`metric_lookup.get_derived_from_semantic_models`). Las compuestas heredan los modelos de sus insumos: por eso
   `item_views_per_user` cae en `events`.
2. Las métricas se agrupan por modelo. El nombre de la tabla es el del modelo (por ejemplo `events` o
   `employee_headcount`). Si una métrica
   mezcla varios modelos, va a una tabla propia llamada `modelo1_modelo2`.
3. Las dimensiones de la tabla son las **comunes a todas** sus métricas.
4. Los nombres `metrics`, `dimensions` y `metric_dimensions` están reservados para el catálogo; un modelo con ese
   nombre recibe el sufijo `_sm`.

---

## 5. Código: paquete `semantic_gateway`

```
MetricFlow/                         # raíz del repositorio
├── semantic_gateway/
│   ├── __init__.py          # documenta la arquitectura
│   ├── __main__.py          # permite: python -m semantic_gateway
│   ├── model.py             # estructuras de datos (contrato común)
│   ├── translator.py        # SQL del cliente → SemanticQuery (sqlglot)
│   ├── engine.py            # MetricFlow como compilador; arma el catálogo
│   ├── executor.py          # ejecuta en Athena el SQL compilado (opcional, --ejecutar)
│   ├── catalogo_pg.py       # catálogo PostgreSQL emulado en DuckDB y compatibilidad con los drivers BI
│   ├── server.py            # servidor Postgres actual (riffq: núcleo Rust, TLS, autenticación)
│   ├── tls_obligatorio.py   # puerta que exige TLS delante de riffq (--tls-obligatorio)
│   └── server_old.py        # servidor anterior (buenavista, Python puro); se conserva como respaldo
├── tests/
│   ├── test_translator.py   # pruebas del traductor, sin base de datos
│   ├── test_gateway.py      # integración por protocolo Postgres (servidor actual o el anterior)
│   └── probar_catalogo_riffq.py  # prueba el catálogo propio de riffq, sin DuckDB (no es parte de la suite)
├── notebooks/
│   └── sqlglot_paso_a_paso.ipynb   # cómo sqlglot procesa una consulta, paso a paso
├── script/
│   ├── compilar_sin_conexion.py    # compila SQL con MetricFlow sin conexión
│   └── manifest_para_snowflake.py  # reescribe las rutas del manifest para Snowflake
├── semantic_manifests/             # un semantic_manifest.json por proyecto dbt
│   ├── dbt_glue_lab/semantic_manifest.json
│   └── it-people/semantic_manifest.json
├── requirements-gateway.txt
├── docs/
│   ├── gateway_semantico.md        # este documento
│   ├── arquitectura_gateway.md     # flujo de una consulta y cobertura del ADR
│   ├── comparativa_protocolo.md    # buenavista vs riffq, y la prueba del catálogo sin DuckDB
│   └── img/                        # arquitectura_gateway_actual.png, gateway_rutas_riffq.png
└── DBT/
    └── dbt_glue_lab/               # proyecto dbt (genera el manifest)
```

| Archivo | Líneas | Responsabilidad | Piezas clave |
| --- | --- | --- | --- |
| `model.py` | 81 | Estructuras de datos, sin lógica ni dependencias | `MetricInfo`, `DimensionInfo`, `TableInfo`, `Catalog`, `SemanticQuery`, `is_metric_time` |
| `translator.py` | 231 | SQL (dialecto Postgres) → `SemanticQuery` | `Translator.is_semantic`, `translate`, `_check_shape`, `_unknown_column`, `_where`, `_time_constraint`, `_metricflow_filter`, `split_explain` |
| `engine.py` | 174 | Carga el manifest, arma el catálogo y compila con MetricFlow sin conexión | `SemanticEngine.catalog`, `_tables`, `explain`, `ClienteSqlSoloDialecto`, `MOTORES`, `SQLGLOT_DIALECT` |
| `executor.py` | 140 | Ejecuta en Athena el SQL compilado y devuelve una tabla Arrow; tiempo máximo y límite de filas | `EjecutorAthena`, `ConfigAthena`, `Resultado`, `ExecutionError`, `ordenar_columnas` |
| `catalogo_pg.py` | 402 | Catálogo PostgreSQL emulado en DuckDB (tablas virtuales, metadatos, vistas `pg_*`), reescritura de las consultas de catálogo de DBeaver, Metabase y Npgsql, y blindaje de DuckDB (sección 10.9) | `install_catalog`, `blindar`, `solo_lectura`, `reescribir`, `_compat_catalogo`, `_consulta_npgsql`, `motor_from_database`, `_es_sonda` |
| `server.py` | 356 | Servidor actual sobre riffq: autenticación, motor por conexión, enrutamiento (semántica / catálogo), describe sin ejecutar, ejecución opcional, TLS | `ConexionSemantica`, `handle_auth`, `handle_query`, `_semantica`, `_duckdb`, `build_server`, `main` |
| `tls_obligatorio.py` | 122 | Con `--tls-obligatorio`: atiende el puerto público, exige `SSLRequest`, cifra y reenvía a riffq en un puerto interno; rechaza con `28000` a quien llega sin SSL | `PuertaTLS`, `puerto_libre` |
| `server_old.py` | 637 | Servidor anterior sobre buenavista, autocontenido (incluye su propia copia del catálogo). Se usa con `python -m semantic_gateway.server_old` | `MotorHandler`, `SemanticRewriter`, `SemanticSession.execute_sql`, `build_server` |

La lista completa de clases y funciones, con quién las llama y su capa del ADR, está en la
[sección 19](#19-referencia-de-funciones).

### model.py

- `MetricInfo(name, label, type, description)` y `DimensionInfo(name, kind, data_type, description)`: lo que se
  publica de cada métrica y dimensión. Inmutables.
- `TableInfo(name, description, metrics, dimensions)`: una tabla virtual.
- `Catalog(schema, tables, metrics, dimensions, metric_dimensions)`: todo lo publicado. `is_metric(nombre, tabla)` e
  `is_dimension(nombre, tabla)` comprueban si una columna pertenece a una tabla; `tables_with(columna)` dice en qué
  tablas está; `table_of_metric(métrica)` dice a qué tabla pertenece.
- `SemanticQuery`: la consulta canónica que viaja del traductor al motor (`metrics`, `group_by`, `where`, `start`,
  `end`, `order_by`, `limit`, `output`).

### translator.py

- `is_semantic(sql)`: la consulta es semántica si lee alguna tabla virtual.
- `translate(sql, dialect)`: parsea con `sqlglot.parse_one`, llama a `_check_shape` (una sola tabla virtual, sin joins,
  subconsultas, `HAVING`, `OFFSET` ni `WITH`) y clasifica cada columna del `SELECT` como métrica o dimensión **de esa
  tabla**.
- Envoltorios de Power BI. Power BI no manda la consulta tal cual: la envuelve en subconsultas propias. El traductor
  las resuelve en dos niveles:
  - `_aplanar`: si las capas externas solo renombran columnas, filtran, ordenan, limitan o agregan con
    `SUM/MAX/MIN/AVG` (Importar y DirectQuery de medidas), se funden en una sola consulta semántica.
  - `separar_nucleo(sql)`: si además calculan cosas que MetricFlow no entiende (DirectQuery de fechas: `CASE`,
    `cast(… as timestamp)`, banderas de nulos, `limit 501`), la **única** subconsulta que lee una tabla semántica es
    el *núcleo* y va a MetricFlow y al motor; el *resto* se ejecuta en una base DuckDB efímera y blindada (sin
    archivos ni red, una sola sentencia `SELECT`) sobre el resultado del núcleo, que ya viene agregado y es chico.
    Solo con motor ejecutor (`dbname=athena`); en `EXPLAIN` o sin ejecutor se devuelve el SQL del núcleo. Si hay dos
    núcleos (joins entre tablas semánticas) o el resto lee otra tabla, se rechaza. Detalle y motivos:
    [duckdb_efimero.md](duckdb_efimero.md).
- `_unknown_column`: si la columna existe en otra tabla, el error lo dice: *La columna 'item__category' no está en la
  tabla 'events'; está en: items*.
- `_where` y `_time_constraint`: las condiciones sobre `metric_time*` con `>=`, `<=`, `=` o `BETWEEN` y fecha literal se
  convierten en rango de tiempo (`start`/`end`). El resto pasa por `_metricflow_filter`, que reescribe cada columna
  como `{{ Dimension('…') }}` o `{{ TimeDimension('…') }}` en el dialecto del motor.

### engine.py

- `SemanticEngine(manifest, motor)`: lee el manifest una vez. Crea un `MetricFlowEngine` por motor la primera vez que
  se pide.
- `ClienteSqlSoloDialecto`: le dice a MetricFlow qué dialecto usar. Si algo intentara ejecutar, falla.
- `catalog(schema)`: arma el `Catalog`, incluidas las tablas por modelo (`_tables`).
- `explain(sq, motor)`: devuelve el SQL compilado.

### server.py (riffq) y catalogo_pg.py

- **riffq** (núcleo Rust sobre `pgwire`) atiende el protocolo PostgreSQL simple y extendido, TLS y la consulta vacía,
  y entrega cada consulta a Python con `handle_query`. Cada consulta corre en un pool de hilos.
- `build_server`: arma motor, catálogo, DuckDB, traductor y el `RiffqServer`.
- `ConexionSemantica.handle_auth` + `motor_from_database`: validan usuario y clave y fijan el motor de la sesión
  según `dbname`.
- `ConexionSemantica._resolver`: decide la ruta. Comandos de control (`BEGIN`, `SET`, `DISCARD`…) → sin efecto;
  `SHOW` → valor fijo; consulta semántica → `_semantica`; el resto (catálogo, `semantic.metrics`, sondas) →
  `catalogo_pg.reescribir` → DuckDB.
- `ConexionSemantica._semantica`: en el describe del protocolo extendido devuelve solo el esquema (sin compilar ni ir
  a Athena); al ejecutar, traduce, compila, y ejecuta o devuelve el SQL.
- `catalogo_pg.install_catalog`: crea en DuckDB las tablas virtuales vacías, las tablas de metadatos y las vistas de
  compatibilidad que esperan los drivers (ver sección 14).
- `main`: argumentos de línea de comandos y variables de entorno.

`server_old.py` es la versión anterior sobre buenavista; conserva sus propias clases (`MotorHandler`,
`SemanticRewriter`, `SemanticSession`) y se puede usar como respaldo.

---

## 6. Recorrido de una consulta

**Al arrancar (una vez)**

1. `SemanticEngine` lee `semantic_manifest.json`.
2. `engine.catalog("semantic")` arma el catálogo y las tablas por modelo.
3. `install_catalog` crea en DuckDB en memoria una tabla vacía `semantic.<modelo>` por modelo (con `dbt_glue_lab`:
   `semantic.events`, de 18 columnas, y `semantic.items`, de 15), `semantic.metrics`, `semantic.dimensions`,
   `semantic.metric_dimensions` y las vistas de compatibilidad.
4. riffq escucha en el puerto (5433 por defecto) y entrega cada consulta a un pool de hilos de Python.

**Al conectarse un cliente**

1. riffq recibe el arranque y llama a `ConexionSemantica.handle_auth(usuario, clave, host, dbname)`.
2. Se valida la clave y `motor_from_database` fija el motor. El log muestra:
   `Conexión · usuario=bi dbname=snowflake → motor=snowflake`.

**Por cada consulta**

1. riffq llama a `ConexionSemantica.handle_query`. En el protocolo extendido llama dos veces: primero para describir
   (`do_describe=True`, solo el esquema) y después para ejecutar.
2. `_resolver` separa `EXPLAIN` y llama a `is_semantic`.
   - Si no es semántica (o es una sonda `LIMIT 0` / `WHERE 1<>1`), `catalogo_pg.reescribir` la adapta y responde
     DuckDB.
   - Si es semántica, sigue.
3. `Translator.translate` produce el `SemanticQuery`, con los filtros en el dialecto del motor de la sesión.
4. `engine.explain(sq, motor)` compila con MetricFlow.
5. El SQL se imprime en el log (`Consulta compilada · motor=… ---------- SQL <motor> ----------`) y se devuelve como
   una fila con la columna `sql`.

---

## 7. Instalación

### 7.1 Requisitos

| Requisito | Detalle |
| --- | --- |
| Python | **3.12** en Windows y Linux, **3.14** en Mac Apple Silicon (versiones con paquete precompilado de riffq). Con otra versión, riffq se compila desde el código fuente y hace falta Rust (`rustup`) |
| dbt | dbt-core 1.12 o superior (spec semántica nueva) con dbt-glue 1.12.4 o superior |
| Red / AWS | **No** se necesitan para el gateway: compila sin conexión |
| Docker | Solo si vas a usar Metabase en contenedor |

### 7.2 Entorno virtual y dependencias

```bash
cd ~/DBT-GLUE
python3 -m venv dbt_venv
source dbt_venv/bin/activate

python3 -m pip install --upgrade pip
python3 -m pip install "dbt-core>=1.12" "dbt-glue>=1.12.4" dbt-metricflow dbt-duckdb   # dbt (README del proyecto dbt, sección 3)

cd MetricFlow
python3 -m pip install -r requirements-gateway.txt                                     # gateway
```

`requirements-gateway.txt`:

| Paquete | Para qué |
| --- | --- |
| `metricflow==0.213.0` | Motor de métricas, usado solo para compilar |
| `riffq==0.1.11` | Servidor del protocolo Postgres actual (núcleo Rust con `pgwire`, TLS, autenticación) |
| `buenavista==0.5.0` | Servidor anterior (`server_old.py`), en Python puro |
| `pyathena[arrow]>=3.0` | Ejecución en Athena (`--ejecutar`) |
| `sqlglot>=30` | Parser SQL del traductor y de la capa de compatibilidad |
| `pyarrow` | Formato de los resultados que se envían al cliente |
| `duckdb` | Catálogo en memoria |
| `psycopg[binary]>=3.2` | Cliente Postgres para las pruebas y para Jupyter |

Opcional, para los notebooks:

```bash
python3 -m pip install jupyterlab ipykernel
```

Comprobar:

```bash
dbt --version
python3 -c "import metricflow, riffq, sqlglot, duckdb, psycopg; print('ok')"
```

Si `pip` falla con `CERTIFICATE_VERIFY_FAILED` (Python de python.org en macOS):

```bash
python3 -m pip install --upgrade certifi
export SSL_CERT_FILE=$(python3 -c "import certifi; print(certifi.where())")
export REQUESTS_CA_BUNDLE=$SSL_CERT_FILE
```

### 7.3 Clientes opcionales

| Cliente | Instalación |
| --- | --- |
| `psql` (macOS) | `brew install libpq` y luego `echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc` |
| Metabase | `docker run -d -p 3000:3000 --name metabase metabase/metabase` y abrir http://localhost:3000 |
| DBeaver | Instalador oficial; usar el driver PostgreSQL |

---

## 8. Generar y validar el manifest

Desde el proyecto dbt (`MetricFlow/DBT/dbt_glue_lab/`), con el entorno activo:

```bash
set -a; source .env; set +a   # AWS_ACCOUNT_ID, DBT_S3_BUCKET… (README del proyecto dbt, sección 4.1)
unset DBT_PROFILES_DIR        # usar el perfil de Glue (profiles.yml del proyecto)
dbt parse                     # escribe target/semantic_manifest.json; no se conecta a Glue
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json   # publicarlo para el gateway
```

Para otro proyecto dbt, el manifest va en su propia carpeta: `semantic_manifests/<proyecto>/semantic_manifest.json`.

Validar la capa semántica con MetricFlow (perfil DuckDB local, ver [README del proyecto dbt](../DBT/dbt_glue_lab/README.md), sección 10):

```bash
export DBT_PROFILES_DIR=$PWD/mf_profiles
dbt parse
mf validate-configs --skip-dw
unset DBT_PROFILES_DIR
dbt parse                     # volver a generar el manifest con el perfil de Glue
```

El gateway usa las rutas de tabla del manifest tal cual (`lab_data_silver.silver_event`). Por eso conviene que el
último `dbt parse` sea con el perfil de Glue.

Ver qué quedó en el manifest:

```bash
python3 -c "
import json
m = json.load(open('semantic_manifests/dbt_glue_lab/semantic_manifest.json'))
print('modelos:', [s['name'] for s in m['semantic_models']])
print('métricas:', [x['name'] for x in m['metrics']])
"
```

---

## 9. Ejecutar las pruebas

Desde la raíz `MetricFlow/` (usan `semantic_manifests/dbt_glue_lab/semantic_manifest.json`):

```bash
# Mac / Linux
python -m tests.test_translator                          # 5 pruebas del traductor, sin base de datos
python -m tests.test_gateway                             # levanta el gateway (riffq) en un hilo y prueba por protocolo Postgres
SEMANTIC_GW_SERVER=old python -m tests.test_gateway      # las mismas pruebas contra el servidor anterior (buenavista)
python -m tests.probar_catalogo_riffq --comparar         # opcional: catálogo de riffq sin DuckDB vs el actual
```

```powershell
# Windows (PowerShell)
python -m tests.test_translator
python -m tests.test_gateway
$env:SEMANTIC_GW_SERVER="old"; python -m tests.test_gateway; Remove-Item Env:SEMANTIC_GW_SERVER
python -m tests.probar_catalogo_riffq --comparar
```

Salida esperada de `test_gateway`:

```
OK · dbname=athena / snowflake / semantic
OK · conexiones simultáneas independientes
OK · dbname desconocido
OK · compatibilidad JDBC (ping, SHOW, parámetros)
OK · sincronización de metadatos tipo Metabase
OK · una tabla virtual por modelo semántico
OK · navegación tipo DBeaver
OK · gateway Postgres wire: motor por conexión
OK · ejecución: filas en dbname=athena, SQL en EXPLAIN y en motores sin ejecutor
OK · ejecutor Athena: --max-filas corta y --timeout cancela la consulta
OK · Npgsql (Power BI): tipos al conectar y error a mitad de lote
OK · seguridad: sin acceso a archivos, catálogo de solo lectura y sin inyección por Jinja
OK · TLS obligatorio: sin SSL se rechaza, con SSL funciona
```

La última solo corre con el servidor actual y necesita `openssl` para generar un certificado de prueba (en Windows
viene con Git para Windows); si no lo encuentra, se omite con un aviso.

`probar_catalogo_riffq` termina con `25/25 con DuckDB · 20/25 sin DuckDB` (detalle en
[comparativa_protocolo.md](comparativa_protocolo.md), sección 5).

Durante `test_gateway` aparecen mensajes de error y un `WARNING` con una consulta: son los casos de error que la
prueba provoca a propósito.

---

## 10. Levantar el gateway

### 10.1 Local (clientes en la misma máquina)

```bash
cd ~/DBT-GLUE/MetricFlow
source ../dbt_venv/bin/activate
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena
```

### 10.2 Para clientes en Docker o en otra máquina (Metabase)

```bash
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena --host 0.0.0.0
```

`--host 0.0.0.0` acepta conexiones desde fuera de `localhost`. Sin TLS ni autenticación fuerte, úsalo solo en tu
máquina o en una red privada.

La terminal queda ocupada mientras el gateway corre; `Ctrl+C` lo detiene. Cuando está listo, muestra:

```
INFO semantic_gateway · Gateway listo en 0.0.0.0:5433 · semantic.{events, items} · motor por defecto=athena (dbname=<motor> lo cambia por conexión) · solo compila e imprime SQL · 11 métricas · 16 dimensiones
```

### 10.3 Opciones

| Opción | Variable de entorno | Por defecto | Para qué |
| --- | --- | --- | --- |
| `--manifest` | `SEMANTIC_GW_MANIFEST` | `semantic_manifests/dbt_glue_lab/semantic_manifest.json` | Ruta del manifest. Otro proyecto: `semantic_manifests/it-people/semantic_manifest.json` |
| `--motor` | `SEMANTIC_GW_MOTOR` | `athena` | Motor para conexiones sin `dbname` o con `dbname=semantic` |
| `--host` | `SEMANTIC_GW_HOST` | `127.0.0.1` | `0.0.0.0` para aceptar conexiones remotas |
| `--port` | `SEMANTIC_GW_PORT` | `5433` | Puerto TCP |
| `--user` / `--password` | `SEMANTIC_GW_USER` / `SEMANTIC_GW_PASSWORD` | `bi` / `bi` | Credencial única |
| `--tls-cert` / `--tls-key` | `SEMANTIC_GW_TLS_CERT` / `SEMANTIC_GW_TLS_KEY` | sin TLS | Certificado y clave PEM; con ambos el gateway **ofrece** TLS: cifra si el cliente lo pide (`sslmode=require`), pero acepta también conexiones sin cifrar (sección 10.7) |
| `--tls-obligatorio` | `SEMANTIC_GW_TLS_OBLIGATORIO=1` | apagado | Con `--tls-cert`/`--tls-key`, **rechaza** las conexiones sin SSL (sección 10.7) |
| `--log-level` | `SEMANTIC_GW_LOG_LEVEL` | `INFO` | `DEBUG` para más detalle |
| `--ejecutar` | `SEMANTIC_GW_EJECUTAR=1` | apagado | Ejecuta en Athena y devuelve filas (sección 10.6) |
| `--athena-workgroup` | `ATHENA_WORKGROUP` | `primary` | Workgroup de Athena |
| `--athena-output` | `ATHENA_OUTPUT` | la del workgroup | `s3://…` donde Athena deja los resultados |
| `--athena-catalog` | `ATHENA_CATALOG` | `awsdatacatalog` | Catálogo de datos (Glue) |
| `--region` | `AWS_REGION` / `AWS_DEFAULT_REGION` | la del perfil | Región de Athena y Glue |
| `--aws-profile` | `AWS_PROFILE` | credenciales por defecto | Perfil de `~/.aws/config` |
| `--timeout` | `SEMANTIC_GW_TIMEOUT` | `100` | Segundos; al vencer se **cancela** la consulta en Athena. `0` = sin límite |
| `--max-filas` | `SEMANTIC_GW_MAX_FILAS` | `0` (sin límite) | Corta el resultado en N filas y lo anota en el log |

Con variables de entorno:

```bash
export SEMANTIC_GW_HOST=0.0.0.0
export SEMANTIC_GW_MOTOR=snowflake
python -m semantic_gateway
```

### 10.4 Qué muestra el log

```
INFO semantic_gateway · Conexión · usuario=bi dbname=athena → motor=athena
INFO semantic_gateway · Consulta compilada · motor=athena metrics=['event_count'] group_by=['event__platform'] ...
---------- SQL athena ----------
SELECT ...
----------------------------------
WARNING semantic_gateway · Consulta de catálogo no analizable por sqlglot (...); se usa el respaldo textual:
```

El `WARNING` es informativo: Metabase envía algunas consultas de catálogo que sqlglot no sabe analizar, y el gateway
las resuelve igual (sección 14).

### 10.5 Recargar cambios

El manifest se lee **solo al arrancar**. Después de un `dbt parse` hay que detener el gateway (`Ctrl+C`) y volver a
lanzarlo. Lo mismo después de actualizar el código.

### 10.6 Ejecutar en Athena y devolver filas

Con `--ejecutar`, el gateway compila con MetricFlow (igual que antes), **ejecuta** ese SQL en Athena y devuelve las
filas al cliente. El SQL compilado queda solo en el log. MetricFlow sigue sin conectarse a nada: ejecuta
`executor.py`, como pide el ADR (el motor de métricas compila, otro componente ejecuta).

| Conexión | Respuesta |
| --- | --- |
| `dbname=athena` (o `semantic`/sin dbname con `--motor athena`) | Filas de Athena, con las columnas en el orden y con los alias del `SELECT` |
| `EXPLAIN SELECT …` | El SQL compilado, sin ejecutar |
| `dbname=snowflake`, `redshift`… | El SQL compilado (no hay ejecutor para esos motores) |
| Catálogo y sondas de los BI (`LIMIT 0`, `WHERE 1<>1`) | DuckDB, sin llamar a Athena |

**Configuración en AWS**

| Qué | Ejemplo |
| --- | --- |
| Credenciales en la máquina del gateway | Las credenciales por defecto de AWS (las mismas que usa dbt), o un perfil con `--aws-profile <perfil>` (o `AWS_PROFILE`). En Windows: [credenciales_aws_windows.md](credenciales_aws_windows.md) |
| Región | `--region us-east-1` |
| Workgroup (motor v3, lee Iceberg) | `--athena-workgroup primary` |
| Resultados en S3 | `--athena-output s3://<BUCKET>/athena-results/`; no hace falta si el workgroup impone su ubicación |
| Tablas | Las bases de Glue del manifest deben existir: `lab_data_silver`, `lab_data_gold` y, para métricas acumuladas o rellenos, `lab_data.metricflow_time_spine` (`dbt run -s metricflow_time_spine`) |

Permisos IAM del usuario o rol:

- `athena:StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`, `GetWorkGroup`
- `glue:GetDatabase`, `GetDatabases`, `GetTable`, `GetTables`, `GetPartitions`
- `s3:GetObject`, `s3:ListBucket` sobre el bucket de datos (archivos y metadata de Iceberg)
- `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `s3:GetBucketLocation` sobre la ruta de resultados
- `lakeformation:GetDataAccess` si las tablas están bajo Lake Formation; `kms:Decrypt` / `kms:GenerateDataKey` si
  los buckets usan KMS

Una forma rápida de validar credenciales y permisos: ejecutar en la consola de Athena el SQL que hoy devuelve el gateway.

**Instalar y levantar**

```bash
python -m pip install -r requirements-gateway.txt        # incluye pyathena[arrow]
aws sts get-caller-identity                              # comprobar las credenciales de AWS
set -a; source DBT/dbt_glue_lab/.env; set +a             # carga DBT_S3_BUCKET

python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --ejecutar --region us-east-1 \
  --athena-workgroup primary --athena-output "s3://$DBT_S3_BUCKET/athena-results/"
#   con un perfil con nombre agregar: --aws-profile <perfil>
```

**Log de una consulta ejecutada**

```
INFO … Ejecución en Athena · workgroup=primary · resultados=s3://… · región=us-east-1 · perfil=(por defecto) · timeout=100 s · max_filas=sin límite
INFO … Consulta compilada · motor=athena metrics=['event_count'] group_by=['event__platform'] …
---------- SQL athena ----------
SELECT …
----------------------------------
INFO … Ejecutada en athena · query_id=1a2b… · 3 filas · 4.2 s · 12.3 MB escaneados
```

Si Athena rechaza la consulta (permisos, tabla inexistente) o vence `--timeout`, el cliente BI recibe el error con
el mensaje de Athena y el log lo registra como `ERROR`.

**Costo y límites.** Athena cobra por datos escaneados (5 USD por TB), no por filas devueltas: `--max-filas` no
abarata nada, solo protege la memoria del gateway. Las consultas semánticas siempre agregan, así que el resultado suele
ser chico. `--timeout` sí corta costo: cancela la consulta en Athena. Metabase lanza consultas solo al abrir
preguntas y dashboards; el log de cada ejecución muestra los bytes escaneados.

---

### 10.7 TLS

riffq cifra la conexión si se le pasan un certificado y una clave PEM. Para pruebas, un certificado autofirmado:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 365 -out server.crt -subj "/CN=localhost"
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena \
  --tls-cert server.crt --tls-key server.key
```

En DBeaver: pestaña *SSL* → *Use SSL* y modo `require`. En psycopg: `sslmode=require`. Los archivos `.crt` y `.key`
no se suben al repo.

**TLS opcional u obligatorio.** Con solo `--tls-cert`/`--tls-key`, riffq *ofrece* TLS: cifra si el cliente lo pide,
pero un cliente con SSL desactivado se conecta igual y conversa en texto plano (PostgreSQL funciona igual sin
`hostssl` en `pg_hba.conf`; riffq 0.1.11 no tiene esa opción). Para rechazar esas conexiones:

```bash
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena \
  --tls-cert server.crt --tls-key server.key --tls-obligatorio
```

| Cliente | Sin `--tls-obligatorio` | Con `--tls-obligatorio` |
| --- | --- | --- |
| SSL desactivado (`sslmode=disable`) | Conecta, sin cifrar | Rechazado: `FATAL: El gateway exige SSL: conéctese con sslmode=require` (código `28000`) |
| `sslmode=prefer` (por defecto en libpq) | Conecta cifrado | Conecta cifrado |
| `sslmode=require` / DBeaver *Use SSL* | Conecta cifrado | Conecta cifrado |

Cómo funciona (`tls_obligatorio.py`): riffq pasa a escuchar sin TLS en un puerto interno de `127.0.0.1`, y una
puerta en Python atiende el puerto público. Los clientes Postgres piden TLS dentro del protocolo: primero envían un
`SSLRequest` sin cifrar y, tras la respuesta `S`, hacen el handshake. La puerta responde `S`, cifra y reenvía el
tráfico a riffq; si el primer mensaje no es un `SSLRequest`, responde el error y cierra. Las cancelaciones de consulta
(`CancelRequest`, que los drivers envían por una conexión nueva) se reenvían igual.

Por esa misma razón, en AWS **el balanceador debe ser un NLB con listener TCP**, no TLS: un listener TLS espera el
handshake de inmediato y los clientes Postgres envían primero el `SSLRequest` en claro. El cifrado lo hace el gateway.

### 10.8 Servidor anterior (buenavista)

La versión anterior se conserva en `semantic_gateway/server_old.py`, con las mismas opciones salvo TLS:

```bash
python -m semantic_gateway.server_old --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena
SEMANTIC_GW_SERVER=old python -m tests.test_gateway        # pruebas contra la versión anterior
```

La comparación entre buenavista, riffq y otras librerías está en [comparativa_protocolo.md](comparativa_protocolo.md).

### 10.9 Seguridad

Revisión de seguridad del 17 de octubre de 2026. Cada ataque se probó contra el gateway y quedó cubierto por
`probar_seguridad` en `tests/test_gateway.py` (con los dos servidores).

**Corregido**

| Problema | Riesgo | Corrección |
| --- | --- | --- |
| Las consultas no semánticas van a DuckDB tal cual: un usuario autenticado podía leer archivos de la máquina del gateway (`read_text('~/.aws/credentials')`, el `.env`, la clave TLS), listar carpetas (`glob`), escribir archivos (`COPY … TO`), abrir otras bases (`ATTACH`) y borrar o crear objetos del catálogo que comparten todas las conexiones | **Crítico**: robo de credenciales de AWS y del certificado; escritura de archivos | `catalogo_pg.blindar`: `enable_external_access = false` (sin archivos, red ni extensiones), `lock_configuration = true` (nadie puede reactivarlo con `SET`) y `memory_limit = 1GB`. `catalogo_pg.solo_lectura`: cada mensaje que llega a DuckDB debe traer **una sola sentencia** y de lectura (`SELECT`); se rechazan `COPY`, `ATTACH`, `DROP`, `CREATE` y varias sentencias en un mismo mensaje. No limita la concurrencia: varias conexiones (por ejemplo varios Power BI) consultan a la vez como antes. En el servidor anterior se permiten además `BEGIN`/`COMMIT` y `SET`, que buenavista necesita |
| MetricFlow procesa los filtros como plantillas Jinja: un valor como `'{{ "%c"\|format(39) }} OR 1=1 OR …'` cerraba la comilla e **inyectaba SQL** en la consulta de Athena, incluso subconsultas a tablas fuera de la capa semántica | **Alto**: lectura de cualquier tabla a la que acceda el rol de AWS del gateway (por ejemplo datos de RRHH), sin pasar por la capa semántica | `translator.py` rechaza filtros con `{{`, `{%` o `{#`. El Jinja de MetricFlow está en modo sandbox, así que no se podía ejecutar Python |
| La puerta de `--tls-obligatorio` esperaba sin límite el primer mensaje y el handshake | Medio: muchas conexiones colgadas agotan los recursos del proceso | Tiempo máximo de 10 s para el primer mensaje y para el handshake TLS |
| La clave se comparaba con `!=` | Bajo: comparación de tiempo variable | `hmac.compare_digest` |
| Sin aviso al exponer el gateway | riffq pide la clave **en texto plano** (no MD5 ni SCRAM): sin TLS viaja legible por la red | Al arrancar con un `--host` distinto de `127.0.0.1`, el log advierte si falta `--tls-obligatorio` o si la clave es la por defecto (`bi`) |

**Verificado sin problemas**

- **Inyección de SQL por comillas:** sqlglot duplica las comillas de los literales (`'x'' OR ''1''=''1'`). Subconsultas,
  `UNION`, `EXISTS` y lecturas de otras tablas se rechazan en el traductor.
- **Secretos en el repositorio:** ni el historial de git ni los archivos pendientes de subir tienen ID de cuenta,
  buckets ni claves. `.gitignore` excluye `.env`, `*.crt`, `*.key` y `*.pem`.
- **Dependencias:** `pip-audit` sobre los 44 paquetes del entorno, sin vulnerabilidades conocidas.

**Riesgos que quedan** (de diseño; se resuelven con el gateway de orquestación del ADR)

| Riesgo | Mitigación hoy |
| --- | --- |
| Un solo usuario y clave para todos; sin identidad por persona, permisos por métrica ni RLS: quien se conecta ve todas las métricas del manifest (incluidas las de RRHH) | Red privada, clave fuerte en `SEMANTIC_GW_PASSWORD` (no en `--password`, que se ve en la lista de procesos), `--tls-obligatorio` |
| Una sola credencial de AWS para todas las consultas | Rol de AWS con permisos mínimos: solo las bases y tablas de los manifests, y la ruta de resultados |
| Sin límite de intentos de clave | Exponer el gateway solo detrás del NLB interno y con security groups |
| Costo y carga: cualquier usuario puede lanzar consultas caras en Athena o resultados grandes | `--timeout`, `--max-filas` y el límite de bytes escaneados por consulta del workgroup de Athena (`BytesScannedCutoffPerQuery`) |
| El log guarda el SQL compilado con los valores de los filtros (por ejemplo nombres o géneros) | Tratar el log como dato sensible: retención y acceso restringidos |
| Con `--tls-obligatorio`, riffq escucha sin TLS en un puerto interno de `127.0.0.1` | Solo accesible desde la misma máquina, y pide clave igual |

---

## 11. Conectarse

> Power BI (DirectQuery e Importar), con el paso a paso para armar paneles:
> [power_bi_directquery.md](power_bi_directquery.md).

### 11.1 Elegir el motor con `dbname`

| `dbname` | Motor usado |
| --- | --- |
| `athena`, `bigquery`, `databricks`, `duckdb`, `postgres`, `redshift`, `snowflake`, `trino` | Ese motor |
| `semantic`, vacío o igual al usuario | El de `--motor` |
| Cualquier otro | La conexión abre y el catálogo responde, pero las consultas semánticas fallan con *Base de datos 'x' no existe…* |

El motor solo cambia el **dialecto** del SQL. Los nombres de base, esquema y tabla salen del manifest.

### 11.2 psql

```bash
PGPASSWORD=bi psql "host=127.0.0.1 port=5433 user=bi dbname=snowflake"
```

```sql
SELECT metric, table_name FROM semantic.metrics;
SELECT event__platform, event_count FROM semantic.events;   -- ejemplo con dbt_glue_lab
```

### 11.3 Python / Jupyter

```python
import psycopg

def sql_para(motor: str, consulta: str) -> str:
    dsn = f"host=127.0.0.1 port=5433 user=bi password=bi dbname={motor}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(consulta).fetchone()[0]

# ejemplos con el manifest de dbt_glue_lab (sin --ejecutar, devuelve el SQL)
print(sql_para("athena", "SELECT metric_time__month, event_count FROM semantic.events"))
print(sql_para("snowflake", "SELECT item__category, total_item_price FROM semantic.items"))
```

Para Jupyter, lanzar desde la raíz `MetricFlow/`:

```bash
jupyter lab
```

El notebook `notebooks/sqlglot_paso_a_paso.ipynb` muestra cada paso del traductor sobre una consulta de ejemplo.

### 11.4 Metabase

1. Levantar el gateway con `--host 0.0.0.0` (sección 10.2).
2. Levantar Metabase: `docker run -d -p 3000:3000 --name metabase metabase/metabase` y abrir http://localhost:3000.
3. En *Admin → Bases de datos → Agregar base de datos*:

| Campo | Valor |
| --- | --- |
| Tipo de base de datos | PostgreSQL |
| Nombre a mostrar | `CapaSemantica` (cualquiera) |
| Host | `host.docker.internal` (Metabase en Docker; `localhost` si Metabase corre fuera de Docker) |
| Puerto | `5433`. **No lo dejes vacío**: el valor por defecto es 5432 |
| Nombre de la base de datos | `athena` (o el motor que quieras) |
| Usuario / Contraseña | `bi` / `bi` |
| SSL | Desactivado (activado si el gateway usa TLS) |
| Esquemas | Solo estos: `semantic` |
| Escaneo de valores de filtro (opciones avanzadas) | Nunca, lo haré manualmente |

4. Guardar y pulsar *Sincronizar el esquema ahora* (*Sync database schema now*).
5. En *Explorar datos → CapaSemantica* aparece una tabla por modelo semántico (con `dbt_glue_lab`, `Events` e `Items`)
   más `Metrics`, `Dimensions` y `Metric Dimensions`.
6. Para ver el SQL compilado: *Nuevo → Consulta SQL*, elegir `CapaSemantica` y escribir una consulta de la sección 12.

Notas:

- Abrir una tabla semántica (por ejemplo `Events`) desde *Explorar datos* no muestra datos: el explorador arma consultas (todas las columnas,
  `SELECT *`) que el traductor no acepta. Usar consultas SQL nativas.
- `Metrics`, `Dimensions` y `Metric Dimensions` sí muestran filas.
- En Linux, Docker no resuelve `host.docker.internal` por defecto: agrega `--add-host=host.docker.internal:host-gateway`
  al `docker run`.
- Una conexión por motor: crea `CapaSemantica Athena` con `athena` y `CapaSemantica Snowflake` con `snowflake` para
  comparar dialectos.

### 11.5 DBeaver

Nueva conexión PostgreSQL: host `127.0.0.1`, puerto `5433`, base de datos = motor, usuario y clave `bi`. En la
pestaña del driver, desactivar *Show all databases*.

---

## 12. Consultar: contrato SQL y ejemplos con respuesta

### 12.1 Reglas

| Se acepta | Cómo se interpreta |
| --- | --- |
| Columna de métrica, sola o dentro de `SUM/MAX/MIN/AVG/MEASURE/AGG` | Métrica pedida. La agregación la decide MetricFlow, así un ratio nunca se suma |
| Columna de dimensión | Agrupación (`GROUP BY` implícito; escribirlo es opcional) |
| `WHERE` sobre `metric_time*` con `>=`, `<=`, `=` o `BETWEEN` y fecha literal | Rango de tiempo |
| Resto del `WHERE` sobre dimensiones | Filtro de MetricFlow |
| `ORDER BY` (nombre, alias u ordinal), `LIMIT`, `DISTINCT`, `EXPLAIN` | Se traspasan |

| Se rechaza | Mensaje |
| --- | --- |
| `SELECT *` | *SELECT \* no está soportado: elija métricas y dimensiones* |
| Columnas de dos tablas | *La columna 'item__category' no está en la tabla 'events'; está en: items…* |
| Filtro sobre una métrica | *No se puede filtrar por la métrica 'event_count' (HAVING no está soportado)* |
| `COUNT(*)` u otras expresiones | *Expresión no soportada: COUNT(\*) (use columnas de la tabla semántica)* |
| Joins, subconsultas, `HAVING`, `OFFSET`, `WITH` | *… no está soportado en la capa semántica* (salvo los envoltorios de Power BI: subconsultas que se aplanan, o un núcleo semántico con el resto ejecutado en DuckDB) |
| Parámetros (`$1`) en consultas semánticas | *La capa semántica no admite parámetros…* |

Las sondas que usan los BI (`SELECT … FROM semantic.<modelo> WHERE 1<>1` o `LIMIT 0`) no se rechazan: devuelven cero
filas con las columnas de la tabla.

### 12.2 Ejemplos con su respuesta (motor athena)

Con el manifest de `dbt_glue_lab`; con otro manifest cambian las tablas y columnas, no las reglas. Se muestra el SQL
que genera el gateway (lo que devuelve sin `--ejecutar` o con `EXPLAIN`; con `--ejecutar` devuelve las filas). Se
omiten los comentarios `-- …` que MetricFlow agrega. Los alias numerados (`events_src_10000`, `subq_6`) cambian entre
ejecuciones.

**Una métrica, sin dimensiones**

```sql
SELECT event_count FROM semantic.events
```
```sql
SELECT
  COUNT(DISTINCT event_id) AS event_count
FROM lab_data_silver.silver_event events_src_10000
```

**Una métrica por mes**

```sql
SELECT metric_time__month, event_count FROM semantic.events ORDER BY metric_time__month
```
```sql
SELECT
  DATE_TRUNC('month', event_date) AS metric_time__month
  , COUNT(DISTINCT event_id) AS event_count
FROM lab_data_silver.silver_event events_src_10000
GROUP BY
  DATE_TRUNC('month', event_date)
ORDER BY metric_time__month
```

**Varias métricas por dimensión**

```sql
SELECT event__platform, event_count, unique_users
FROM semantic.events
ORDER BY event_count DESC
LIMIT 10
```
```sql
SELECT
  platform AS event__platform
  , COUNT(DISTINCT event_id) AS event_count
  , COUNT(DISTINCT user_id) AS unique_users
FROM lab_data_silver.silver_event events_src_10000
GROUP BY
  platform
ORDER BY event_count DESC
LIMIT 10
```

**Métricas de items**

```sql
SELECT item__category, total_item_price, avg_item_price, item_count
FROM semantic.items
ORDER BY total_item_price DESC
LIMIT 5
```
```sql
SELECT
  category AS item__category
  , SUM(price) AS total_item_price
  , AVG(price) AS avg_item_price
  , COUNT(DISTINCT item_id) AS item_count
FROM lab_data_silver.silver_item items_src_10000
GROUP BY
  category
ORDER BY total_item_price DESC
LIMIT 5
```

**Rango de fechas: mismo pedido, dos motores**

```sql
SELECT metric_time__day, event_count
FROM semantic.events
WHERE metric_time__day BETWEEN '2024-01-01' AND '2024-03-31'
ORDER BY metric_time__day
```

Con `dbname=athena`:

```sql
WHERE DATE_TRUNC('day', event_date) BETWEEN timestamp '2024-01-01' AND timestamp '2024-03-31'
```

Con `dbname=snowflake`:

```sql
WHERE DATE_TRUNC('day', event_date) BETWEEN '2024-01-01' AND '2024-03-31'
```

**Métrica de ratio** (respuesta de unas 45 líneas)

```sql
SELECT event__platform, item_views_per_user FROM semantic.events
```

MetricFlow calcula `item_views` (con el filtro `event_name = 'view_item'` de su definición) y `unique_users` por
separado, los une con `FULL OUTER JOIN` por plataforma y divide:

```sql
CAST(item_views AS DOUBLE) / CAST(NULLIF(unique_users, 0) AS DOUBLE) AS item_views_per_user
```

**Filtro por dimensión**

```sql
SELECT event__platform, event_count
FROM semantic.events
WHERE event__platform IN ('ios', 'android')
```

El filtro queda como `WHERE event__platform IN ('ios', 'android')` sobre una subconsulta. Los valores son de ejemplo;
consulta `semantic.dimensions` para ver la descripción de cada dimensión.

---

## 13. Agregar métricas o modelos semánticos

1. Editar el YAML:
   - Métrica simple: en el bloque `metrics:` del modelo, en `models/silver/schema.yml` o `models/gold/schema.yml`.
   - Ratio, derived, cumulative o conversion: en `models/semantic/metrics.yml` (o en otro YAML con bloque `metrics:`).
   - Modelo semántico nuevo: `semantic_model: enabled: true` en el modelo, con entidades y dimensiones en sus columnas.

   Ejemplo de ratio:

   ```yaml
   metrics:
     - name: eventos_por_usuario
       label: Eventos por usuario
       type: ratio
       numerator: event_count
       denominator: unique_users
   ```

2. Regenerar y validar el manifest (sección 8).
3. Reiniciar el gateway (`Ctrl+C` y volver a lanzarlo).
4. En Metabase, *Sincronizar el esquema ahora*.

Resultado: `eventos_por_usuario` aparece como columna de la tabla de su modelo (aquí `semantic.events`), con su fila
en `semantic.metrics`. Si
agregaste un modelo semántico con métricas, aparece su tabla. No hay que tocar el código del gateway.

---

## 14. Compatibilidad con clientes BI (JDBC)

Metabase, DBeaver y Power BI usan drivers que, al conectarse y sincronizar, envían muchas consultas propias de
PostgreSQL. riffq resuelve el protocolo; DuckDB no entiende todas esas consultas de catálogo, así que
`catalogo_pg.py` las adapta. (riffq trae su propio catálogo emulado, pero resuelve solo 20 de las 25 consultas de
catálogo de DBeaver, Power BI y Metabase, por ejemplo falla con `::regnamespace` de Metabase: por eso el gateway lo
desactiva y usa el suyo. Prueba en [comparativa_protocolo.md](comparativa_protocolo.md), sección 5.)

| Lo que envía el driver | Qué hace el gateway |
| --- | --- |
| `;` o consulta vacía (ping de JDBC) | Respuesta vacía sin error |
| Parámetros con tipo (`$1` texto, entero…) | riffq los decodifica y llegan a DuckDB. **Los parámetros de texto sin tipo llegan como `NULL`** (limitación de riffq; JDBC y Npgsql siempre envían el tipo, psycopg no: ver sección 16) |
| Describe del protocolo extendido | Se responde solo con el esquema: no se compila ni se ejecuta en Athena |
| SQL en minúsculas al ejecutar (riffq, protocolo extendido) | Se recupera el texto original guardado en el describe, para no alterar literales como `'F'` |
| `DISCARD ALL`, `RESET ALL`, `BEGIN`, `SET …` (Npgsql/JDBC) | Se aceptan sin efecto |
| Catálogo de tipos de Npgsql (`typreceive`, `pg_range`) | Lista fija de tipos de PostgreSQL |
| `x::regnamespace`, `'pg_class'::regclass` | Se reemplazan por subconsultas a `pg_namespace` / `pg_class` |
| `has_table_privilege`, `has_schema_privilege`, `has_any_column_privilege`, … | Devuelven `TRUE` (el gateway no aplica permisos de catálogo) |
| `pg_matviews`, `pg_stat_user_tables`, `pg_stat_all_tables` | Vistas vacías con las columnas esperadas |
| `pg_type`, `pg_attribute` | Vistas propias (`gw_pg_type`, `gw_pg_attribute`) con los OID reales de PostgreSQL (DuckDB informa tipos incorrectos) |
| `information_schema.tables` / `columns` / `views` / `schemata` | Vistas `gw_info_*` sin el esquema auxiliar `main` (navegador de Power BI) |
| `pg_class`, `pg_namespace` | Vistas `gw_pg_class`, `gw_pg_namespace` sin el esquema auxiliar `main` (sincronización de Metabase) |
| `pg_get_expr` (con 3 argumentos), `pg_get_partkeydef`, `pg_tablespace_location` | Macros `gw_*` que devuelven `NULL` (no hay particiones, expresiones por defecto ni tablespaces) |
| `pg_relation_size`, `pg_total_relation_size`, `pg_table_size`, `pg_indexes_size`, `pg_stat_get_numscans` | Macros `gw_*` que devuelven `0` |
| `pg_settings` | Vista propia `gw_pg_settings` con las 17 columnas de PostgreSQL (incluye `reset_val`) y unos pocos parámetros (`search_path`, `TimeZone`…) |
| `pg_get_keywords()` | Función de tabla `gw_pg_get_keywords` (word, catcode, catdesc) armada desde `duckdb_keywords()` |
| Literales de arreglo `'{a,b}'::text[]` | Se reescriben a `['a','b']`, que es lo que entiende DuckDB |
| `pg_trigger`, `pg_roles`, `pg_extension` | Vistas vacías con las columnas esperadas |
| `pg_database` | Vista `gw_pg_database` con una fila por motor más `semantic`, para que DBeaver encuentre la base conectada |
| `show timezone` | Responde `UTC` |
| `BEGIN READ ONLY`, `START TRANSACTION …` | Se ejecutan como `BEGIN` |
| Claves primarias (`information_schema._pg_expandarray`) | Resultado vacío, conservando los parámetros recibidos |
| Columnas `NULL AS …` | Se informan como texto |
| `SELECT TRUE FROM semantic.<modelo> WHERE 1<>1 LIMIT 0` | Sonda de permisos: responde el catálogo, sin MetricFlow |
| Consulta que sqlglot no puede analizar | Respaldo por expresiones regulares + `WARNING` con la consulta en el log |

Si aparece un error nuevo al conectar otro cliente, el log del gateway muestra la consulta que lo provocó.

---

## 15. Errores frecuentes

| Síntoma | Causa | Solución |
| --- | --- | --- |
| `connection refused` | El gateway no está corriendo o usa otro puerto | Levantarlo; revisar `--port` |
| Metabase: *contraseña incorrecta* | Puerto vacío: Metabase usó 5432 (otro Postgres) | Poner `5433` |
| Metabase no conecta desde Docker | El gateway escucha solo en `127.0.0.1` | Levantarlo con `--host 0.0.0.0`; host `host.docker.internal` |
| Metabase muestra 0 tablas | Gateway con código antiguo | Reiniciar el gateway y sincronizar de nuevo |
| DBeaver: *Scalar Function with name pg_get_partkeydef does not exist* (u otra función `pg_*`) | Gateway con código antiguo | Reiniciar el gateway; si la función es otra, agregarla a `_FUNCIONES_PROPIAS` en `server.py` |
| *Table with name dbt_glue_lab does not exist* | Consulta o pregunta guardada de la versión anterior (tabla única) | Usar las tablas `semantic.<modelo>` (con `dbt_glue_lab`, `semantic.events` o `semantic.items`) |
| *La columna 'x' no está en la tabla 'y'; está en: z* | Columnas de dos modelos en una consulta | Consultar la tabla indicada; ver `semantic.metric_dimensions` |
| *Got error(s) during query resolution* (MetricFlow) | Combinación que MetricFlow no puede resolver | Revisar `semantic.metric_dimensions` |
| *Base de datos 'x' no existe…* | `dbname` mal escrito | Usar un motor de la lista o `semantic` |
| *SELECT \* no está soportado* | El contrato exige columnas explícitas | Listar métricas y dimensiones |
| La métrica nueva no aparece | El gateway lee el manifest solo al arrancar | `dbt parse`, reiniciar, sincronizar en Metabase |
| `No module named 'semantic_gateway'` | Se ejecuta fuera de la raíz `MetricFlow/` | `cd MetricFlow`; en Jupyter, la última celda del notebook lo resuelve |
| `FileNotFoundError: …semantic_manifest.json` | No se copió el manifest a `semantic_manifests/` o la ruta de `--manifest` es otra | `cp target/semantic_manifest.json ../../semantic_manifests/<proyecto>/` desde el proyecto dbt |
| `psql: command not found` | Cliente no instalado | `brew install libpq` o usar psycopg |
| Docker: *Cannot connect to the Docker daemon* | Docker Desktop cerrado | `open -a Docker` y esperar a que arranque |

---

## 16. Limitaciones

- **Ejecuta solo en Athena** (con `--ejecutar`). Otros motores reciben el SQL compilado. Todas las conexiones usan la
  misma credencial de AWS: no hay permisos por usuario. Las tablas virtuales del catálogo siguen vacías.
- **Seguridad mínima:** un usuario y clave fijos (TLS con `--tls-cert`/`--tls-key`, obligatorio con
  `--tls-obligatorio`), sin seguridad por filas ni auditoría. Solo en red privada. Detalle y riesgos en la
  sección 10.9.
- **Tipos aproximados:** tiempo como `DATE`, el resto de dimensiones como `VARCHAR`, métricas como `DOUBLE`.
- **SQL limitado:** sin `SELECT *`, expresiones, joins, subconsultas ni parámetros en consultas semánticas. El
  constructor visual de Metabase genera SQL que hoy no se acepta; usar consultas SQL nativas.
- **Manifest estático:** se carga al arrancar; no hay recarga en caliente.
- **Rutas físicas:** las de Glue (`lab_data_silver.…`) para cualquier motor. Para otro motor, usar un manifest con
  rutas reescritas ([README del proyecto dbt](../DBT/dbt_glue_lab/README.md), sección 11).
- **riffq en preversión (0.1.x):** dos comportamientos a tener en cuenta, ya cubiertos por el gateway o por las
  pruebas: en el protocolo extendido pasa el SQL a minúsculas al ejecutar (el gateway recupera el original del
  describe) y entrega como `NULL` los parámetros de texto sin tipo (psycopg los envía así por defecto; JDBC y Npgsql
  no). Paquetes precompilados solo para Python 3.12 (Windows, Linux) y 3.14 (Mac Apple Silicon). Al actualizar riffq,
  correr `python -m tests.test_gateway`.

---

## 17. Correspondencia con el ADR y próximos pasos

| Artefacto del ADR | Componente | Pieza en el código | Estado |
| --- | --- | --- | --- |
| Semantic models | Definición de métricas | `schema.yml`, `metrics.yml`, `dbt parse` | Hecho |
| Serving layer | Endpoint Postgres wire | `server.py` (riffq + catálogo DuckDB); `server_old.py` (buenavista) como respaldo | Hecho (PoC), probado con DBeaver, Metabase y Power BI (Importar y DirectQuery) |
| Serving layer | Traductor de peticiones | `translator.py`, `model.py` | Hecho |
| Serving layer | Publicación por dominio | Una tabla por modelo semántico (`engine._tables`) | Hecho |
| Serving layer | Autenticación OIDC/SAML | Usuario y clave fijos en `handle_auth` (punto de extensión para OIDC); TLS opcional | Mínimo |
| Serving layer | REST/GraphQL, MCP, Arrow Flight SQL | — | Pendiente |
| Gateway | Autorización, RLS, PII | — | Pendiente |
| Gateway | Caché de resultados | — | Pendiente |
| Gateway | Gestor del manifiesto | Carga única al arrancar | Mínimo |
| Gateway | Conectores de motor y enrutador | El cliente elige el motor con `dbname`; ejecutor de Athena (`executor.py`, `--ejecutar`) | Parcial (solo Athena) |
| Metric engine | MetricFlow OSS embebido, solo compila | `engine.py` | Hecho |
| Transversal | Validador en CI, sincronizador OpenMetadata, observabilidad | — | Pendiente |

Próximos pasos sugeridos:

1. **Recarga en caliente del manifest** (desde disco o S3), sin cortar conexiones.
2. **Ejecutores** para otros motores (Snowflake, Redshift…), con la misma interfaz `Ejecutor` de `executor.py`.
3. **Capa gateway** entre traductor y motor (autorización, RLS, caché), con `SemanticQuery` como contrato.
4. **Catálogo en PostgreSQL real** si se necesitan varias réplicas o permisos por rol, cargado por CI desde el manifest.
5. **Despliegue en AWS:** imagen de contenedor en EKS con alta disponibilidad (como pide el ADR), detrás de un balanceador
   interno con listener TCP (no TLS: ver sección 10.7), manifest en S3, secretos en Secrets Manager, logs en
   CloudWatch, `--tls-obligatorio`.

El detalle completo de qué falta para cumplir el ADR, componente por componente, está en el [README del proyecto dbt](../DBT/dbt_glue_lab/README.md), sección 15.

---

## 18. Chuleta de comandos

```bash
# Entorno (Mac: venv con Python 3.14; Linux: 3.12)
cd ~/DBT-GLUE && source dbt_venv/bin/activate && cd MetricFlow

# Instalar (una vez)
python3 -m pip install -r requirements-gateway.txt

# Manifest (cada vez que cambie la capa semántica)
cd DBT/dbt_glue_lab && set -a && source .env && set +a && unset DBT_PROFILES_DIR && dbt parse
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json && cd ../..

# Pruebas
python -m tests.test_translator
python -m tests.test_gateway                             # servidor actual (riffq)
SEMANTIC_GW_SERVER=old python -m tests.test_gateway      # servidor anterior (buenavista)
python -m tests.probar_catalogo_riffq --comparar         # catálogo de riffq sin DuckDB (comparativa, sección 5)

# Gateway local / para Docker
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena --host 0.0.0.0
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --port 5434

# Ejecutando en Athena (filas al cliente, SQL al log)
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena \
  --ejecutar --aws-profile <perfil> --region us-east-1 --athena-workgroup <workgroup>

# Con TLS (agregar --tls-obligatorio para rechazar conexiones sin SSL) / servidor anterior
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --tls-cert server.crt --tls-key server.key --tls-obligatorio
python -m semantic_gateway.server_old --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena

# Consultar (ejemplo con dbt_glue_lab; la tabla cambia según el manifest)
PGPASSWORD=bi psql "host=127.0.0.1 port=5433 user=bi dbname=athena" \
  -c "SELECT event__platform, event_count FROM semantic.events"

# Metabase
docker run -d -p 3000:3000 --name metabase metabase/metabase

# Git (dbt_venv, target/, logs/ y backup/ están en .gitignore)
git add -A && git commit -m "mensaje" && git push
```

```powershell
# Windows (PowerShell, Python 3.12). Credenciales de AWS: credenciales_aws_windows.md
cd "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica"
py -3.12 -m venv dbt_venv                                 # una vez
.\dbt_venv\Scripts\Activate.ps1
. .\aws_cenco.ps1                                          # perfil de AWS (opción C)
cd dbt-semantic-layer                                     # raíz del repositorio
python -m pip install -r requirements-gateway.txt         # una vez

python -m tests.test_gateway
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer
python -m semantic_gateway.server_old --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer       # respaldo con buenavista
```

---

## 19. Referencia de funciones

Todas las clases y funciones del paquete `semantic_gateway`, archivo por archivo. La columna **Capa del ADR** indica
a qué artefacto de la arquitectura pertenece cada pieza: **Serving** (serving layer), **Gateway** (gateway de
orquestación), **Engine** (metric engine) o **Contrato** (estructuras que comparten las capas).

Convención de nombres: las funciones que empiezan con `_` son internas de su archivo; el resto se usa desde otros
archivos o desde las pruebas.

### 19.1 `model.py`: estructuras de datos (Contrato)

No tiene lógica ni depende de otros archivos del paquete. Lo importan los otros tres.

| Clase / función | Qué hace | La usa | Capa del ADR |
| --- | --- | --- | --- |
| `METRIC_TIME`, `METRIC_TIME_GRAINS` | Constantes: nombre de la dimensión de tiempo común (`metric_time`) y granularidades publicadas (`week`, `month`, `quarter`, `year`) | `engine.catalog`, `translator` | Contrato |
| `is_metric_time(nombre)` | Dice si una columna es `metric_time` o una de sus granularidades (`metric_time__month`) | `Translator._where`, `Translator._metricflow_filter` | Contrato |
| `MetricInfo` | Una métrica publicada: `name`, `label`, `type` (simple, ratio, derived), `description`. Inmutable | `engine.catalog`, `install_catalog` | Contrato |
| `DimensionInfo` | Una dimensión publicada: `name` (p. ej. `event__platform`), `kind` (`time` o `categorical`), `data_type` (`DATE` o `VARCHAR`), `description`. Inmutable | `engine.catalog`, `install_catalog`, traductor | Contrato |
| `TableInfo` | Una tabla virtual: `name`, `description`, `metrics` y `dimensions` (las comunes a todas sus métricas). Inmutable | `engine._tables`, `install_catalog`, traductor | Contrato |
| `Catalog` | Todo lo publicado: `schema`, `tables`, `metrics`, `dimensions`, `metric_dimensions` (pares válidos). Inmutable | Todo el paquete | Contrato |
| `Catalog.is_metric(nombre, tabla=None)` | ¿Es una métrica? Con `tabla`, ¿es una métrica **de esa tabla**? | `Translator.translate`, `_where` | Contrato |
| `Catalog.is_dimension(nombre, tabla=None)` | Igual que la anterior, para dimensiones | `Translator.translate`, `_where` | Contrato |
| `Catalog.tables_with(columna)` | Lista las tablas que publican una columna | `Translator._unknown_column` | Contrato |
| `Catalog.table_of_metric(métrica)` | Devuelve la tabla a la que pertenece una métrica | `install_catalog` (columna `table_name` de `semantic.metrics`) | Contrato |
| `SemanticQuery` | La consulta semántica canónica: `metrics`, `group_by`, `where` (filtros Jinja de MetricFlow), `start`/`end` (rango de tiempo), `order_by` (`-` = descendente), `limit`, `output` (columna y alias del `SELECT`). Mutable: el traductor la llena paso a paso | Sale de `Translator.translate`, entra a `SemanticEngine.explain` | Contrato entre serving layer y metric engine (y el futuro gateway) |

### 19.2 `translator.py`: SQL del cliente → `SemanticQuery` (Serving, traductor de peticiones)

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `TranslationError` | Excepción para consultas que no cumplen el contrato. Su mensaje llega al cliente como error SQL | Todo el traductor; `SemanticSession.execute_sql` | Serving |
| `split_explain(sql)` | Separa un `EXPLAIN` inicial: devuelve `(True, consulta)` o `(False, sql)` | `SemanticSession.execute_sql`, `is_semantic`, pruebas | Serving |
| `Translator(catalog, read, warehouse_dialect)` | Guarda el catálogo, el dialecto de entrada (`postgres`) y el dialecto por defecto para escribir filtros | `build_server` | Serving |
| `Translator.is_semantic(sql)` | Decide la ruta: ¿la consulta lee alguna tabla virtual `semantic.<modelo>`? Si no, la responde el catálogo | `SemanticSession.execute_sql`, `SemanticRewriter.rewrite` | Serving |
| `Translator.translate(sql, dialect)` | Paso principal. Parsea con `sqlglot.parse_one`, valida la forma, clasifica cada columna del `SELECT` en métrica o dimensión de la tabla, procesa `WHERE`, `ORDER BY` y `LIMIT`, y devuelve el `SemanticQuery` | `SemanticSession.execute_sql` | Serving |
| `Translator.separar_nucleo(sql)` | Si la consulta tiene una sola subconsulta que lee una tabla semántica, devuelve `(núcleo, resto)`; el resto lee la tabla `__nucleo_semantico` y se ejecuta con `server._resto_en_duckdb` | `ConexionSemantica._semantica` (cuando `translate` rechaza) | Serving |
| `Translator._check_shape(árbol)` | Exige una sola tabla virtual y rechaza joins, `HAVING`, `OFFSET`, `WITH` y subconsultas. Devuelve el nombre de la tabla | `translate` | Serving |
| `Translator._unknown_column(nombre, tabla)` | Arma el error de columna desconocida; si existe en otra tabla, lo dice (*está en: items*) | `translate`, `_where` | Serving |
| `Translator._column(nodo)` | Extrae el nombre de la columna, aceptando que venga dentro de `SUM/MAX/MIN/AVG/MEASURE/AGG`; rechaza otras expresiones (`COUNT(*)`, `x * 2`) | `translate`, `_order_name` | Serving |
| `Translator._conjuncts(nodo)` | Parte el `WHERE` en condiciones unidas por `AND` | `translate` | Serving |
| `Translator._where(condición, sq, dialecto, tabla)` | Valida una condición (sin métricas; solo dimensiones de la tabla) y decide si es rango de tiempo o filtro | `translate` | Serving |
| `Translator._time_constraint(condición, sq)` | Si la condición es `metric_time*` con `>=`, `<=`, `=` o `BETWEEN` y fecha literal, la convierte en `start`/`end`. Devuelve si lo logró | `_where` | Serving |
| `Translator._metricflow_filter(condición, dialecto)` | Reescribe la condición como filtro de MetricFlow: cada columna pasa a `{{ Dimension('…') }}` o `{{ TimeDimension('…') }}`, en el dialecto del motor | `_where` | Serving |
| `to_jinja(nodo)` | Función interna de `_metricflow_filter` que hace el reemplazo de cada columna | `_metricflow_filter` (vía `transform` de sqlglot) | Serving |
| `Translator._order_name(nodo, sq, alias)` | Resuelve un `ORDER BY` por nombre, alias u ordinal (`ORDER BY 1`) y exige que la columna esté en el `SELECT` | `translate` | Serving |
| `_date(nodo)` | Convierte un literal (`'2024-01-01'` o `DATE '2024-01-01'`) en fecha; `None` si no lo es | `_time_constraint` | Serving |

### 19.3 `engine.py`: MetricFlow como compilador (Engine)

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `MOTORES` | Motores soportados y, para cada uno, el tipo de motor y el renderizador SQL de MetricFlow | `ClienteSqlSoloDialecto`, `_check`, `motor_from_database`, `main` | Engine |
| `SQLGLOT_DIALECT` | Equivalencia motor → dialecto de sqlglot, para escribir los filtros del `WHERE` | `build_server`, `SemanticSession.execute_sql` | Engine |
| `RESERVADAS` | Nombres que un modelo semántico no puede usar como tabla (`metrics`, `dimensions`, `metric_dimensions`) | `_tables` | Engine |
| `ClienteSqlSoloDialecto(motor)` | Cliente SQL falso que MetricFlow necesita para saber el dialecto. No abre conexión | `_engine_for` | Engine |
| `.sql_engine_type`, `.sql_plan_renderer` | Informan a MetricFlow el tipo de motor y con qué renderizador escribir el SQL | MetricFlow | Engine |
| `.render_bind_parameter_key(clave)` | Requisito de la interfaz de MetricFlow; devuelve la clave tal cual | MetricFlow | Engine |
| `.query`, `.execute`, `.dry_run` (`_sin_conexion`) | Fallan siempre: garantizan que MetricFlow **no ejecute** nada, como pide el ADR | Nadie (protección) | Engine |
| `.close()` | Requisito de la interfaz; no hace nada | MetricFlow | Engine |
| `SemanticEngine(manifest, motor)` | Lee y valida `semantic_manifest.json` una vez, guarda el motor por defecto y crea su `MetricFlowEngine` | `build_server` | Engine (y gestor del manifiesto mínimo) |
| `SemanticEngine._check(motor)` | Valida que el motor esté en `MOTORES` | `__init__`, `explain` | Engine |
| `SemanticEngine._engine_for(motor)` | Devuelve el `MetricFlowEngine` de un motor, creándolo la primera vez (uno por dialecto) | `__init__`, `explain` | Engine |
| `SemanticEngine.catalog(schema)` | Arma el `Catalog`: métricas, dimensiones (con `metric_time` y sus granularidades), pares válidos y tablas | `build_server` | Engine → Serving (catálogo) |
| `SemanticEngine._tables(métricas, dimensiones, pares)` | Agrupa las métricas por modelo semántico (`get_derived_from_semantic_models` de MetricFlow) y calcula las dimensiones comunes de cada grupo. Resultado: una tabla virtual por modelo | `catalog` | Engine → Serving |
| `SemanticEngine.explain(sq, motor)` | Compila un `SemanticQuery` al SQL del motor pedido. Las compilaciones se hacen de a una (candado), porque todas las conexiones comparten los mismos `MetricFlowEngine` | `SemanticSession.execute_sql` | Engine |
| `SemanticEngine._request(sq)` | Convierte el `SemanticQuery` en un `MetricFlowQueryRequest` (métricas, agrupación, rango, filtros, orden, límite) | `explain` | Engine |
| `ts(fecha)` | Función interna de `_request`: convierte una fecha en fecha y hora | `_request` | Engine |

### 19.4 `server.py`: servidor sobre riffq (Serving)

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `ConexionSemantica` | Una instancia por cliente conectado (subclase de `riffq.BaseConnection`). Sus atributos de clase (`engine`, `translator`, `db`, `ejecutores`, credenciales) los fija `build_server` | riffq (`RiffqServer.get_connection`) | Serving |
| `ConexionSemantica.handle_auth(usuario, clave, host, database, callback)` | Valida usuario y clave (rechazo con `28P01`), fija el motor con `motor_from_database` y lo registra en el log | riffq, al conectar | Serving + Gateway (enrutador provisional) |
| `ConexionSemantica.handle_query(sql, callback, do_describe, query_args)` | Envía la consulta al pool de hilos (`_responder`): una consulta lenta no bloquea al resto | riffq, por cada consulta | Serving |
| `_responder(sql, callback, describir, parámetros)` | Resuelve la consulta y entrega al callback una tabla Arrow, una etiqueta (`SET`, `BEGIN`…) o un error SQL | `handle_query` | Serving |
| `_original(sql, describir)` | Guarda el SQL del describe y lo recupera al ejecutar: riffq pasa el SQL a minúsculas en el `Execute` del protocolo extendido | `_responder` | Serving |
| `_resolver(sql, describir, parámetros)` | Ruta: ping → vacío; `BEGIN`/`SET`/`DISCARD`… → etiqueta sin efecto; `SHOW` → valor fijo (`_show`); semántica → `_semantica`; resto → `_duckdb` | `_responder` | Serving |
| `_duckdb(sql, parámetros)` | Catálogo, tablas de metadatos y sondas: `catalogo_pg.reescribir` y ejecución en DuckDB con un cursor propio | `_resolver`, `_semantica` | Serving |
| `_semantica(sql, describir, parámetros)` | Sonda → DuckDB; rechaza parámetros y `dbname` inválido; `translate`; en el describe devuelve solo el esquema; si no, `explain`, log del SQL y ejecución (`EjecutorAthena`) o el SQL en la columna `sql` | `_resolver` | Serving + Gateway |
| `_esquema(sq)` | Tipos del resultado según el catálogo (dimensiones con su tipo, métricas `DOUBLE`) | `_semantica` | Serving |
| `_ajustar_tipos(tabla, esquema)` | Convierte el resultado de Athena a los tipos informados en el describe (los clientes que leen en binario, como Npgsql, dependen de eso) | `_semantica` | Serving |
| `Gateway` | Envuelve el `RiffqServer` configurado; `start()` bloquea hasta Ctrl+C | `main`, `start_in_thread` | Serving (arranque) |
| `build_server(host, puerto, usuario, clave, manifest, motor, ejecutores, tls_cert, tls_key, tls_obligatorio)` | Arma `SemanticEngine`, catálogo en DuckDB, `Translator`, la clase de conexión, el `RiffqServer` (pool de 32 hilos) y TLS si corresponde. Con `tls_obligatorio`, riffq escucha sin TLS en un puerto interno y `PuertaTLS` atiende el puerto público | `main`, `start_in_thread` | Serving (arranque) |
| `PuertaTLS` (`tls_obligatorio.py`) | Exige `SSLRequest` antes de entregar la conexión a riffq: responde `S`, cifra y reenvía; `GSSENCRequest` → `N`; `CancelRequest` → se reenvía; cualquier otro inicio → error `28000` | `Gateway.start` | Serving (seguridad) |
| `start_in_thread(…)` | Igual que `build_server`, pero arranca en un hilo y espera a que el puerto acepte conexiones | `tests/test_gateway.py` | Pruebas |
| `main()` | Punto de entrada de `python -m semantic_gateway`: argumentos, variables `SEMANTIC_GW_*`, ejecutor de Athena y arranque | `__main__.py` | Serving (arranque) |

### 19.4.1 `catalogo_pg.py`: catálogo PostgreSQL emulado (Serving)

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `blindar(db)` | Después de `install_catalog`: `memory_limit`, `enable_external_access = false` y `lock_configuration = true` (sección 10.9) | `build_server` | Serving (seguridad) |
| `solo_lectura(sql, tambien)` | Rechaza lo que no sea una única sentencia `SELECT` (`COPY`, `ATTACH`, `DROP`, varias sentencias…) antes de ejecutarla en DuckDB | `_duckdb`, `SemanticSession.execute_sql` (servidor anterior) | Serving (seguridad) |
| `install_catalog(db, catalog)` | Crea en DuckDB las tablas virtuales vacías, `semantic.metrics`/`dimensions`/`metric_dimensions` con filas, y vistas y macros de compatibilidad (`gw_pg_type`, `gw_pg_attribute`, `gw_pg_settings`, `gw_info_*`, `gw_pg_class`, `gw_pg_namespace`, `gw_pg_get_keywords`…) | `build_server` | Serving (catálogo) |
| `reescribir(sql)` | Adapta una consulta de catálogo a DuckDB: ping, `version()`, catálogo de tipos de Npgsql, claves primarias de JDBC, literales de arreglo, `_compat_catalogo` (o su respaldo textual) y traducción de dialecto Postgres → DuckDB con sqlglot | `_duckdb` | Serving |
| `motor_from_database`, `_es_sonda`, `_consulta_vacia` | Reglas comunes: motor según `dbname`, consultas de los BI que no piden datos, ping | `server.py` | Serving |
| `_compat_catalogo`, `_compat_catalogo_texto`, `_consulta_npgsql`, `_literales_arreglo` | Las adaptaciones de la sección 14 | `reescribir` | Serving |

### 19.4.2 `server_old.py`: versión anterior sobre buenavista (Serving)

Se conserva como respaldo (`python -m semantic_gateway.server_old`). Sus funciones de catálogo son una copia de las
de `catalogo_pg.py`; las propias de buenavista son estas:

**Ajustes sobre buenavista, al cargar el módulo**

| Clase / función | Qué hace | La usa | Capa del ADR |
| --- | --- | --- | --- |
| `postgres.TYPE_OIDS` (ampliado) | Agrega tipos de parámetro que buenavista no conocía (texto, varchar, bool, numeric) y hace que el valor de muestra de un parámetro sin tipo sea `NULL` | buenavista (protocolo extendido) | Serving |
| `_ParserSeguro.parse(texto)` | Reemplaza al parser de fechas de buenavista: devuelve fecha si puede y texto si no. Evita que un parámetro de texto corte la conexión | buenavista (`handle_bind`) | Serving |
| `_to_bvtype_con_nulos(tipo)` | Reemplaza la conversión de tipos de buenavista: una columna `NULL` se informa como texto en vez de fallar | buenavista (resultados de DuckDB) | Serving |
| `_consulta_vacia(sql)` | Cierto para `''`, `';'` o solo comentarios: el «ping» de los drivers JDBC | `MotorHandler.handle_query`, `SemanticRewriter.rewrite` | Serving |

**Catálogo en DuckDB**

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `install_catalog(db, catalog)` | Crea el esquema `semantic`, una tabla virtual vacía por modelo (con comentarios de tabla y columna), las tablas `metrics`, `dimensions` y `metric_dimensions`, y las vistas, funciones y macros de compatibilidad | `build_server` | Serving (exponer el catálogo) |
| `_install_pg_views(db)` | Crea `gw_pg_type` y `gw_pg_attribute`, con los códigos de tipo reales de PostgreSQL (DuckDB los informa mal) | `install_catalog` | Serving |
| `_quote(texto)` | Escapa comillas simples para los comentarios SQL | `install_catalog` | Serving |
| `_PG_TIPOS`, `_FUNCIONES_PROPIAS`, `_VISTAS_PROPIAS`, `_PRIVILEGIOS`, `_REG_TIPOS` | Tablas de configuración de la compatibilidad: tipos de PostgreSQL, funciones reemplazadas por macros `gw_*`, vistas propias, funciones de permisos y casts `reg*` | `install_catalog`, `_compat_catalogo`, `_compat_catalogo_texto` | Serving |

**Sesión y enrutamiento**

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `motor_from_database(dbname, usuario, por_defecto)` | Traduce el `dbname` de la conexión a un motor: un motor conocido, el por defecto (`semantic`, vacío o igual al usuario) o un error que se informa en la primera consulta semántica | `MotorHandler.handle_startup` | Gateway (enrutador provisional: hoy elige el cliente) |
| `_es_sonda(sql)` | Detecta `SELECT … WHERE 1<>1` o `LIMIT 0`: consultas de los BI que no piden datos | `SemanticSession.execute_sql` | Serving |
| `SemanticSession` | Sesión de un cliente (una por conexión). Guarda `motor` y `motor_error` | `SemanticConnection.new_session` | Serving |
| `SemanticSession.execute_sql(sql, params)` | Ruta de cada consulta: catálogo y sondas → DuckDB; consulta semántica → `translate` → `explain` → imprime el SQL en el log → si hay ejecutor para el motor (y no es `EXPLAIN`), ejecuta y devuelve las filas; si no, devuelve el SQL en la columna `sql`. Rechaza parámetros y `dbname` inválido | buenavista | Serving; hoy cumple también el papel del gateway |
| `SemanticConnection` | Conexión a DuckDB compartida por todas las sesiones | `build_server` | Serving |
| `SemanticConnection.parameters()` | Parámetros que el servidor anuncia al cliente: `server_version 14.0`, `UTF8`, `DateStyle ISO` | buenavista, al conectar | Serving |
| `SemanticConnection.new_session()` | Abre un cursor de DuckDB con `search_path = semantic,main` y crea la `SemanticSession` | buenavista, al conectar | Serving |
| `SemanticRewriter` | Adapta al dialecto DuckDB lo que no es semántico | buenavista, antes de cada consulta | Serving |
| `SemanticRewriter.rewrite(sql)` | Deja pasar intactas las consultas semánticas. Al resto: responde ping, `version()`, `BEGIN READ ONLY`, `show timezone` y claves primarias de JDBC; aplica `_compat_catalogo` (o `_compat_catalogo_texto` si sqlglot no puede analizar) y traduce de PostgreSQL a DuckDB | buenavista | Serving |
| `_compat_catalogo(nodo)` | Transformación del árbol sqlglot: casts `::regnamespace`/`::regclass`/`::regtype` → subconsultas, `has_*_privilege` → `TRUE`, funciones de `_FUNCIONES_PROPIAS` → macros `gw_*`, tablas de `_VISTAS_PROPIAS` → vistas propias | `rewrite` (vía `transform` de sqlglot) | Serving |
| `_compat_catalogo_texto(sql)` | Lo mismo que `_compat_catalogo`, con expresiones regulares, para consultas que sqlglot no analiza (deja un `WARNING` en el log) | `rewrite` | Serving |

**Servidor y arranque**

| Clase / función | Qué hace | La llama | Capa del ADR |
| --- | --- | --- | --- |
| `MotorHandler` | Atiende una conexión (un hilo por cliente). Extiende el manejador de buenavista | `MotorServer` | Serving (endpoint Postgres wire) |
| `MotorHandler.handle_startup(conn)` | Tras el arranque de buenavista, lee `database` y `user`, fija el motor de la sesión y lo registra en el log (`Conexión · usuario=… dbname=… → motor=…`) | buenavista | Serving + Gateway (enrutador provisional) |
| `MotorHandler.handle_query(ctx, payload)` | Responde la consulta vacía (ping) con `EmptyQueryResponse`; el resto lo pasa a buenavista | buenavista | Serving |
| `MotorServer` | Servidor TCP de buenavista (`ThreadingTCPServer`) que usa `MotorHandler` | `build_server` | Serving |
| `build_server(host, puerto, usuario, clave, manifest, motor)` | Arma todo: `SemanticEngine`, catálogo, DuckDB en memoria, ajustes de buenavista y de logs, `Translator`, `SemanticRewriter` y `MotorServer`. Habilita conexiones remotas si `host` no es local | `main`, `start_in_thread` | Serving (arranque) |
| `start_in_thread(…)` | Igual que `build_server`, pero deja el servidor corriendo en un hilo de fondo | `tests/test_gateway.py` | Pruebas |
| `main()` | Punto de entrada de `python -m semantic_gateway`: lee argumentos y variables `SEMANTIC_GW_*`, configura el log y llama a `serve_forever` | `__main__.py` | Serving (arranque) |

### 19.5 `executor.py`: ejecución en Athena (Gateway, conector de motor)

| Clase / función | Qué hace | La usa | Capa del ADR |
| --- | --- | --- | --- |
| `ConfigAthena` | Configuración: región, workgroup, ubicación de resultados, perfil, catálogo, `timeout`, `max_filas` | `main` | Gateway |
| `EjecutorAthena(config)` | Abre una conexión de PyAthena (`ArrowCursor`); se crea una vez al arrancar | `main` | Gateway |
| `EjecutorAthena.ejecutar(sql)` | Lanza la consulta en Athena en un hilo, espera hasta `timeout` (si vence, `StopQueryExecution`), lee el resultado como tabla Arrow, corta en `max_filas` y devuelve un `Resultado` | `_semantica` (`server.py`), `SemanticSession.execute_sql` (`server_old.py`) | Gateway |
| `Resultado` | Tabla Arrow + `query_id`, segundos, bytes escaneados y si se truncó | `_semantica` (log) | Gateway |
| `ExecutionError` | Error de Athena o tiempo vencido; su mensaje llega al cliente como error SQL | `_semantica` | Gateway |
| `ordenar_columnas(tabla, salida)` | Pone las columnas en el orden del `SELECT` del cliente y con sus alias (MetricFlow devuelve primero las dimensiones; Athena, en minúsculas). Una dimensión de tiempo pedida sin grano (`event__event_date`) vuelve como `event__event_date__day` y se renombra | `_semantica` (`server.py`), `SemanticSession.execute_sql` (`server_old.py`) | Serving |
| `Ejecutor` (protocolo) | Interfaz común: `motor` y `ejecutar(sql) → Resultado`. Un ejecutor nuevo (Snowflake…) solo tiene que cumplirla | `build_server` | Gateway |

### 19.6 Recorrido de una consulta por las funciones

```
Cliente BI
  │  conexión (riffq: protocolo; TLS opcional, u obligatorio con PuertaTLS)
  ├─► ConexionSemantica.handle_auth ─► motor_from_database        (valida clave y fija el motor de la sesión)
  │
  │  consulta (riffq ─► handle_query ─► pool de hilos ─► _responder)
  └─► _resolver   (_original recupera el texto si riffq lo pasó a minúsculas)
         ├─ ping / BEGIN / SET / DISCARD ─► etiqueta sin efecto
         ├─ SHOW ─► _show
         ├─ no semántica ─► _duckdb ─► catalogo_pg.reescribir ─► DuckDB (catálogo, metadatos)
         └─ semántica ─► _semantica
              ├─ _es_sonda ─► _duckdb (tabla virtual vacía)
              Translator.translate
                 ├─ _check_shape
                 ├─ _column + Catalog.is_metric / is_dimension (o _unknown_column)
                 ├─ _conjuncts ─► _where ─► _time_constraint | _metricflow_filter (to_jinja)
                 └─ _order_name, LIMIT              ─► SemanticQuery
              ├─ describe (do_describe) ─► _esquema ─► tabla vacía con los tipos
              SemanticEngine.explain ─► _check ─► _engine_for ─► _request ─► MetricFlow
              ─► SQL del motor: se imprime en el log
              ├─ sin ejecutor o EXPLAIN ─► se devuelve en la columna sql
              └─ con ejecutor (athena) ─► EjecutorAthena.ejecutar ─► ordenar_columnas ─► _ajustar_tipos ─► filas
```
