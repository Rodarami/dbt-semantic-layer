# dbt_glue_lab: guía de configuración paso a paso

Proyecto dbt que transforma datos en **AWS Glue** (tablas **Iceberg** en S3, catálogo Glue) con capas bronze → silver → gold, y define una **capa semántica de MetricFlow** sobre esos modelos.

> Este proyecto vive en `MetricFlow/DBT/dbt_glue_lab/`. El gateway semántico, los scripts y los manifests están
> en la raíz `MetricFlow/`: ver [README de MetricFlow](../../README.md).

Esta guía recorre toda la configuración en orden. Cada paso indica qué archivo interviene y cómo comprobar que funcionó.

```
data/*.csv ──► bronze (lab_data_bronze) ──► silver (lab_data_silver) ──► gold (lab_data_gold)
                                                   │                        │
                                                   └──── capa semántica (YAML) ───► semantic_manifest.json
                                                                                         │
                                       mf (local, perfil DuckDB)  ◄──────────────────────┤
                                       gateway semántico (Postgres wire)  ◄──────────────┘  → DBeaver, Metabase, Power BI, psql
                                                                                              (compila y ejecuta en Athena)
```

---

## Índice

1. [Estructura del proyecto](#1-estructura-del-proyecto)
2. [Requisitos previos](#2-requisitos-previos)
3. [Entorno virtual e instalación](#3-entorno-virtual-e-instalación)
4. [Perfil de conexión a Glue](#4-perfil-de-conexión-a-glue)
5. [Configuración del proyecto (`dbt_project.yml`)](#5-configuración-del-proyecto-dbt_projectyml)
6. [Fuentes (bronze)](#6-fuentes-bronze)
7. [Modelos silver y gold](#7-modelos-silver-y-gold)
8. [Capa semántica](#8-capa-semántica)
9. [Generar y validar el `semantic_manifest.json`](#9-generar-y-validar-el-semantic_manifestjson)
10. [Explorar la capa con MetricFlow (`mf`)](#10-explorar-la-capa-con-metricflow-mf)
11. [Llevar el manifest a Snowflake](#11-llevar-el-manifest-a-snowflake)
    - [11 bis. Metric engine: compilar SQL sin conectarse a ningún motor](#11-bis-metric-engine-compilar-sql-sin-conectarse-a-ningún-motor)
    - [11 ter. Gateway semántico (Postgres wire)](#11-ter-gateway-semántico-postgres-wire)
12. [Consumir desde Power BI](#12-consumir-desde-power-bi)
13. [Problemas conocidos](#13-problemas-conocidos)
14. [Referencia de comandos `mf`](#14-referencia-de-comandos-mf)
15. [Estado frente al ADR de la capa semántica (CencoSL)](#15-estado-frente-al-adr-de-la-capa-semántica-cencosl)

---

## 1. Estructura del proyecto

```
DBT-GLUE/
├── data/                                    # CSV de prueba (event.csv, item.csv, ...)
├── dbt_venv/                                # entorno virtual (no se versiona)
├── ssl/                                     # certificado y clave TLS del gateway (fuera del repo)
└── MetricFlow/                              # raíz del repositorio git
    ├── semantic_gateway/                    # gateway semántico por protocolo Postgres (11 ter)
    ├── tests/                               # pruebas del gateway
    ├── script/                              # compilar_sin_conexion.py (11 bis), manifest_para_snowflake.py (11)
    ├── semantic_manifests/                  # un semantic_manifest.json por proyecto
    │   ├── dbt_glue_lab/semantic_manifest.json
    │   └── it-people/semantic_manifest.json
    ├── notebooks/                           # sqlglot_paso_a_paso.ipynb
    ├── docs/                                # gateway_semantico.md, arquitectura_gateway.md, consultas_*.md…
    ├── requirements-gateway.txt             # dependencias del gateway
    ├── README.md                            # guía de MetricFlow (gateway, scripts, manifests)
    └── DBT/
        └── dbt_glue_lab/                    # ESTE proyecto dbt
            ├── dbt_project.yml              # configuración del proyecto
            ├── profiles.yml                 # perfil de conexión a Glue (el que usa dbt run)
            ├── .env                         # ID de cuenta, bucket y rol (local, no se versiona; sección 4.1)
            ├── macros/
            │   └── generate_database_name.sql
            ├── models/
            │   ├── source_tables.yaml       # fuentes: lab_data_bronze.events / items
            │   ├── silver/
            │   │   ├── silver_event.sql
            │   │   ├── silver_item.sql
            │   │   └── schema.yml           # documentación + semantic models events e items
            │   ├── gold/
            │   │   ├── gold_top_item.sql
            │   │   └── schema.yml           # documentación + semantic model top_items
            │   └── semantic/
            │       ├── metrics.yml          # métricas ratio / derived
            │       ├── metricflow_time_spine.sql
            │       └── time_spine.yml       # tabla calendario de MetricFlow
            ├── mf_profiles/profiles.yml     # perfil DuckDB sólo para usar `mf` en local
            ├── sf_profiles/profiles.yml     # perfil Snowflake sólo para generar el manifest
            ├── evidencias/                  # evidencia: mf no soporta el adaptador glue
            ├── target/                      # salida de dbt parse (no se versiona)
            └── backup/                      # copias locales anteriores (no se versionan)
```

Flujo del manifest: `dbt parse` lo escribe en `target/semantic_manifest.json` de este proyecto, y se copia a
`MetricFlow/semantic_manifests/dbt_glue_lab/`, que es de donde lo leen el gateway y los scripts:

```bash
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json
```

---

## 2. Requisitos previos

| Requisito | Detalle |
|---|---|
| Python | **3.12** (Windows, Linux) o **3.14** (Mac Apple Silicon): son las versiones con paquete precompilado de riffq, el servidor del gateway. dbt solo funciona desde 3.10 |
| Credenciales AWS | Perfil o variables con permiso para asumir el rol de Glue (`aws sts get-caller-identity` debe responder) |
| Rol de Glue | `arn:aws:iam::<ID_CUENTA>:role/GlueInteractiveSessionRole`, usado por las sesiones interactivas |
| Bucket S3 | `s3://<BUCKET>` (warehouse de Iceberg) |
| Bases en el catálogo Glue | `lab_data_bronze` con las tablas `events` e `items` |
| Lake Formation | El rol del proyecto necesita permisos sobre las bases y tablas `lab_data_*` |
| Tabla de bloqueo DynamoDB | `myGlueLockTable` (la usa Iceberg, definida en `profiles.yml`) |

---

## 3. Entorno virtual e instalación

La capa semántica usa la **spec nueva de dbt**, que exige **dbt 1.12 o superior**. Con dbt 1.10 `dbt parse` falla (ver [problemas conocidos](#13-problemas-conocidos)).

```bash
cd ~/DBT-GLUE
python3.14 -m venv dbt_venv     # Mac Apple Silicon; en Linux, python3.12
source dbt_venv/bin/activate

python3 -m pip install --upgrade pip
python3 -m pip install "dbt-core>=1.12" "dbt-glue>=1.12.4" dbt-metricflow dbt-duckdb
```

| Paquete | Para qué |
|---|---|
| `dbt-core` + `dbt-glue` | Construir los modelos en Glue (`dbt run`, `dbt parse`) |
| `dbt-metricflow` | Instala el comando `mf` |
| `dbt-duckdb` | Perfil local para que `mf` funcione (`mf` no soporta el adaptador `glue`) |
| `dbt-snowflake` | Opcional: sólo si generas el manifest con `sf_profiles` (paso 11, opción B) |

Dependencias del gateway semántico (sección 11 ter), desde la raíz `MetricFlow/`:

```bash
python3 -m pip install -r requirements-gateway.txt   # metricflow, riffq, buenavista (respaldo), sqlglot, duckdb, pyarrow, pyathena, psycopg
python3 -m pip install jupyterlab ipykernel          # opcional, para los notebooks
```

Comprobar:

```bash
dbt --version     # Core 1.12.x y plugin glue 1.12.x
mf --version
python3 -c "import metricflow, riffq, sqlglot, duckdb, psycopg; print('gateway ok')"
```

Si `pip` falla con `CERTIFICATE_VERIFY_FAILED`:

```bash
python3 -m pip install --upgrade certifi
export SSL_CERT_FILE=$(python3 -c "import certifi; print(certifi.where())")
export REQUESTS_CA_BUNDLE=$SSL_CERT_FILE
# y repetir la instalación
```

Si también falla `certifi`, ejecuta `"/Applications/Python 3.14/Install Certificates.command"` (Python instalado desde python.org; ajustar la versión).

---

## 4. Perfil de conexión a Glue

Archivo: `dbt_glue_lab/profiles.yml`. dbt lo encuentra porque está en la carpeta desde la que se ejecuta, siempre que `DBT_PROFILES_DIR` no esté definido.

Campos principales:

| Campo | Valor | Significado |
|---|---|---|
| `type` | `glue` | Adaptador dbt-glue |
| `role_arn` | `arn:aws:iam::{{ env_var('AWS_ACCOUNT_ID') }}:role/{{ env_var('GLUE_ROLE_NAME', …) }}` | Rol que asume la sesión interactiva |
| `region` | `us-east-1` | Región AWS |
| `glue_version` | `"5.0"` | Versión de Glue |
| `workers` / `worker_type` | `2` / `G.1X` | Capacidad de la sesión |
| `idle_timeout` | `2` | Minutos sin uso antes de cerrar la sesión |
| `session_provisioning_timeout_in_seconds` | `300` | Espera máxima para crear la sesión |
| `glue_session_reuse` | `True` | Reutiliza la sesión entre comandos |
| `schema` | `lab_data` | Prefijo de las bases (`lab_data_silver`, `lab_data_gold`) |
| `location` | `s3://{{ env_var('DBT_S3_BUCKET') }}` | Ubicación base de las tablas |
| `datalake_formats` | `iceberg` | Formato de tablas |
| `conf` | `--conf spark...` | Catálogo Iceberg sobre Glue, lock en DynamoDB y OpenLineage |

### 4.1 Variables de entorno (`.env`)

El ID de cuenta, el bucket y el rol no están escritos en el código: `profiles.yml` y los modelos los leen con
`env_var()`. Sus valores van en `dbt_glue_lab/.env`, que **no se versiona** (`.gitignore`):

```bash
# dbt_glue_lab/.env
AWS_ACCOUNT_ID=<ID_CUENTA>
DBT_S3_BUCKET=<BUCKET>
GLUE_ROLE_NAME=GlueInteractiveSessionRole
```

| Variable | La usan | Para qué |
|---|---|---|
| `AWS_ACCOUNT_ID` | `profiles.yml` | ARN del rol de Glue y `spark.glue.accountId` |
| `DBT_S3_BUCKET` | `profiles.yml`, `custom_location` de los modelos | Warehouse de Iceberg y ubicación de cada tabla |
| `GLUE_ROLE_NAME` | `profiles.yml` | Nombre del rol (por defecto `GlueInteractiveSessionRole`) |

Cargarlas en cada terminal nueva, antes de **cualquier** comando dbt (también `dbt parse`, porque los modelos usan
`env_var()` en su configuración):

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a     # set -a exporta las variables para que dbt las vea
echo $DBT_S3_BUCKET             # comprobar
```

Si falta una, dbt falla con `Env var required but not provided: 'AWS_ACCOUNT_ID'`.

### 4.2 Comprobar la conexión

Comprobar la conexión (crea una sesión de Glue, puede tardar unos minutos):

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
unset DBT_PROFILES_DIR
dbt debug
```

---

## 5. Configuración del proyecto (`dbt_project.yml`)

```yaml
models:
  dbt_glue_lab:
    bronze: { +schema: bronze, +table_type: iceberg }
    silver: { +schema: silver, +table_type: iceberg }
    gold:   { +schema: gold,   +table_type: iceberg }
```

dbt arma el nombre de cada base como `<schema del perfil>_<schema del modelo>`:

| Carpeta | Base en Glue |
|---|---|
| `models/silver` | `lab_data_silver` |
| `models/gold` | `lab_data_gold` |
| `models/semantic` (sin `+schema`) | `lab_data` |

La macro `macros/generate_database_name.sql` (`glue__generate_database_name`) hace que, en Glue, la "base" sea el `schema` del perfil.

---

## 6. Fuentes (bronze)

Archivo: `models/source_tables.yaml`.

| Fuente dbt | Tabla en Glue | Origen de datos |
|---|---|---|
| `source('raw', 'events')` | `lab_data_bronze.events` | `data/event.csv` (event_id, event_time, user_id, payload JSON) |
| `source('raw', 'items')` | `lab_data_bronze.items` | `data/item.csv` (id, name, category, adjective, modifier, price, created_at) |

Las tablas bronze deben existir antes de correr los modelos. `data/yellow_tripdata_sample_2019-01.csv` no lo usa ningún modelo.

---

## 7. Modelos silver y gold

| Modelo | Base | Grano | Qué hace |
|---|---|---|---|
| `silver_event` | `lab_data_silver` | 1 fila por evento | Extrae `event_name`, `platform`, `parameter_name`, `parameter_value` del JSON; agrega `event_date` y `event_year` |
| `silver_item` | `lab_data_silver` | 1 fila por item | Tipifica id y precio; reemplaza nulos de `adjective`/`modifier` por `'unknown'`; agrega `created_at_date` |
| `gold_top_item` | `lab_data_gold` | 1 fila por (item, año) | Vistas por item y año, plataforma más usada y ranking anual |

Todos son `incremental` con `insert_overwrite`, Iceberg v2 y `custom_location` en S3.

Construir:

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
unset DBT_PROFILES_DIR
dbt run -s silver_event silver_item     # silver
dbt run -s gold_top_item                # gold
dbt run                                 # todo (incluye la tabla calendario del paso 8)
```

Documentación navegable:

```bash
dbt docs generate
dbt docs serve
```

---

## 8. Capa semántica

Se usa la **spec nueva de dbt** (dbt 1.12+): el semantic model se declara dentro del YAML del modelo (`semantic_model: enabled: true`), las entidades y dimensiones en cada columna, y las métricas simples dentro del modelo.

### 8.1 Semantic models

| Semantic model | Modelo dbt | Entidades | Tiempo (`agg_time_dimension`) | Dimensiones |
|---|---|---|---|---|
| `events` | `silver_event` | `event` (primaria), `user` (foránea) | `event_date` (día) | `event_name`, `platform`, `parameter_name`, `event_year` |
| `items` | `silver_item` | `item` (primaria) | `created_at_date` (día) | `item_name`, `category`, `adjective`, `modifier` |
| `top_items` | `gold_top_item` | `item` (foránea; `primary_entity: item`) | — (no tiene columna fecha) | `event_year`, `most_used_platform`, `item_rank_in_year` |

### 8.2 Métricas

| Métrica | Tipo | Definición | Archivo | Tabla en el gateway |
|---|---|---|---|---|
| `event_count` | simple | `count_distinct(event_id)` | `silver/schema.yml` | `semantic.events` |
| `unique_users` | simple | `count_distinct(user_id)` | `silver/schema.yml` | `semantic.events` |
| `item_views` | simple | `count_distinct(event_id)` donde `event_name = 'view_item'` | `silver/schema.yml` | `semantic.events` |
| `profile_views` | simple | `count_distinct(event_id)` donde `event_name = 'view_user_profile'` | `silver/schema.yml` | `semantic.events` |
| `item_count` | simple | `count_distinct(item_id)` | `silver/schema.yml` | `semantic.items` |
| `total_item_price` | simple | `sum(price)` | `silver/schema.yml` | `semantic.items` |
| `avg_item_price` | simple | `average(price)` | `silver/schema.yml` | `semantic.items` |
| `max_item_price` | simple | `max(price)` | `silver/schema.yml` | `semantic.items` |
| `item_views_per_user` | ratio | `item_views / unique_users` | `semantic/metrics.yml` | `semantic.events` |
| `item_views_share` | ratio | `item_views / event_count` | `semantic/metrics.yml` | `semantic.events` |
| `engagement_events` | derived | `item_views + profile_views` | `semantic/metrics.yml` | `semantic.events` |

**Qué mide cada métrica.** En las métricas de eventos, el tiempo (`metric_time`) es la fecha del evento
(`event_date`); en las de items, la fecha de creación del item (`created_at_date`).

- **`event_count` (Eventos).** Volumen total de actividad de la aplicación: cuántos eventos distintos se
  registraron, sea cual sea su tipo (vista de item, vista de perfil o asignación de test). Cada `event_id` cuenta una
  vez. Responde «¿cuánta actividad hubo?».
- **`unique_users` (Usuarios únicos).** Cuántas personas distintas generaron al menos un evento en el periodo y la
  agrupación pedidos. Un usuario con 100 eventos cuenta 1. **No es sumable**: los usuarios únicos de un trimestre no
  son la suma de los de sus meses, porque la misma persona puede estar activa en varios. Siempre hay que pedirla
  con la agrupación final.
- **`item_views` (Vistas de items).** Cuántas veces se vio la ficha de un item (eventos `view_item`). Mide el interés
  por el catálogo. El filtro por tipo de evento es parte de la definición: quien consulta no tiene que agregarlo.
- **`profile_views` (Vistas de perfil).** Cuántas veces se vio el perfil de otro usuario (eventos
  `view_user_profile`). Mide la actividad social o de exploración entre usuarios.
- **`item_count` (Items).** Cuántos items distintos hay en el catálogo. Agrupada por tiempo, cuenta los items
  **creados** en cada periodo; por categoría, el tamaño de cada categoría.
- **`total_item_price` (Suma de precios).** Suma de los precios de lista de los items: el valor total del catálogo,
  o de una categoría o periodo de creación. **No son ventas**: el modelo de items no tiene transacciones.
- **`avg_item_price` (Precio promedio).** Precio medio de los items del grupo pedido. **No es sumable ni
  promediable** entre grupos: el promedio de dos categorías no es el promedio de sus promedios.
- **`max_item_price` (Precio máximo).** Precio del item más caro del grupo pedido. Sirve para detectar valores
  extremos.
- **`item_views_per_user` (Vistas de items por usuario).** Intensidad de uso: cuántas fichas de items ve, en
  promedio, cada usuario activo (`item_views / unique_users`). El denominador incluye a todos los usuarios con algún
  evento, no solo a los que vieron items. MetricFlow calcula numerador y denominador por separado y recién después
  divide.
- **`item_views_share` (Porcentaje de eventos que son vistas de items).** Composición de la actividad: qué
  fracción de todos los eventos son vistas de items (`item_views / event_count`), entre 0 y 1. Por ejemplo, 0,4
  significa que el 40 % de los eventos fueron vistas de items.
- **`engagement_events` (Vistas de items + perfiles).** Interacciones de contenido: vistas de items más vistas de
  perfil. Excluye las asignaciones de test, que son eventos técnicos. Siempre cumple
  `engagement_events = item_views + profile_views` para el mismo filtro y agrupación.

### 8.3 Tabla calendario (time spine)

`models/semantic/metricflow_time_spine.sql` genera una fila por día (2010-01-01 a 2030-12-31) y `time_spine.yml` la registra como time spine. dbt 1.12 **no parsea** el proyecto sin ella. MetricFlow la usa para métricas acumuladas, desfases de tiempo y `join_to_timespine`. Las métricas actuales no la necesitan, pero debe existir en el warehouse antes de usar esas funciones:

```bash
dbt run -s metricflow_time_spine     # crea lab_data.metricflow_time_spine en Glue
```

### 8.4 Limitaciones del diseño actual

- Las métricas de `events` **no se pueden agrupar por dimensiones de `items`** (`item__category`, etc.): en `silver_event` el id del item viene como texto en `parameter_value` y no hay entidad `item`. Para habilitarlo habría que agregar a `silver_event` una columna `item_id` convertida a `BIGINT`.
- `top_items` no define métricas porque `gold_top_item` no tiene columna de fecha (sólo `event_year` entero). Para agregarlas, crear una columna `date` (p. ej. `make_date(event_year, 1, 1)`) y declararla como dimensión de tiempo.

---

## 9. Generar y validar el `semantic_manifest.json`

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
unset DBT_PROFILES_DIR      # perfil de Glue
dbt parse
```

`dbt parse` lee modelos y YAML, valida la capa semántica y escribe `target/semantic_manifest.json`. **No se conecta al warehouse.** Repetirlo cada vez que cambie un modelo o un YAML.

Ver las rutas de tabla que quedaron en el manifest:

```bash
python3 -c "
import json
m = json.load(open('target/semantic_manifest.json'))
for s in m['semantic_models']:
    print(s['name'], s['node_relation']['relation_name'])
"
```

Con el perfil de Glue salen rutas de dos partes, por ejemplo `lab_data_silver.silver_event`.

> El manifest tiene dos partes. La **lógica** (entidades, dimensiones, métricas) no depende del motor. Las **rutas físicas** (`node_relation`) indican dónde buscar cada tabla. El dialecto SQL no va en el manifest: lo elige MetricFlow según el motor al que se conecte. Un mismo manifest sirve en cualquier motor soportado **si las rutas existen en ese motor**.

---

## 10. Explorar la capa con MetricFlow (`mf`)

`mf` **no soporta el adaptador `glue`**: con el perfil de Glue falla con `Adapter type glue is not supported` (evidencia en `evidencias/evidencias.md`). Acepta athena, databricks, postgres, snowflake, redshift, bigquery, duckdb, trino y vertica.

Para explorar y ver el SQL en local se usa `mf_profiles/profiles.yml`, un perfil **DuckDB con el mismo nombre** (`dbt_glue_lab`) que no se conecta a AWS:

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
export DBT_PROFILES_DIR=$PWD/mf_profiles
dbt parse
mf validate-configs --skip-dw
mf list metrics
mf query --metrics item_views --group-by metric_time__month,event__platform --explain
unset DBT_PROFILES_DIR                      # volver al perfil de Glue
```

Con este perfil:
- El SQL sale en **dialecto DuckDB**, útil para revisar la lógica, pero no es el SQL exacto de Spark.
- Usar siempre `--explain`: las tablas no existen en DuckDB, así que ejecutar la consulta falla.

La lista completa de comandos está en la [sección 14](#14-referencia-de-comandos-mf).

---

## 11. Llevar el manifest a Snowflake

Objetivo: que el SQL que compila MetricFlow (sección 11 bis y gateway de la 11 ter) pueda ejecutarse en **Snowflake**, que lee las tablas Iceberg de Glue como tablas externas.

### 11.1 Preparar Snowflake

1. Crear una integración de catálogo con Glue (`CREATE CATALOG INTEGRATION ... CATALOG_SOURCE = GLUE`), con la política IAM (`glue:GetTable`, `glue:GetTables`) y la política de confianza con el usuario y external ID que entrega Snowflake. Snowflake recomienda usar el endpoint REST de Iceberg de Glue.
2. Crear una tabla Iceberg externa por modelo: `silver_event`, `silver_item`, `gold_top_item` (y `metricflow_time_spine` si se usarán métricas que la necesitan). Las columnas deben llamarse igual que en los modelos dbt.
3. Programar `ALTER ICEBERG TABLE ... REFRESH` para que Snowflake vea los cambios de Glue.
4. Probar con un conteo que coincida con Glue:
   ```sql
   SELECT COUNT(*) FROM <base>.<esquema>.silver_event;
   ```
   Si las tablas se crearon con nombres entre comillas en minúsculas, la ruta sin comillas que genera MetricFlow no las encontrará.

### 11.2 Elegir cómo alinear las rutas

| Situación en Snowflake | Qué hacer |
|---|---|
| Esquemas con los **mismos nombres** que en Glue (`lab_data_silver`, `lab_data_gold`) | Nada: usar el manifest de Glue tal cual y poner la base en el perfil del servidor (opción A) |
| Base y esquemas **distintos** (p. ej. `customer.silver.silver_event`) | Reescribir rutas con el script (opción B) |

**Opción A: mismos nombres que Glue (recomendada).** El manifest de Glue trae rutas sin base (`lab_data_silver.silver_event`). Snowflake las busca en la base configurada en el perfil del servidor (`database:`), así que no hay que modificar el manifest.

**Opción B: script, sin cambiar de adaptador.** Se genera el manifest con Glue y se reescriben las rutas:

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
unset DBT_PROFILES_DIR
dbt parse
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json
cd ../..                                     # raíz MetricFlow/
python3 script/manifest_para_snowflake.py --database customer \
  --schema-map lab_data_silver=silver,lab_data_gold=gold
# -> semantic_manifests/dbt_glue_lab/semantic_manifest_snowflake.json
```

Resultado:

```
events                -> customer.silver.silver_event
items                 -> customer.silver.silver_item
top_items             -> customer.gold.gold_top_item
metricflow_time_spine -> customer.lab_data.metricflow_time_spine
```

- `--database` es obligatorio; `--schema-map` es opcional (`origen=destino,...`).
- La tabla calendario está en `lab_data`; agregar p. ej. `lab_data=semantic` para moverla.
- El original queda intacto en `semantic_manifests/dbt_glue_lab/semantic_manifest.json`.
- El script depende del formato interno del manifest (`node_relation`); volver a probarlo al actualizar dbt.

**Opción C: generar el manifest con un perfil de Snowflake.** Requiere instalar `dbt-snowflake`. `sf_profiles/profiles.yml` lee base, esquema y credenciales de variables de entorno (`dbt parse` no se conecta, así que sirven valores de relleno):

```bash
export DBT_PROFILES_DIR=$PWD/sf_profiles
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_SCHEMA=lab_data
dbt parse     # rutas: ANALYTICS.lab_data_silver.silver_event, ...
unset DBT_PROFILES_DIR
```

Con esta opción el esquema sigue siendo `<SNOWFLAKE_SCHEMA>_silver`; si Snowflake usa otros esquemas, conviene la opción B.

---

## 11 bis. Metric engine: compilar SQL sin conectarse a ningún motor

El ADR de la capa semántica define que MetricFlow **sólo compila** y que otro componente (el gateway) ejecuta. El CLI `mf` no sirve para eso, porque siempre abre una conexión al warehouse. `MetricFlow/script/compilar_sin_conexion.py` usa la **librería `metricflow`** directamente, con un cliente SQL que sólo aporta el dialecto y falla si algo intenta ejecutar.

```bash
cd ~/DBT-GLUE/MetricFlow/DBT/dbt_glue_lab
set -a; source .env; set +a   # variables AWS_ACCOUNT_ID, DBT_S3_BUCKET… (sección 4.1)
unset DBT_PROFILES_DIR
dbt parse                                   # manifest de Glue, sin conexión
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json
cd ../..                                    # raíz MetricFlow/

python3 script/compilar_sin_conexion.py --motor athena \
  --metrics item_views --group-by metric_time__month,event__platform

python3 script/compilar_sin_conexion.py --motor snowflake \
  --manifest semantic_manifests/dbt_glue_lab/semantic_manifest_snowflake.json \
  --metrics item_views_per_user --group-by metric_time__year \
  --where "{{ Dimension('event__platform') }} = 'web'" --limit 100
```

- Motores: `athena`, `snowflake`, `databricks`, `redshift`, `bigquery`, `postgres`, `trino`, `duckdb`.
- Las rutas de tabla las toma del manifest. Con el manifest de Glue, Athena funciona tal cual (`lab_data_silver.silver_event`). Para otro motor, usar el manifest reescrito del paso 11.
- Requiere `pip install metricflow` (se instala con `dbt-metricflow`); no requiere `dbt-glue`, credenciales ni red.
- Es la base del módulo `gateway/compile` del serving layer.

## 11 ter. Gateway semántico (Postgres wire)

> Documentación completa (arquitectura, código, compatibilidad con BI, ejemplos con respuesta, errores y próximos
> pasos): [docs/gateway_semantico.md](../../docs/gateway_semantico.md).

El paquete `MetricFlow/semantic_gateway/` es un servidor que **habla el protocolo de PostgreSQL**. Publica la capa semántica
como **una tabla virtual por modelo semántico** con métricas, para cualquier manifest (con este proyecto,
`semantic.events` y `semantic.items`). Cualquier cliente Postgres
(psql, psycopg/Jupyter, DBeaver, Metabase, Power BI) se conecta y pide métricas y dimensiones como columnas.

MetricFlow **solo compila** la consulta, sin conectarse a ningún motor. Con `--ejecutar`, el gateway **ejecuta ese
SQL en Athena** (`executor.py`) y devuelve las filas; el SQL queda en el log. Sin `--ejecutar` (o con `EXPLAIN`),
devuelve el SQL como una fila con la columna `sql`.

```
cliente BI ──Postgres wire──► server.py (riffq; TLS opcional u obligatorio)
                                 ├─ metadatos ──► catalogo_pg.py · DuckDB en memoria (tablas vacías + catálogo)
                                 └─ semantic.<modelo> (aquí events, items) ──► translator.py (sqlglot) ──► engine.py (MetricFlow)
                                                                                                        └─► executor.py (Athena) o SQL
```

| Archivo | Rol |
| --- | --- |
| `MetricFlow/semantic_gateway/model.py` | Estructuras: `TableInfo`, `Catalog`, `SemanticQuery` |
| `MetricFlow/semantic_gateway/translator.py` | SQL del cliente → `SemanticQuery`; valida que las columnas sean de la tabla consultada |
| `MetricFlow/semantic_gateway/engine.py` | Carga el manifest, arma una tabla por modelo semántico y compila con MetricFlow, sin conexión |
| `MetricFlow/semantic_gateway/executor.py` | Ejecuta en Athena el SQL compilado (`--ejecutar`): tiempo máximo con cancelación, límite de filas |
| `MetricFlow/semantic_gateway/server.py` | Servidor Postgres sobre riffq (Rust, TLS, autenticación), motor por conexión y enrutamiento |
| `MetricFlow/semantic_gateway/catalogo_pg.py` | Catálogo PostgreSQL en DuckDB y compatibilidad con los drivers (DBeaver, Metabase, Npgsql) |
| `MetricFlow/semantic_gateway/tls_obligatorio.py` | Exige TLS delante de riffq (`--tls-obligatorio`) |
| `MetricFlow/semantic_gateway/server_old.py` | Versión anterior sobre buenavista, como respaldo |
| `MetricFlow/tests/` | `test_translator.py`, `test_gateway.py` y `probar_catalogo_riffq.py` |
| `MetricFlow/notebooks/sqlglot_paso_a_paso.ipynb` | Recorrido del traductor paso a paso |

Qué hace cada clase y función, quién la llama y a qué capa del ADR pertenece (serving layer, gateway o metric engine):
[docs/gateway_semantico.md, sección 19](../../docs/gateway_semantico.md#19-referencia-de-funciones).

### Qué publica

| Tabla | Contenido |
| --- | --- |
| `semantic.events` | 7 métricas de eventos (`event_count`, `unique_users`, `item_views`, `profile_views`, `item_views_per_user`, `item_views_share`, `engagement_events`) y sus dimensiones (`event__*`, `metric_time__*`) |
| `semantic.items` | 4 métricas de items (`item_count`, `total_item_price`, `avg_item_price`, `max_item_price`) y sus dimensiones (`item__*`, `metric_time__*`) |
| `semantic.metrics` | Catálogo: una fila por métrica, con `table_name` (tabla a la que pertenece) |
| `semantic.dimensions` | Catálogo: una fila por dimensión, con tipo y descripción |
| `semantic.metric_dimensions` | Catálogo: qué dimensiones admite cada métrica |

Cada tabla virtual tiene solo las dimensiones que se pueden cruzar con todas sus métricas, así que cualquier
combinación de sus columnas es consultable. Las tablas se generan desde el manifest: un modelo semántico nuevo con
métricas aparece como tabla sin tocar el código.

### Instalar y probar

```bash
cd ~/DBT-GLUE && source dbt_venv/bin/activate
cd MetricFlow/DBT/dbt_glue_lab
unset DBT_PROFILES_DIR && dbt parse         # genera target/semantic_manifest.json
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json
cd ../..                                    # raíz MetricFlow/
python3 -m pip install -r requirements-gateway.txt
python -m tests.test_translator             # traductor, sin base de datos
python -m tests.test_gateway                # integración por protocolo Postgres (servidor riffq)
SEMANTIC_GW_SERVER=old python -m tests.test_gateway   # lo mismo contra el servidor anterior (buenavista)
```

riffq necesita Python 3.12 (Windows y Linux) o 3.14 (Mac Apple Silicon). Arquitectura y comandos de Windows: README
de la raíz `MetricFlow/`. Consultas listas para usar: [docs/consultas_dbt_glue_lab.md](../../docs/consultas_dbt_glue_lab.md)
e [it-people](../../docs/consultas_it_people.md).

### Levantar

```bash
# desde la raíz MetricFlow/
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena                 # solo local
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena --host 0.0.0.0  # Metabase en Docker
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --port 5434     # otro proyecto
python -m semantic_gateway.server_old --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena      # servidor anterior (buenavista)

# ejecutando en Athena (filas al cliente, SQL al log) y con TLS obligatorio
set -a; source DBT/dbt_glue_lab/.env; set +a
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --ejecutar --region us-east-1 --athena-workgroup primary --athena-output "s3://$DBT_S3_BUCKET/athena-results/" \
  --tls-cert ../ssl/server.crt --tls-key ../ssl/server.key --tls-obligatorio
```

Opciones: `--motor` (por defecto `athena`), `--host` (`127.0.0.1`), `--port` (`5433`), `--user`/`--password`
(`bi`/`bi`), `--log-level` (`INFO`); ejecución: `--ejecutar`, `--athena-workgroup`, `--athena-output`, `--region`,
`--aws-profile`, `--timeout` (100 s), `--max-filas`; TLS: `--tls-cert`, `--tls-key`, `--tls-obligatorio`. También
por variables `SEMANTIC_GW_*`. El manifest se lee al arrancar: después de cada `dbt parse` hay que reiniciar el
gateway. Detalle de cada opción: [docs/gateway_semantico.md, sección 10](../../docs/gateway_semantico.md#10-levantar-el-gateway).

### Credenciales de conexión

Credenciales por defecto del gateway (entorno local de pruebas):

| Campo | DBeaver (en tu máquina) | Metabase en Docker |
| --- | --- | --- |
| Tipo / driver | PostgreSQL | PostgreSQL |
| Host | `127.0.0.1` | `host.docker.internal` |
| Puerto | `5433` | `5433` (no dejarlo vacío: el valor por defecto es 5432) |
| Base de datos | Motor del SQL: `athena`, `snowflake`, `redshift`, `databricks`, `trino`, `bigquery`, `postgres` o `duckdb` | Igual que DBeaver |
| Usuario | `bi` | `bi` |
| Contraseña | `bi` | `bi` |
| SSL | Desactivado (`require` si el gateway usa TLS) | Igual que DBeaver |
| Esquema | `semantic` | *Esquemas → Solo estos:* `semantic` |

Cadena de conexión (JDBC, DBeaver): `jdbc:postgresql://127.0.0.1:5433/athena?user=bi&password=bi&sslmode=disable`

Requisitos y notas:

* El gateway debe estar corriendo. Para Metabase en Docker, levantarlo con `--host 0.0.0.0`; para DBeaver basta el
  valor por defecto (`127.0.0.1`).
* En DBeaver, en la pestaña *PostgreSQL* de la conexión, desactivar *Show all databases*.
* En Metabase, poner el escaneo de valores de filtro en *Nunca* y pulsar *Sincronizar el esquema ahora*.
* Usuario y clave se cambian con `--user` / `--password` o con `SEMANTIC_GW_USER` / `SEMANTIC_GW_PASSWORD`. `bi`/`bi`
  es solo para pruebas locales: fuera de tu máquina, usar otra clave y TLS (`--tls-cert`, `--tls-key`, `--tls-obligatorio`).

### Elegir el motor en la conexión

El nombre de la base de datos (`dbname`) elige el dialecto del SQL compilado:

| `dbname` | Motor usado |
| --- | --- |
| `athena`, `bigquery`, `databricks`, `duckdb`, `postgres`, `redshift`, `snowflake`, `trino` | Ese motor |
| `semantic` o vacío | El de `--motor` |
| Otro | Conecta, pero las consultas semánticas responden *Base de datos 'x' no existe…* |

### Consultar

```bash
PGPASSWORD=bi psql "host=127.0.0.1 port=5433 user=bi dbname=snowflake"
```
```sql
SELECT metric_time__month, event__platform, item_views
FROM   semantic.events
WHERE  event__platform IN ('ios','android') AND metric_time >= DATE '2024-01-01'
ORDER  BY 1 LIMIT 100;                      -- dbname=snowflake: devuelve el SQL de Snowflake (solo Athena se ejecuta)

SELECT item__category, total_item_price FROM semantic.items;
SELECT metric, label, type, table_name FROM semantic.metrics;
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'item_views';
```

Reglas: métrica = columna (sola o dentro de `SUM/MAX/MIN/AVG/MEASURE/AGG`; la agregación la decide MetricFlow); las
dimensiones del `SELECT` son el `GROUP BY` implícito; `metric_time*` con `>=`, `<=`, `=` o `BETWEEN` es rango de
tiempo; el resto del `WHERE` son filtros de dimensión; se admiten `ORDER BY`, `LIMIT`, `DISTINCT` y `EXPLAIN`. Se
rechazan `SELECT *`, columnas de dos tablas, expresiones (`COUNT(*)`), joins, subconsultas, `HAVING`, `OFFSET`,
filtros sobre métricas y parámetros.

### Ejemplos para una plataforma de BI

Cinco consultas típicas de un dashboard. Se escriben tal cual en el editor SQL de la herramienta: en Metabase,
*Nuevo → Consulta SQL* sobre la conexión del gateway; en DBeaver, un *SQL Editor* sobre la conexión; en Power BI,
*Base de datos PostgreSQL* con la consulta en *Opciones avanzadas* (modo Importar).

Para cada ejemplo se muestra la consulta que escribe el analista, el SQL que genera el gateway (motor `athena`, sin
los comentarios de MetricFlow) y qué resultado da. Con `--ejecutar` el gateway devuelve esas filas; sin esa opción
devuelve el SQL en la columna `sql`. Los alias numerados (`events_src_10000`, `subq_4`) cambian entre ejecuciones.
Más consultas, con su descripción: [docs/consultas_dbt_glue_lab.md](../../docs/consultas_dbt_glue_lab.md).

#### 1. Tarjeta de KPI: actividad del año

Pregunta: *¿cuántos eventos hubo en 2024 y cuántas personas los generaron?*

```sql
SELECT event_count, unique_users
FROM semantic.events
WHERE metric_time BETWEEN '2024-01-01' AND '2024-12-31'
```

SQL generado:

```sql
SELECT
  COUNT(DISTINCT event_id) AS event_count
  , COUNT(DISTINCT user_id) AS unique_users
FROM lab_data_silver.silver_event events_src_10000
WHERE DATE_TRUNC('day', event_date) BETWEEN timestamp '2024-01-01' AND timestamp '2024-12-31'
```

**Resultado:** una fila con dos columnas. `event_count` es el número de eventos distintos de 2024 y `unique_users`
el número de usuarios distintos que generaron al menos un evento ese año. En Metabase se visualiza como dos
tarjetas de tipo *Número*. Ojo: los usuarios únicos del año **no** son la suma de los usuarios únicos de cada mes,
porque una persona activa en varios meses cuenta una sola vez. Por eso se piden al gateway y no se suman en el BI.

#### 2. Serie temporal: vistas de items por mes y plataforma

Pregunta: *¿cómo evolucionan las vistas de items cada mes, por plataforma, desde enero de 2024?*

```sql
SELECT metric_time__month, event__platform, item_views
FROM semantic.events
WHERE metric_time >= '2024-01-01'
ORDER BY metric_time__month, event__platform
```

SQL generado:

```sql
SELECT
  metric_time__month
  , event__platform
  , COUNT(DISTINCT item_views) AS item_views
FROM (
  SELECT
    DATE_TRUNC('month', event_date) AS metric_time__month
    , event_name AS event__event_name
    , platform AS event__platform
    , event_id AS item_views
  FROM lab_data_silver.silver_event events_src_10000
  WHERE DATE_TRUNC('day', event_date) BETWEEN timestamp '2024-01-01' AND timestamp '2040-12-31'
) subq_4
WHERE event__event_name = 'view_item'
GROUP BY
  metric_time__month
  , event__platform
ORDER BY metric_time__month, event__platform
```

**Resultado:** una fila por cada combinación de mes y plataforma, con el número de vistas de items
(`event_name = 'view_item'`, condición que sale de la definición de `item_views` en el YAML, no de la consulta). Se
visualiza como gráfico de líneas: mes en el eje X, una línea por plataforma. El rango sin fecha de fin aparece como
`BETWEEN '2024-01-01' AND '2040-12-31'`: MetricFlow completa el extremo abierto con su fecha máxima.

#### 3. Ratios por plataforma

Pregunta: *¿en qué plataforma cada usuario ve más items, y qué parte de la actividad son vistas de items?*

```sql
SELECT event__platform, item_views_per_user, item_views_share
FROM semantic.events
ORDER BY item_views_per_user DESC
```

<details>
<summary>SQL generado (73 líneas)</summary>

```sql
WITH sma_10001_cte AS (
  SELECT
    event_name AS event__event_name
    , platform AS event__platform
    , event_id AS __event_count
    , user_id AS __unique_users
    , event_id AS __item_views
  FROM lab_data_silver.silver_event events_src_10000
)

, cm_5_cte AS (
  SELECT
    event__platform
    , COUNT(DISTINCT item_views) AS item_views
  FROM (
    SELECT
      event__event_name
      , event__platform
      , __item_views AS item_views
    FROM sma_10001_cte
  ) subq_2
  WHERE event__event_name = 'view_item'
  GROUP BY
    event__platform
)

SELECT
  COALESCE(subq_12.event__platform, subq_20.event__platform) AS event__platform
  , MAX(CAST(subq_12.item_views AS DOUBLE) / CAST(NULLIF(subq_12.unique_users, 0) AS DOUBLE)) AS item_views_per_user
  , MAX(CAST(subq_20.item_views AS DOUBLE) / CAST(NULLIF(subq_20.event_count, 0) AS DOUBLE)) AS item_views_share
FROM (
  SELECT
    COALESCE(cm_5_cte.event__platform, subq_11.event__platform) AS event__platform
    , MAX(cm_5_cte.item_views) AS item_views
    , MAX(subq_11.unique_users) AS unique_users
  FROM cm_5_cte
  FULL OUTER JOIN (
    SELECT
      event__platform
      , COUNT(DISTINCT __unique_users) AS unique_users
    FROM sma_10001_cte
    GROUP BY
      event__platform
  ) subq_11
  ON
    cm_5_cte.event__platform = subq_11.event__platform
  GROUP BY
    COALESCE(cm_5_cte.event__platform, subq_11.event__platform)
) subq_12
FULL OUTER JOIN (
  SELECT
    COALESCE(cm_5_cte.event__platform, subq_19.event__platform) AS event__platform
    , MAX(cm_5_cte.item_views) AS item_views
    , MAX(subq_19.event_count) AS event_count
  FROM cm_5_cte
  FULL OUTER JOIN (
    SELECT
      event__platform
      , COUNT(DISTINCT __event_count) AS event_count
    FROM sma_10001_cte
    GROUP BY
      event__platform
  ) subq_19
  ON
    cm_5_cte.event__platform = subq_19.event__platform
  GROUP BY
    COALESCE(cm_5_cte.event__platform, subq_19.event__platform)
) subq_20
ON
  subq_12.event__platform = subq_20.event__platform
GROUP BY
  COALESCE(subq_12.event__platform, subq_20.event__platform)
ORDER BY item_views_per_user DESC
```

</details>

**Resultado:** una fila por plataforma con dos columnas decimales:

- `item_views_per_user`: vistas de items divididas por usuarios únicos de esa plataforma. Un valor de 3,2
  significaría que cada usuario vio en promedio 3,2 items.
- `item_views_share`: fracción de los eventos de esa plataforma que son vistas de items, entre 0 y 1. Un valor de
  0,4 significaría que el 40 % de los eventos fueron vistas de items.

Se visualiza como gráfico de barras ordenado de mayor a menor. Lo importante está en el SQL: MetricFlow calcula por
separado numerador y denominador de cada plataforma (`cm_5_cte`, `subq_11`, `subq_19`) y recién después divide.
Así el ratio es correcto; promediar ratios ya calculados daría otro número.

#### 4. Ranking de categorías del catálogo

Pregunta: *¿cuáles son las 5 categorías con mayor valor de catálogo?*

```sql
SELECT item__category, item_count, total_item_price, avg_item_price, max_item_price
FROM semantic.items
ORDER BY total_item_price DESC
LIMIT 5
```

SQL generado:

```sql
SELECT
  category AS item__category
  , COUNT(DISTINCT item_id) AS item_count
  , SUM(price) AS total_item_price
  , AVG(price) AS avg_item_price
  , MAX(price) AS max_item_price
FROM lab_data_silver.silver_item items_src_10000
GROUP BY
  category
ORDER BY total_item_price DESC
LIMIT 5
```

**Resultado:** como máximo 5 filas, una por categoría, ordenadas por `total_item_price`. Columnas: número de items
distintos, suma de precios, precio promedio y precio máximo de la categoría. Se visualiza como tabla o barras
horizontales. Es el **valor del catálogo** (precios de lista de los items), no ventas: el modelo `items` no tiene
transacciones.

#### 5. Engagement semanal filtrado por plataforma

Pregunta: *¿cuántas interacciones de contenido hubo cada semana del primer trimestre de 2024 en web y android, y
cómo se reparten entre vistas de items y de perfiles?*

```sql
SELECT metric_time__week, engagement_events, item_views, profile_views
FROM semantic.events
WHERE event__platform IN ('web', 'android')
  AND metric_time BETWEEN '2024-01-01' AND '2024-03-31'
ORDER BY metric_time__week
```

<details>
<summary>SQL generado (89 líneas)</summary>

```sql
WITH sma_10001_cte AS (
  SELECT
    DATE_TRUNC('day', event_date) AS metric_time__day
    , DATE_TRUNC('week', event_date) AS metric_time__week
    , event_name AS event__event_name
    , platform AS event__platform
    , event_id AS __item_views
    , event_id AS __profile_views
  FROM lab_data_silver.silver_event events_src_10000
)

, cm_3_cte AS (
  SELECT
    metric_time__week
    , COUNT(DISTINCT item_views) AS item_views
  FROM (
    SELECT
      metric_time__week
      , event__event_name
      , event__platform
      , __item_views AS item_views
    FROM sma_10001_cte
    WHERE metric_time__day BETWEEN timestamp '2024-01-01' AND timestamp '2024-03-31'
  ) subq_5
  WHERE (
    event__event_name = 'view_item'
  ) AND (
    event__platform IN ('web', 'android')
  )
  GROUP BY
    metric_time__week
)

, cm_4_cte AS (
  SELECT
    metric_time__week
    , COUNT(DISTINCT profile_views) AS profile_views
  FROM (
    SELECT
      metric_time__week
      , event__event_name
      , event__platform
      , __profile_views AS profile_views
    FROM sma_10001_cte
    WHERE metric_time__day BETWEEN timestamp '2024-01-01' AND timestamp '2024-03-31'
  ) subq_12
  WHERE (
    event__event_name = 'view_user_profile'
  ) AND (
    event__platform IN ('web', 'android')
  )
  GROUP BY
    metric_time__week
)

SELECT
  COALESCE(subq_18.metric_time__week, cm_3_cte.metric_time__week, cm_4_cte.metric_time__week) AS metric_time__week
  , MAX(subq_18.engagement_events) AS engagement_events
  , MAX(cm_3_cte.item_views) AS item_views
  , MAX(cm_4_cte.profile_views) AS profile_views
FROM (
  SELECT
    metric_time__week
    , item_views + profile_views AS engagement_events
  FROM (
    SELECT
      COALESCE(cm_3_cte.metric_time__week, cm_4_cte.metric_time__week) AS metric_time__week
      , MAX(cm_3_cte.item_views) AS item_views
      , MAX(cm_4_cte.profile_views) AS profile_views
    FROM cm_3_cte
    FULL OUTER JOIN
      cm_4_cte
    ON
      cm_3_cte.metric_time__week = cm_4_cte.metric_time__week
    GROUP BY
      COALESCE(cm_3_cte.metric_time__week, cm_4_cte.metric_time__week)
  ) subq_17
) subq_18
FULL OUTER JOIN
  cm_3_cte
ON
  subq_18.metric_time__week = cm_3_cte.metric_time__week
FULL OUTER JOIN
  cm_4_cte
ON
  COALESCE(subq_18.metric_time__week, cm_3_cte.metric_time__week) = cm_4_cte.metric_time__week
GROUP BY
  COALESCE(subq_18.metric_time__week, cm_3_cte.metric_time__week, cm_4_cte.metric_time__week)
ORDER BY metric_time__week
```

</details>

**Resultado:** una fila por semana del 1 de enero al 31 de marzo de 2024 (13 semanas: del lunes 1 de enero al lunes 25 de marzo), con tres columnas:
`engagement_events` (total), `item_views` y `profile_views`. Siempre se cumple
`engagement_events = item_views + profile_views`, porque así está definida la métrica derivada. Se visualiza como
barras apiladas (vistas de items y de perfiles) con una línea para el total. El filtro de plataforma se aplica
dentro del cálculo de **cada** métrica de origen (`cm_3_cte`, `cm_4_cte`) antes de sumarlas; el analista no tiene
que repetirlo.

### Metabase

Con el gateway levantado con `--host 0.0.0.0` y Metabase en Docker
(`docker run -d -p 3000:3000 --name metabase metabase/metabase`), agregar una base PostgreSQL con host
`host.docker.internal`, **puerto `5433`**, base de datos `athena` (o el motor), usuario y clave `bi`, SSL
desactivado, esquema `semantic` y escaneo de valores en *Nunca*. Después, *Sincronizar el esquema ahora*. Las
consultas se escriben en *Nuevo → Consulta SQL*. Detalle en
[docs/gateway_semantico.md, sección 11.4](../../docs/gateway_semantico.md#114-metabase).

### Limitaciones de esta versión

* Ejecuta solo en Athena (`--ejecutar`), con una única credencial de AWS para todos; otros motores reciben el SQL.
  Las tablas virtuales del catálogo están vacías (la pestaña *Datos* de DBeaver no muestra filas).
* Un usuario con clave fija (TLS opcional u obligatorio), sin seguridad por filas, caché ni auditoría: solo en red privada.
* Tipos aproximados: tiempo `DATE`, resto de dimensiones `VARCHAR`, métricas `DOUBLE`.
* El constructor visual de Metabase genera SQL que el traductor no acepta: usar consultas SQL nativas.
* El manifest se carga solo al arrancar.
* El protocolo lo resuelve riffq (preversión 0.1.x). Al actualizarlo, correr `python -m tests.test_gateway`; la versión
  anterior sobre buenavista sigue disponible en `server_old.py`.

## 12. Consumir desde Power BI

| Opción | Cómo | Limitaciones |
|---|---|---|
| **Tablas gold + Athena** | Materializar métricas en modelos gold y leerlos con el conector Amazon Athena de Power BI | No usa el motor de MetricFlow; la definición queda duplicada en SQL/DAX |
| **Gateway semántico (11 ter)** | Conector PostgreSQL de Power BI apuntando al gateway (servidor `host:5433`, base de datos `athena`), con `--ejecutar` | Modo Importar: conecta y navega (catálogo de tipos de Npgsql, `DISCARD ALL` y errores a mitad de lote resueltos). DirectQuery: el SQL que genera Power BI (subconsultas, `SUM` sobre columnas) requiere ampliar el traductor. En Power BI Service hace falta un data gateway y, con TLS, un certificado de una CA de confianza |
| **dbt Cloud Semantic Layer** | Conector oficial de Power BI (Preview, DirectQuery) | Requiere licencia dbt platform y Snowflake/BigQuery/Databricks/Redshift/Postgres/Trino; no soporta Glue |

---

## 13. Problemas conocidos

| Síntoma | Causa | Solución |
|---|---|---|
| `Invalid metrics config ... Additional properties are not allowed ('denominator', 'numerator' were unexpected)` | dbt 1.10 no entiende la spec nueva | Actualizar a dbt 1.12+ (paso 3) |
| `CERTIFICATE_VERIFY_FAILED` al instalar `dbt-core-experimental-parser` | Python no encuentra certificados SSL | `certifi` + `SSL_CERT_FILE` (paso 3) |
| `ERROR: Adapter type glue is not supported` en `mf` | `mf` no soporta dbt-glue | Usar `mf_profiles` (paso 10) |
| `Could not find adapter type snowflake!` | Se usa `sf_profiles` sin `dbt-snowflake` | `pip install dbt-snowflake`, o usar la opción B del paso 11 |
| `The semantic layer requires a time spine model` | Falta la tabla calendario | Mantener `models/semantic/metricflow_time_spine.sql` y `time_spine.yml` |
| `mf query` sin `--explain` falla con "Table ... does not exist" | Con el perfil DuckDB las tablas no existen | Usar `--explain` |
| Una dimensión no aparece para una métrica | No hay camino de join | Revisar `mf list dimensions --metrics <métrica>` (ver 8.4) |
| `dbt run` usa el perfil equivocado | `DBT_PROFILES_DIR` quedó definido | `unset DBT_PROFILES_DIR` |
| `Env var required but not provided: 'AWS_ACCOUNT_ID'` (o `DBT_S3_BUCKET`) | No se cargó `.env` en esta terminal | `set -a; source .env; set +a` (4.1) |
| Glue/Iceberg: *Location does not exist* al crear `metricflow_time_spine` | Quedó en Glue una tabla huérfana de una corrida anterior, con la ubicación de otro bucket | Borrarla (`aws glue delete-table --database-name lab_data --name metricflow_time_spine`) y volver a correr `dbt run -s metricflow_time_spine` |
| Metabase: *contraseña incorrecta* al conectar al gateway | Puerto vacío: Metabase usa 5432 | Poner `5433` (11 ter) |
| DBeaver: *Scalar Function with name pg_get_partkeydef does not exist* | Gateway sin la compatibilidad con DBeaver | Actualizar `server.py` y reiniciar el gateway |
| Metabase en Docker no llega al gateway | El gateway escucha solo en `127.0.0.1` | `--host 0.0.0.0` y host `host.docker.internal` |
| *Table with name dbt_glue_lab does not exist* | Consulta de la versión anterior del gateway (tabla única) | Usar las tablas `semantic.<modelo>` (en este proyecto, `semantic.events` o `semantic.items`) |
| Una métrica nueva no aparece en el gateway | El manifest se lee solo al arrancar | `dbt parse`, reiniciar el gateway y sincronizar en Metabase |

Más errores del gateway: [docs/gateway_semantico.md, sección 15](../../docs/gateway_semantico.md#15-errores-frecuentes).

---

## 14. Referencia de comandos `mf`

Ayuda: `mf <comando> --help`. Versión: `mf --version`. Usar con `export DBT_PROFILES_DIR=$PWD/mf_profiles` (paso 10).

### `mf list`: explorar la capa semántica

| Comando | Qué hace |
|---|---|
| `mf list metrics` | Lista las métricas con algunas de sus dimensiones |
| `mf list metrics --search views` | Filtra métricas por texto |
| `mf list metrics --show-all-dimensions` | Muestra todas las dimensiones de cada métrica |
| `mf list dimensions` | Lista todas las dimensiones |
| `mf list dimensions --metrics item_views` | Dimensiones válidas para esa métrica (intersección si son varias) |
| `mf list entities` | Lista las entidades |
| `mf list entities --metrics item_views` | Entidades válidas para esa métrica |
| `mf list dimension-values --dimension event__platform --metrics item_views` | Valores de una dimensión; **ejecuta una consulta**, requiere tablas reales |
| `mf list saved-queries` | Lista las saved queries (hoy no hay) |

### `mf query`: consultar o ver el SQL

```bash
# Simples (events)
mf query --metrics event_count --explain
mf query --metrics item_views --group-by metric_time__month,event__platform --explain
mf query --metrics item_views,profile_views,unique_users --group-by event__event_name --explain
mf query --metrics event_count --group-by event__user --explain

# Simples (items)
mf query --metrics item_count,avg_item_price,max_item_price,total_item_price --group-by item__category --explain
mf query --metrics avg_item_price --group-by item__adjective,item__modifier --explain

# Ratio y derived
mf query --metrics item_views_per_user --group-by metric_time__year --explain
mf query --metrics item_views_share --group-by event__platform --explain
mf query --metrics engagement_events --group-by metric_time__month --explain

# Filtros (varios --where se combinan con AND)
mf query --metrics avg_item_price,item_count --group-by item__category \
  --where "{{ Dimension('item__category') }} = 'tool'" --explain
mf query --metrics item_views --group-by metric_time__month \
  --where "{{ TimeDimension('metric_time','month') }} >= '2018-01-01'" \
  --where "{{ Dimension('event__platform') }} = 'web'" --explain

# Rango de fechas, orden y límite
mf query --metrics item_views,unique_users --group-by metric_time__day \
  --start-time 2018-01-01 --end-time 2018-12-31 --order -item_views --limit 10 --explain

# Ejecutar (sólo con un perfil cuyo warehouse tenga las tablas)
mf query --metrics item_views --group-by event__platform
mf query --metrics item_views --group-by event__platform --csv resultado.csv
mf query --metrics avg_item_price --group-by item__category --decimals 2

# Depuración
mf query --metrics item_views --group-by event__platform --explain --show-dataflow-plan
mf query --metrics item_views --group-by event__platform --explain --show-sql-descriptions
mf query --metrics item_views --group-by event__platform --explain --display-plans
```

| Opción | Qué hace |
|---|---|
| `--metrics a,b` | Métricas a calcular |
| `--group-by x,y` | Dimensiones o entidades, formato `<entidad>__<dimensión>` |
| `metric_time__<grano>` | Tiempo estándar: `day`, `week`, `month`, `quarter`, `year` |
| `--where "..."` | Filtro Jinja con `Dimension()`, `TimeDimension()`, `Entity()`; repetible |
| `--start-time` / `--end-time` | Rango ISO 8601 inclusivo |
| `--order a,-b` | Orden; `-` es descendente |
| `--limit N` | Máximo de filas |
| `--explain` | Muestra el SQL sin ejecutar |
| `--show-dataflow-plan` | Con `--explain`, imprime el plan de dataflow |
| `--show-sql-descriptions` | Con `--explain`, comenta cada nodo del SQL |
| `--display-plans` | Abre los planes en el navegador |
| `--csv archivo` | Guarda el resultado en CSV |
| `--decimals N` | Decimales al mostrar números |
| `--saved-query nombre` | Ejecuta una saved query |
| `--quiet` | Reduce la salida |

### `mf validate-configs`: validar la capa

| Comando | Qué hace |
|---|---|
| `mf validate-configs --skip-dw` | Valida sintaxis y semántica sin conectarse al warehouse |
| `mf validate-configs` | Además ejecuta consultas de prueba en el warehouse |
| `mf validate-configs --skip-dw --show-all` | Incluye warnings y future errors |
| `mf validate-configs --skip-dw --verbose-issues` | Más detalle por problema |
| `mf validate-configs --dw-timeout 120` | Timeout de las validaciones contra el warehouse |

### Otros

| Comando | Qué hace |
|---|---|
| `mf health-checks` | Comprueba la conexión al warehouse del perfil |
| `mf tutorial` | Tutorial interactivo; crea modelos de ejemplo, **no usar en este proyecto** |

---

## 15. Estado frente al ADR de la capa semántica (CencoSL)

El ADR *Capa semántica corporativa* (2026-10-08, ACCEPTED) adopta la **opción C**: una capa semántica propia,
llamada CencoSL. CencoSL tiene dos piezas de desarrollo propio, el **serving layer** (protocolos, autenticación y
traducción de peticiones) y el **gateway de orquestación** (autorización, RLS, PII, caché, ejecución en los motores,
costo y auditoría). Como **metric engine** usa MetricFlow OSS embebido, que solo compila.

Esta sección compara lo que pide el ADR con lo que existe en este repositorio. Estados: **Hecho** · **Parcial**
(existe, pero no cubre todo lo que pide el ADR) · **Mínimo** (solución provisional de laboratorio) · **Pendiente**.

### 15.1 Resumen

| Capa | Hecho | Parcial / mínimo | Pendiente |
| --- | --- | --- | --- |
| Semantic models | YAML con la spec nueva, manifest | Sobre modelos silver (el ADR pide Gold); sin owner ni SLA | — |
| Metric engine | MetricFlow OSS embebido, solo compila, 8 dialectos | — | — |
| Serving layer | Endpoint Postgres wire (riffq), traductor, catálogo, compatibilidad DBeaver/Metabase/Npgsql, TLS | Autenticación (un usuario fijo) | REST/GraphQL, MCP, Arrow Flight SQL, XMLA/DAX, OIDC/SAML |
| Gateway | Ejecución en Athena (`--ejecutar`, tiempo máximo con cancelación) | Enrutador (el cliente elige el dialecto), gestor del manifiesto (carga al arrancar), log por consulta | Ejecución en otros motores, RLS/PII, caché, costo, auditoría |
| Transversal | Pruebas automáticas, documentación | Validación manual (`dbt parse`, `mf validate-configs`) | CI/CD en GitHub Enterprise, OpenMetadata, observabilidad, despliegue en EKS |

En una frase: **la ruta de consulta funciona de punta a punta en Athena** (petición → consulta semántica → SQL
compilado → ejecución → filas al cliente). **Falta la parte de «gobernar»** (paso 3 y parte de 5 y 7: identidad por
usuario, RLS/PII, caché, auditoría formal y otros motores).

### 15.2 Ruta de consulta del ADR, paso a paso

| Paso del ADR | Estado | Dónde / qué falta |
| --- | --- | --- |
| 1. La presentation layer envía la petición al serving layer | Hecho (Postgres wire) | `server.py` sobre riffq. Probado con psql, Jupyter, DBeaver y Metabase; Power BI en modo Importar |
| 2. El serving layer autentica y traduce a consulta semántica | Parcial | Traducción hecha (`translator.py` → `SemanticQuery`). Autenticación: un usuario fijo, sin IdP; TLS opcional u obligatorio (`--tls-obligatorio`) |
| 3. El gateway autoriza, añade RLS/objeto/PII y revisa la caché | Pendiente | No existe capa gateway: `server.py` llama directo a `engine.py` |
| 4. MetricFlow compila a SQL contra el `semantic_manifest.json` | Hecho | `engine.py` (`SemanticEngine.explain`), sin conexión |
| 5. El gateway ejecuta el SQL en el compute engine con sus conectores | Parcial | `executor.py` ejecuta en Athena con PyAthena (`--ejecutar`): tiempo máximo con cancelación (`StopQueryExecution`), límite de filas, log de bytes escaneados. Solo Athena y una credencial compartida |
| 6. El compute engine ejecuta sobre la storage layer | Hecho (Athena) | Athena sobre Iceberg en S3 registrado en Glue; probado con los manifests `dbt_glue_lab` e `it-people` |
| 7. Resultados → auditoría y caché → consumidor | Parcial | El consumidor recibe las filas con el orden y alias pedidos; el log registra cada consulta. Sin caché ni auditoría formal |

### 15.3 Componentes del ADR

**Semantic models y metric engine**

| Componente | Estado | Qué hay | Qué falta |
| --- | --- | --- | --- |
| Semantic models en YAML (spec dbt 1.12) | Hecho | `events`, `items`, `top_items`; 11 métricas (simple, ratio, derived); time spine | — |
| Semantic models sobre modelos Gold | Parcial | `events` e `items` se apoyan en `silver_event` y `silver_item` | Moverlos a modelos Gold, como pide el ADR («cada semantic model se apoya en un modelo dbt Gold») |
| Métricas certificadas (owner, SLA, etc.) | Pendiente | — | Declarar los atributos obligatorios en el YAML (`config.meta`) y validarlos en CI |
| Metric engine: MetricFlow OSS embebido, solo compila | Hecho | `engine.py`, `metricflow==0.213.0` fijada, cliente «solo dialecto» que impide ejecutar | — |
| Aislar MetricFlow tras una interfaz interna (mitigación del ADR) | Hecho | Todo pasa por `SemanticEngine` y el contrato `SemanticQuery` | — |

**Serving layer**

| Componente | Estado | Qué hay | Qué falta |
| --- | --- | --- | --- |
| Endpoint Postgres wire | Hecho (PoC) | `server.py` sobre riffq (TLS opcional u obligatorio con `--tls-obligatorio`); catálogo en DuckDB; compatibilidad con Metabase, DBeaver y Npgsql. `server_old.py` (buenavista) como respaldo | Probar Power BI en DirectQuery (vía data gateway) y MicroStrategy; medir rendimiento |
| Traductor de peticiones | Parcial | SQL simple → `SemanticQuery` (métricas, group by, filtros, rango de tiempo, orden, límite) | Aceptar el SQL que generan los BI (constructor de Metabase, DirectQuery de Power BI: `DATE_TRUNC`, `COUNT(*)`, subconsultas, parámetros) y `SELECT *` |
| Vistas de consumo como tablas virtuales | Parcial | Una tabla por modelo semántico, generada desde cualquier manifest (aquí `semantic.events` y `semantic.items`; con IT People, `semantic.employee_headcount`) | Vistas de consumo curadas por dominio, si se definen en el YAML |
| Exponer el catálogo de métricas y dimensiones | Hecho | `semantic.metrics`, `semantic.dimensions`, `semantic.metric_dimensions` | Agregar owner, SLA y glosario cuando vengan del YAML y de OpenMetadata |
| API REST/GraphQL (data apps, notebooks) | Pendiente | — | Exponer el mismo `SemanticQuery` por HTTP |
| Servidor MCP (agentes IA) | Pendiente | — | Servidor MCP sobre el mismo motor (listar métricas, consultar) |
| Arrow Flight SQL (ADBC) | Pendiente | — | Fase posterior al MVP |
| XMLA/DAX | Pendiente | — | Solo si la PoC demuestra que DirectQuery no basta |
| Autenticación OIDC/SAML y Entra ID | Mínimo | Usuario y clave fijos (`--user`/`--password`) validados en `handle_auth`; TLS con `--tls-obligatorio` | Integración con el IdP corporativo y Entra ID; identidad por consumidor |

**Gateway de orquestación**

| Componente | Estado | Qué hay | Qué falta |
| --- | --- | --- | --- |
| Conectores de motor y enrutador | Parcial | Compila para athena, bigquery, databricks, duckdb, postgres, redshift, snowflake y trino; el cliente elige el dialecto con `dbname`. Ejecuta en Athena (workgroup, `--timeout` con cancelación, `--max-filas`) | Ejecutar en los demás motores; elegir el motor según dónde vive el producto de datos; límites de escaneo |
| Inyección de seguridad (RLS, seguridad de objeto, PII) | Pendiente | El contrato `SemanticQuery` permite añadir filtros antes de compilar | Derivar políticas de OpenMetadata y Lake Formation; validar que el filtro se aplica en todas las rutas de join |
| Caché de resultados | Pendiente | — | Caché con invalidación por snapshot de Iceberg o build de dbt |
| Gestor del manifiesto | Mínimo | Lee el manifest de `semantic_manifests/` al arrancar | Carga en caliente del manifest que publica el CI (por ejemplo, desde S3) y versionado por entorno |
| Límites de costo y aislamiento por dominio | Pendiente | — | Cuotas, workgroups y límites por dominio o por métrica, configurables sin cambiar código |
| Auditoría | Mínimo | El log de consola registra conexión, motor, SQL compilado, `query_id`, filas, segundos y bytes escaneados | Registro persistente por usuario, métrica, motor, costo y resultado |
| Materialización gobernada | Pendiente | — | Solo cuando se requiera (patrón #10, si la PoC lo justifica) |

**Transversal**

| Componente | Estado | Qué hay | Qué falta |
| --- | --- | --- | --- |
| Métricas como código | Parcial | YAML versionado en git (`.gitignore` excluye el entorno virtual) | Repositorio en GitHub Enterprise con CODEOWNERS por dominio y PR obligatorio |
| Validador del modelo en CI | Parcial | `dbt parse` y `mf validate-configs` manuales; pruebas `tests/` del gateway | Pipeline que valide el modelo y los atributos obligatorios, y publique el manifest por entorno |
| Sincronizador OpenMetadata (bidireccional) | Pendiente | — | Entrada: glosario, owner, clasificación. Salida: métricas, modelos semánticos, linaje y uso |
| Observabilidad | Pendiente | Logs en consola | Latencia, errores, costo y uso por métrica, dominio y consumidor |
| Despliegue en alta disponibilidad (EKS) | Pendiente | Se ejecuta como proceso local | Imagen de contenedor, despliegue en EKS, SLO, runbooks y degradación controlada (servir desde caché) |
| CI/CD del runtime con promoción entre entornos | Pendiente | — | Pipeline de build, pruebas y despliegue del gateway |

### 15.4 Requisitos técnicos clave del ADR

| Requisito | Estado | Comentario |
| --- | --- | --- |
| Serving layer único | Parcial | Un endpoint por Postgres wire; faltan los demás protocolos y que los BI dejen de conectarse directo a los motores |
| Multimotor | Parcial | Compila 8 dialectos; ejecuta solo en Athena. Las rutas físicas del manifest son las de Glue: otros motores necesitan un manifest con rutas reescritas (sección 11) |
| Los datos no se mueven (pushdown) | Hecho | Athena calcula sobre las tablas Iceberg; el gateway solo recibe el resultado agregado |
| Protocolos: Postgres wire (MVP), REST/GraphQL y MCP | Parcial | Solo Postgres wire |
| Métricas como código | Parcial | YAML en git; falta CI en GitHub Enterprise |
| Métricas certificadas (owner, SLA) | Pendiente | — |
| OpenMetadata en ambos sentidos | Pendiente | — |
| Identidad extremo a extremo (Entra ID, OIDC/SAML, cuenta por motor) | Pendiente | — |

### 15.5 Patrones de consumo del ADR

| Consumidor | Patrón del ADR | Estado en el laboratorio |
| --- | --- | --- |
| Power BI | Conector PostgreSQL en DirectQuery vía on-premises data gateway | Importar: conecta y navega, tras adaptar el catálogo a Npgsql. DirectQuery: pendiente (requiere ampliar el traductor). Es el riesgo «paridad con Power BI» del ADR |
| MicroStrategy | Fuente PostgreSQL genérica | No probado |
| Metabase | Fuente PostgreSQL; cada vista de consumo como una tabla | Probado: conexión, sincronización de esquema y SQL nativo. El constructor visual aún no |
| Agentes IA | Solo vía MCP | Pendiente (no hay servidor MCP) |
| Data apps y notebooks | REST/GraphQL; luego Arrow Flight SQL | Notebooks vía psycopg (Postgres wire); REST/GraphQL pendiente |

### 15.6 Riesgos del ADR que este laboratorio ayuda a validar

| Riesgo del ADR | Qué se sabe hoy |
| --- | --- |
| Cobertura de motores de MetricFlow («Athena está por validar») | MetricFlow compila SQL de Athena para las 11 métricas y el gateway lo **ejecuta** en Athena (manifests `dbt_glue_lab` e `it-people`). Falta comparar sistemáticamente contra conteos directos |
| Paridad con Power BI (DirectQuery sobre Postgres wire) | Modo Importar funcionando tras adaptar el catálogo a Npgsql (tipos, `DISCARD ALL`, errores a mitad de lote). DirectQuery sin probar: requiere ampliar el traductor |
| Divergencia del formato del modelo (dbt 1.12 eliminó `measures`) | El proyecto ya usa la spec nueva; MetricFlow fijado en 0.213.0 |
| Rendimiento y costo en Athena | El log registra segundos y bytes escaneados por consulta; falta medirlo con carga real y agregar caché |
| Dependencia de MetricFlow OSS | Mitigado en el diseño: MetricFlow está aislado tras `SemanticEngine` |

### 15.7 Hoja de ruta sugerida para cerrar el MVP

Orden propuesto, de lo que más desbloquea a lo que más depende de otras piezas:

1. **Ejecutor y enrutador.** Hecho para Athena (`--ejecutar`). Falta comparar resultados contra consultas directas
   (atributo «functional correctness») y sumar Snowflake o Redshift.
2. **Capa gateway explícita** entre traductor y motor, recibiendo `SemanticQuery`: autorización, inyección de
   RLS/objeto/PII y auditoría.
3. **Autenticación real**: OIDC/SAML con el IdP corporativo y Entra ID; cuenta de servicio por motor. TLS ya está
   (`--tls-obligatorio`); en producción falta un certificado de la CA corporativa.
4. **Caché** con invalidación por snapshot de Iceberg o build de dbt; límites de escaneo y workgroups.
5. **Gestor del manifiesto**: el CI publica el manifest por entorno y el gateway lo recarga en caliente.
6. **CI/CD en GitHub Enterprise**: validación del modelo, atributos obligatorios (owner, SLA), CODEOWNERS por
   dominio, y despliegue del runtime en EKS con alta disponibilidad.
7. **Power BI en DirectQuery** (vía on-premises data gateway) y MicroStrategy, con informes reales; ampliar el
   traductor al SQL que generan (el modo Importar ya funciona).
8. **Semantic models sobre Gold** y atributos de certificación en el YAML.
9. **Observabilidad**: latencia p95, errores, costo y uso por métrica, dominio y consumidor.
10. **Nuevos protocolos**: API REST/GraphQL y servidor MCP sobre el mismo motor.
11. **Sincronizador OpenMetadata** bidireccional.
12. Fases posteriores al MVP: Arrow Flight SQL y, si hace falta, XMLA/DAX.

---

## Referencias

- [Semantic models (spec nueva)](https://docs.getdbt.com/docs/build/semantic-models)
- [Migrar a la spec nueva](https://docs.getdbt.com/docs/build/latest-metrics-spec)
- [Métricas](https://docs.getdbt.com/docs/build/metrics-overview)
- [Time spine](https://docs.getdbt.com/docs/build/metricflow-time-spine)
- [Snowflake: integración de catálogo con AWS Glue](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-glue)
- [dbt Semantic Layer + Power BI](https://docs.getdbt.com/docs/cloud-integrations/semantic-layer/power-bi)
