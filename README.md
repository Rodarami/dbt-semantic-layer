# MetricFlow: capa semántica, gateway y manifests

Repositorio de la capa semántica: proyectos dbt que definen las métricas, el **gateway semántico** que las publica
por protocolo PostgreSQL, scripts de apoyo y los `semantic_manifest.json` de cada proyecto.

El gateway sirve **cualquier** `semantic_manifest.json`: cada modelo semántico con métricas aparece como una tabla
`semantic.<modelo>`, sin tocar código. Hoy hay dos manifests en `semantic_manifests/`: `dbt_glue_lab` (tablas
`semantic.events` y `semantic.items`) e `it-people` (`semantic.employee_headcount`).

El gateway **compila** cada consulta con MetricFlow al SQL del motor elegido (Athena, Snowflake, Redshift…). Por
defecto devuelve ese SQL; con `--ejecutar` lo **ejecuta en Athena** y devuelve las filas, dejando el SQL en el log.
El protocolo PostgreSQL lo resuelve **riffq** (núcleo en Rust, con TLS y autenticación); la versión anterior, sobre
buenavista, se conserva como respaldo en `server_old.py`.

## Arquitectura

![Arquitectura del gateway semántico](docs/img/arquitectura_gateway_actual.png)

| Pieza | Archivo | Qué hace |
| --- | --- | --- |
| Protocolo PostgreSQL | `server.py` sobre **riffq** (Rust) | Conexiones, autenticación, TLS, protocolo simple y extendido |
| Enrutamiento | `server.py` (`ConexionSemantica._resolver`) | Decide quién responde cada consulta (imagen siguiente) |
| Catálogo | `catalogo_pg.py` + **DuckDB** en memoria | `information_schema`, `pg_catalog`, tablas vacías `semantic.<modelo>` y `semantic.metrics`/`dimensions` |
| Traductor | `translator.py` (sqlglot) | SQL del cliente → consulta semántica; aplana las subconsultas de Power BI y, si traen `CASE`/`cast`, separa núcleo y resto |
| Compilador | `engine.py` (MetricFlow) | Consulta semántica → SQL del motor, sin conectarse |
| Ejecutor | `executor.py` (PyAthena) | Con `--ejecutar`: corre el SQL en Athena y devuelve filas (cancela a los 100 s) |
| Resto del BI | `server.py` (`_resto_en_duckdb`) + DuckDB efímero | Aplica sobre las filas del núcleo lo que Power BI DirectQuery agrega alrededor (`CASE`, `cast`, orden, límite). Ver [docs/duckdb_efimero.md](docs/duckdb_efimero.md) |
| TLS obligatorio | `tls_obligatorio.py` (`PuertaTLS`) | Con `--tls-obligatorio`: rechaza clientes sin SSL antes de llegar a riffq |

![Quién responde cada consulta](docs/img/gateway_rutas_riffq.png)

DuckDB sigue siendo necesario: el catálogo propio de riffq resuelve solo 20 de las 25 consultas de catálogo de DBeaver,
Power BI y Metabase ([docs/comparativa_protocolo.md](docs/comparativa_protocolo.md), sección 5).

El recorrido completo de una consulta, pieza por pieza, y la cobertura del ADR están en
[docs/arquitectura_gateway.md](docs/arquitectura_gateway.md).

## Estructura

```
MetricFlow/                              # raíz del repositorio git
├── semantic_gateway/                    # servidor del gateway
│   ├── model.py                         #   estructuras: TableInfo, Catalog, SemanticQuery
│   ├── translator.py                    #   SQL del cliente -> consulta semántica (sqlglot)
│   ├── engine.py                        #   MetricFlow como compilador; una tabla por modelo semántico
│   ├── executor.py                      #   ejecuta en Athena el SQL compilado (--ejecutar)
│   ├── catalogo_pg.py                   #   catálogo PostgreSQL en DuckDB y compatibilidad con los drivers BI
│   ├── server.py                        #   servidor actual: protocolo Postgres con riffq (Rust), TLS, autenticación
│   ├── tls_obligatorio.py               #   exige TLS delante de riffq (--tls-obligatorio)
│   └── server_old.py                    #   servidor anterior con buenavista (Python puro), como respaldo
├── tests/                               # test_translator.py, test_gateway.py, probar_catalogo_riffq.py
├── script/
│   ├── compilar_sin_conexion.py         # compila una consulta a SQL sin gateway ni conexión
│   └── manifest_para_snowflake.py       # reescribe las rutas del manifest para Snowflake
├── semantic_manifests/                  # un semantic_manifest.json por proyecto dbt
│   ├── dbt_glue_lab/
│   │   ├── semantic_manifest.json
│   │   └── consultas_sql_dbt_glue_lab.md   # consultas con el SQL de Athena que genera cada una
│   └── it-people/
│       ├── semantic_manifest.json
│       ├── consultas_sql_it_people.md      # consultas con el SQL de Athena que genera cada una
│       └── it_people_capa_semantica.md     # cómo se definió la capa semántica de IT People
├── notebooks/sqlglot_paso_a_paso.ipynb  # recorrido del traductor
├── docs/gateway_semantico.md            # documentación completa del gateway
├── docs/arquitectura_gateway.md         # flujo de una consulta, diagramas y cobertura del ADR
├── docs/consultas_dbt_glue_lab.md       # 35 consultas para el manifest dbt_glue_lab (eventos e items)
├── docs/consultas_it_people.md          # 23 consultas para el manifest it-people (dotación)
├── docs/comparativa_protocolo.md        # buenavista vs riffq vs alternativas, y prueba del catálogo sin DuckDB
├── docs/credenciales_aws_windows.md     # credenciales de AWS en Windows para --ejecutar
├── docs/power_bi_directquery.md         # Power BI: DirectQuery, conexión y armado de paneles
├── docs/duckdb_efimero.md               # por qué existe el DuckDB efímero (núcleo + resto) y qué resuelve
├── docs/img/                            # arquitectura_gateway_actual.png, gateway_rutas_riffq.png
├── requirements-gateway.txt
└── DBT/
    └── dbt_glue_lab/                    # proyecto dbt (Glue + Iceberg) con su capa semántica
```

| Carpeta | Documentación |
| --- | --- |
| `semantic_gateway/`, `tests/`, `notebooks/` | [docs/gateway_semantico.md](docs/gateway_semantico.md) (instalación, uso y referencia) |
| Cómo funciona el gateway por dentro y cobertura del ADR | [docs/arquitectura_gateway.md](docs/arquitectura_gateway.md) |
| Consultas para DBeaver y Metabase, una guía por manifest | [dbt_glue_lab](docs/consultas_dbt_glue_lab.md) · [it-people](docs/consultas_it_people.md) |
| DuckDB efímero: por qué se creó, qué problema resuelve y diferencias con el del catálogo | [docs/duckdb_efimero.md](docs/duckdb_efimero.md) |
| Power BI en DirectQuery: cómo funciona, conexión y paneles | [docs/power_bi_directquery.md](docs/power_bi_directquery.md) |
| Credenciales de AWS en Windows (PowerShell) para ejecutar en Athena | [docs/credenciales_aws_windows.md](docs/credenciales_aws_windows.md) |
| Librería del protocolo: buenavista vs riffq vs alternativas | [docs/comparativa_protocolo.md](docs/comparativa_protocolo.md) |
| `DBT/dbt_glue_lab/` | [DBT/dbt_glue_lab/README.md](DBT/dbt_glue_lab/README.md) (incluye el estado frente al ADR, sección 15) |
| SQL de Athena que genera cada consulta | [dbt_glue_lab](semantic_manifests/dbt_glue_lab/consultas_sql_dbt_glue_lab.md) · [it-people](semantic_manifests/it-people/consultas_sql_it_people.md) |

## Flujo de un manifest

```
DBT/<proyecto>/models/*.yml ──dbt parse──► DBT/<proyecto>/target/semantic_manifest.json
                                                 │ cp
                                                 ▼
                         semantic_manifests/<proyecto>/semantic_manifest.json ──► gateway / scripts
```

Cada proyecto dbt genera su manifest en su `target/` (que no se versiona). Se copia a `semantic_manifests/`, que
sí se versiona y es de donde lo leen el gateway y los scripts. Para un proyecto externo (por ejemplo IT People),
basta con dejar su `semantic_manifest.json` en `semantic_manifests/<proyecto>/`.

## Instalación

riffq trae paquetes precompilados solo para **Python 3.12** (Windows y Linux) y **Python 3.14** (Mac Apple Silicon).
Con otra versión, `pip` intenta compilarlo y hace falta Rust (`rustup`). Crear el entorno con esa versión:

```bash
# Mac (Apple Silicon): Python 3.14
cd ~/DBT-GLUE
python3.14 -m venv dbt_venv                  # una vez
source dbt_venv/bin/activate
python -m pip install "dbt-core>=1.12" "dbt-glue>=1.12.4" dbt-metricflow dbt-duckdb
cd MetricFlow
python -m pip install -r requirements-gateway.txt
```

```powershell
# Windows (PowerShell): Python 3.12
py -3.12 -m venv dbt_venv
.\dbt_venv\Scripts\Activate.ps1
python -m pip install -r requirements-gateway.txt
```

Comprobar: `python -c "import riffq, metricflow, duckdb; print('ok')"`.

## Comandos

Todos desde la raíz `MetricFlow/`, salvo `dbt parse`.

### Mac / Linux

```bash
# 1. Generar y publicar el manifest de dbt_glue_lab
cd DBT/dbt_glue_lab
set -a; source .env; set +a                  # AWS_ACCOUNT_ID, DBT_S3_BUCKET… (README del proyecto dbt, sección 4.1)
unset DBT_PROFILES_DIR && dbt parse
cp target/semantic_manifest.json ../../semantic_manifests/dbt_glue_lab/semantic_manifest.json
cd ../..

# 2. Pruebas del gateway
python -m tests.test_translator
python -m tests.test_gateway                             # servidor actual (riffq): 13 OK (incluye seguridad y TLS)
SEMANTIC_GW_SERVER=old python -m tests.test_gateway      # servidor anterior (buenavista): 12 OK
python -m tests.probar_catalogo_riffq --comparar         # opcional: catálogo de riffq sin DuckDB (20/25) vs DuckDB (25/25)

# 3. Levantar el gateway (uno por manifest; puertos distintos si van a la vez)
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --port 5434
#    para Metabase en Docker agregar: --host 0.0.0.0

# 3b. Levantar el gateway ejecutando en Athena (devuelve filas; el SQL queda en el log)
aws sts get-caller-identity                  # credenciales de AWS por defecto (las mismas que usa dbt)
set -a; source DBT/dbt_glue_lab/.env; set +a # carga DBT_S3_BUCKET para la ruta de resultados
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --ejecutar --region us-east-1 \
  --athena-workgroup primary --athena-output "s3://$DBT_S3_BUCKET/athena-results/"
#    opcionales: --aws-profile <perfil>, --timeout 100 (cancela en Athena al vencer), --max-filas N (0 = sin límite)

# 3c. Con TLS obligatorio (los clientes usan sslmode=require; sin SSL se rechazan)
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --tls-cert ../ssl/server.crt --tls-key ../ssl/server.key --tls-obligatorio
#    sin --tls-obligatorio el TLS es opcional: cifra si el cliente lo pide, pero acepta conexiones sin cifrar

# 3d. Servidor anterior (buenavista), mismas opciones salvo TLS
python -m semantic_gateway.server_old --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena

# 4. Compilar una consulta sin gateway
python3 script/compilar_sin_conexion.py --motor athena --metrics event_count --group-by event__platform
python3 script/compilar_sin_conexion.py --manifest semantic_manifests/it-people/semantic_manifest.json \
  --motor athena --metrics dotacion_total,ratio_internos --group-by employee__position
```

### Windows (PowerShell)

Python 3.12. Las credenciales de AWS (perfil en `CapaSemantica\.aws` y `aws_cenco.ps1`) están en
[docs/credenciales_aws_windows.md](docs/credenciales_aws_windows.md). En PowerShell la línea se continúa con `` ` ``.

```powershell
# 1. Entorno (cada terminal nueva) y credenciales
cd "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica"
py -3.12 -m venv dbt_venv                                 # solo la primera vez
.\dbt_venv\Scripts\Activate.ps1
. .\aws_cenco.ps1
cd dbt-semantic-layer                                     # raíz del repositorio
python -m pip install -r requirements-gateway.txt         # solo la primera vez

# 2. Pruebas
python -m tests.test_translator
python -m tests.test_gateway
$env:SEMANTIC_GW_SERVER="old"; python -m tests.test_gateway; Remove-Item Env:SEMANTIC_GW_SERVER
python -m tests.probar_catalogo_riffq --comparar

# 3. Gateway ejecutando en Athena (servidor actual, riffq)
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer
#    con TLS obligatorio agregar: --tls-cert ..\server.crt --tls-key ..\server.key --tls-obligatorio

# 3b. Servidor anterior (buenavista), por si un cliente falla con riffq
python -m semantic_gateway.server_old --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer

# Detener: Ctrl+C. Ver si quedó otro gateway en el puerto:
netstat -ano | findstr :5433
```

## Conexión desde DBeaver, Metabase o Power BI

| Campo | Valor |
| --- | --- |
| Tipo | PostgreSQL |
| Host | `127.0.0.1` (Metabase en Docker: `host.docker.internal`) |
| Puerto | `5433`, o el de `--port` |
| Base de datos | Motor del SQL: `athena`, `snowflake`, `redshift`, `databricks`, `trino`, `bigquery`, `postgres`, `duckdb` |
| Usuario / contraseña | `bi` / `bi` (solo pruebas locales) |
| SSL | Desactivado; con `--tls-cert`/`--tls-key`, `require` (con `--tls-obligatorio`, las conexiones sin SSL se rechazan) |

En Power BI: *Obtener datos* → *Base de datos PostgreSQL*, servidor `127.0.0.1:5433`, base `athena`, modo
**DirectQuery** (recomendado: cada visual pide a MetricFlow la métrica a su grano, así los ratios y totales salen
bien) o *Importar*. En el navegador marcar la tabla `semantic.<modelo>` y *Cargar*. Con TLS, Power BI valida el
certificado: uno autofirmado no sirve (hace falta uno de una CA de confianza). Guía completa, reglas y armado de
paneles: [docs/power_bi_directquery.md](docs/power_bi_directquery.md).

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic';  -- qué tablas publicó el manifest
SELECT metric, label, type, table_name FROM semantic.metrics;                      -- métricas y la tabla de cada una
SELECT <dimensión>, <métrica> FROM semantic.<modelo>;                              -- filas (con --ejecutar) o el SQL
EXPLAIN SELECT <dimensión>, <métrica> FROM semantic.<modelo>;                      -- siempre el SQL, sin ejecutar
-- por ejemplo, con dbt_glue_lab:  SELECT event__platform, event_count FROM semantic.events;
-- con it-people:                  SELECT employee__position, dotacion_total FROM semantic.employee_headcount;
```

Consultas listas para cada manifest: [dbt_glue_lab](docs/consultas_dbt_glue_lab.md) · [it-people](docs/consultas_it_people.md).

**Seguridad:** el gateway no permite leer ni escribir archivos desde SQL, solo acepta consultas de lectura sobre el
catálogo y rechaza filtros con Jinja. Riesgos que quedan y recomendaciones para exponerlo en una red:
[docs/gateway_semantico.md, sección 10.9](docs/gateway_semantico.md#109-seguridad).

Los certificados y claves TLS se guardan fuera del repo (por ejemplo `~/DBT-GLUE/ssl/`); `.gitignore` excluye
`*.crt`, `*.key` y `*.pem`, igual que el `.env` del proyecto dbt.


Para ejecutar en Athena, la máquina del gateway necesita credenciales de AWS con permisos de Athena, Glue y S3 (datos
y ubicación de resultados). Configuración, permisos IAM y costo: [docs/gateway_semantico.md, sección
10.6](docs/gateway_semantico.md#106-ejecutar-en-athena-y-devolver-filas).

Si dos gateways quedan escuchando en el mismo puerto (en Windows ocurre sin aviso), DBeaver puede conectarse al
viejo: revisar con `netstat -ano | findstr :5433` (Windows) o `lsof -i :5433` (Mac) y cerrar el que sobra.
