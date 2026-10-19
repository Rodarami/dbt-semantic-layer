# Librerías para el protocolo PostgreSQL: buenavista, riffq y alternativas

El gateway semántico se presenta ante las herramientas BI como un servidor PostgreSQL. Esa capa de protocolo la hace
una librería; el resto del gateway (traductor, MetricFlow, ejecutor de Athena y catálogo) no depende de cuál se use.

Este documento compara las opciones y deja registrado qué se aprendió al migrar de **buenavista** (la versión
anterior, `semantic_gateway/server_old.py`) a **riffq** (la actual, `semantic_gateway/server.py`).

## 1. Comparación

| | **buenavista** (anterior) | **riffq** (actual) | **datafusion-postgres / pgwire** |
| --- | --- | --- | --- |
| Lenguaje | Python puro | Núcleo en Rust (`pgwire`); la lógica se escribe en Python | Rust; no se encontró forma de usarlo desde Python |
| Versión y estado | 0.5.0, de octubre de 2024. Sin publicaciones nuevas desde entonces, un solo mantenedor | 0.1.11, de julio de 2026. Preversión (0.1.x) | 0.18.0, de julio de 2026. Desarrollo activo |
| Licencia | Apache 2.0 | MIT o Apache 2.0 | MIT o Apache 2.0 |
| Protocolo | Simple y extendido, con fallos que hubo que corregir en el gateway: consulta vacía, respuestas de más tras un error, `DISCARD ALL` | Simple y extendido sobre `pgwire`; consulta vacía, `DISCARD ALL` y descarte de mensajes tras un error resueltos de fábrica | Igual que riffq |
| TLS | No | Sí, opcional (`set_tls`) | Sí |
| Autenticación | Usuario y clave fijos (MD5) | Función propia en Python (`handle_auth`) y filtro por IP (`handle_connect`) | Opcional; la documentación no explica cómo configurarla |
| Catálogo (`pg_catalog`) | No trae: el gateway lo arma con DuckDB | Trae uno emulado en Rust (`pg_catalog_rs` con DataFusion) | Trae uno (`datafusion-pg-catalog`) |
| Resultados | Tabla Arrow → filas | Arrow sin copias (PyCapsule) | Arrow |
| Concurrencia | Un hilo por conexión | Rust asíncrono + pool de hilos de Python; una consulta lenta no bloquea al resto | Rust asíncrono |
| Describe del protocolo extendido | Ejecuta la consulta para conocer sus columnas | Pide el esquema por separado (`do_describe=True`): el gateway lo responde sin ejecutar | Igual que riffq |
| Clientes probados según su documentación | psql, DBeaver | DataGrip, DBeaver, psql, VS Code. No menciona Power BI ni Metabase | psql, DBeaver, Metabase, Grafana. Power BI sin marcar |
| Instalación | `pip`, cualquier Python ≥ 3.8 | Paquetes precompilados solo para Python 3.12 (Windows, Linux con glibc ≥ 2.38) y 3.14 (Mac Apple Silicon). Con otra versión se compila y hace falta Rust | Compilar con Cargo |

## 2. Qué se encontró al migrar a riffq

Todo se probó con `tests/test_gateway.py`: las 12 pruebas comunes (incluida la de seguridad) pasan con las dos
versiones, y el servidor actual suma una de TLS obligatorio. También pasan las 21
consultas de catálogo que envía DBeaver y el lote de tipos de Npgsql (Power BI).

| Hallazgo | Impacto | Cómo se resolvió |
| --- | --- | --- |
| El catálogo emulado de riffq no entiende varias consultas reales, por ejemplo `::regnamespace` de Metabase. Esas consultas se resuelven en Rust y Python no puede corregirlas | Metabase no podría sincronizar | Se desactiva (`catalog_emulation=False`) y se usa el catálogo propio en DuckDB, ahora en `catalogo_pg.py` |
| En el protocolo extendido, riffq pasa el SQL **a minúsculas** antes de ejecutarlo (no al describirlo) | Un filtro `WHERE employee__gender = 'F'` llegaría como `'f'` | `ConexionSemantica._original` guarda el texto del describe y lo recupera al ejecutar. Hay una prueba que lo comprueba |
| Los parámetros de texto **sin tipo** llegan como `NULL` | Afecta a psycopg (los envía sin tipo por defecto). JDBC (DBeaver, Metabase) y Npgsql (Power BI) envían el tipo y no se ven afectados | Documentado. En las pruebas, psycopg declara el tipo (`StrDumper`) |
| El describe llega a Python por separado de la ejecución | Ventaja: no hace falta ejecutar en Athena para conocer las columnas | `_semantica` devuelve solo el esquema en el describe; la consulta va a Athena una sola vez |
| Los clientes que leen en binario (Npgsql) interpretan los bytes según el tipo anunciado en el describe | Si Athena devolviera un entero donde se anunció `DOUBLE`, el valor llegaría mal | `_ajustar_tipos` convierte el resultado a los tipos anunciados |
| El núcleo Rust registra cada consulta en nivel INFO | Log muy ruidoso | `RUST_LOG=warn` por defecto |
| Ctrl+C | Termina limpio aunque haya clientes conectados | `main` muestra `Gateway detenido` |
| TLS | Funciona con `sslmode=require` (probado con un certificado autofirmado) | Opciones `--tls-cert` y `--tls-key` |
| TLS es **opcional** para el cliente: con certificado, un cliente con SSL desactivado se conecta igual y sin cifrar. riffq no permite exigirlo ni informa a Python si la conexión vino cifrada | Las credenciales y los datos podrían viajar en claro | `--tls-obligatorio`: una puerta en Python (`tls_obligatorio.py`) atiende el puerto público, exige el `SSLRequest`, cifra y reenvía a riffq en un puerto interno. Probado con psycopg (libpq) y node-postgres; hay una prueba en `test_gateway` |
| Metabase veía las vistas auxiliares del esquema `main` como tablas (lee `pg_class` y `pg_namespace`, no `information_schema`) | Al sincronizar habría aparecido un esquema `main` con vistas internas | `catalogo_pg.py` responde `pg_class` y `pg_namespace` sin `main`, igual que ya hacía con `information_schema` para Power BI. Hay una prueba que lo comprueba |

## 3. Decisión

- **riffq como base del gateway.** Aporta lo que el ADR va a exigir y buenavista no tenía (TLS, autenticación propia,
  un protocolo más robusto y concurrencia real), y la lógica sigue en Python, donde está MetricFlow.
- **buenavista se conserva como respaldo** (`server_old.py`), por si aparece un problema con riffq en algún cliente.
  Se usa con `python -m semantic_gateway.server_old`.
- **Catálogo propio en DuckDB**, no el de riffq, mientras su catálogo emulado no cubra las consultas de los clientes
  BI (sección 5: hoy resuelve 20 de 25). Se puede revisar en cada versión nueva de riffq.
- **datafusion-postgres o pgwire directo** solo tendrían sentido si el gateway se reescribiera en Rust; no compensa
  porque MetricFlow es Python.

## 4. Riesgos que siguen abiertos

| Riesgo | Mitigación |
| --- | --- |
| riffq está en preversión (0.1.x) | Fijar la versión en `requirements-gateway.txt` y correr `python -m tests.test_gateway` antes de actualizar |
| Paquetes precompilados para pocas versiones de Python | Usar Python 3.12 (Windows) o 3.14 (Mac Apple Silicon), o instalar Rust para compilarlo |
| Power BI genera SQL propio en DirectQuery (subconsultas, `CASE`, `cast`) que puede cambiar con cada versión | Probado de punta a punta en Importar y DirectQuery (octubre de 2026). Si una versión nueva manda otra forma, el gateway la registra con `--log-sql` como *Consulta rechazada*; ver [power_bi_directquery.md](power_bi_directquery.md) |
| Parámetros de texto sin tipo | Solo afecta a clientes que no declaran tipos (psycopg por defecto) |
| La puerta de `--tls-obligatorio` es Python (asyncio) y suma un salto local | Despreciable frente a la latencia de Athena. Si riffq agrega una opción para exigir TLS, se reemplaza |
| Balanceador en AWS | Debe ser un NLB con listener **TCP**: un listener TLS no sirve, porque los clientes Postgres envían primero el `SSLRequest` sin cifrar |
| DuckDB sigue siendo necesario | Es liviano (librería en memoria, sin servidor). Repetir la prueba de la sección 5 con cada versión de riffq |

## 5. Prueba: ¿se puede usar el catálogo de riffq y prescindir de DuckDB?

riffq puede responder él mismo las consultas de catálogo (`start(catalog_emulation=True)`), con las tablas registradas
desde Python. Si cubriera lo que piden los clientes BI, DuckDB sobraría. Para comprobarlo,
`tests/probar_catalogo_riffq.py` levanta un riffq mínimo con ese catálogo, registra las tablas semánticas del manifest
con `set_lazy_catalog` y lanza las consultas de catálogo de DBeaver, Power BI (Npgsql) y Metabase. Con `--comparar`
lanza las mismas contra el gateway actual (catálogo en DuckDB).

```bash
python -m tests.probar_catalogo_riffq --comparar
```

Resultado con riffq 0.1.11 (16 de octubre de 2026), manifest de `dbt_glue_lab`:

| Cliente | Consultas | Catálogo de riffq (sin DuckDB) | Catálogo en DuckDB (actual) |
| --- | --- | --- | --- |
| DBeaver (conectar y navegar) | 18 | 14 ✅ · **4 ❌** | 18 ✅ |
| Power BI (tipos de Npgsql, navegador, columnas) | 3 | 3 ✅ | 3 ✅ |
| Metabase (esquemas, tablas, columnas, privilegios) | 4 | 3 ✅ · **1 ❌** | 4 ✅ |
| **Total** | **25** | **20** | **25** |

Lo que falla con el catálogo de riffq:

| Consulta | Error | Efecto en el cliente |
| --- | --- | --- |
| DBeaver · lista de esquemas (`pg_namespace` con `'pg_namespace'::regclass`) | `Optimizer rule 'simplify_expressions' failed` | El árbol de navegación no muestra esquemas |
| DBeaver · lista de tablas (`pg_class` con `'pg_class'::regclass`) | El mismo | No se ven las tablas |
| DBeaver · restricciones | No existe `pg_get_constraintdef` | Error en la pestaña de propiedades de la tabla |
| DBeaver · tamaño de la tabla | No existen `pg_table_size` ni `pg_indexes_size` | Error en las estadísticas de la tabla |
| Metabase · columnas (`::regnamespace`) | `Unsupported SQL type regnamespace` | La sincronización de columnas falla |

Además:

- **No se pueden corregir desde Python.** Con el catálogo de riffq activo, esas consultas las resuelve DataFusion en
  Rust y el error vuelve directo al cliente: no pasan por `handle_query`. Con DuckDB, en cambio, `catalogo_pg.py` las
  intercepta y las reescribe.
- **`semantic.metrics`, `semantic.dimensions` y `semantic.metric_dimensions` no existen** en ese catálogo. Habría que
  responderlas en Python, incluidos los filtros, el orden y los `JOIN` que escriba el usuario: es justo lo que hoy hace
  DuckDB.
- **Registro de tablas:** con `set_lazy_catalog` y OID de tipo, las columnas salen con su tipo real (`double precision`,
  `date`, `timestamp`). Con `register_table`, todo lo que no es entero o booleano sale como `text`. El error de OID
  duplicado que apareció en la primera prueba no se repite con `set_lazy_catalog`.

**Conclusión:** DuckDB se mantiene. El catálogo de riffq sirve para Power BI, pero deja a DBeaver sin navegación y a
Metabase sin columnas, y no hay forma de corregirlo desde el gateway. Cuando una versión nueva de riffq pase las 25
consultas, bastaría con activar `catalog_emulation=True`, registrar las tablas con `set_lazy_catalog` y responder en
Python las tablas de metadatos.

## Fuentes

- [riffq en PyPI](https://pypi.org/project/riffq/)
- [buenavista en PyPI](https://pypi.org/project/buenavista)
- [buenavista en GitHub](https://github.com/jwills/buenavista)
- [datafusion-postgres en docs.rs](https://docs.rs/crate/datafusion-postgres/latest)
- [pgwire en docs.rs](https://docs.rs/crate/pgwire/latest)
