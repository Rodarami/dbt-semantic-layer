# Power BI con el gateway semántico: DirectQuery y paneles

Guía para conectar Power BI Desktop al gateway semántico y armar paneles con las métricas de MetricFlow. Sirve para
cualquier manifest; los ejemplos usan el de IT People (`semantic.employee_headcount`) y, entre paréntesis, el de
`dbt_glue_lab` (`semantic.events`).

1. [Importar o DirectQuery](#1-importar-o-directquery)
2. [Qué pasa con cada visual](#2-qué-pasa-con-cada-visual)
3. [Levantar el gateway](#3-levantar-el-gateway)
4. [Conectar Power BI en DirectQuery](#4-conectar-power-bi-en-directquery)
5. [Qué contiene cada tabla](#5-qué-contiene-cada-tabla)
6. [Armar los paneles](#6-armar-los-paneles)
7. [Reglas para que funcione](#7-reglas-para-que-funcione)
8. [Rendimiento y costo](#8-rendimiento-y-costo)
9. [Publicar en Power BI Service](#9-publicar-en-power-bi-service)
10. [Problemas y diagnóstico](#10-problemas-y-diagnóstico)
11. [Cómo lo resuelve el gateway (detalle técnico)](#11-cómo-lo-resuelve-el-gateway-detalle-técnico)

---

## 1. Importar o DirectQuery

| | Importar | DirectQuery |
| --- | --- | --- |
| Dónde quedan los datos | Copiados dentro del `.pbix` | En Athena; Power BI no guarda filas |
| Cuándo consulta al gateway | Al cargar y en cada *Actualizar* | Cada vez que un visual se dibuja, se filtra o cambia |
| Frescura | La de la última actualización | Siempre la última carga de Athena |
| Velocidad del panel | Inmediata (memoria) | Unos segundos por visual (lo que tarde Athena) |
| Métricas | Hay que traer la consulta con el grano exacto que se quiere ver | MetricFlow calcula la métrica al grano de cada visual |
| Ratios y conteos distintos | Correctos solo al grano importado; sumarlos en Power BI da números falsos | Correctos en cualquier grano y en los totales |
| Costo en Athena | Una consulta por actualización | Una consulta por visual y por interacción |

**Recomendación:** DirectQuery es la forma natural de usar la capa semántica. Cada visual le pide a MetricFlow la
métrica al grano exacto del visual, así que los ratios (`ratio_internos`), los conteos distintos
(`proveedores_distintos`) y los totales salen bien sin escribir DAX. Importar conviene solo para tablas chicas y de
grano fijo, o cuando no se quiere que cada clic consulte Athena.

## 2. Qué pasa con cada visual

```
Power BI (visual)                 Gateway semántico                                   Athena
─────────────────                 ─────────────────                                   ──────
"dotacion_total por      ──SQL──► 1. translator.py: busca la consulta semántica
 employee__position"               2. MetricFlow la compila a SQL de Athena  ──SQL──► ejecuta
                                   3. (si Power BI agregó CASE, cast, orden…)  ◄─filas─
                                      los aplica DuckDB sobre esas pocas filas
                         ◄─filas── 4. devuelve el resultado con los tipos de Postgres
```

Power BI cree que habla con un PostgreSQL: ve un esquema `semantic` con una tabla por modelo semántico, cuyas columnas
son las métricas y las dimensiones. Las tablas no tienen filas propias; cada consulta se traduce y se calcula en ese
momento.

## 3. Levantar el gateway

En Windows (PowerShell), desde la raíz del repo `MetricFlow`, con el entorno `capasemantica` (Python 3.12) y las
credenciales de AWS activas ([credenciales_aws_windows.md](credenciales_aws_windows.md)):

```powershell
conda activate capasemantica
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer --log-sql
```

- `--ejecutar` es obligatorio para Power BI: sin él el gateway devuelve el SQL compilado en vez de filas.
- `--log-sql` muestra en la consola cada consulta que llega de Power BI y el SQL que se envió a Athena. Conviene
  dejarlo mientras se arma el panel.
- Para otro manifest, cambiar `--manifest` (por ejemplo `semantic_manifests/dbt_glue_lab/semantic_manifest.json`).
- Después de un `git pull` hay que detener el gateway (Ctrl+C) y volver a levantarlo.

## 4. Conectar Power BI en DirectQuery

1. *Inicio* → *Obtener datos* → *Base de datos PostgreSQL*.
2. Completar:

   | Campo | Valor |
   | --- | --- |
   | Servidor | `127.0.0.1:5433` (o `host:puerto` del gateway) |
   | Base de datos | `athena` (el motor que ejecuta; con otro nombre el gateway devuelve SQL) |
   | Modo de conectividad | **DirectQuery** |
   | Opciones avanzadas | Vacías: no escribir una instrucción SQL |

3. Credenciales: pestaña *Base de datos*, usuario y contraseña del gateway (`bi` / `bi` en pruebas locales).
4. Si el gateway corre sin TLS, Power BI avisa que no pudo cifrar la conexión: aceptar *Conectar sin cifrar*
   (solo en local). Con TLS, el certificado debe ser de una CA de confianza; Power BI rechaza los autofirmados.
5. En el *Navegador*, marcar la tabla del modelo (`semantic.employee_headcount`; con `dbt_glue_lab`,
   `semantic.events` y `semantic.items`) y pulsar **Cargar**, no *Transformar datos*.

Para cambiar después el servidor, la base o las credenciales: *Inicio* → *Transformar datos* → *Configuración de
origen de datos* → *Cambiar origen…* / *Editar permisos*. Tras actualizar el gateway no hace falta tocar la conexión:
basta con *Inicio* → *Actualizar*.

## 5. Qué contiene cada tabla

| Tabla | Contenido | Uso en Power BI |
| --- | --- | --- |
| `semantic.<modelo>` (por ejemplo `employee_headcount`) | Una columna por métrica (Σ) y por dimensión del modelo semántico | **La que se usa para los paneles** |
| `semantic.metrics` | Catálogo: nombre, etiqueta, tipo y tabla de cada métrica | Opcional, como documentación |
| `semantic.dimensions` | Catálogo de dimensiones y su tipo | Opcional |
| `semantic.metric_dimensions` | Qué dimensiones admite cada métrica | Opcional |

Columnas de la tabla del modelo:

- **Métricas** (con Σ): `dotacion_total`, `dotacion_internos`, `ratio_internos`… (`event_count`, `unique_users`…).
  Cada una ya es un cálculo de MetricFlow: suma, conteo distinto o ratio, según su definición en dbt.
- **Dimensiones**: `employee__position`, `employee__gender`, `employee__process_date`… (`event__platform`…).
- **Tiempo**: `metric_time` (día) y sus granos `metric_time__week`, `__month`, `__quarter`, `__year`. Usar la columna
  del grano que se quiere ver, en vez de la jerarquía de fechas de Power BI.

## 6. Armar los paneles

### 6.0 Qué combinaciones se pueden hacer

**Regla:** dentro de una misma tabla `semantic.<modelo>`, **cualquier métrica se puede combinar con cualquier
dimensión** de esa tabla, en cualquier cantidad. El gateway solo publica en cada tabla las dimensiones que sirven para
todas sus métricas, así que no hay combinaciones inválidas dentro de una tabla. Lo que no se puede es mezclar campos
de dos tablas semánticas en un mismo visual.

Para ver las combinaciones desde Power BI, cargar también las tablas de catálogo (están en el mismo navegador):

| Pregunta | Cómo verlo en Power BI |
| --- | --- |
| ¿Qué métricas hay y qué calcula cada una? | Tabla con `semantic metrics`: `metric`, `label`, `type`, `description`, `table_name` |
| ¿Qué dimensiones hay y cuáles son de tiempo? | Tabla con `semantic dimensions`: `dimension`, `kind` (`time` / `categorical`), `data_type`, `description` |
| ¿Con qué dimensiones se puede cortar una métrica? | Matriz con `semantic metric_dimensions`: filas `dimension`, columnas `metric`, valores *Recuento de dimension*. Cada 1 es una combinación válida. O una tabla con `metric` y `dimension` y un segmentador por `metric` |

Las tablas de catálogo son información (las responde el gateway sin ir a Athena): no se cruzan con
`semantic.<modelo>` en un mismo visual ni se relacionan con ella.

Ejemplos de combinaciones con IT People (`semantic.employee_headcount`):

| Visual | Dimensiones | Métricas |
| --- | --- | --- |
| Tarjeta | — | `dotacion_total` |
| Barras | `employee__position` | `dotacion_total`, `dotacion_vacantes` |
| Tabla | `employee__gender`, `employee__position` | `dotacion_total`, `ratio_internos`, `ratio_externos` |
| Línea | `metric_time__month` | `dotacion_total` |
| Segmentador | `employee__gender` | — |

**Clic en un visual = filtro para los demás:** al seleccionar una fila (por ejemplo *No informado* en la tabla de
género), Power BI filtra los otros visuales de la página con ese valor. La tarjeta de `dotacion_total` muestra entonces
la dotación de *No informado*, no el total. Para volver al total, hacer clic de nuevo en la fila o en un espacio vacío.
Para que un visual no se filtre: seleccionar el visual de origen → *Formato* → *Editar interacciones* → *Ninguno* en el
visual destino.

Cada visual se arma arrastrando una o más dimensiones y una o más métricas **de la misma tabla**. En los ejemplos,
entre paréntesis, el equivalente con `dbt_glue_lab`.

### 6.1 Tarjeta de KPI

- Visual: *Tarjeta*.
- Campo: `dotacion_total` (`event_count`).
- Gateway: consulta sin dimensiones → MetricFlow devuelve el total.

### 6.2 Barras por categoría

- Visual: *Gráfico de barras agrupadas*.
- Eje Y: `employee__position` (`event__platform`). Eje X: `dotacion_total` (`event_count`).
- Para ordenar, usar el menú `…` del visual → *Ordenar por*.

### 6.3 Línea en el tiempo

- Visual: *Gráfico de líneas*.
- Eje X: `metric_time__month` (o `__week`, `__day`). Eje Y: `dotacion_total` (`unique_users`).
- Leyenda opcional: `employee__gender` (`event__platform`) para una línea por valor.
- Si el eje X muestra una jerarquía (Año, Trimestre…), cambiarla en el campo del eje a `metric_time__month` sin
  jerarquía.

### 6.4 Tabla o matriz con ratios

- Visual: *Tabla* o *Matriz*.
- Columnas: `employee__position`, `employee__gender`, `dotacion_total`, `ratio_internos`.
- El ratio de cada fila y el del total los calcula MetricFlow (el total es otra consulta, sin dimensiones), así que
  no son sumas ni promedios de las filas. Power BI pide el total como `SUM` sobre la consulta ya agrupada; el gateway
  lo reconoce y pide a MetricFlow la métrica sin esas dimensiones. Con un gateway anterior al 19 de octubre de 2026 el
  total de un ratio aparecía sumado (por ejemplo 70,6).

### 6.5 Segmentador y filtros

- Visual: *Segmentación de datos* con `employee__gender` (`event__platform`).
- El segmentador pide los valores de la dimensión (consulta solo con dimensión, sin métricas) y, al elegir un valor,
  los demás visuales mandan ese filtro a MetricFlow como `WHERE`.
- Los filtros del panel lateral (*Filtros de este objeto visual / de esta página*) funcionan igual. Los filtros sobre
  fechas (`metric_time`) se convierten en rango de tiempo de MetricFlow.

### 6.6 Columna de fecha de una dimensión

- Una dimensión de fecha propia del modelo (`employee__process_date`) se puede poner en una tabla o en un eje.
- Power BI envuelve estas consultas con `CASE`, `cast` y orden de nulos; el gateway manda a Athena solo la parte
  semántica y aplica el resto en DuckDB (sección 11).

## 7. Reglas para que funcione

| Regla | Por qué |
| --- | --- |
| En un visual, campos de **una sola** tabla semántica | Mezclar `events` e `items` en un visual genera un `JOIN`, que el gateway rechaza. Para combinarlos, una métrica derivada en dbt |
| **No crear relaciones** entre tablas semánticas ni con otras tablas | Las relaciones generan `JOIN`; cada tabla ya trae sus dimensiones |
| Dejar las métricas con su resumen por defecto (*Suma*) | El gateway ignora la función de resumen (`SUM`, `MIN`, `MAX`, `AVG` devuelven la métrica de MetricFlow). *Recuento* y *Recuento distinto* se calculan en DuckDB sobre el resultado, no sobre los datos originales |
| No crear columnas calculadas ni medidas DAX que recorran filas | No se pueden traducir a MetricFlow. Las métricas nuevas se definen en dbt y se regenera el manifest |
| No usar *Transformar datos* (Power Query) sobre la tabla | Los pasos de Power Query que no se pliegan a SQL fallan en DirectQuery |
| Usar `metric_time__<grano>` para el tiempo | Es el grano que entiende MetricFlow; la jerarquía automática de Power BI no aplica |

Métricas nuevas o dimensiones nuevas: se agregan en el proyecto dbt, se regenera `semantic_manifest.json`, se
reinicia el gateway y en Power BI *Inicio* → *Actualizar* (o *Transformar datos* → *Actualizar vista previa* para
que aparezcan las columnas nuevas).

## 8. Rendimiento y costo

Cada visual es una consulta a Athena (unos segundos y cobro por datos escaneados). Una página con 6 visuales y un
segmentador hace 6 consultas cada vez que se cambia el segmentador.

- *Archivo* → *Opciones y configuración* → *Opciones* → *Reducción de consultas*: activar *Agregar un botón Aplicar a
  las segmentaciones* y *a los filtros*. Así los cambios se envían de una vez y no con cada clic.
- Mantener pocas métricas por página y evitar tablas con muchas dimensiones combinadas.
- Power BI guarda en caché los resultados mientras el informe está abierto; *Actualizar* los vuelve a pedir.
- El gateway corta las consultas que tardan más de `--timeout` (100 s por defecto) y las filas por encima de
  `--max-filas`.
- En la consola (`--log-sql`) cada consulta muestra el `query_id` de Athena, los segundos y los bytes escaneados.

## 9. Publicar en Power BI Service

Con DirectQuery, el servicio en la nube también tiene que llegar al gateway semántico:

1. Instalar un **on-premises data gateway** de Power BI en una máquina que vea el gateway semántico.
2. Exponer el gateway semántico con TLS obligatorio (`--tls-cert`, `--tls-key`, `--tls-obligatorio`), un
   **certificado de una CA de confianza** y usuario y clave propios (no `bi`/`bi`). Ver
   [gateway_semantico.md, secciones 10.7 y 10.9](gateway_semantico.md#109-seguridad).
3. En el servicio, configurar el origen PostgreSQL (`host:puerto`, base `athena`) en ese data gateway y publicar el
   informe.

## 10. Problemas y diagnóstico

| Síntoma en Power BI | Causa | Qué hacer |
| --- | --- | --- |
| *No se ha podido plegar la expresión al origen de datos* | Gateway antiguo (tipos de columnas sin nombres de Postgres) | `git pull` y reiniciar el gateway |
| *Las subconsultas no están soportadas…* / *Expresión no soportada: CASE WHEN…* | Gateway antiguo, sin aplanado de envoltorios ni núcleo/resto | `git pull` y reiniciar el gateway |
| *Query contains duplicate output column names: metric_time__day* | Gateway antiguo (`metric_time` y `metric_time__day` a la vez) | `git pull` y reiniciar |
| *…no está soportado en la capa semántica* con un `JOIN` en el log | Campos de dos tablas semánticas o una relación | Usar una sola tabla por visual y borrar las relaciones |
| *No se puede filtrar por la métrica…* | Filtro de nivel visual sobre una métrica (por ejemplo `dotacion_total > 10`) en Importar | En DirectQuery se aplica en DuckDB; en Importar, quitarlo |
| El visual sigue mostrando el error anterior | Caché de Power BI | *Archivo* → *Opciones* → *Carga de datos* → *Borrar caché*, o borrar y volver a crear el visual |
| No aparece nada en la consola del gateway | Power BI usa caché o apunta a otro gateway | *Actualizar*; revisar `netstat -ano \| findstr :5433` por gateways duplicados |
| Error de credenciales de AWS en la consola | Sesión SSO vencida | `aws sso login --profile <perfil>` y reiniciar el gateway |

Diagnóstico:

- **Consola del gateway con `--log-sql`:** cada consulta rechazada aparece como `Consulta rechazada: <motivo>` con el
  SQL recibido. Con eso se puede reproducir en DBeaver.
- **Power BI:** *Archivo* → *Opciones* → *Diagnósticos* → *Habilitar seguimiento*; o *Ver* → *Analizador de
  rendimiento* → *Iniciar grabación* → *Actualizar objetos visuales* para ver la consulta y el tiempo de cada visual.

## 11. Cómo lo resuelve el gateway (detalle técnico)

Power BI no envía `SELECT dimension, metrica FROM tabla`: envuelve la consulta en subconsultas propias. El gateway
la procesa en tres niveles, en este orden:

| Nivel | Cuándo | Qué hace | Dónde |
| --- | --- | --- | --- |
| 1. Directo | La consulta ya es semántica | MetricFlow → Athena | `translator.translate` |
| 2. Aplanado | Las capas externas solo renombran, filtran, ordenan, limitan o agregan con `SUM/MAX/MIN/AVG` (Importar y medidas en DirectQuery) | Funde las capas en una sola consulta semántica → MetricFlow → Athena | `translator._aplanar` |
| 3. Núcleo + resto | Las capas externas calculan cosas que MetricFlow no entiende (`CASE`, `cast`, banderas de nulos, `COUNT`, filtros sobre métricas) | El **núcleo** (la única subconsulta que lee la tabla semántica) va a MetricFlow → Athena; el **resto** se ejecuta en DuckDB sobre ese resultado | `translator.separar_nucleo`, `server._nucleo_y_resto` |

Ejemplo real (visual de tabla con `employee__process_date`):

```sql
-- lo que envía Power BI
select "_"."employee__process_date" as "c8"
from (select "rows"."employee__process_date" as "employee__process_date",
             case when "rows"."o0" is not null then cast("rows"."o0" as timestamp)
                  else timestamp '1899-12-28 00:00:00' end as "t0_0",
             case when "rows"."o0" is null then 0 else 1 end as "t1_0"
      from (select "employee__process_date" as "employee__process_date", "employee__process_date" as "o0"
            from "semantic"."employee_headcount" "$Table"
            group by "employee__process_date", "o0") "rows") "_"
order by "_"."t0_0", "_"."t1_0"
limit 501

-- núcleo → MetricFlow → Athena
SELECT "employee__process_date", "employee__process_date" AS "o0"
FROM "semantic"."employee_headcount" GROUP BY ...

-- resto → DuckDB, sobre la tabla __nucleo_semantico con las filas que devolvió Athena
SELECT ... FROM (SELECT * FROM __nucleo_semantico) AS "rows" ... ORDER BY ... LIMIT 501
```

Condiciones y seguridad del nivel 3:

- Solo con motor ejecutor (`dbname=athena` y `--ejecutar`). En `EXPLAIN` o con otros motores se devuelve el SQL del
  núcleo.
- Debe haber **un solo** núcleo y el resto solo puede leer `__nucleo_semantico`. Dos tablas semánticas, o una
  semántica con otra tabla, se rechazan.
- El resto corre en una base DuckDB efímera por consulta, blindada como el catálogo: sin archivos, red ni
  extensiones, configuración bloqueada y una sola sentencia `SELECT`.
- Los filtros que Power BI ponga en una capa externa se aplican en DuckDB, no en Athena: el resultado es correcto, pero
  Athena calcula todas las filas del núcleo. Los filtros que Power BI pone en la capa interna (los habituales de
  segmentadores) sí llegan a MetricFlow.

Por qué se eligió esta solución y en qué se diferencia del DuckDB del catálogo: [duckdb_efimero.md](duckdb_efimero.md).

Pruebas: `tests/test_translator.py` (`test_envoltorios_de_power_bi`, `test_nucleo_y_resto`) y
`tests/test_gateway.py` (`probar_ejecucion`, con el SQL de Power BI para fechas).
