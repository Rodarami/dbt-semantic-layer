# Consultas para DBeaver: dbt_glue_lab (eventos e items)

Manifest: `semantic_manifests/dbt_glue_lab/semantic_manifest.json`

Proyecto `DBT/dbt_glue_lab`: eventos de la aplicación (vistas de items y de perfiles) y catálogo de items, sobre tablas Iceberg en Glue (`lab_data_silver`).

Este archivo muestra el SQL de Athena que genera cada consulta. La guía completa de consultas para este manifest, con
descripción de cada una, está en [docs/consultas_dbt_glue_lab.md](../../docs/consultas_dbt_glue_lab.md).

## Cómo usarlas

1. Levantar el gateway con este manifest, desde la raíz `MetricFlow/`:

   ```bash
   python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena
   ```

2. En DBeaver, conexión PostgreSQL: host `127.0.0.1`, puerto `5433` (o el de `--port`), base de datos `athena`, usuario y contraseña `bi`, SSL desactivado.
3. Abrir un *SQL Editor* sobre la conexión y ejecutar las consultas.

Las consultas del catálogo devuelven filas. Con el gateway levantado así (sin `--ejecutar`), las consultas de métricas devuelven **una fila con la columna `sql`**: la traducción para Athena que se muestra en cada ejemplo. En cada consulta, «Resultado» explica qué devuelve ese SQL al ejecutarlo en Athena. Los alias numerados (`subq_4`, `…_src_10000`) cambian entre ejecuciones.

## Qué publica

Tabla **`semantic.events`**

| Métrica | Tipo | Descripción |
| --- | --- | --- |
| `engagement_events` | derived | Suma de vistas de items y de perfiles. |
| `event_count` | simple | Cantidad total de eventos. |
| `item_views` | simple | Cantidad de eventos view_item. |
| `item_views_per_user` | ratio | Vistas de items divididas por usuarios únicos. |
| `item_views_share` | ratio | Proporción de eventos que corresponden a view_item. |
| `profile_views` | simple | Cantidad de eventos view_user_profile. |
| `unique_users` | simple | Cantidad de usuarios distintos que generaron eventos. |

Dimensiones: `event__event_date`, `event__event_name`, `event__event_year`, `event__parameter_name`, `event__platform`, `metric_time`, `metric_time__day`, `metric_time__month`, `metric_time__quarter`, `metric_time__week`, `metric_time__year`.

Tabla **`semantic.items`**

| Métrica | Tipo | Descripción |
| --- | --- | --- |
| `avg_item_price` | simple | Precio promedio de los items. |
| `item_count` | simple | Cantidad de items en el catálogo. |
| `max_item_price` | simple | Precio máximo de los items. |
| `total_item_price` | simple | Suma de los precios de los items. |

Dimensiones: `item__adjective`, `item__category`, `item__created_at_date`, `item__item_name`, `item__modifier`, `metric_time`, `metric_time__day`, `metric_time__month`, `metric_time__quarter`, `metric_time__week`, `metric_time__year`.

## Catálogo

```sql
-- Tablas que cargó el gateway (sirve para confirmar que es el manifest correcto)
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic';

-- Métricas disponibles, su tipo, descripción y tabla
SELECT metric, label, type, description, table_name FROM semantic.metrics;

-- Dimensiones disponibles
SELECT dimension, kind, data_type, description FROM semantic.dimensions;

-- Con qué dimensiones se puede cruzar cada métrica
SELECT metric, dimension FROM semantic.metric_dimensions ORDER BY metric, dimension;
```

## Consultas de métricas

### 1. KPI de actividad del año

Total de eventos y de usuarios distintos en 2024. Ideal para tarjetas de KPI.

```sql
SELECT event_count, unique_users
FROM semantic.events
WHERE metric_time BETWEEN '2024-01-01' AND '2024-12-31';
```

Traducción a SQL de Athena:

```sql
SELECT
  COUNT(DISTINCT event_id) AS event_count
  , COUNT(DISTINCT user_id) AS unique_users
FROM lab_data_silver.silver_event events_src_10000
WHERE DATE_TRUNC('day', event_date) BETWEEN timestamp '2024-01-01' AND timestamp '2024-12-31'
```

**Resultado:** una fila con dos columnas: eventos distintos de 2024 y usuarios distintos que generaron al menos un evento. Los usuarios del año no son la suma de los de cada mes: una persona activa en varios meses cuenta una vez.

### 2. Eventos y usuarios por plataforma

Volumen de actividad por plataforma, de mayor a menor.

```sql
SELECT event__platform, event_count, unique_users
FROM semantic.events
ORDER BY event_count DESC;
```

Traducción a SQL de Athena:

```sql
SELECT
  platform AS event__platform
  , COUNT(DISTINCT event_id) AS event_count
  , COUNT(DISTINCT user_id) AS unique_users
FROM lab_data_silver.silver_event events_src_10000
GROUP BY
  platform
ORDER BY event_count DESC
```

**Resultado:** una fila por plataforma (web, android, iOS…) con su número de eventos y de usuarios distintos.

### 3. Vistas de items por mes y plataforma

Serie temporal para un gráfico de líneas. El filtro `view_item` lo agrega la definición de la métrica, no la consulta.

```sql
SELECT metric_time__month, event__platform, item_views
FROM semantic.events
WHERE metric_time >= '2024-01-01'
ORDER BY metric_time__month, event__platform;
```

Traducción a SQL de Athena:

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

**Resultado:** una fila por mes y plataforma con las vistas de items. Un rango sin fecha de fin aparece como `BETWEEN '2024-01-01' AND '2040-12-31'`: MetricFlow completa el extremo abierto.

### 4. Filtro por dimensión

Eventos por tipo de evento, solo para dos plataformas.

```sql
SELECT event__event_name, event_count
FROM semantic.events
WHERE event__platform IN ('web', 'android')
ORDER BY event_count DESC;
```

Traducción a SQL de Athena:

```sql
SELECT
  event__event_name
  , COUNT(DISTINCT event_count) AS event_count
FROM (
  SELECT
    event_name AS event__event_name
    , platform AS event__platform
    , event_id AS event_count
  FROM lab_data_silver.silver_event events_src_10000
) subq_2
WHERE event__platform IN ('web', 'android')
GROUP BY
  event__event_name
ORDER BY event_count DESC
```

**Resultado:** una fila por tipo de evento (`view_item`, `view_user_profile`, `test_assignment`) con sus eventos en web y android.

### 5. Ratios por plataforma

Intensidad de uso (vistas de items por usuario) y peso de las vistas de items sobre el total de eventos.

```sql
SELECT event__platform, item_views_per_user, item_views_share
FROM semantic.events
ORDER BY item_views_per_user DESC;
```

<details>
<summary>Traducción a SQL de Athena (73 líneas)</summary>

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

**Resultado:** una fila por plataforma con dos decimales: vistas de items por usuario activo, y fracción de eventos que son vistas de items (0 a 1). MetricFlow calcula numerador y denominador por separado y después divide, así el ratio es correcto.

### 6. Engagement semanal del primer trimestre

Interacciones de contenido (vistas de items + vistas de perfil) por semana.

```sql
SELECT metric_time__week, engagement_events, item_views, profile_views
FROM semantic.events
WHERE metric_time BETWEEN '2024-01-01' AND '2024-03-31'
ORDER BY metric_time__week;
```

<details>
<summary>Traducción a SQL de Athena (78 líneas)</summary>

```sql
WITH sma_10001_cte AS (
  SELECT
    DATE_TRUNC('day', event_date) AS metric_time__day
    , DATE_TRUNC('week', event_date) AS metric_time__week
    , event_name AS event__event_name
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
      , __item_views AS item_views
    FROM sma_10001_cte
    WHERE metric_time__day BETWEEN timestamp '2024-01-01' AND timestamp '2024-03-31'
  ) subq_5
  WHERE event__event_name = 'view_item'
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
      , __profile_views AS profile_views
    FROM sma_10001_cte
    WHERE metric_time__day BETWEEN timestamp '2024-01-01' AND timestamp '2024-03-31'
  ) subq_12
  WHERE event__event_name = 'view_user_profile'
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

**Resultado:** 13 filas, una por semana, con el total y su desglose. Siempre se cumple `engagement_events = item_views + profile_views`.

### 7. Ranking de categorías del catálogo

Las 5 categorías con mayor valor de catálogo (tabla `items`).

```sql
SELECT item__category, item_count, total_item_price, avg_item_price, max_item_price
FROM semantic.items
ORDER BY total_item_price DESC
LIMIT 5;
```

Traducción a SQL de Athena:

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

**Resultado:** hasta 5 filas, una por categoría: items distintos, suma, promedio y máximo de precios. Es valor de catálogo, no ventas.

### 8. Items creados por mes

Cuántos items se crearon cada mes y su precio promedio. En `items`, el tiempo es la fecha de creación.

```sql
SELECT metric_time__month, item_count, avg_item_price
FROM semantic.items
ORDER BY metric_time__month;
```

Traducción a SQL de Athena:

```sql
SELECT
  DATE_TRUNC('month', created_at_date) AS metric_time__month
  , COUNT(DISTINCT item_id) AS item_count
  , AVG(price) AS avg_item_price
FROM lab_data_silver.silver_item items_src_10000
GROUP BY
  DATE_TRUNC('month', created_at_date)
ORDER BY metric_time__month
```

**Resultado:** una fila por mes con los items creados ese mes y su precio promedio.

## Lo que no se puede consultar

| Consulta | Motivo |
| --- | --- |
| `SELECT * FROM semantic.<tabla>` | Hay que nombrar las métricas y dimensiones |
| `WHERE <métrica> > 0` | No se puede filtrar por métricas, solo por dimensiones |
| `COUNT(*)`, expresiones (`metrica * 2`), joins, subconsultas | El contrato solo acepta columnas de la tabla semántica |
| Columnas de dos tablas en la misma consulta | Cada consulta lee una sola tabla semántica |
| Pestaña *Datos* de la tabla en DBeaver | La tabla es virtual y está vacía; usar la pestaña *Columnas* |

## Notas

- Las métricas de eventos están en `semantic.events` y las de items en `semantic.items`: no se pueden mezclar en una consulta (por ejemplo `item__category` con `event_count`).
- Los valores de los filtros (`'web'`, `'android'`) son de ejemplo; usar los que existan en los datos.
