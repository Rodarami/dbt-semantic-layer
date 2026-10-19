# Consultas para DBeaver: IT People (dotación)

Manifest: `semantic_manifests/it-people/semantic_manifest.json`

Proyecto de dotación IT People: modelo semántico `employee_headcount` sobre `dlk_gld_it_people_dwh.fact_empleado` (snapshot de colaboradores).

Este archivo muestra el SQL de Athena que genera cada consulta. La guía completa de consultas para este manifest, con
descripción de cada una, está en [docs/consultas_it_people.md](../../docs/consultas_it_people.md).

## Cómo usarlas

1. Levantar el gateway con este manifest, desde la raíz `MetricFlow/`:

   ```bash
   python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena
   ```

2. En DBeaver, conexión PostgreSQL: host `127.0.0.1`, puerto `5433` (o el de `--port`), base de datos `athena`, usuario y contraseña `bi`, SSL desactivado.
3. Abrir un *SQL Editor* sobre la conexión y ejecutar las consultas.

Las consultas del catálogo devuelven filas. Con el gateway levantado así (sin `--ejecutar`), las consultas de métricas devuelven **una fila con la columna `sql`**: la traducción para Athena que se muestra en cada ejemplo. En cada consulta, «Resultado» explica qué devuelve ese SQL al ejecutarlo en Athena. Los alias numerados (`subq_4`, `…_src_10000`) cambian entre ejecuciones.

## Qué publica

Tabla **`semantic.employee_headcount`**

| Métrica | Tipo | Descripción |
| --- | --- | --- |
| `dotacion_externos` | simple | Cantidad total de colaboradores externos |
| `dotacion_internos` | simple | Cantidad total de colaboradores internos |
| `dotacion_total` | simple | Cantidad total de colaboradores vigentes |
| `dotacion_vacantes` | simple | Cantidad de vacantes vigentes |
| `proveedores_distintos` | simple | Cantidad de proveedores distintos |
| `ratio_externos` | ratio | Participación de colaboradores externos sobre la dotación total |
| `ratio_internos` | ratio | Participación de colaboradores internos sobre la dotación total |

Dimensiones: `employee__gender`, `employee__position`, `employee__process_date`, `metric_time`, `metric_time__day`, `metric_time__month`, `metric_time__quarter`, `metric_time__week`, `metric_time__year`.

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

### 1. KPIs generales de dotación

Dotación total, internos, externos y vacantes.

```sql
SELECT dotacion_total, dotacion_internos, dotacion_externos, dotacion_vacantes
FROM semantic.employee_headcount;
```

Traducción a SQL de Athena:

```sql
SELECT
  COUNT(DISTINCT user_ad) AS dotacion_total
  , SUM(internal_employee) AS dotacion_internos
  , SUM(outsourcing_employee) AS dotacion_externos
  , SUM(vacancy) AS dotacion_vacantes
FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
```

**Resultado:** una fila con los cuatro totales.

### 2. Mix interno/externo por cargo (top 10)

Dotación por cargo y proporción de internos y externos.

```sql
SELECT employee__position, dotacion_total, ratio_internos, ratio_externos
FROM semantic.employee_headcount
ORDER BY dotacion_total DESC
LIMIT 10;
```

<details>
<summary>Traducción a SQL de Athena (38 líneas)</summary>

```sql
WITH sma_10001_cte AS (
  SELECT
    position AS employee__position
    , user_ad AS __dotacion_total
    , internal_employee AS __dotacion_internos
    , outsourcing_employee AS __dotacion_externos
  FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
)

SELECT
  COALESCE(subq_5.employee__position, subq_10.employee__position) AS employee__position
  , MAX(subq_5.dotacion_total) AS dotacion_total
  , MAX(CAST(subq_10.dotacion_internos AS DOUBLE) / CAST(NULLIF(subq_10.dotacion_total, 0) AS DOUBLE)) AS ratio_internos
  , MAX(CAST(subq_10.dotacion_externos AS DOUBLE) / CAST(NULLIF(subq_10.dotacion_total, 0) AS DOUBLE)) AS ratio_externos
FROM (
  SELECT
    employee__position
    , COUNT(DISTINCT __dotacion_total) AS dotacion_total
  FROM sma_10001_cte
  GROUP BY
    employee__position
) subq_5
FULL OUTER JOIN (
  SELECT
    employee__position
    , SUM(__dotacion_internos) AS dotacion_internos
    , COUNT(DISTINCT __dotacion_total) AS dotacion_total
    , SUM(__dotacion_externos) AS dotacion_externos
  FROM sma_10001_cte
  GROUP BY
    employee__position
) subq_10
ON
  subq_5.employee__position = subq_10.employee__position
GROUP BY
  COALESCE(subq_5.employee__position, subq_10.employee__position)
ORDER BY dotacion_total DESC
LIMIT 10
```

</details>

**Resultado:** hasta 10 filas, una por cargo. Los ratios van de 0 a 1: 0,75 significa que el 75 % son internos.

### 3. Dotación por género

Total, internos y externos por género.

```sql
SELECT employee__gender, dotacion_total, dotacion_internos, dotacion_externos
FROM semantic.employee_headcount;
```

Traducción a SQL de Athena:

```sql
SELECT
  gender AS employee__gender
  , COUNT(DISTINCT user_ad) AS dotacion_total
  , SUM(internal_employee) AS dotacion_internos
  , SUM(outsourcing_employee) AS dotacion_externos
FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
GROUP BY
  gender
```

**Resultado:** una fila por género.

### 4. Foto de un día

Dotación vigente a una fecha concreta. Evita sumar fotos de varios días.

```sql
SELECT employee__process_date, dotacion_total, dotacion_internos, dotacion_vacantes
FROM semantic.employee_headcount
WHERE employee__process_date = '2026-10-13';
```

Traducción a SQL de Athena:

```sql
SELECT
  employee__process_date__day
  , COUNT(DISTINCT dotacion_total) AS dotacion_total
  , SUM(dotacion_internos) AS dotacion_internos
  , SUM(dotacion_vacantes) AS dotacion_vacantes
FROM (
  SELECT
    DATE_TRUNC('day', process_date) AS employee__process_date__day
    , user_ad AS dotacion_total
    , internal_employee AS dotacion_internos
    , vacancy AS dotacion_vacantes
  FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
) subq_2
WHERE employee__process_date__day = '2026-10-13'
GROUP BY
  employee__process_date__day
```

**Resultado:** una fila con la dotación de ese día.

### 5. Evolución mensual

Dotación y vacantes por mes desde enero de 2026.

```sql
SELECT metric_time__month, dotacion_total, dotacion_vacantes
FROM semantic.employee_headcount
WHERE metric_time >= '2026-01-01'
ORDER BY metric_time__month;
```

Traducción a SQL de Athena:

```sql
SELECT
  DATE_TRUNC('month', process_date) AS metric_time__month
  , COUNT(DISTINCT user_ad) AS dotacion_total
  , SUM(vacancy) AS dotacion_vacantes
FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
WHERE DATE_TRUNC('day', process_date) BETWEEN timestamp '2026-01-01' AND timestamp '2040-12-31'
GROUP BY
  DATE_TRUNC('month', process_date)
ORDER BY metric_time__month
```

**Resultado:** una fila por mes. `dotacion_total` cuenta personas distintas del mes; `dotacion_vacantes` suma las vacantes de todos los días del mes si la tabla guarda una foto por día (ver Notas).

### 6. Externos y proveedores por cargo

Cuántos externos tiene cada cargo y de cuántos proveedores distintos vienen.

```sql
SELECT employee__position, dotacion_externos, proveedores_distintos
FROM semantic.employee_headcount
ORDER BY proveedores_distintos DESC;
```

Traducción a SQL de Athena:

```sql
SELECT
  position AS employee__position
  , SUM(outsourcing_employee) AS dotacion_externos
  , COUNT(DISTINCT supplier_id) AS proveedores_distintos
FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
GROUP BY
  position
ORDER BY proveedores_distintos DESC
```

**Resultado:** una fila por cargo con externos y proveedores distintos.

### 7. Filtro por género y cargo

Dotación por cargo para un género.

```sql
SELECT employee__position, employee__gender, dotacion_total
FROM semantic.employee_headcount
WHERE employee__gender = 'F'
ORDER BY 3 DESC;
```

Traducción a SQL de Athena:

```sql
SELECT
  employee__position
  , employee__gender
  , COUNT(DISTINCT dotacion_total) AS dotacion_total
FROM (
  SELECT
    position AS employee__position
    , gender AS employee__gender
    , user_ad AS dotacion_total
  FROM dlk_gld_it_people_dwh.fact_empleado employee_headcount_src_10000
) subq_2
WHERE employee__gender = 'F'
GROUP BY
  employee__position
  , employee__gender
ORDER BY dotacion_total DESC
```

**Resultado:** una fila por cargo con la dotación de ese género. Cambiar `'F'` por el valor real de la tabla.

## Lo que no se puede consultar

| Consulta | Motivo |
| --- | --- |
| `SELECT * FROM semantic.<tabla>` | Hay que nombrar las métricas y dimensiones |
| `WHERE <métrica> > 0` | No se puede filtrar por métricas, solo por dimensiones |
| `COUNT(*)`, expresiones (`metrica * 2`), joins, subconsultas | El contrato solo acepta columnas de la tabla semántica |
| Columnas de dos tablas en la misma consulta | Cada consulta lee una sola tabla semántica |
| Pestaña *Datos* de la tabla en DBeaver | La tabla es virtual y está vacía; usar la pestaña *Columnas* |

## Notas

- **Snapshot:** las medidas `internals`, `externals` y `vacancies` son `sum` sin `non_additive_dimension`. Si `fact_empleado` guarda una foto por día, al agrupar por semana, mes o año se suman las fotos de todos los días. Para corregirlo, agregar a esas medidas `non_additive_dimension: {name: process_date, window_choice: max}`.
- `ratio_internos` y `ratio_externos` dividen por `dotacion_total` (`COUNT(DISTINCT user_ad)`): son correctos si hay una fila por colaborador y fecha.
