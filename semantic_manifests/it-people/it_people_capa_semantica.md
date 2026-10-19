# Capa semántica IT People: ajustes al YAML y consultas en DBeaver

Modelo semántico `employee_headcount` (dotación IT People), validado con dbt 1.12 y servido por el gateway semántico.

## 1. Cambios en `semantic_models.yml`

Se hizo **un solo cambio**: se eliminó la línea `primary_entity: employee`.

```yaml
  - name: employee_headcount
    description: "Snapshot vigente de dotación IT People"
    model: ref('fact_empleado')
    primary_entity: employee        # ← eliminada
    defaults:
      agg_time_dimension: process_date
    entities:
      - name: employee
        type: primary               # ← la entidad primaria ya estaba declarada aquí
        expr: user_ad
```

**Por qué.** La entidad primaria quedaba declarada dos veces: con `primary_entity: employee` y con la entidad
`employee` de tipo `primary`. MetricFlow no admite ambas. `primary_entity` existe solo para modelos que **no**
tienen ninguna entidad de tipo `primary` en su lista. Con las dos, dbt mostraba este aviso y luego fallaba la
validación:

```
[WARNING]: The semantic model `employee_headcount` has an entity named `employee` with type primary
but it also has the `primary_entity` field set to `employee`. Both should not be present in the model.
[ERROR]: Parsing Error
  Semantic Manifest validation failed.
```

El resto del archivo (entidades, dimensiones y medidas) no cambió.

> El error posterior *«the value of 'semantic_models' is not a list»* no fue un cambio de contenido: la
> indentación se desarmó al borrar la línea. Bajo `semantic_models:` cada modelo debe empezar con
> `  - name:` (dos espacios y un guion), y el archivo debe usar espacios, no tabulaciones.

## 2. Por qué se agregó el time spine

Se agregaron dos archivos:

`models/semantic/metricflow_time_spine.sql`: crea una tabla calendario, con una fila por día.

```sql
{{ config(
    materialized='table',
    table_type='iceberg'
) }}

WITH days AS (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
)

SELECT cast(date_day AS date) AS date_day
FROM days
```

`models/semantic/time_spine.yml`: la registra como calendario de MetricFlow.

```yaml
version: 2

models:
  - name: metricflow_time_spine
    description: "Tabla calendario diaria que MetricFlow usa como time spine."
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day
```

**1. dbt 1.12 lo exige.** En cuanto el proyecto tiene un modelo semántico, `dbt parse` falla si no encuentra un
time spine con granularidad diaria o menor:

```
Parsing Error
  The semantic layer requires a time spine model with granularity DAY or smaller in the project, but none was found.
```

**2. MetricFlow lo usa como calendario de referencia.** La tabla de hechos solo tiene las fechas en las que hubo
datos; algunas funciones necesitan **todas** las fechas:

- **Rellenar periodos sin datos:** mostrar un mes sin registros como 0 en vez de omitirlo (`join_to_timespine`,
  `fill_nulls_with`).
- **Métricas acumuladas:** por ejemplo, dotación acumulada en el año o en una ventana móvil de 30 días.
- **Comparaciones entre periodos:** por ejemplo, este mes contra el mismo mes del año anterior (`offset_window`).
- **Granularidades de `metric_time`:** el time spine define la granularidad mínima (día) desde la que se arman
  semana, mes, trimestre y año.

**3. Las métricas actuales no la leen.** Agrupar por mes se resuelve con `DATE_TRUNC('month', process_date)`
directamente sobre `fact_empleado`. Por eso basta con definirla para que `dbt parse` funcione. Antes de usar las
funciones del punto 2, hay que crearla físicamente:

```bash
dbt run -s metricflow_time_spine
```

## 3. Qué publica

**Tabla `semantic.employee_headcount`**

| Métrica | Qué calcula |
| --- | --- |
| `dotacion_total` | Colaboradores distintos (`COUNT(DISTINCT user_ad)`) |
| `dotacion_internos` | Suma de `internal_employee` |
| `dotacion_externos` | Suma de `outsourcing_employee` |
| `dotacion_vacantes` | Suma de `vacancy` |
| `proveedores_distintos` | Proveedores distintos (`COUNT(DISTINCT supplier_id)`) |
| `ratio_internos` | `dotacion_internos / dotacion_total` (entre 0 y 1) |
| `ratio_externos` | `dotacion_externos / dotacion_total` (entre 0 y 1) |

**Dimensiones:** `employee__position` (cargo), `employee__gender` (género), `employee__process_date` y
`metric_time`, `metric_time__day`, `__week`, `__month`, `__quarter`, `__year` (todas salen de `process_date`).

## 4. Consultas para DBeaver

Se ejecutan en un *SQL Editor* sobre la conexión del gateway. Con el gateway levantado con `--ejecutar`, las de la
tabla semántica devuelven **las filas de Athena**; sin esa opción devuelven una fila con la columna `sql` (la
consulta lista para ejecutar en Athena). La guía completa, con 23 consultas descritas, está en
[docs/consultas_it_people.md](../../docs/consultas_it_people.md).

### Catálogo

```sql
-- Qué métricas existen, su tipo y descripción
SELECT metric, label, type, description FROM semantic.metrics;

-- Qué dimensiones existen
SELECT dimension, kind, data_type FROM semantic.dimensions;

-- Con qué dimensiones se puede cruzar una métrica
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'ratio_internos';

-- Confirmar que el gateway cargó el manifest correcto
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic';
```

### Métricas

**1. KPIs generales.** Una fila con la dotación total, internos, externos y vacantes.

```sql
SELECT dotacion_total, dotacion_internos, dotacion_externos, dotacion_vacantes
FROM semantic.employee_headcount;
```

**2. Mix interno/externo por cargo (top 10).** Una fila por cargo con su dotación y la proporción de internos y
externos, ordenada por dotación.

```sql
SELECT employee__position, dotacion_total, ratio_internos, ratio_externos
FROM semantic.employee_headcount
ORDER BY dotacion_total DESC
LIMIT 10;
```

**3. Dotación por género.** Una fila por género con total, internos y externos.

```sql
SELECT employee__gender, dotacion_total, dotacion_internos, dotacion_externos
FROM semantic.employee_headcount;
```

**4. Foto de un día.** Dotación vigente a una fecha. Evita el problema de sumar fotos de varios días.

```sql
SELECT employee__process_date, dotacion_total, dotacion_internos, dotacion_vacantes
FROM semantic.employee_headcount
WHERE employee__process_date = '2026-10-13';
```

**5. Evolución mensual.** Una fila por mes desde enero de 2026. Si la tabla guarda fotos diarias, las vacantes se suman entre
días.

```sql
SELECT metric_time__month, dotacion_total, dotacion_vacantes
FROM semantic.employee_headcount
WHERE metric_time >= '2026-01-01'
ORDER BY metric_time__month;
```

**6. Externos y proveedores por cargo.** Cuántos externos tiene cada cargo y de cuántos proveedores distintos
vienen.

```sql
SELECT employee__position, dotacion_externos, proveedores_distintos
FROM semantic.employee_headcount
ORDER BY proveedores_distintos DESC;
```

**7. Filtro por dimensión.** Dotación por cargo para un género. Cambiar `'F'` por el valor real de la tabla.

```sql
SELECT employee__position, employee__gender, dotacion_total
FROM semantic.employee_headcount
WHERE employee__gender = 'F'
ORDER BY 3 DESC;
```

**8. Ver el SQL generado.** Igual que una consulta normal: devuelve el SQL sin ejecutar nada.

```sql
EXPLAIN SELECT employee__position, ratio_internos
FROM semantic.employee_headcount;
```

### Lo que no funciona

| Consulta | Motivo |
| --- | --- |
| `WHERE dotacion_externos > 0` | No se puede filtrar por métricas; solo por dimensiones |
| `SELECT * FROM semantic.employee_headcount` | Hay que nombrar las métricas y dimensiones |
| `COUNT(*)`, expresiones, joins, subconsultas | El contrato pide columnas de la tabla semántica |
| Pestaña *Datos* de `employee_headcount` | La tabla es virtual y está vacía; usar la pestaña *Columnas* |
