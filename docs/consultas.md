# Consultas para DBeaver y Metabase

Consultas listas para ejecutar contra el gateway semántico con los dos manifests de `semantic_manifests/`:
`dbt_glue_lab` e `it-people`. Todas se probaron contra el gateway.

## Antes de empezar

**Levantar el gateway** con el manifest que se quiere consultar, desde `~/DBT-GLUE/MetricFlow`:

```bash
# dbt_glue_lab (eventos e items)
python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena --ejecutar \
  --region us-east-1 --athena-workgroup primary --athena-output s3://<BUCKET>/athena-results/

# IT People (dotación), en otro puerto si van los dos a la vez
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar \
  --region us-east-1 --athena-workgroup primary --athena-output s3://<BUCKET>/athena-results/ --port 5434
```

**Conexión en DBeaver o Metabase:** PostgreSQL · host `127.0.0.1` · puerto `5433` (o `5434`) · base de datos `athena` ·
usuario `bi` · contraseña `bi` · SSL desactivado.

**Qué devuelve cada consulta**

| Tipo de consulta | Con `--ejecutar` | Sin `--ejecutar` |
| --- | --- | --- |
| Catálogo (`information_schema`, `semantic.metrics`…) | Filas del catálogo | Filas del catálogo |
| Sobre una tabla semántica (`semantic.events`, `semantic.employee_headcount`…) | **Filas de Athena** | Una fila con el SQL de Athena (columna `sql`) |
| `EXPLAIN SELECT …` | El SQL, sin ejecutar | El SQL, sin ejecutar |

En DBeaver, ejecutar cada consulta con **Cmd+Enter** (cursor sobre la consulta), no como script. Un comentario
suelto al final de un script se envía como consulta vacía y devuelve una columna `1` sin filas.

**Reglas de las consultas semánticas**

- Una sola tabla de `semantic.*` por consulta; sin `JOIN`, subconsultas ni `SELECT *`.
- En el `SELECT` van métricas y dimensiones de esa tabla, con alias opcional. El `GROUP BY` es implícito: se agrupa
  por todas las dimensiones del `SELECT`.
- `WHERE` solo sobre dimensiones. Sobre `metric_time` con `>=`, `<=`, `=` o `BETWEEN` y una fecha, actúa como rango de
  tiempo.
- `ORDER BY` por nombre, alias o posición, y `LIMIT`.

---

## 1. dbt_glue_lab

Manifest: `semantic_manifests/dbt_glue_lab/semantic_manifest.json`.

| Tabla | Métricas | Dimensiones |
| --- | --- | --- |
| `semantic.events` | `event_count`, `unique_users`, `item_views`, `profile_views`, `item_views_per_user` (ratio), `item_views_share` (ratio), `engagement_events` (derivada) | `event__platform`, `event__event_name`, `event__parameter_name`, `event__event_date`, `event__event_year`, `metric_time` (`__day`, `__week`, `__month`, `__quarter`, `__year`) |
| `semantic.items` | `item_count`, `total_item_price`, `avg_item_price`, `max_item_price` | `item__category`, `item__item_name`, `item__adjective`, `item__modifier`, `item__created_at_date`, `metric_time` (`__day` … `__year`) |

En `events`, `metric_time` es la fecha del evento; en `items`, la fecha de creación del item.

### 1.1 Catálogo

**C1. Tablas publicadas**

Lista las tablas que el gateway cargó desde el manifest. Sirve para confirmar que se levantó con el manifest correcto.

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic' ORDER BY table_name;
```

*Devuelve:* Una fila por tabla: `dimensions`, `events`, `items`, `metric_dimensions`, `metrics`.

**C2. Métricas disponibles**

Catálogo de métricas: nombre, etiqueta, tipo (simple, ratio, derived), en qué tabla consultarla y qué mide.

```sql
SELECT metric, label, type, table_name, description FROM semantic.metrics ORDER BY table_name, metric;
```

*Devuelve:* 11 filas, una por métrica. Se responde desde el catálogo, sin ir a Athena.

**C3. Dimensiones disponibles**

Todas las dimensiones por las que se puede agrupar o filtrar, con su tipo (categórica o de tiempo).

```sql
SELECT dimension, kind, data_type, description FROM semantic.dimensions ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión, incluidas las granularidades de `metric_time`.

**C4. Con qué se puede cruzar una métrica**

Dimensiones compatibles con una métrica. Cambiar el nombre de la métrica para consultar otra.

```sql
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'item_views_per_user' ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión compatible.

**C5. Columnas de una tabla semántica**

Columnas de la tabla virtual `events`: primero las dimensiones y luego las métricas, con su tipo.

```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semantic' AND table_name = 'events' ORDER BY ordinal_position;
```

*Devuelve:* Una fila por columna.

### 1.2 Eventos (`semantic.events`)

**E1. KPIs generales de actividad**

Totales de toda la historia: eventos, usuarios únicos, vistas de items y vistas de perfil. Sin dimensiones, MetricFlow no agrupa.

```sql
SELECT event_count, unique_users, item_views, profile_views FROM semantic.events;
```

*Devuelve:* Una sola fila con los cuatro totales.

**E2. Actividad por plataforma**

Eventos y usuarios únicos de cada plataforma, de la más activa a la menos activa. El `GROUP BY` es implícito: lo agrega MetricFlow por la dimensión.

```sql
SELECT event__platform, event_count, unique_users FROM semantic.events ORDER BY event_count DESC;
```

*Devuelve:* Una fila por plataforma.

**E3. Evolución mensual**

Serie mensual de eventos y usuarios únicos según la fecha del evento. Útil para un gráfico de líneas en Metabase.

```sql
SELECT metric_time__month, event_count, unique_users FROM semantic.events ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes con datos.

**E4. Eventos por tipo**

Cuántos eventos hay de cada tipo (`view_item`, `view_user_profile`, asignaciones de test…). `ORDER BY 2` ordena por la segunda columna.

```sql
SELECT event__event_name, event_count FROM semantic.events ORDER BY 2 DESC;
```

*Devuelve:* Una fila por tipo de evento.

**E5. Ratios de uso por plataforma**

Vistas de items por usuario y proporción de eventos que son vistas de items, por plataforma. MetricFlow calcula numerador y denominador por separado y después divide, así que el ratio es correcto en cualquier agrupación.

```sql
SELECT event__platform, item_views_per_user, item_views_share FROM semantic.events;
```

*Devuelve:* Una fila por plataforma; `item_views_share` va de 0 a 1.

**E6. Interacciones semanales de un trimestre**

Métrica derivada `engagement_events` (vistas de items + vistas de perfil) semana a semana en el primer trimestre de 2024. El filtro sobre `metric_time` se aplica como rango de tiempo.

```sql
SELECT metric_time__week, engagement_events, item_views, profile_views FROM semantic.events WHERE metric_time BETWEEN '2024-01-01' AND '2024-03-31' ORDER BY metric_time__week;
```

*Devuelve:* Una fila por semana; en cada una `engagement_events = item_views + profile_views`.

**E7. Usuarios que vieron items, por plataforma**

Usuarios distintos que vieron al menos una ficha de item, por plataforma. El filtro por dimensión se aplica antes de contar.

```sql
SELECT event__platform, unique_users FROM semantic.events WHERE event__event_name = 'view_item';
```

*Devuelve:* Una fila por plataforma.

**E8. Comparación anual entre plataformas**

Eventos por año y plataforma, solo para iOS y Android. Muestra el uso de `IN` en el filtro.

```sql
SELECT event__event_year, event__platform, event_count FROM semantic.events WHERE event__platform IN ('ios', 'android') ORDER BY event__event_year, event__platform;
```

*Devuelve:* Una fila por año y plataforma.

**E9. Vistas trimestrales con alias**

Vistas de items por trimestre con nombres propios para las columnas. El `SUM(...)` se acepta para que la sintaxis sea la habitual en BI, pero la agregación la define la métrica, no el envoltorio.

```sql
SELECT metric_time__quarter AS trimestre, SUM(item_views) AS vistas FROM semantic.events ORDER BY trimestre;
```

*Devuelve:* Columnas `trimestre` y `vistas`, una fila por trimestre.

**E10. Top 10 parámetros de evento**

Los diez valores de `parameter_name` con más eventos. `ORDER BY` y `LIMIT` se trasladan al SQL del motor.

```sql
SELECT event__parameter_name, event_count FROM semantic.events ORDER BY event_count DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas.

### 1.3 Items (`semantic.items`)

**I1. KPIs del catálogo de items**

Tamaño y precios del catálogo: cantidad de items, suma, promedio y máximo de precios de lista. No son ventas.

```sql
SELECT item_count, total_item_price, avg_item_price, max_item_price FROM semantic.items;
```

*Devuelve:* Una sola fila.

**I2. Items y precio promedio por categoría**

Cuántos items tiene cada categoría y su precio promedio, de la categoría más grande a la más chica.

```sql
SELECT item__category, item_count, avg_item_price FROM semantic.items ORDER BY item_count DESC;
```

*Devuelve:* Una fila por categoría.

**I3. Items creados por mes**

En la tabla de items, `metric_time` es la fecha de creación: esta consulta muestra cuántos items se agregaron al catálogo cada mes.

```sql
SELECT metric_time__month, item_count FROM semantic.items ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

**I4. Precio máximo por categoría y adjetivo**

Combinaciones de categoría y adjetivo con el item más caro de cada una; muestra las 20 más caras.

```sql
SELECT item__category, item__adjective, max_item_price FROM semantic.items ORDER BY max_item_price DESC LIMIT 20;
```

*Devuelve:* Hasta 20 filas.

**I5. Los 10 items más caros**

Ranking de items por precio. Como cada nombre es un item, `max_item_price` es su precio.

```sql
SELECT item__item_name, max_item_price FROM semantic.items ORDER BY max_item_price DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas.

### 1.4 Ver el SQL

**X1. Ver el SQL sin ejecutar**

Devuelve el SQL que MetricFlow genera para Athena, sin ejecutarlo. Sirve para revisar cómo se calcula una métrica o para copiar el SQL a la consola de Athena.

```sql
EXPLAIN SELECT event__platform, item_views_per_user FROM semantic.events;
```

*Devuelve:* Una fila con la columna `sql`.

---

## 2. IT People

Manifest: `semantic_manifests/it-people/semantic_manifest.json`.

| Tabla | Métricas | Dimensiones |
| --- | --- | --- |
| `semantic.employee_headcount` | `dotacion_total`, `dotacion_internos`, `dotacion_externos`, `dotacion_vacantes`, `proveedores_distintos`, `ratio_internos` (ratio), `ratio_externos` (ratio) | `employee__position`, `employee__gender`, `employee__process_date`, `metric_time` (`__day` … `__year`) |

`metric_time` sale de `process_date`, la fecha de la foto de dotación.

### 2.1 Catálogo

**C1. Tablas publicadas**

Confirma que el gateway cargó el manifest de IT People.

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic' ORDER BY table_name;
```

*Devuelve:* Una fila por tabla: `dimensions`, `employee_headcount`, `metric_dimensions`, `metrics`.

**C2. Métricas disponibles**

Las siete métricas de dotación con su tipo y descripción.

```sql
SELECT metric, label, type, description FROM semantic.metrics ORDER BY metric;
```

*Devuelve:* 7 filas.

**C3. Dimensiones disponibles**

Cargo, género, fecha de proceso y las granularidades de `metric_time`.

```sql
SELECT dimension, kind, data_type FROM semantic.dimensions ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión.

**C4. Con qué se puede cruzar un ratio**

Dimensiones compatibles con `ratio_internos`.

```sql
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'ratio_internos' ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión.

### 2.2 Dotación (`semantic.employee_headcount`)

**P1. KPIs de dotación**

Dotación total (colaboradores distintos), internos, externos y vacantes. Si la tabla guarda una foto por día, internos, externos y vacantes se suman entre fechas: conviene filtrar una fecha (P4).

```sql
SELECT dotacion_total, dotacion_internos, dotacion_externos, dotacion_vacantes FROM semantic.employee_headcount;
```

*Devuelve:* Una sola fila.

**P2. Mix interno/externo por cargo**

Los diez cargos con más dotación y qué proporción es interna y externa.

```sql
SELECT employee__position, dotacion_total, ratio_internos, ratio_externos FROM semantic.employee_headcount ORDER BY dotacion_total DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas; los ratios van de 0 a 1.

**P3. Dotación por género**

Dotación total, internos y externos por género.

```sql
SELECT employee__gender, dotacion_total, dotacion_internos, dotacion_externos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila por género.

**P4. Foto de un día**

Dotación vigente en una fecha de proceso. Cambiar la fecha por una que exista en la tabla.

```sql
SELECT employee__process_date, dotacion_total, dotacion_internos, dotacion_vacantes FROM semantic.employee_headcount WHERE employee__process_date = '2026-10-13';
```

*Devuelve:* Una fila.

**P5. Evolución mensual**

Dotación y vacantes por mes desde enero de 2026. Con fotos diarias, las vacantes se suman entre días del mes.

```sql
SELECT metric_time__month, dotacion_total, dotacion_vacantes FROM semantic.employee_headcount WHERE metric_time >= '2026-01-01' ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

**P6. Externos y proveedores por cargo**

Cuántos externos tiene cada cargo y de cuántos proveedores distintos vienen.

```sql
SELECT employee__position, dotacion_externos, proveedores_distintos FROM semantic.employee_headcount ORDER BY proveedores_distintos DESC;
```

*Devuelve:* Una fila por cargo.

**P7. Dotación por cargo para un género**

Dotación por cargo filtrada por género. Cambiar `'F'` por el valor que use la tabla.

```sql
SELECT employee__position, employee__gender, dotacion_total FROM semantic.employee_headcount WHERE employee__gender = 'F' ORDER BY 3 DESC;
```

*Devuelve:* Una fila por cargo.

**P8. Ratio de internos por trimestre**

Proporción de colaboradores internos por trimestre, con alias para la columna de tiempo.

```sql
SELECT metric_time__quarter AS trimestre, ratio_internos FROM semantic.employee_headcount ORDER BY trimestre;
```

*Devuelve:* Una fila por trimestre.

### 2.3 Ver el SQL

**X1. Ver el SQL sin ejecutar**

Devuelve el SQL de Athena para el ratio por cargo, sin ejecutarlo.

```sql
EXPLAIN SELECT employee__position, ratio_internos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila con la columna `sql`.

---

## 3. Consultas que no funcionan y por qué

| Consulta | Mensaje del gateway | Qué hacer |
| --- | --- | --- |
| `SELECT * FROM semantic.events` | SELECT * no está soportado: elija métricas y dimensiones | Hay que nombrar las columnas. |
| `SELECT item__category, event_count FROM semantic.events` | La columna 'item__category' no está en la tabla 'events'; está en: items | Una consulta solo combina columnas de una misma tabla. |
| `SELECT event__platform, event_count FROM semantic.events WHERE event_count > 100` | No se puede filtrar por la métrica 'event_count' | Solo se filtra por dimensiones; no hay HAVING. |
| `SELECT COUNT(*) FROM semantic.events` | Expresión no soportada | Las métricas ya traen su agregación; usar `event_count`. |
| Pestaña *Datos* de una tabla semántica en DBeaver | `SELECT * no está soportado…` | Usar la pestaña *Columnas* o escribir la consulta con columnas |

Más ejemplos con el SQL que genera cada consulta: `semantic_manifests/dbt_glue_lab/CONSULTAS.md` y
`semantic_manifests/it-people/CONSULTAS.md`.
