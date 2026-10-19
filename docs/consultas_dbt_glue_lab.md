# Consultas sobre la capa semántica de dbt_glue_lab

Consultas para DBeaver o Metabase sobre el manifest `semantic_manifests/dbt_glue_lab/semantic_manifest.json`, servido
por el gateway semántico y ejecutadas en Athena. Todas se probaron contra el gateway con este manifest.

Las consultas del otro manifest están en [consultas_it_people.md](consultas_it_people.md). El SQL de Athena que genera
cada consulta está en `semantic_manifests/dbt_glue_lab/consultas_sql_dbt_glue_lab.md`.

## 1. Antes de empezar

**Credenciales de AWS.** En el Mac, el gateway usa las credenciales por defecto de AWS (las mismas que `dbt run`).
Comprobar:

```bash
aws sts get-caller-identity
```

**Levantar el gateway** desde la raíz `MetricFlow/`. El workgroup `primary` no tiene ruta de resultados, así que se
pasa una en el bucket del `.env` de dbt:

```bash
set -a; source DBT/dbt_glue_lab/.env; set +a      # carga DBT_S3_BUCKET

python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json --motor athena \
  --ejecutar --region us-east-1 \
  --athena-workgroup primary --athena-output "s3://$DBT_S3_BUCKET/athena-results/"
#   con TLS agregar:     --tls-cert ../ssl/server.crt --tls-key ../ssl/server.key --tls-obligatorio
#   con otro perfil:     --aws-profile <perfil>
#   junto a IT People:   --port 5434
```

**Conexión en DBeaver o Metabase:** PostgreSQL · host `127.0.0.1` · puerto `5433` (o el de `--port`) · base de datos
`athena` · usuario `bi` · contraseña `bi` · SSL desactivado (o `require` si el gateway se levantó con TLS).

En DBeaver, ejecutar cada consulta con **Cmd+Enter** (Ctrl+Enter en Windows) con el cursor sobre la consulta, no como
script: un comentario suelto al final de un script se envía como consulta vacía y devuelve una columna `1` sin filas.

Con `--ejecutar`, las consultas sobre `semantic.events` y `semantic.items` devuelven **filas de Athena**. Sin esa
opción, o con `EXPLAIN`, devuelven el SQL generado en una columna `sql`. Las de catálogo siempre se responden en el
gateway, sin ir a Athena.

## 2. Qué hay en la capa semántica

Dos tablas, sobre las tablas Iceberg de `lab_data_silver`:

| Tabla | Modelo semántico | Tabla en Glue | `metric_time` es… |
| --- | --- | --- | --- |
| `semantic.events` | `events` | `lab_data_silver.silver_event` | la fecha del evento (`event_date`) |
| `semantic.items` | `items` | `lab_data_silver.silver_item` | la fecha de creación del item (`created_at_date`) |

El manifest también trae el modelo `top_items` (`lab_data_gold.gold_top_item`), pero no tiene métricas, así que el
gateway no lo publica como tabla.

### 2.1 Métricas y dimensiones de `semantic.events`

| Métrica | Tipo | Qué calcula |
| --- | --- | --- |
| `event_count` | simple | Eventos distintos (`COUNT(DISTINCT event_id)`) |
| `unique_users` | simple | Usuarios distintos que generaron eventos (`COUNT(DISTINCT user_id)`) |
| `item_views` | simple | Eventos `view_item` (`COUNT(DISTINCT event_id)` con filtro) |
| `profile_views` | simple | Eventos `view_user_profile` (`COUNT(DISTINCT event_id)` con filtro) |
| `item_views_per_user` | ratio | `item_views / unique_users` |
| `item_views_share` | ratio | `item_views / event_count` (entre 0 y 1) |
| `engagement_events` | derivada | `item_views + profile_views` |

| Dimensión | Qué es |
| --- | --- |
| `event__platform` | Plataforma (`web`, `android`, `ios`…) |
| `event__event_name` | Tipo de evento (`view_item`, `view_user_profile`, `test_assignment`…) |
| `event__parameter_name` | Nombre del parámetro del evento |
| `event__event_year` | Año del evento, como texto |
| `event__event_date` | Fecha del evento |
| `metric_time`, `metric_time__day`, `__week`, `__month`, `__quarter`, `__year` | La fecha del evento, en distintas granularidades |

### 2.2 Métricas y dimensiones de `semantic.items`

| Métrica | Tipo | Qué calcula |
| --- | --- | --- |
| `item_count` | simple | Items distintos (`COUNT(DISTINCT item_id)`) |
| `total_item_price` | simple | Suma de los precios de lista (`SUM(price)`) |
| `avg_item_price` | simple | Precio promedio (`AVG(price)`) |
| `max_item_price` | simple | Precio máximo (`MAX(price)`) |

| Dimensión | Qué es |
| --- | --- |
| `item__category` | Categoría del item |
| `item__item_name` | Nombre del item (uno por item) |
| `item__adjective`, `item__modifier` | Atributos descriptivos del nombre |
| `item__created_at_date` | Fecha de creación del item |
| `metric_time`, `metric_time__day`, `__week`, `__month`, `__quarter`, `__year` | La fecha de creación, en distintas granularidades |

Los precios son de lista del catálogo, no ventas.

## 3. Consultas

Reglas: una sola tabla por consulta (no se mezclan columnas de `events` e `items`), sin `SELECT *`, sin `JOIN` ni
subconsultas. El `GROUP BY` es implícito (se agrupa por las dimensiones del `SELECT`). `WHERE` solo sobre
dimensiones; sobre `metric_time` con fechas, actúa como rango de tiempo. `ORDER BY` por nombre, alias o posición, y
`LIMIT`.

Los valores de ejemplo (`'web'`, `'android'`, `'2024-01-01'`, nombres de categorías) hay que cambiarlos por valores
reales de la tabla. Las consultas 10, 14 y 26 sirven para descubrirlos.

### 3.1 Catálogo: qué se puede consultar

**1. Tablas que publicó el gateway**

Confirma que el gateway se levantó con el manifest de dbt_glue_lab.

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic' ORDER BY table_name;
```

*Devuelve:* `dimensions`, `events`, `items`, `metric_dimensions` y `metrics`.

**2. Métricas disponibles**

Las once métricas con su etiqueta, tipo (simple, ratio, derived), tabla donde consultarla y qué mide.

```sql
SELECT metric, label, type, table_name, description FROM semantic.metrics ORDER BY table_name, metric;
```

*Devuelve:* 11 filas: 7 de `events` y 4 de `items`.

**3. Dimensiones disponibles**

Por qué se puede agrupar o filtrar, con su tipo (categórica o de tiempo).

```sql
SELECT dimension, kind, data_type, description FROM semantic.dimensions ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión, incluidas las granularidades de `metric_time`.

**4. Con qué se puede cruzar una métrica**

Dimensiones compatibles con una métrica. Cambiar el nombre para consultar otra.

```sql
SELECT dimension FROM semantic.metric_dimensions WHERE metric = 'item_views_per_user' ORDER BY dimension;
```

*Devuelve:* Una fila por dimensión compatible.

**5. Columnas de la tabla de eventos**

Columnas que se pueden poner en el `SELECT` de `semantic.events`: primero las dimensiones y luego las métricas.

```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semantic' AND table_name = 'events' ORDER BY ordinal_position;
```

*Devuelve:* 18 filas: 11 dimensiones y 7 métricas.

**6. Columnas de la tabla de items**

Lo mismo para `semantic.items`.

```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semantic' AND table_name = 'items' ORDER BY ordinal_position;
```

*Devuelve:* 15 filas: 11 dimensiones y 4 métricas.

### 3.2 Eventos: indicadores generales

**7. Resumen de actividad**

Totales de toda la historia: eventos, usuarios únicos, vistas de items y vistas de perfil. Sin dimensiones, MetricFlow
no agrupa.

```sql
SELECT event_count, unique_users, item_views, profile_views FROM semantic.events;
```

*Devuelve:* Una fila.

**8. Ratios de uso**

Vistas de items por usuario y proporción de eventos que son vistas de items. MetricFlow calcula numerador y
denominador por separado y después divide, así que el ratio es correcto en cualquier agrupación.

```sql
SELECT item_views_per_user, item_views_share FROM semantic.events;
```

*Devuelve:* Una fila; `item_views_share` entre 0 y 1.

**9. Interacciones**

Métrica derivada `engagement_events` junto a sus dos componentes.

```sql
SELECT engagement_events, item_views, profile_views FROM semantic.events;
```

*Devuelve:* Una fila; `engagement_events = item_views + profile_views`.

### 3.3 Eventos por fecha

**10. Fechas con eventos**

Las últimas 30 fechas con actividad. Sirve para saber qué período cubren los datos antes de filtrar por fecha.

```sql
SELECT event__event_date, event_count, unique_users FROM semantic.events ORDER BY event__event_date DESC LIMIT 30;
```

*Devuelve:* Hasta 30 filas.

**11. Evolución mensual**

Serie mensual de eventos y usuarios únicos. Útil para un gráfico de líneas en Metabase.

```sql
SELECT metric_time__month, event_count, unique_users FROM semantic.events ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes con datos.

**12. Interacciones semanales de un trimestre**

`engagement_events` semana a semana en el primer trimestre de 2024. El `BETWEEN` sobre `metric_time` se aplica como
rango de tiempo.

```sql
SELECT metric_time__week, engagement_events, item_views, profile_views FROM semantic.events WHERE metric_time BETWEEN '2024-01-01' AND '2024-03-31' ORDER BY metric_time__week;
```

*Devuelve:* Una fila por semana.

**13. Vistas trimestrales con alias**

Vistas de items por trimestre con nombres propios para las columnas. El `SUM(...)` se acepta para que la sintaxis sea
la habitual en BI, pero la agregación la define la métrica, no el envoltorio.

```sql
SELECT metric_time__quarter AS trimestre, SUM(item_views) AS vistas FROM semantic.events ORDER BY trimestre;
```

*Devuelve:* Columnas `trimestre` y `vistas`, una fila por trimestre.

**14. Comparación anual**

Eventos y usuarios por año. `event__event_year` es texto; `metric_time__year` daría el mismo año como fecha
(`2024-01-01`).

```sql
SELECT event__event_year, event_count, unique_users FROM semantic.events ORDER BY event__event_year;
```

*Devuelve:* Una fila por año.

### 3.4 Eventos por plataforma y tipo

**15. Actividad por plataforma**

Eventos y usuarios únicos de cada plataforma, de la más activa a la menos activa.

```sql
SELECT event__platform, event_count, unique_users FROM semantic.events ORDER BY event_count DESC;
```

*Devuelve:* Una fila por plataforma.

**16. Ratios por plataforma**

Vistas de items por usuario y participación de las vistas de items en cada plataforma.

```sql
SELECT event__platform, item_views_per_user, item_views_share FROM semantic.events ORDER BY item_views_per_user DESC;
```

*Devuelve:* Una fila por plataforma.

**17. Eventos por tipo**

Cuántos eventos hay de cada tipo. `ORDER BY 2` ordena por la segunda columna.

```sql
SELECT event__event_name, event_count FROM semantic.events ORDER BY 2 DESC;
```

*Devuelve:* Una fila por tipo de evento.

**18. Plataforma y tipo de evento**

Matriz de eventos por plataforma y tipo; en Metabase se puede mostrar como tabla dinámica.

```sql
SELECT event__platform, event__event_name, event_count FROM semantic.events ORDER BY event__platform, event__event_name;
```

*Devuelve:* Una fila por combinación de plataforma y tipo.

**19. Top 10 parámetros de evento**

Los diez valores de `parameter_name` con más eventos. `ORDER BY` y `LIMIT` se trasladan al SQL de Athena.

```sql
SELECT event__parameter_name, event_count FROM semantic.events ORDER BY event_count DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas.

**20. Evolución mensual por plataforma**

Vistas de items por mes y plataforma, para comparar tendencias.

```sql
SELECT metric_time__month, event__platform, item_views FROM semantic.events ORDER BY metric_time__month, event__platform;
```

*Devuelve:* Una fila por mes y plataforma.

### 3.5 Eventos con filtros

**21. Un tipo de evento**

Usuarios distintos que vieron al menos una ficha de item, por plataforma. El filtro se aplica antes de contar.

```sql
SELECT event__platform, unique_users FROM semantic.events WHERE event__event_name = 'view_item';
```

*Devuelve:* Una fila por plataforma.

**22. Varias plataformas**

Eventos por año, solo para web y Android. Muestra el uso de `IN`.

```sql
SELECT event__event_year, event__platform, event_count FROM semantic.events WHERE event__platform IN ('web', 'android') ORDER BY event__event_year, event__platform;
```

*Devuelve:* Una fila por año y plataforma de la lista.

**23. Excluir un valor**

Eventos por tipo sin contar las asignaciones de test.

```sql
SELECT event__event_name, event_count FROM semantic.events WHERE event__event_name <> 'test_assignment' ORDER BY event_count DESC;
```

*Devuelve:* Una fila por tipo de evento, sin `test_assignment`.

**24. Tipos de evento que empiezan por una palabra**

Todos los eventos cuyo nombre empieza por `view_`. `LIKE` distingue mayúsculas de minúsculas en Athena.

```sql
SELECT event__event_name, event_count FROM semantic.events WHERE event__event_name LIKE 'view_%' ORDER BY event_count DESC;
```

*Devuelve:* Una fila por tipo de evento que coincide.

**25. Combinar filtros de fecha y dimensión**

Usuarios y vistas de items por mes en Android desde enero de 2024. Varias condiciones con `AND` se aplican todas.

```sql
SELECT metric_time__month, unique_users, item_views FROM semantic.events WHERE metric_time >= '2024-01-01' AND event__platform = 'android' ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

### 3.6 Items

**26. Resumen del catálogo**

Tamaño y precios del catálogo: cantidad de items, suma, promedio y máximo de precios de lista.

```sql
SELECT item_count, total_item_price, avg_item_price, max_item_price FROM semantic.items;
```

*Devuelve:* Una fila.

**27. Items y precio por categoría**

Cuántos items tiene cada categoría, su precio promedio y el máximo, de la categoría más grande a la más chica. Sirve
para conocer los valores de `item__category`.

```sql
SELECT item__category, item_count, avg_item_price, max_item_price FROM semantic.items ORDER BY item_count DESC;
```

*Devuelve:* Una fila por categoría.

**28. Items creados por mes**

En la tabla de items, `metric_time` es la fecha de creación: cuántos items se agregaron al catálogo cada mes.

```sql
SELECT metric_time__month, item_count FROM semantic.items ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

**29. Items creados por año y categoría**

Crecimiento del catálogo por año y categoría.

```sql
SELECT metric_time__year, item__category, item_count FROM semantic.items ORDER BY metric_time__year, item__category;
```

*Devuelve:* Una fila por año y categoría.

**30. Precio máximo por categoría y adjetivo**

Combinaciones de categoría y adjetivo con el item más caro de cada una; las 20 más caras.

```sql
SELECT item__category, item__adjective, max_item_price FROM semantic.items ORDER BY max_item_price DESC LIMIT 20;
```

*Devuelve:* Hasta 20 filas.

**31. Los 10 items más caros**

Ranking de items por precio. Como cada nombre es un item, `max_item_price` es su precio.

```sql
SELECT item__item_name, max_item_price FROM semantic.items ORDER BY max_item_price DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas.

**32. Precio promedio por modificador**

Cómo varía el precio promedio según el modificador del nombre.

```sql
SELECT item__modifier, item_count, avg_item_price FROM semantic.items ORDER BY avg_item_price DESC;
```

*Devuelve:* Una fila por modificador.

**33. Una categoría en particular**

Items de una categoría por adjetivo. Cambiar el nombre por una categoría real (ver la consulta 27).

```sql
SELECT item__adjective, item_count, avg_item_price FROM semantic.items WHERE item__category = 'Electronics' ORDER BY item_count DESC;
```

*Devuelve:* Una fila por adjetivo dentro de la categoría.

**34. Items creados en un período**

Items agregados por categoría durante 2024.

```sql
SELECT item__category, item_count, total_item_price FROM semantic.items WHERE metric_time BETWEEN '2024-01-01' AND '2024-12-31' ORDER BY item_count DESC;
```

*Devuelve:* Una fila por categoría con items creados en el período.

### 3.7 Ver el SQL sin ejecutar

**35. Ver el SQL de Athena sin ejecutar**

Devuelve el SQL que MetricFlow genera para el ratio por plataforma, sin ejecutarlo. Sirve para revisar cómo se
calcula una métrica o para copiarlo a la consola de Athena.

```sql
EXPLAIN SELECT event__platform, item_views_per_user FROM semantic.events;
```

*Devuelve:* Una fila con la columna `sql`.

---

## 4. Consultas que no funcionan y por qué

| Consulta | Mensaje del gateway | Qué hacer |
| --- | --- | --- |
| `SELECT * FROM semantic.events` | SELECT * no está soportado: elija métricas y dimensiones | Nombrar las columnas. |
| `SELECT item__category, event_count FROM semantic.events` | La columna 'item__category' no está en la tabla 'events'; está en: items | Una consulta solo combina columnas de una misma tabla. |
| `SELECT event__platform, event_count FROM semantic.events WHERE event_count > 100` | No se puede filtrar por la métrica 'event_count' | Solo se filtra por dimensiones; ordenar con `ORDER BY` y usar `LIMIT`. |
| `SELECT COUNT(*) FROM semantic.events` | Expresión no soportada | Las métricas ya traen su agregación; usar `event_count`. |
| `SELECT event__platform, event_count * 2 FROM semantic.events` | Expresión no soportada | Las operaciones van en la definición de la métrica (dbt), no en la consulta. |
| Pestaña *Datos* de una tabla semántica en DBeaver | `SELECT * no está soportado…` | Usar la pestaña *Columnas* o escribir la consulta con columnas. |

**Errores de Athena** (llegan a DBeaver con el mensaje original):

| Mensaje | Causa |
| --- | --- |
| `Unable to locate credentials` / `ExpiredToken` | No hay credenciales de AWS o vencieron: renovarlas y reiniciar el gateway |
| `AccessDenied` | El rol no tiene permiso sobre Athena, Glue, la tabla en S3 o la ruta de resultados |
| `TABLE_NOT_FOUND` / `SCHEMA_NOT_FOUND` | El manifest apunta a una tabla que no existe en Glue (correr `dbt run` y volver a copiar el manifest) |
| `No output location provided` | El workgroup no tiene ruta de resultados: pasar `--athena-output` |
| `La consulta superó el tiempo máximo de 100 s` | La consulta tardó demasiado y se canceló en Athena; subir `--timeout` si hace falta |
