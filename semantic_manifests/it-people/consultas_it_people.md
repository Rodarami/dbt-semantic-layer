# Consultas sobre la capa semántica de IT People

Consultas para DBeaver o Metabase sobre el manifest `semantic_manifests/it-people/semantic_manifest.json`, servido por el
gateway semántico y ejecutadas en Athena. Todas se probaron contra el gateway con este manifest.

## 1. Antes de empezar

**Credenciales de AWS** (PowerShell, credenciales temporales de la cuenta de IT People):

```powershell
$env:AWS_ACCESS_KEY_ID     = "ASIA..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN     = "..."
$env:AWS_DEFAULT_REGION    = "us-east-1"
python -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"   # comprobar
```

**Levantar el gateway** desde la raíz del repo, con el workgroup `DataEngineer` (ya trae su ruta de resultados):

```powershell
python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer
```

**Conexión en DBeaver o Metabase:** PostgreSQL · host `127.0.0.1` · puerto `5433` · base de datos `athena` ·
usuario `bi` · contraseña `bi` · SSL desactivado.

En DBeaver, ejecutar cada consulta con **Ctrl+Enter** (cursor sobre la consulta), no como script.

Con `--ejecutar`, las consultas sobre `semantic.employee_headcount` devuelven **filas de Athena**. Sin esa opción, o con
`EXPLAIN`, devuelven el SQL generado en una columna `sql`. Las de catálogo siempre se responden en el gateway, sin ir a
Athena.

## 2. Qué hay en la capa semántica

Una sola tabla: **`semantic.employee_headcount`**, sobre `dlk_gld_it_people_dwh.fact_empleado`.

### 2.1 Métricas y dimensiones

| Métrica | Tipo | Qué calcula |
| --- | --- | --- |
| `dotacion_total` | simple | Colaboradores distintos (`COUNT(DISTINCT user_ad)`) |
| `dotacion_internos` | simple | Suma de `internal_employee` |
| `dotacion_externos` | simple | Suma de `outsourcing_employee` |
| `dotacion_vacantes` | simple | Suma de `vacancy` |
| `proveedores_distintos` | simple | Proveedores distintos (`COUNT(DISTINCT supplier_id)`) |
| `ratio_internos` | ratio | `dotacion_internos / dotacion_total` (entre 0 y 1) |
| `ratio_externos` | ratio | `dotacion_externos / dotacion_total` (entre 0 y 1) |

| Dimensión | Qué es |
| --- | --- |
| `employee__position` | Cargo |
| `employee__gender` | Género |
| `employee__process_date` | Fecha de proceso (fecha de la foto de dotación) |
| `metric_time`, `metric_time__day`, `__week`, `__month`, `__quarter`, `__year` | La misma fecha de proceso, en distintas granularidades |

### 2.2 Cuidado con las fotos diarias

Si `fact_empleado` guarda una foto por día, `dotacion_internos`, `dotacion_externos` y `dotacion_vacantes` **se suman
entre fechas**: agrupar por mes da unas 30 veces la dotación real. `dotacion_total` y `proveedores_distintos` cuentan
valores distintos y no tienen ese problema, y los ratios tampoco se distorsionan. Para leer las sumas, filtrar una fecha
de proceso (consulta 7) o agregar `non_additive_dimension` a esas medidas en el YAML.

## 3. Consultas

Reglas: una sola tabla, sin `SELECT *`, sin `JOIN` ni subconsultas. El `GROUP BY` es implícito (se agrupa por las
dimensiones del `SELECT`). `WHERE` solo sobre dimensiones; sobre `metric_time` con fechas, actúa como rango de tiempo.
`ORDER BY` por nombre, alias o posición, y `LIMIT`.

Los valores de ejemplo (`'2026-10-13'`, `'F'`, `'M'`, nombres de cargos) hay que cambiarlos por valores reales de la
tabla. Las consultas 8 y 16 sirven para descubrirlos.

### 3.1 Catálogo: qué se puede consultar

**1. Tablas que publicó el gateway**

Confirma que el gateway se levantó con el manifest de IT People.

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic' ORDER BY table_name;
```

*Devuelve:* `dimensions`, `employee_headcount`, `metric_dimensions` y `metrics`.

**2. Métricas de dotación**

Las siete métricas con su tipo (simple o ratio) y qué mide cada una.

```sql
SELECT metric, label, type, description FROM semantic.metrics ORDER BY metric;
```

*Devuelve:* 7 filas.

**3. Dimensiones disponibles**

Por qué se puede agrupar o filtrar: cargo, género, fecha de proceso y las granularidades de `metric_time`.

```sql
SELECT dimension, kind, data_type, description FROM semantic.dimensions ORDER BY dimension;
```

*Devuelve:* 9 filas.

**4. Columnas de la tabla de dotación**

Columnas que se pueden poner en el `SELECT` de `semantic.employee_headcount`.

```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'semantic' AND table_name = 'employee_headcount' ORDER BY ordinal_position;
```

*Devuelve:* 16 filas: 9 dimensiones y 7 métricas.

### 3.2 Indicadores generales

**5. Resumen de dotación**

Los cinco indicadores de volumen sobre toda la tabla. `dotacion_total` cuenta colaboradores distintos; internos, externos y vacantes son sumas, así que con fotos de varios días se acumulan entre fechas (ver sección 2.2).

```sql
SELECT dotacion_total, dotacion_internos, dotacion_externos, dotacion_vacantes, proveedores_distintos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila.

**6. Composición interna/externa**

Proporción de colaboradores internos y externos sobre la dotación total. MetricFlow calcula numerador y denominador por separado y luego divide.

```sql
SELECT ratio_internos, ratio_externos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila; valores entre 0 y 1.

### 3.3 Por fecha

**7. Foto de un día**

Dotación vigente en una fecha de proceso. Es la forma correcta de leer las sumas si la tabla guarda una foto por día. Cambiar la fecha por una existente.

```sql
SELECT employee__process_date, dotacion_total, dotacion_internos, dotacion_externos, dotacion_vacantes FROM semantic.employee_headcount WHERE employee__process_date = '2026-10-13';
```

*Devuelve:* Una fila.

**8. Fechas de proceso disponibles**

Las últimas 30 fechas de proceso con su dotación. Sirve para saber qué fotos existen y elegir una fecha para las demás consultas.

```sql
SELECT employee__process_date, dotacion_total FROM semantic.employee_headcount ORDER BY employee__process_date DESC LIMIT 30;
```

*Devuelve:* Hasta 30 filas.

**9. Evolución mensual**

Dotación y vacantes por mes desde enero de 2026. `dotacion_total` cuenta personas distintas en el mes; las vacantes se suman entre fotos del mes.

```sql
SELECT metric_time__month, dotacion_total, dotacion_vacantes FROM semantic.employee_headcount WHERE metric_time >= '2026-01-01' ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

**10. Evolución trimestral de la composición**

Cómo cambia la proporción de internos y externos trimestre a trimestre. Los ratios no se distorsionan con varias fotos, porque numerador y denominador se calculan sobre el mismo período.

```sql
SELECT metric_time__quarter AS trimestre, ratio_internos, ratio_externos FROM semantic.employee_headcount ORDER BY trimestre;
```

*Devuelve:* Una fila por trimestre.

**11. Rango de fechas**

Dotación semana a semana en el tercer trimestre de 2026. El `BETWEEN` sobre `metric_time` se aplica como rango de tiempo.

```sql
SELECT metric_time__week, dotacion_total FROM semantic.employee_headcount WHERE metric_time BETWEEN '2026-07-01' AND '2026-09-30' ORDER BY metric_time__week;
```

*Devuelve:* Una fila por semana.

### 3.4 Por cargo y género

**12. Dotación por cargo**

Colaboradores por cargo, separando internos y externos, del cargo con más personas al que tiene menos.

```sql
SELECT employee__position, dotacion_total, dotacion_internos, dotacion_externos FROM semantic.employee_headcount ORDER BY dotacion_total DESC;
```

*Devuelve:* Una fila por cargo.

**13. Top 10 cargos con más externos**

Los diez cargos con más colaboradores externos y qué proporción representan dentro del cargo.

```sql
SELECT employee__position, dotacion_externos, ratio_externos FROM semantic.employee_headcount ORDER BY dotacion_externos DESC LIMIT 10;
```

*Devuelve:* Hasta 10 filas.

**14. Proveedores por cargo**

Cuántos externos tiene cada cargo y de cuántos proveedores distintos vienen. Un cargo con muchos proveedores tiene la contratación externa más fragmentada.

```sql
SELECT employee__position, dotacion_externos, proveedores_distintos FROM semantic.employee_headcount ORDER BY proveedores_distintos DESC;
```

*Devuelve:* Una fila por cargo.

**15. Vacantes por cargo**

Los 15 cargos con más vacantes, junto a su dotación actual.

```sql
SELECT employee__position, dotacion_vacantes, dotacion_total FROM semantic.employee_headcount ORDER BY dotacion_vacantes DESC LIMIT 15;
```

*Devuelve:* Hasta 15 filas.

**16. Dotación por género**

Colaboradores por género y la proporción de internos dentro de cada uno.

```sql
SELECT employee__gender, dotacion_total, ratio_internos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila por género.

**17. Cargo y género**

Matriz de dotación por cargo y género; en Metabase se puede mostrar como tabla dinámica.

```sql
SELECT employee__position, employee__gender, dotacion_total FROM semantic.employee_headcount ORDER BY employee__position, employee__gender;
```

*Devuelve:* Una fila por combinación de cargo y género.

### 3.5 Filtros

**18. Un género en particular**

Dotación por cargo solo para un género. Cambiar `'F'` por el valor que use la tabla (ver la consulta «Dotación por género»).

```sql
SELECT employee__position, dotacion_total FROM semantic.employee_headcount WHERE employee__gender = 'F' ORDER BY dotacion_total DESC;
```

*Devuelve:* Una fila por cargo.

**19. Varios cargos**

Compara cargos específicos. Reemplazar los nombres por cargos reales de la tabla.

```sql
SELECT employee__position, dotacion_total, dotacion_vacantes FROM semantic.employee_headcount WHERE employee__position IN ('Analista', 'Ingeniero de Datos');
```

*Devuelve:* Una fila por cargo de la lista.

**20. Cargos que contienen una palabra**

Todos los cargos cuyo nombre contiene «Analista». `LIKE` distingue mayúsculas de minúsculas en Athena.

```sql
SELECT employee__position, dotacion_total FROM semantic.employee_headcount WHERE employee__position LIKE '%Analista%' ORDER BY dotacion_total DESC;
```

*Devuelve:* Una fila por cargo que coincide.

**21. Combinar filtros de fecha y dimensión**

Evolución mensual de externos y proveedores para un género desde enero de 2026. Varias condiciones con `AND` se aplican todas.

```sql
SELECT metric_time__month, dotacion_externos, proveedores_distintos FROM semantic.employee_headcount WHERE metric_time >= '2026-01-01' AND employee__gender = 'M' ORDER BY metric_time__month;
```

*Devuelve:* Una fila por mes.

### 3.6 Ver el SQL sin ejecutar

**22. Ver el SQL de Athena sin ejecutar**

Devuelve el SQL que MetricFlow genera para el ratio por cargo, sin ejecutarlo. Sirve para revisarlo o copiarlo a la consola de Athena.

```sql
EXPLAIN SELECT employee__position, ratio_internos FROM semantic.employee_headcount;
```

*Devuelve:* Una fila con la columna `sql`.

---

## 4. Consultas que no funcionan y por qué

| Consulta | Mensaje del gateway | Qué hacer |
| --- | --- | --- |
| `SELECT * FROM semantic.employee_headcount` | SELECT * no está soportado: elija métricas y dimensiones | Nombrar las columnas. |
| `SELECT employee__position, dotacion_total FROM semantic.employee_headcount WHERE dotacion_total > 10` | No se puede filtrar por la métrica 'dotacion_total' | Solo se filtra por dimensiones; ordenar con `ORDER BY` y usar `LIMIT`. |
| `SELECT COUNT(*) FROM semantic.employee_headcount` | Expresión no soportada | Usar `dotacion_total`. |
| `SELECT employee__position, dotacion_total / 2 FROM semantic.employee_headcount` | Expresión no soportada | Las operaciones van en la definición de la métrica (dbt), no en la consulta. |
| Pestaña *Datos* de `employee_headcount` en DBeaver | `SELECT * no está soportado…` | Usar la pestaña *Columnas* o escribir la consulta |

**Errores de Athena** (llegan a DBeaver con el mensaje original):

| Mensaje | Causa |
| --- | --- |
| `ExpiredToken` | Vencieron las credenciales temporales: cargarlas de nuevo y reiniciar el gateway |
| `AccessDenied` / Lake Formation | El rol no tiene permiso de lectura sobre la tabla o sus datos en S3 |
| `TABLE_NOT_FOUND` / `SCHEMA_NOT_FOUND` | El manifest apunta a una tabla que no existe en Glue con ese nombre |
| `La consulta superó el tiempo máximo de 100 s` | La consulta tardó demasiado y se canceló en Athena; subir `--timeout` si hace falta |
