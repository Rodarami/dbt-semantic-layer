"""Catálogo PostgreSQL emulado en DuckDB y compatibilidad con los drivers de los clientes BI.

Lo usa `server.py` (riffq). `server_old.py` (buenavista) conserva su propia copia de estas piezas. Contiene:
* `install_catalog`: tablas virtuales vacías por modelo semántico, tablas de metadatos (`semantic.metrics`…) y vistas
  y macros con lo que DBeaver, Metabase y Npgsql (Power BI) consultan en `pg_catalog` y DuckDB no trae;
* `reescribir`: adapta al dialecto de DuckDB una consulta de catálogo enviada en dialecto PostgreSQL;
* `motor_from_database`, `_es_sonda`, `_consulta_vacia`: reglas de enrutamiento comunes.
"""

from __future__ import annotations

import logging
import re

import duckdb
import sqlglot
from sqlglot import exp

from .engine import MOTORES
from .model import Catalog

logger = logging.getLogger("semantic_gateway")

SCHEMA = "semantic"

def _consulta_vacia(sql: str) -> bool:
    """Cierto para '', ';' o solo comentarios: los drivers lo usan como 'ping' (isValid)."""
    sin_comentarios = re.sub(r"--[^\n]*|/\*.*?\*/", "", sql, flags=re.S)
    return not sin_comentarios.strip(" \t\r\n;")


# ---------------------------------------------------------------------------------------------- catálogo
# DuckDB informa tipos erróneos en pg_attribute/pg_type; se sirven vistas propias con los OID reales de PostgreSQL
_PG_TIPOS = [(16, "bool", 1), (17, "bytea", -1), (19, "name", 64), (20, "int8", 8), (21, "int2", 2), (23, "int4", 4),
             (25, "text", -1), (700, "float4", 4), (701, "float8", 8), (1042, "bpchar", -1), (1043, "varchar", -1),
             (1082, "date", 4), (1083, "time", 8), (1114, "timestamp", 8), (1184, "timestamptz", 8),
             (1186, "interval", 16), (1700, "numeric", -1), (2950, "uuid", 16)]


def _install_pg_views(db: duckdb.DuckDBPyConnection) -> None:
    valores = ", ".join(f"({o}, '{n}', {ln})" for o, n, ln in _PG_TIPOS)
    db.execute(
        "CREATE VIEW main.gw_pg_type AS SELECT oid::BIGINT AS oid, typname, "
        "(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='pg_catalog' LIMIT 1) AS typnamespace, "
        "typlen::SMALLINT AS typlen, 'b' AS typtype, 0::BIGINT AS typelem, 0::BIGINT AS typbasetype, "
        "'U' AS typcategory, false AS typnotnull, -1 AS typtypmod, ',' AS typdelim, 0::BIGINT AS typrelid, "
        f"0::BIGINT AS typarray FROM (VALUES {valores}) AS t(oid, typname, typlen)"
    )
    db.execute(
        "CREATE VIEW main.gw_pg_attribute AS SELECT table_oid::BIGINT AS attrelid, column_name AS attname, "
        "CASE WHEN upper(data_type) LIKE 'VARCHAR%' THEN 25 WHEN upper(data_type) = 'DATE' THEN 1082 "
        "WHEN upper(data_type) = 'BIGINT' THEN 20 WHEN upper(data_type) = 'INTEGER' THEN 23 "
        "WHEN upper(data_type) = 'DOUBLE' THEN 701 WHEN upper(data_type) LIKE 'DECIMAL%' THEN 1700 "
        "WHEN upper(data_type) = 'BOOLEAN' THEN 16 WHEN upper(data_type) LIKE 'TIMESTAMP%' THEN 1114 "
        "ELSE 25 END::BIGINT AS atttypid, -1 AS atttypmod, column_index::SMALLINT AS attnum, false AS attnotnull, "
        "false AS attisdropped, '' AS attidentity, '' AS attgenerated, false AS atthasdef, 0 AS attndims, "
        "0::BIGINT AS attcollation, 0::BIGINT AS attlen FROM duckdb_columns() WHERE NOT internal"
    )


# Tipos de columna con los nombres de PostgreSQL. DuckDB informa VARCHAR, DOUBLE, DATE…; Power BI en DirectQuery
# valida los tipos antes de generar SQL y, si no los reconoce, no pliega la consulta ("No se ha podido plegar la
# expresión al origen de datos"). Coinciden con los OID de gw_pg_attribute (texto = text, 25).
_TIPOS_PG_COLUMNAS = (
    "CASE WHEN upper(data_type) LIKE 'VARCHAR%' THEN 'text' WHEN upper(data_type) = 'DOUBLE' THEN 'double precision' "
    "WHEN upper(data_type) = 'FLOAT' THEN 'real' WHEN upper(data_type) = 'BIGINT' THEN 'bigint' "
    "WHEN upper(data_type) = 'INTEGER' THEN 'integer' WHEN upper(data_type) = 'SMALLINT' THEN 'smallint' "
    "WHEN upper(data_type) = 'BOOLEAN' THEN 'boolean' WHEN upper(data_type) = 'DATE' THEN 'date' "
    "WHEN upper(data_type) LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 'timestamp with time zone' "
    "WHEN upper(data_type) LIKE 'TIMESTAMP%' THEN 'timestamp without time zone' "
    "WHEN upper(data_type) LIKE 'DECIMAL%' THEN 'numeric' ELSE lower(data_type) END AS data_type, "
    "CASE WHEN upper(data_type) LIKE 'VARCHAR%' THEN 'text' WHEN upper(data_type) = 'DOUBLE' THEN 'float8' "
    "WHEN upper(data_type) = 'FLOAT' THEN 'float4' WHEN upper(data_type) = 'BIGINT' THEN 'int8' "
    "WHEN upper(data_type) = 'INTEGER' THEN 'int4' WHEN upper(data_type) = 'SMALLINT' THEN 'int2' "
    "WHEN upper(data_type) = 'BOOLEAN' THEN 'bool' WHEN upper(data_type) = 'DATE' THEN 'date' "
    "WHEN upper(data_type) LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 'timestamptz' "
    "WHEN upper(data_type) LIKE 'TIMESTAMP%' THEN 'timestamp' WHEN upper(data_type) LIKE 'DECIMAL%' THEN 'numeric' "
    "ELSE lower(data_type) END AS udt_name"
)

def install_catalog(db: duckdb.DuckDBPyConnection, catalog: Catalog) -> None:
    """Crea en DuckDB las tablas virtuales (vacías, con tipos y descripciones) y las tablas de metadatos."""
    s = catalog.schema
    db.execute(f"CREATE SCHEMA {s}")
    _install_pg_views(db)
    for nombre in sorted(_PRIVILEGIOS):  # respaldo textual si la consulta no se puede analizar con sqlglot
        db.execute(f"CREATE OR REPLACE MACRO main.{nombre}(a, b) AS true, (a, b, c) AS true")
    # vistas que los clientes esperan en pg_catalog y DuckDB no trae (sin filas)
    for vista in ("pg_stat_user_tables", "pg_stat_all_tables"):  # estadísticas: sin filas (el LEFT JOIN del cliente queda en NULL)
        db.execute(
            f"CREATE VIEW main.{vista} AS SELECT 0::BIGINT AS relid, NULL::VARCHAR AS schemaname, NULL::VARCHAR AS relname, "
            "0::BIGINT AS seq_scan, 0::BIGINT AS seq_tup_read, 0::BIGINT AS idx_scan, 0::BIGINT AS idx_tup_fetch, "
            "0::BIGINT AS n_tup_ins, 0::BIGINT AS n_tup_upd, 0::BIGINT AS n_tup_del, 0::BIGINT AS n_tup_hot_upd, "
            "0::BIGINT AS n_live_tup, 0::BIGINT AS n_dead_tup, NULL::TIMESTAMP AS last_vacuum, "
            "NULL::TIMESTAMP AS last_autovacuum, NULL::TIMESTAMP AS last_analyze, NULL::TIMESTAMP AS last_autoanalyze "
            "WHERE false")
    for nombre, cuerpo in _FUNCIONES_PROPIAS.items():
        db.execute(f"CREATE OR REPLACE MACRO main.gw_{nombre}{cuerpo}")
    # una "base de datos" por motor (+ semantic): DBeaver busca la base conectada en pg_database
    bases = ", ".join(f"({i}, '{n}')" for i, n in enumerate([SCHEMA, *sorted(MOTORES)], start=16384))
    db.execute("CREATE VIEW main.gw_pg_database AS SELECT oid::BIGINT AS oid, datname, 10::BIGINT AS datdba, "
               "6 AS encoding, 'C' AS datcollate, 'C' AS datctype, false AS datistemplate, true AS datallowconn, "
               "-1 AS datconnlimit, 1663::BIGINT AS dattablespace, NULL::VARCHAR AS datacl "
               f"FROM (VALUES {bases}) AS t(oid, datname)")
    # pg_settings de PostgreSQL (DuckDB trae solo 4 columnas): DBeaver lee reset_val de search_path
    # information_schema sin el esquema `main` (vistas y macros auxiliares del gateway): así los navegadores de los BI
    # (Power BI, Metabase) solo muestran el esquema `semantic`
    for vista, columna in _INFO_SIN_MAIN.items():
        reemplazo = f" REPLACE ({_TIPOS_PG_COLUMNAS})" if vista == "columns" else ""
        db.execute(f"CREATE VIEW main.gw_info_{vista} AS SELECT *{reemplazo} FROM information_schema.{vista} "
                   f"WHERE {columna} <> 'main'")
    # lo mismo en pg_catalog: Metabase sincroniza las tablas leyendo pg_class y pg_namespace
    db.execute("CREATE VIEW main.gw_pg_namespace AS SELECT * FROM pg_catalog.pg_namespace WHERE nspname <> 'main'")
    db.execute("CREATE VIEW main.gw_pg_class AS SELECT * FROM pg_catalog.pg_class WHERE relnamespace NOT IN "
               "(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'main')")
    db.execute("CREATE VIEW main.gw_pg_settings AS SELECT name, setting, NULL::VARCHAR AS unit, 'Gateway' AS category, "
               "short_desc, NULL::VARCHAR AS extra_desc, 'user' AS context, vartype, 'default' AS source, "
               "NULL::VARCHAR AS min_val, NULL::VARCHAR AS max_val, NULL::VARCHAR AS enumvals, setting AS boot_val, "
               "setting AS reset_val, NULL::VARCHAR AS sourcefile, NULL::INTEGER AS sourceline, false AS pending_restart "
               "FROM (VALUES ('search_path', 'semantic, main', 'Esquemas donde se buscan las tablas', 'string'), "
               "('standard_conforming_strings', 'on', 'Las barras invertidas son literales', 'bool'), "
               "('TimeZone', 'UTC', 'Zona horaria de la sesión', 'string'), "
               "('server_version', '14.0', 'Versión del servidor', 'string'), "
               "('server_encoding', 'UTF8', 'Codificación del servidor', 'string'), "
               "('client_encoding', 'UTF8', 'Codificación del cliente', 'string'), "
               "('max_identifier_length', '63', 'Largo máximo de identificadores', 'integer'), "
               "('default_transaction_read_only', 'on', 'Las transacciones son de solo lectura', 'bool')"
               ") AS t(name, setting, short_desc, vartype)")
    # vistas de catálogo que DBeaver consulta y DuckDB no trae (sin filas)
    db.execute("CREATE VIEW main.pg_trigger AS SELECT 0::BIGINT AS oid, 0::BIGINT AS tgrelid, NULL::VARCHAR AS tgname, "
               "0::BIGINT AS tgfoid, 0::SMALLINT AS tgtype, NULL::VARCHAR AS tgenabled, false AS tgisinternal, "
               "0::BIGINT AS tgconstrrelid, 0::BIGINT AS tgconstrindid, 0::BIGINT AS tgconstraint, false AS tgdeferrable, "
               "false AS tginitdeferred, 0::SMALLINT AS tgnargs, NULL::VARCHAR AS tgattr, NULL::VARCHAR AS tgargs, "
               "NULL::VARCHAR AS tgqual, NULL::VARCHAR AS tgoldtable, NULL::VARCHAR AS tgnewtable WHERE false")
    db.execute("CREATE VIEW main.pg_roles AS SELECT 0::BIGINT AS oid, NULL::VARCHAR AS rolname, false AS rolsuper, "
               "false AS rolinherit, false AS rolcreaterole, false AS rolcreatedb, false AS rolcanlogin, "
               "false AS rolreplication, -1 AS rolconnlimit, NULL::VARCHAR AS rolpassword, NULL::TIMESTAMP AS rolvaliduntil, "
               "false AS rolbypassrls, NULL::VARCHAR[] AS rolconfig WHERE false")
    db.execute("CREATE VIEW main.pg_extension AS SELECT 0::BIGINT AS oid, NULL::VARCHAR AS extname, 0::BIGINT AS extowner, "
               "0::BIGINT AS extnamespace, false AS extrelocatable, NULL::VARCHAR AS extversion, "
               "NULL::BIGINT[] AS extconfig, NULL::VARCHAR[] AS extcondition WHERE false")
    db.execute("CREATE VIEW main.pg_matviews AS SELECT NULL::VARCHAR AS schemaname, NULL::VARCHAR AS matviewname, "
               "NULL::VARCHAR AS matviewowner, NULL::VARCHAR AS definition WHERE false")
    for table in catalog.tables.values():
        columns = [f'"{d}" {catalog.dimensions[d].data_type}' for d in table.dimensions]
        columns += [f'"{m}" DOUBLE' for m in table.metrics]
        db.execute(f"CREATE TABLE {s}.{table.name} ({', '.join(columns)})")
        db.execute(f"COMMENT ON TABLE {s}.{table.name} IS '{_quote(table.description)}'")
        comments = {d: catalog.dimensions[d].description for d in table.dimensions}
        comments.update({m: catalog.metrics[m].label for m in table.metrics})
        for column, text in comments.items():
            if text:
                db.execute(f"COMMENT ON COLUMN {s}.{table.name}.\"{column}\" IS '{_quote(text)}'")

    db.execute(f"CREATE TABLE {s}.metrics (metric VARCHAR, label VARCHAR, type VARCHAR, description VARCHAR, "
               "table_name VARCHAR)")
    db.executemany(
        f"INSERT INTO {s}.metrics VALUES (?, ?, ?, ?, ?)",
        [(m.name, m.label, m.type, m.description, catalog.table_of_metric(m.name)) for m in catalog.metrics.values()],
    )
    db.execute(f"CREATE TABLE {s}.dimensions (dimension VARCHAR, kind VARCHAR, data_type VARCHAR, description VARCHAR)")
    db.executemany(
        f"INSERT INTO {s}.dimensions VALUES (?, ?, ?, ?)",
        [(d.name, d.kind, d.data_type, d.description) for d in catalog.dimensions.values()],
    )
    db.execute(f"CREATE TABLE {s}.metric_dimensions (metric VARCHAR, dimension VARCHAR)")
    db.executemany(f"INSERT INTO {s}.metric_dimensions VALUES (?, ?)", list(catalog.metric_dimensions))


# ---------------------------------------------------------------------------------------------- seguridad de DuckDB
# Las consultas que no son semánticas (catálogo, metadatos, sondas) se ejecutan en DuckDB. Sin estas protecciones,
# cualquier usuario autenticado podría leer y escribir archivos de la máquina del gateway (`read_text`, `COPY … TO`,
# `ATTACH`), instalar extensiones o borrar las vistas del catálogo que comparten todas las conexiones.

_TIPOS_PERMITIDOS = {"SELECT"}  # PRAGMA de solo lectura (p. ej. `PRAGMA version`) también se informa como SELECT


def blindar(db: duckdb.DuckDBPyConnection, memoria: str = "1GB") -> None:
    """Llamar después de `install_catalog`: sin acceso a archivos, red ni extensiones, y configuración bloqueada."""
    db.execute(f"SET memory_limit = '{memoria}'")
    db.execute("SET enable_external_access = false")  # read_*, COPY, ATTACH, INSTALL/LOAD, glob…
    db.execute("SET lock_configuration = true")       # nadie puede volver a activarlo con SET


def solo_lectura(sql: str, tambien: frozenset[str] = frozenset()) -> None:
    """Rechaza todo lo que no sea una única consulta de lectura (DROP, CREATE, COPY, ATTACH, varias sentencias…).

    `tambien` agrega tipos permitidos: el servidor anterior (buenavista) envía a DuckDB `BEGIN`/`COMMIT` y `SET`.
    """
    try:
        sentencias = duckdb.extract_statements(sql)
    except duckdb.Error:
        return  # SQL inválido: DuckDB devolverá su propio error de sintaxis al ejecutarlo
    if len(sentencias) != 1 or sentencias[0].type.name not in _TIPOS_PERMITIDOS | tambien:
        tipos = ", ".join(s.type.name for s in sentencias) or "vacía"
        raise duckdb.PermissionException(
            f"El gateway solo admite una consulta de lectura (SELECT) por vez; se recibió: {tipos}")


def _quote(text: str) -> str:
    return text.replace("'", "''")

def motor_from_database(database: str | None, user: str | None, default: str) -> tuple[str, str | None]:
    """Motor pedido en la conexión (`dbname=snowflake`). Devuelve (motor, error).

    * sin dbname, `semantic` o el nombre por defecto que manda el cliente (= usuario) → motor por defecto
    * nombre de un motor soportado (athena, snowflake, …) → ese motor
    * cualquier otro → error, que se informa al hacer la primera consulta semántica
    """
    name = (database or "").strip().lower()
    if not name or name == SCHEMA or name == (user or "").lower():
        return default, None
    if name in MOTORES:
        return name, None
    return default, (f"Base de datos '{database}' no existe. Use dbname={SCHEMA} (motor por defecto: {default}) "
                     f"o uno de: {', '.join(sorted(MOTORES))}")


def _es_sonda(sql: str) -> bool:
    """`SELECT … FROM tabla WHERE 1<>1` o `LIMIT 0`: los BI las usan para validar permisos o leer la forma de la
    tabla. No piden datos, así que las responde el catálogo (la tabla virtual existe vacía, con sus tipos)."""
    try:
        arbol = sqlglot.parse_one(sql, read="postgres")
    except sqlglot.errors.SqlglotError:
        return False
    if not isinstance(arbol, exp.Select):
        return False
    limite = arbol.args.get("limit")
    if limite is not None and limite.expression.sql() == "0":
        return True
    donde = arbol.args.get("where")
    if donde is None:
        return False
    cond = donde.this
    if isinstance(cond, exp.Boolean):
        return not cond.this
    if isinstance(cond, (exp.NEQ, exp.EQ)) and isinstance(cond.left, exp.Literal) and isinstance(cond.right, exp.Literal):
        igual = cond.left.sql() == cond.right.sql()
        return igual if isinstance(cond, exp.NEQ) else not igual
    return False

_PRIVILEGIOS = {"has_schema_privilege", "has_table_privilege", "has_any_column_privilege", "has_column_privilege",
                "has_database_privilege", "has_function_privilege", "has_sequence_privilege"}
# Funciones de PostgreSQL que DuckDB no tiene (o no acepta con esos argumentos): se reemplazan por macros gw_<nombre>
# que devuelven NULL. Las usa DBeaver al listar tablas, columnas e índices; el gateway no tiene particiones,
# expresiones por defecto, tablespaces ni estadísticas.
_FUNCIONES_PROPIAS = {
    "pg_get_expr": "(a, b) AS NULL::VARCHAR, (a, b, c) AS NULL::VARCHAR",
    # nombre del tipo según PostgreSQL (el format_type de DuckDB no conoce varios OID, p. ej. 1082 = date)
    "format_type": "(oid, typmod) AS CASE oid WHEN 25 THEN 'text' WHEN 1043 THEN 'character varying' "
                   "WHEN 701 THEN 'double precision' WHEN 700 THEN 'real' WHEN 20 THEN 'bigint' WHEN 23 THEN 'integer' "
                   "WHEN 21 THEN 'smallint' WHEN 16 THEN 'boolean' WHEN 1082 THEN 'date' "
                   "WHEN 1114 THEN 'timestamp without time zone' WHEN 1184 THEN 'timestamp with time zone' "
                   "WHEN 1700 THEN 'numeric' ELSE NULL END",
    "pg_get_partkeydef": "(a) AS NULL::VARCHAR",
    "pg_tablespace_location": "(a) AS NULL::VARCHAR",
    # tamaños y estadísticas (DBeaver los muestra en las propiedades de tablas e índices): 0
    "pg_relation_size": "(a) AS 0::BIGINT, (a, b) AS 0::BIGINT",
    "pg_total_relation_size": "(a) AS 0::BIGINT",
    "pg_table_size": "(a) AS 0::BIGINT",
    "pg_indexes_size": "(a) AS 0::BIGINT",
    "pg_stat_get_numscans": "(a) AS 0::BIGINT",
    "pg_advisory_unlock_all": "() AS NULL",  # Npgsql al reutilizar conexiones
    # función de tabla: DBeaver pide las palabras reservadas para resaltar la sintaxis
    "pg_get_keywords": "() AS TABLE SELECT lower(keyword_name) AS word, CASE keyword_category WHEN 'reserved' THEN 'R' "
                       "WHEN 'unreserved' THEN 'U' WHEN 'type_function' THEN 'T' ELSE 'C' END AS catcode, "
                       "keyword_category AS catdesc FROM duckdb_keywords()",
}
_VISTAS_PROPIAS = {"pg_database": "gw_pg_database", "pg_trigger": "pg_trigger", "pg_roles": "pg_roles", "pg_extension": "pg_extension",
                   "pg_matviews": "pg_matviews", "pg_stat_user_tables": "pg_stat_user_tables",
                   "pg_stat_all_tables": "pg_stat_all_tables", "pg_settings": "gw_pg_settings", "pg_type": "gw_pg_type", "pg_attribute": "gw_pg_attribute",
                   "pg_namespace": "gw_pg_namespace", "pg_class": "gw_pg_class"}  # sin el esquema auxiliar `main`
_REG_TIPOS = {"regnamespace": ("pg_namespace", "oid", "nspname"), "regclass": ("pg_class", "oid", "relname"),
              "regtype": ("pg_type", "oid", "typname")}


_INFO_SIN_MAIN = {"tables": "table_schema", "columns": "table_schema", "views": "table_schema", "schemata": "schema_name"}


def _compat_catalogo(nodo: exp.Expression) -> exp.Expression:
    """Adapta lo que los drivers (Metabase/JDBC) usan en catálogo y DuckDB no tiene:
    * `x::regnamespace` / `::regclass` / `::regtype` → subconsulta que resuelve el nombre
    * `'nombre'::regclass` → subconsulta que resuelve el oid
    * `has_*_privilege(...)` → TRUE (el gateway no aplica permisos de catálogo)
    * `pg_get_expr`, `pg_get_partkeydef`, `pg_tablespace_location` → macros `gw_*` que devuelven NULL
    * `pg_trigger`, `pg_roles`, `pg_extension`, `pg_matviews`, `pg_stat_*`, `pg_type`, `pg_attribute` → vistas propias
    * `pg_namespace`, `pg_class` → las mismas sin el esquema auxiliar `main`
    """
    if isinstance(nodo, exp.Cast):
        tipo = str(nodo.to.this).lower() if isinstance(nodo.to, exp.ObjectIdentifier) else ""
        if tipo in _REG_TIPOS:
            tabla, oid, nombre = _REG_TIPOS[tipo]
            if isinstance(nodo.this, exp.Literal) and nodo.this.is_string:
                return sqlglot.parse_one(f"(SELECT {oid} FROM pg_catalog.{tabla} WHERE {nombre} = {nodo.this.sql()} LIMIT 1)")
            return sqlglot.parse_one(f"(SELECT {nombre} FROM pg_catalog.{tabla} WHERE {oid} = {nodo.this.sql()} LIMIT 1)")
    if isinstance(nodo, exp.Dot) and isinstance(nodo.expression, exp.Anonymous) \
            and nodo.expression.name.lower() in _PRIVILEGIOS:  # pg_catalog.has_table_privilege(...)
        return exp.true()
    funcion = nodo.expression if isinstance(nodo, exp.Dot) else nodo  # pg_catalog.fn(...) o fn(...)
    if isinstance(funcion, exp.Anonymous) and funcion.name.lower() in _FUNCIONES_PROPIAS:
        return exp.Anonymous(this=f"gw_{funcion.name.lower()}", expressions=funcion.expressions)
    if isinstance(nodo, exp.Table) and isinstance(nodo.this, exp.Anonymous) \
            and nodo.this.name.lower() in _FUNCIONES_PROPIAS:  # FROM pg_catalog.pg_get_keywords(): sin el prefijo del esquema
        nueva = exp.Table(this=exp.Anonymous(this=f"gw_{nodo.this.name.lower()}", expressions=nodo.this.expressions))
        if nodo.args.get("alias"):
            nueva.set("alias", nodo.args["alias"])
        return nueva
    if isinstance(nodo, exp.Table) and isinstance(nodo.this, exp.Identifier) and nodo.db.lower() == "information_schema" \
            and nodo.name.lower() in _INFO_SIN_MAIN:  # information_schema.tables… sin las vistas auxiliares de `main`
        nueva = exp.to_table(f"gw_info_{nodo.name.lower()}")
        nueva.set("alias", nodo.args.get("alias") or exp.TableAlias(this=exp.to_identifier(nodo.name.lower())))
        return nueva
    if isinstance(nodo, exp.Table) and nodo.name.lower() in _VISTAS_PROPIAS:
        nueva = exp.to_table(_VISTAS_PROPIAS[nodo.name.lower()])
        if nodo.args.get("alias"):
            nueva.set("alias", nodo.args["alias"])
        return nueva
    if isinstance(nodo, (exp.Anonymous, exp.Func)) and (getattr(nodo, "name", "") or "").lower() in _PRIVILEGIOS:
        return exp.true()
    return nodo


def _compat_catalogo_texto(sql: str) -> str:
    """Respaldo por expresiones regulares cuando sqlglot no logra analizar la consulta del cliente."""
    for tipo, (tabla, oid, nombre) in _REG_TIPOS.items():
        sql = re.sub(rf"([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)\s*::\s*{tipo}\b",
                     rf"(SELECT {nombre} FROM pg_catalog.{tabla} WHERE {oid} = \1 LIMIT 1)", sql, flags=re.I)
    sql = re.sub(r"\bpg_catalog\.(has_\w+_privilege)\b", r"\1", sql, flags=re.I)
    for nombre in _FUNCIONES_PROPIAS:
        sql = re.sub(rf"(?<![\w.])(?:pg_catalog\.)?{nombre}\s*\(", f"gw_{nombre}(", sql, flags=re.I)
    sql = re.sub(r'\binformation_schema\s*\.\s*"?(tables|columns|views|schemata)"?(?![\w"])',
                 lambda m: f"gw_info_{m.group(1).lower()} AS {m.group(1).lower()}", sql, flags=re.I)
    for original, propia in _VISTAS_PROPIAS.items():
        sql = re.sub(rf"(?<![\w.])(?:pg_catalog\.)?{original}\b", propia, sql, flags=re.I)
    return sql


_ARREGLO_TEXTO = re.compile(r"(?:CAST\s*\(\s*)?'\{([^{}']*)\}'\s*(?:AS\s+|::\s*)(?:TEXT|VARCHAR|NAME)\s*\[\s*\]\s*\)?", re.I)


def _literales_arreglo(sql: str) -> str:
    """`'{a,b}'::text[]` (literal de arreglo de PostgreSQL) → `['a','b']`, que sí entiende DuckDB."""
    def a_lista(m: re.Match) -> str:
        items = [i.strip().strip('"').replace("'", "''") for i in m.group(1).split(",") if i.strip()]
        return "[" + ", ".join(f"'{i}'" for i in items) + "]"
    return _ARREGLO_TEXTO.sub(a_lista, sql)


# Tipos que se informan a Npgsql (driver de Power BI) al conectar: (oid, nombre, typtype, oid del elemento si es arreglo)
_NPGSQL_TIPOS = [
    (16, "bool", "b", None), (17, "bytea", "b", None), (18, "char", "b", None), (19, "name", "b", None),
    (20, "int8", "b", None), (21, "int2", "b", None), (23, "int4", "b", None), (25, "text", "b", None),
    (26, "oid", "b", None), (114, "json", "b", None), (700, "float4", "b", None), (701, "float8", "b", None),
    (1042, "bpchar", "b", None), (1043, "varchar", "b", None), (1082, "date", "b", None), (1083, "time", "b", None),
    (1114, "timestamp", "b", None), (1184, "timestamptz", "b", None), (1186, "interval", "b", None),
    (1700, "numeric", "b", None), (2950, "uuid", "b", None), (3802, "jsonb", "b", None),
    (2249, "record", "p", None), (2278, "void", "p", None),
    (1000, "_bool", "a", 16), (1001, "_bytea", "a", 17), (1002, "_char", "a", 18), (1003, "_name", "a", 19),
    (1005, "_int2", "a", 21), (1007, "_int4", "a", 23), (1009, "_text", "a", 25), (1014, "_bpchar", "a", 1042),
    (1015, "_varchar", "a", 1043), (1016, "_int8", "a", 20), (1021, "_float4", "a", 700), (1022, "_float8", "a", 701),
    (1028, "_oid", "a", 26), (1115, "_timestamp", "a", 1114), (1182, "_date", "a", 1082), (1183, "_time", "a", 1083),
    (1185, "_timestamptz", "a", 1184), (1187, "_interval", "a", 1186), (1231, "_numeric", "a", 1700),
    (199, "_json", "a", 114), (2951, "_uuid", "a", 2950), (3807, "_jsonb", "a", 3802),
]


# Comandos con que los drivers limpian la sesión (Npgsql/Power BI los envía al reutilizar una conexión del pool).
# El gateway no guarda estado de sesión que limpiar: se responden como un comando sin efecto.
_REINICIO_SESION = re.compile(r"\s*(discard\s+all|discard\s+(plans|sequences|temp|temporary)|reset\s+all|deallocate\s+all"
                              r"|unlisten\s+\*|close\s+all|set\s+session\s+authorization\s+default)\s*;?\s*$", re.I)


def _consulta_npgsql(sql: str) -> str | None:
    """Consultas de catálogo que Npgsql (Power BI) envía al conectar y que DuckDB no puede responder.

    * tipos (`pg_type` + `typreceive`/`array_recv` + `pg_range`): lista fija de tipos base y sus arreglos;
    * campos de tipos compuestos (`attname`, `atttypid`): sin filas;
    * etiquetas de enums (`pg_enum`): sin filas.
    Npgsql pide estos resultados como texto y los lee por nombre de columna; se incluyen los nombres de 4.0 y 4.1+.
    """
    texto = sql.lower()
    if "typreceive" in texto and "array_recv" in texto:
        filas = ", ".join(
            "('pg_catalog', '{1}', '{0}', '0', '0', '{2}', '{2}', {3}, {3}, '{4}')".format(
                oid, nombre, tipo, f"'{elem}'" if elem else "NULL", 3 if tipo == "a" else 0)
            for oid, nombre, tipo, elem in _NPGSQL_TIPOS)
        return ("SELECT * FROM (VALUES " + filas + ") AS t(nspname, typname, oid, typrelid, typbasetype, type, "
                "typtype, elemoid, elemtypoid, ord) ORDER BY ord")
    if "atttypid" in texto and "attname" in texto and "typtype" in texto and "pg_attribute" in texto:
        return "SELECT NULL::VARCHAR AS oid, NULL::VARCHAR AS attname, NULL::VARCHAR AS atttypid WHERE false"
    if "enumlabel" in texto and "pg_enum" in texto:
        return "SELECT NULL::VARCHAR AS oid, NULL::VARCHAR AS enumlabel WHERE false"
    return None



def reescribir(sql: str) -> str:
    """Consulta de catálogo (dialecto PostgreSQL) → SQL que DuckDB puede ejecutar contra el catálogo emulado.

    No recibe consultas semánticas (esas van al traductor). Equivale al `SemanticRewriter` de `server_old.py` más los
    ajustes que buenavista hacía por su cuenta (`SHOW search_path`, `max_index_keys`, `PG_GET_USERBYID`…).
    """
    if _consulta_vacia(sql):
        return "SELECT 1 WHERE FALSE"  # ping de los drivers (';'): respuesta vacía sin error
    if sql.strip().rstrip(";").lower() in ("select version()", "select pg_catalog.version()"):
        return "SELECT 'PostgreSQL 14.0 (gateway semántico sobre MetricFlow)' AS version"
    npgsql = _consulta_npgsql(sql)
    if npgsql is not None:
        return npgsql
    if "_pg_expandarray" in sql:  # getPrimaryKeys de JDBC: la capa semántica no tiene claves primarias
        columnas = ("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME")
        n = max([int(i) for i in re.findall(r"\$(\d+)", sql)] or [0])  # la consulta original recibe n parámetros
        usa_parametros = "".join(f" AND ${i}::VARCHAR IS NULL" for i in range(1, n + 1))
        return "SELECT " + ", ".join(f'NULL::VARCHAR AS "{c}"' for c in columnas) + " WHERE false" + usa_parametros
    if re.search(r"max_index_keys", sql, re.I):
        return "SELECT '32' AS setting"
    sql = _literales_arreglo(sql)
    sql = re.sub(r"pg_catalog\.PG_GET_USERBYID", "", sql, flags=re.I)  # \d de psql: muestra el oid del dueño
    try:
        arbol = sqlglot.parse_one(sql, read="postgres")
        if arbol is not None and not isinstance(arbol, exp.Command):
            sql = arbol.transform(_compat_catalogo).sql(dialect="postgres")
    except sqlglot.errors.SqlglotError as error:
        logger.warning("Consulta de catálogo no analizable por sqlglot (%s); se usa el respaldo textual:\n%s",
                       str(error).splitlines()[0], sql)
        sql = _compat_catalogo_texto(sql)
    try:
        return ";\n".join(sqlglot.transpile(sql, read="postgres", write="duckdb"))
    except sqlglot.errors.SqlglotError:
        return sql
