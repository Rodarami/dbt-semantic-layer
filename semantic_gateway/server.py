"""Servidor del gateway semántico de dbt_glue_lab por protocolo PostgreSQL (wire protocol v3, vía buenavista).

Cada consulta se enruta según la tabla que lee:
* una tabla virtual por modelo semántico (`semantic.events`, `semantic.items`…) → traductor → MetricFlow (compila) →
  se IMPRIME el SQL y se devuelve al cliente como una fila con la columna `sql`. No se ejecuta nada contra ningún
  warehouse (con o sin EXPLAIN).
* Cualquier otra (`semantic.metrics`, `semantic.dimensions`, `semantic.metric_dimensions`, information_schema,
  pg_catalog, SET, version()…) → catálogo en DuckDB en memoria, que contiene las tablas virtuales vacías con sus tipos.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import struct
import threading

import duckdb
import pyarrow as pa
import sqlglot
from buenavista.backends import duckdb as bv_duckdb
from buenavista.core import BVType
from sqlglot import exp
from buenavista import bv_dialects, postgres, rewrite
from buenavista.backends.duckdb import DuckDBConnection, DuckDBQueryResult, DuckDBSession

from .engine import MOTORES, SQLGLOT_DIALECT, SemanticEngine
from .executor import ConfigAthena, Ejecutor, EjecutorAthena, ExecutionError, formato_bytes, ordenar_columnas
from .model import Catalog
from .translator import TranslationError, Translator, split_explain

logger = logging.getLogger("semantic_gateway")

SCHEMA = "semantic"

# --- compatibilidad con drivers JDBC (Metabase, DBeaver…): ajustes sobre buenavista -------------------------
# buenavista solo conoce unos pocos tipos de parámetro y trata el tipo 0 (sin especificar) como fecha: un
# parámetro de texto rompía la conexión. Se declaran los tipos de texto/bool/numérico habituales y, si un valor
# de tipo 0 no es una fecha, se deja como texto.
postgres.TYPE_OIDS[0] = ("unknown", None, None)  # valor de muestra de describe_statement: NULL sirve en cualquier contexto de tipo
postgres.TYPE_OIDS.update({
    16: ("BOOLOID", "!?", True),
    25: ("TEXTOID", None, None),
    1042: ("BPCHAROID", None, None),
    1043: ("VARCHAROID", None, None),
    19: ("NAMEOID", None, None),
    1700: ("NUMERICOID", None, "0"),
})


_to_bvtype = bv_duckdb.to_bvtype


def _to_bvtype_con_nulos(tipo):
    """Una columna NULL (p. ej. `NULL AS TABLE_CAT` de JDBC) se informa como texto en vez de fallar."""
    return BVType.TEXT if str(tipo) == "null" else _to_bvtype(tipo)


bv_duckdb.to_bvtype = _to_bvtype_con_nulos

_parse_fecha = postgres.dateutil.parser.parse  # el original, antes de sustituirlo


class _ParserSeguro:
    """Sustituye a `dateutil.parser` dentro de buenavista: fecha si se puede, texto si no."""

    @staticmethod
    def parse(texto: str):
        try:
            return _parse_fecha(texto)
        except (ValueError, OverflowError):
            return texto


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


def _quote(text: str) -> str:
    return text.replace("'", "''")


# ---------------------------------------------------------------------------------------------- sesión
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


class SemanticSession(DuckDBSession):
    def __init__(self, cursor, engine: SemanticEngine, translator: Translator,
                 ejecutores: dict[str, Ejecutor] | None = None) -> None:
        super().__init__(cursor)
        self._engine = engine
        self._translator = translator
        self._ejecutores = ejecutores or {}  # motor → ejecutor; sin ejecutor se devuelve el SQL compilado
        self.motor: str = engine.motor  # lo fija el handler al conectar, según dbname
        self.motor_error: str | None = None

    def execute_sql(self, sql: str, params=None):
        es_explain, body = split_explain(sql)  # EXPLAIN devuelve el SQL compilado sin ejecutar
        if not self._translator.is_semantic(body):
            return super().execute_sql(sql, params)  # catálogo (DuckDB)
        if not params and _es_sonda(body):
            return super().execute_sql(sql, params)  # sin filas: la tabla virtual vacía responde con su forma
        if params:
            raise TranslationError("La capa semántica no admite parámetros: envíe la consulta con literales")

        if self.motor_error:
            raise TranslationError(self.motor_error)
        sq = self._translator.translate(body, dialect=SQLGLOT_DIALECT[self.motor])
        sql_motor = self._engine.explain(sq, self.motor)
        logger.info(
            "Consulta compilada · motor=%s metrics=%s group_by=%s where=%s time=%s..%s order_by=%s limit=%s\n"
            "---------- SQL %s ----------\n%s\n----------------------------------",
            self.motor, sq.metrics, sq.group_by, sq.where, sq.start, sq.end, sq.order_by, sq.limit,
            self.motor, sql_motor,
        )
        ejecutor = self._ejecutores.get(self.motor)
        if es_explain or ejecutor is None:
            return DuckDBQueryResult(pa.table({"sql": [sql_motor]}).to_reader(), status="SELECT")

        try:
            resultado = ejecutor.ejecutar(sql_motor)
        except ExecutionError as e:
            logger.error("Falló la ejecución en %s: %s", self.motor, e)
            raise
        tabla = ordenar_columnas(resultado.tabla, sq.output)
        logger.info("Ejecutada en %s · query_id=%s · %d filas · %.1f s · %s escaneados%s", self.motor,
                    resultado.query_id or "-", tabla.num_rows, resultado.segundos,
                    formato_bytes(resultado.bytes_escaneados),
                    " · TRUNCADO por --max-filas" if resultado.truncado else "")
        return DuckDBQueryResult(tabla.to_reader(), status="SELECT")


class SemanticConnection(DuckDBConnection):
    def __init__(self, db: duckdb.DuckDBPyConnection, engine: SemanticEngine, translator: Translator,
                 ejecutores: dict[str, Ejecutor] | None = None) -> None:
        super().__init__(db)
        self._engine = engine
        self._translator = translator
        self._ejecutores = ejecutores or {}

    def parameters(self) -> dict[str, str]:
        return {"server_version": "14.0", "client_encoding": "UTF8", "DateStyle": "ISO"}

    def new_session(self) -> SemanticSession:
        cursor = self.db.cursor()
        cursor.execute(f"SET search_path='{self._translator.catalog.schema},main'")
        return SemanticSession(cursor, self._engine, self._translator, self._ejecutores)


_PRIVILEGIOS = {"has_schema_privilege", "has_table_privilege", "has_any_column_privilege", "has_column_privilege",
                "has_database_privilege", "has_function_privilege", "has_sequence_privilege"}
# Funciones de PostgreSQL que DuckDB no tiene (o no acepta con esos argumentos): se reemplazan por macros gw_<nombre>
# que devuelven NULL. Las usa DBeaver al listar tablas, columnas e índices; el gateway no tiene particiones,
# expresiones por defecto, tablespaces ni estadísticas.
_FUNCIONES_PROPIAS = {
    "pg_get_expr": "(a, b) AS NULL::VARCHAR, (a, b, c) AS NULL::VARCHAR",
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
                   "pg_stat_all_tables": "pg_stat_all_tables", "pg_settings": "gw_pg_settings", "pg_type": "gw_pg_type", "pg_attribute": "gw_pg_attribute"}
_REG_TIPOS = {"regnamespace": ("pg_namespace", "oid", "nspname"), "regclass": ("pg_class", "oid", "relname"),
              "regtype": ("pg_type", "oid", "typname")}


def _compat_catalogo(nodo: exp.Expression) -> exp.Expression:
    """Adapta lo que los drivers (Metabase/JDBC) usan en catálogo y DuckDB no tiene:
    * `x::regnamespace` / `::regclass` / `::regtype` → subconsulta que resuelve el nombre
    * `'nombre'::regclass` → subconsulta que resuelve el oid
    * `has_*_privilege(...)` → TRUE (el gateway no aplica permisos de catálogo)
    * `pg_get_expr`, `pg_get_partkeydef`, `pg_tablespace_location` → macros `gw_*` que devuelven NULL
    * `pg_trigger`, `pg_roles`, `pg_extension`, `pg_matviews`, `pg_stat_*`, `pg_type`, `pg_attribute` → vistas propias
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


class SemanticRewriter(rewrite.Rewriter):
    """Adapta al dialecto DuckDB las consultas de catálogo; las semánticas pasan intactas al traductor."""

    def __init__(self, translator: Translator) -> None:
        super().__init__(bv_dialects.BVPostgres(), bv_dialects.BVDuckDB())
        self._translator = translator

    def rewrite(self, sql: str) -> str:
        if self._translator.is_semantic(sql):
            return sql
        if _consulta_vacia(sql):
            return "SELECT 1 WHERE FALSE"  # ping de los drivers (';'): respuesta vacía sin error
        if sql.strip().rstrip(";").lower() in ("select version()", "select pg_catalog.version()"):
            return "SELECT 'PostgreSQL 14.0 (gateway semántico sobre MetricFlow)' AS version"
        if re.match(r"\s*(begin|start\s+transaction)\b", sql, flags=re.I):
            return "BEGIN"  # BEGIN READ ONLY / ISOLATION LEVEL … no existen en DuckDB
        if _REINICIO_SESION.match(sql):  # DISCARD ALL, RESET ALL… (Npgsql al reutilizar conexiones): sin efecto
            return "SET enable_progress_bar = false"
        npgsql = _consulta_npgsql(sql)
        if npgsql is not None:
            return npgsql
        if "_pg_expandarray" in sql:  # getPrimaryKeys de JDBC: la capa semántica no tiene claves primarias
            columnas = ("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME")
            n = max([int(i) for i in re.findall(r"\$(\d+)", sql)] or [0])  # la consulta original recibe n parámetros
            usa_parametros = "".join(f" AND ${i}::VARCHAR IS NULL" for i in range(1, n + 1))
            return "SELECT " + ", ".join(f'NULL::VARCHAR AS "{c}"' for c in columnas) + " WHERE false" + usa_parametros
        sql = _literales_arreglo(sql)
        m = re.fullmatch(r"\s*show\s+(time\s*zone|timezone)\s*;?\s*", sql, flags=re.I)
        if m:
            return 'SELECT \'UTC\' AS "TimeZone"'
        try:
            arbol = sqlglot.parse_one(sql, read="postgres")
            if arbol is not None and not isinstance(arbol, exp.Command):
                sql = arbol.transform(_compat_catalogo).sql(dialect="postgres")
        except sqlglot.errors.SqlglotError as error:
            logger.warning("Consulta de catálogo no analizable por sqlglot (%s); se usa el respaldo textual:\n%s",
                           str(error).splitlines()[0], sql)
            sql = _compat_catalogo_texto(sql)
        return super().rewrite(sql)


# ---------------------------------------------------------------------------------------------- conexión
class MotorHandler(postgres.BuenaVistaHandler):
    """Lee los parámetros de arranque del cliente y fija el motor de la sesión según `dbname`.

    buenavista crea la sesión antes de conocer los parámetros; aquí se completa justo después del arranque.
    """

    def handle_query(self, ctx, payload):
        # buenavista falla con una consulta vacía (';') que los drivers JDBC envían como ping → EmptyQueryResponse
        if payload is not None and _consulta_vacia(payload.decode("utf-8", "replace").rstrip("\x00")):
            self.wfile.write(struct.pack("!ci", postgres.ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return
        super().handle_query(ctx, payload)

    # Protocolo extendido: después de un ErrorResponse, PostgreSQL descarta los mensajes hasta el Sync. buenavista
    # seguía respondiendo ParseComplete/BindComplete y Npgsql (Power BI) cortaba la conexión con
    # "Received unexpected backend message ParseComplete".
    def handle_parse(self, ctx, payload):
        if not ctx.has_error:
            super().handle_parse(ctx, payload)

    def handle_bind(self, ctx, payload):
        if not ctx.has_error:
            super().handle_bind(ctx, payload)

    def handle_describe(self, ctx, payload):
        if not ctx.has_error:
            super().handle_describe(ctx, payload)

    def handle_close(self, ctx, payload):
        if not ctx.has_error:
            super().handle_close(ctx, payload)

    def handle_startup(self, conn):
        ctx = super().handle_startup(conn)
        if ctx is not None and isinstance(ctx.session, SemanticSession):
            params = ctx.params
            motor, error = motor_from_database(params.get("database"), params.get("user"), ctx.session._engine.motor)
            ctx.session.motor, ctx.session.motor_error = motor, error
            logger.info("Conexión · usuario=%s dbname=%s → motor=%s%s", params.get("user"), params.get("database"),
                        motor, f" (ERROR: {error})" if error else "")
        return ctx


class MotorServer(postgres.BuenaVistaServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.RequestHandlerClass = MotorHandler


# ---------------------------------------------------------------------------------------------- arranque
def build_server(host: str, port: int, user: str, password: str, manifest: str, motor: str,
                 ejecutores: dict[str, Ejecutor] | None = None) -> MotorServer:
    engine = SemanticEngine(manifest, motor)
    postgres.dateutil = type("_Du", (), {"parser": _ParserSeguro})  # ver _ParserSeguro
    logging.getLogger("sqlglot").setLevel(logging.ERROR)  # avisos de SHOW/SET analizados como Command
    for noisy in ("buenavista", "metricflow", "metricflow_semantics", "dbt_metricflow"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    catalog = engine.catalog(SCHEMA)
    db = duckdb.connect()  # catálogo en memoria, independiente del warehouse
    install_catalog(db, catalog)
    translator = Translator(catalog, warehouse_dialect=SQLGLOT_DIALECT[motor])
    if host not in ("127.0.0.1", "localhost"):
        os.environ["BUENAVISTA_HOST"] = host  # buenavista solo acepta conexiones remotas con esta variable
    server = MotorServer(
        (host, port), SemanticConnection(db, engine, translator, ejecutores), rewriter=SemanticRewriter(translator),
        auth={user: password},
    )
    logger.info(
        "Gateway listo en %s:%d · %s.{%s} · motor por defecto=%s (dbname=<motor> lo cambia por conexión) · %s · %d métricas · %d dimensiones",
        host, port, SCHEMA, ", ".join(catalog.tables), motor,
        f"ejecuta en: {', '.join(sorted(ejecutores))} (el resto solo compila)" if ejecutores else "solo compila y devuelve el SQL",
        len(catalog.metrics), len(catalog.dimensions),
    )
    return server


def start_in_thread(host: str, port: int, user: str, password: str, manifest: str, motor: str,
                    ejecutores: dict[str, Ejecutor] | None = None) -> MotorServer:
    server = build_server(host, port, user, password, manifest, motor, ejecutores)
    threading.Thread(target=server.serve_forever, name="semantic-gateway", daemon=True).start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Gateway semántico de dbt_glue_lab por protocolo PostgreSQL")
    parser.add_argument("--manifest", default=os.environ.get("SEMANTIC_GW_MANIFEST", "semantic_manifests/dbt_glue_lab/semantic_manifest.json"))
    parser.add_argument("--motor", default=os.environ.get("SEMANTIC_GW_MOTOR", "athena"), choices=sorted(MOTORES),
                        help="motor por defecto; cada conexión puede pedir otro con dbname=<motor>")
    parser.add_argument("--host", default=os.environ.get("SEMANTIC_GW_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SEMANTIC_GW_PORT", "5433")))
    parser.add_argument("--user", default=os.environ.get("SEMANTIC_GW_USER", "bi"))
    parser.add_argument("--password", default=os.environ.get("SEMANTIC_GW_PASSWORD", "bi"))
    parser.add_argument("--log-level", default=os.environ.get("SEMANTIC_GW_LOG_LEVEL", "INFO"))
    ejecucion = parser.add_argument_group("ejecución en Athena (sin --ejecutar el gateway solo devuelve el SQL)")
    ejecucion.add_argument("--ejecutar", action="store_true", default=os.environ.get("SEMANTIC_GW_EJECUTAR", "") == "1",
                           help="ejecuta en Athena las consultas de conexiones con dbname=athena y devuelve las filas")
    ejecucion.add_argument("--athena-workgroup", default=os.environ.get("ATHENA_WORKGROUP", "primary"))
    ejecucion.add_argument("--athena-output", default=os.environ.get("ATHENA_OUTPUT"),
                           help="s3://… donde Athena deja los resultados (si el workgroup no impone una)")
    ejecucion.add_argument("--athena-catalog", default=os.environ.get("ATHENA_CATALOG", "awsdatacatalog"))
    ejecucion.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    ejecucion.add_argument("--aws-profile", default=os.environ.get("AWS_PROFILE"))
    ejecucion.add_argument("--timeout", type=float, default=float(os.environ.get("SEMANTIC_GW_TIMEOUT", "100")),
                           help="segundos; al vencer se cancela la consulta en Athena (0 = sin límite)")
    ejecucion.add_argument("--max-filas", type=int, default=int(os.environ.get("SEMANTIC_GW_MAX_FILAS", "0")),
                           help="corta el resultado en N filas (0 = sin límite, por defecto)")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s · %(message)s")
    ejecutores: dict[str, Ejecutor] = {}
    if args.ejecutar:
        ejecutores["athena"] = EjecutorAthena(ConfigAthena(
            region=args.region, workgroup=args.athena_workgroup, output=args.athena_output, profile=args.aws_profile,
            catalog=args.athena_catalog, timeout=args.timeout, max_filas=args.max_filas))
        logger.info("Ejecución en Athena · workgroup=%s · resultados=%s · región=%s · perfil=%s · timeout=%s · max_filas=%s",
                    args.athena_workgroup, args.athena_output or "(la del workgroup)", args.region or "(por defecto)",
                    args.aws_profile or "(por defecto)", f"{args.timeout:.0f} s" if args.timeout else "sin límite",
                    args.max_filas or "sin límite")
    server = build_server(args.host, args.port, args.user, args.password, args.manifest, args.motor, ejecutores)
    try:
        server.serve_forever()
    finally:
        server.shutdown()
