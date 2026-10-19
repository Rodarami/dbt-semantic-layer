"""Prueba el catálogo emulado de riffq (catalog_emulation=True) SIN DuckDB.

No es parte de las pruebas del gateway: sirve para decidir si se puede prescindir de DuckDB. Levanta un servidor
riffq mínimo que registra las tablas semánticas del manifest con set_lazy_catalog y lanza las consultas de catálogo
que envían DBeaver, Power BI (Npgsql) y Metabase. Resultado documentado en docs/comparativa_protocolo.md, sección 5.

Uso (desde la raíz MetricFlow/):
    python -m tests.probar_catalogo_riffq
    python -m tests.probar_catalogo_riffq --comparar     # y las mismas consultas contra el gateway actual
    python -m tests.probar_catalogo_riffq --manifest semantic_manifests/it-people/semantic_manifest.json
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import socket
import threading
import time

os.environ.setdefault("RUST_LOG", "off")

import psycopg  # noqa: E402
import pyarrow as pa  # noqa: E402
import riffq  # noqa: E402
from psycopg.types.string import StrDumper  # noqa: E402

from semantic_gateway.engine import SemanticEngine  # noqa: E402

PUERTO = 15999
_OID = {"VARCHAR": 25, "TEXT": 25, "DOUBLE": 701, "BIGINT": 20, "INTEGER": 23, "BOOLEAN": 16, "DATE": 1082,
        "TIMESTAMP": 1114}

# Consultas que envía DBeaver al conectar y navegar (las mismas que valida el catálogo en DuckDB)
Q = [
 ("current_schema", "SELECT current_schema(),session_user", None),
 ("settings", "select * from pg_catalog.pg_settings where name='standard_conforming_strings'", None),
 ("database", "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname=%s", ("athena",)),
 ("namespaces", "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname", None),
 ("ns_oid", "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='semantic'", None),
 ("tables", "SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \nFROM pg_catalog.pg_class c\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\nWHERE c.relnamespace=%s AND c.relkind not in ('i','I','c')", "NS"),
 ("tbl_oid", "SELECT oid FROM pg_catalog.pg_class WHERE relname='events'", None),
 ("columns", "SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid\nFROM pg_catalog.pg_attribute a\nINNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\nLEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\nLEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid\nWHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.oid=%s\nORDER BY a.attnum", "TBL"),
 ("types", "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description\nFROM pg_catalog.pg_type t\nLEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem\nLEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid\nWHERE t.typname IS NOT NULL\nAND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')", None),
 ("indexes", "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description,pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr,pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr,pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size,pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans\nFROM pg_catalog.pg_index i\nINNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid\nINNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid\nWHERE  i.indrelid=%s ORDER BY tabrelname, c.relname", "TBL"),
 ("constraints", "SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, case when c.contype='c' then \"substring\"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy\nFROM pg_catalog.pg_constraint c\nINNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid\nLEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass\nWHERE c.conrelid=%s\nORDER BY c.oid", "TBL"),
 ("triggers", "SELECT t.oid,t.*,c.relname,c.relnamespace FROM pg_catalog.pg_trigger t, pg_catalog.pg_class c WHERE t.tgrelid=c.oid AND c.oid=%s AND NOT t.tgisinternal", "TBL"),
 ("procs", "SELECT p.oid,p.*,pg_catalog.pg_get_expr(p.proargdefaults, 0)as arg_defaults,d.description\nFROM pg_catalog.pg_proc p\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=p.oid\nWHERE p.pronamespace=%s\nORDER BY p.proname", "NS"),
 ("roles", "select * from pg_catalog.pg_roles", None),
 ("extensions", "SELECT e.oid,e.*,n.nspname FROM pg_catalog.pg_extension e, pg_catalog.pg_namespace n WHERE n.oid=e.extnamespace ORDER BY e.extname", None),
 ("tablespaces", "SELECT t.oid,t.*,pg_tablespace_location(t.oid) loc FROM pg_catalog.pg_tablespace t", None),
 ("view_data", "SELECT event__platform, event_count FROM semantic.events", None),
 ("read_data_preview", "SELECT * FROM semantic.metrics LIMIT 200", None),
 ("row_count", "SELECT count(*) FROM semantic.metrics", None),
 ("table_stats", "SELECT pg_catalog.pg_total_relation_size(c.oid), pg_catalog.pg_relation_size(c.oid), pg_catalog.pg_table_size(c.oid), pg_catalog.pg_indexes_size(c.oid) FROM pg_catalog.pg_class c WHERE c.oid=%s", "TBL"),
 ("sequences", "SELECT c.oid,c.relname FROM pg_catalog.pg_class c WHERE c.relkind='S' AND c.relnamespace=%s", "NS"),
]

TIPOS_NPGSQL = """SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
CASE WHEN pg_proc.proname='array_recv' THEN a.typelem WHEN a.typtype='r' THEN rngsubtype ELSE 0 END AS elemoid,
CASE WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3 WHEN a.typtype='r' THEN 2 WHEN a.typtype='d' THEN 1 ELSE 0 END AS ord
FROM pg_type AS a
JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
JOIN pg_proc ON pg_proc.oid = a.typreceive
LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
WHERE a.typtype IN ('b', 'r', 'e', 'd') OR (a.typtype = 'c' AND cls.relkind='c')
 OR (pg_proc.proname='array_recv' AND (b.typtype IN ('b', 'r', 'e', 'd') OR (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR (b.typtype = 'c' AND elemcls.relkind='c')))
 OR (a.typtype = 'p' AND a.typname IN ('record', 'void'))
ORDER BY ord"""

OTRAS = [
    ("Power BI · tipos Npgsql", TIPOS_NPGSQL),
    ("Power BI · navegador", "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.tables where "
     "TABLE_SCHEMA not in ('information_schema', 'pg_catalog') order by TABLE_SCHEMA, TABLE_NAME"),
    ("Power BI · columnas", "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE from INFORMATION_SCHEMA.columns "
     "where TABLE_SCHEMA = 'semantic' order by TABLE_NAME, ORDINAL_POSITION"),
    ("Metabase · esquemas", "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_' "
     "AND nspname <> 'information_schema'"),
    ("Metabase · tablas", "SELECT n.nspname AS schema, c.relname AS name, d.description, stat.n_live_tup "
     "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
     "LEFT JOIN pg_catalog.pg_description d ON c.oid = d.objoid AND d.objsubid = 0 "
     "LEFT JOIN pg_stat_user_tables stat ON n.nspname = stat.schemaname AND c.relname = stat.relname "
     "WHERE c.relkind IN ('r','p','v','f','m') AND n.nspname !~ '^information_schema' AND n.nspname !~ '^pg_'"),
    ("Metabase · columnas", "SELECT c.relnamespace::regnamespace::text AS table_schema, c.relname, a.attname, "
     "format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c "
     "ON a.attrelid = c.oid WHERE c.relkind IN ('r','v') AND a.attnum > 0 AND NOT a.attisdropped"),
    ("Metabase · privilegios", "SELECT n.nspname, c.relname, has_table_privilege(current_user, c.oid, 'SELECT') "
     "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v')"),
]


def _fuente(catalogo):
    """Catálogo perezoso para riffq: una base 'athena', el esquema 'semantic' y una tabla por modelo."""
    tablas = list(catalogo.tables)

    class Fuente:
        def databases(self, cb):
            cb([{"oid": 90001, "name": "athena"}])

        def schemas(self, db, cb):
            cb([{"oid": 90002, "name": "semantic"}] if db == "athena" else [])

        def relations(self, db, esquema, cb):
            cb([{"oid": 91000 + 2 * i, "reltype_oid": 91001 + 2 * i, "name": n, "kind": "table"}
                for i, n in enumerate(tablas)] if esquema == "semantic" else [])

        def columns(self, db, esquema, tabla, cb):
            t = catalogo.tables.get(tabla)
            if esquema != "semantic" or t is None:
                return cb([])
            cb([{"name": m, "type_oid": 701, "nullable": True} for m in t.metrics]
               + [{"name": d, "type_oid": _OID.get(catalogo.dimensions[d].data_type.upper(), 25), "nullable": True}
                  for d in t.dimensions])

    return Fuente()


class _Conexion(riffq.BaseConnection):
    """Todo lo que no resuelve el catálogo de riffq llega aquí: se contesta con una fila fija."""

    def handle_auth(self, user, password, host, database=None, callback=None):
        callback(True)

    def handle_query(self, sql, callback, **kwargs):
        if re.match(r"\s*(set|begin|commit|discard|reset)\b", sql, re.I):
            return callback("SET", is_tag=True)
        self.send_reader(pa.table({"python": pa.array([1], pa.int64())}).to_reader(), callback)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--manifest", default="semantic_manifests/dbt_glue_lab/semantic_manifest.json")
    p.add_argument("--comparar", action="store_true",
                   help="corre además las mismas consultas contra el gateway actual (catálogo en DuckDB)")
    args = p.parse_args()
    logging.disable(logging.CRITICAL)

    catalogo = SemanticEngine(args.manifest, "athena").catalog("semantic")
    servidor = riffq.RiffqServer(f"127.0.0.1:{PUERTO}", connection_cls=_Conexion)
    servidor.set_lazy_catalog(_fuente(catalogo))
    threading.Thread(target=lambda: servidor.start(catalog_emulation=True), daemon=True).start()
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", PUERTO), 0.2).close()
            break
        except OSError:
            time.sleep(0.1)

    psycopg.adapters.register_dumper(str, StrDumper)
    primera = next(iter(catalogo.tables))
    sin_duckdb = _correr(PUERTO, primera)
    print(f"\n{sin_duckdb[0]}/{sin_duckdb[1]} consultas de catálogo resueltas por el catálogo de riffq (sin DuckDB)")
    print("semantic.metrics y semantic.dimensions no existen en ese catálogo: habría que responderlas en Python.")
    if args.comparar:
        from semantic_gateway.server import start_in_thread
        start_in_thread("127.0.0.1", PUERTO + 1, "bi", "bi", args.manifest, "athena")
        print("\n== Mismas consultas contra el gateway actual (catálogo en DuckDB) ==")
        con_duckdb = _correr(PUERTO + 1, primera)
        print(f"\n{con_duckdb[0]}/{con_duckdb[1]} con DuckDB  ·  {sin_duckdb[0]}/{sin_duckdb[1]} sin DuckDB")
    os._exit(0)


_NO_CATALOGO = {"view_data", "read_data_preview", "row_count"}  # leen tablas semánticas, no el catálogo


def _correr(puerto: int, primera: str) -> tuple[int, int]:
    c = psycopg.connect(f"host=127.0.0.1 port={puerto} dbname=athena user=bi password=bi", autocommit=True,
                        prepare_threshold=0)
    ids, ok, total = {}, 0, 0

    def probar(nombre, sql, params=None):
        nonlocal ok, total
        total += 1
        try:
            cur = c.execute(sql, params)
            filas = cur.fetchall() if cur.description else []
            ok += 1
            print(f"OK   {nombre:36} {len(filas)} filas")
            return filas
        except Exception as e:  # noqa: BLE001
            print(f"ERR  {nombre:36} {str(e).splitlines()[0][:110]}")
            return None

    consultas = [q for q in Q if q[0] not in _NO_CATALOGO]
    print(f"== DBeaver ({len(consultas)} consultas de catálogo) ==")
    for nombre, sql, p_ in consultas:
        sql = sql.replace("relname='events'", f"relname='{primera}'")
        params = (ids.get("ns"),) if p_ == "NS" else (ids.get("tbl"),) if p_ == "TBL" else p_
        filas = probar("DBeaver · " + nombre, sql, params)
        if filas and nombre == "ns_oid":
            ids["ns"] = filas[0][0]
        if filas and nombre == "tbl_oid":
            ids["tbl"] = filas[0][0]
    print("== Power BI y Metabase ==")
    for nombre, sql in OTRAS:
        probar(nombre, sql)
    c.close()
    return ok, total


if __name__ == "__main__":
    main()
