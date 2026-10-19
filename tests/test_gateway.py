"""Prueba de integración por protocolo Postgres: un solo servidor, el motor se elige en la conexión (dbname).

Uso (desde la raíz del repo):  python -m tests.test_gateway [ruta/semantic_manifest.json]
Servidor anterior (buenavista):  SEMANTIC_GW_SERVER=old python -m tests.test_gateway
"""

from __future__ import annotations

import importlib
import os
import re
import socket
import sys

import psycopg
from psycopg.types.string import StrDumper

# Parámetros de texto con tipo, como los envían JDBC (DBeaver, Metabase) y Npgsql (Power BI). Por defecto psycopg los
# manda sin tipo y riffq los entrega como NULL.
psycopg.adapters.register_dumper(str, StrDumper)

_SERVIDOR = "server_old" if os.environ.get("SEMANTIC_GW_SERVER") == "old" else "server"
start_in_thread = importlib.import_module(f"semantic_gateway.{_SERVIDOR}").start_in_thread

QUERY = ("SELECT event__platform, MEASURE(item_views) AS vistas FROM semantic.events "
         "WHERE event__platform IN ('ios','android') AND metric_time >= DATE '2024-01-01' GROUP BY 1 ORDER BY 1")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _connect(port: int, dbname: str):
    return psycopg.connect(f"host=127.0.0.1 port={port} user=bi password=bi dbname={dbname}", autocommit=True)


def _norm(sql: str) -> str:
    """MetricFlow numera los alias internos (subq_3, subq_4…) con un contador global: se normalizan para comparar."""
    return re.sub(r"_\d+\b", "_N", sql)


def _sql(port: int, dbname: str) -> str:
    with _connect(port, dbname) as c:
        return _norm(c.execute(QUERY).fetchone()[0])


def main(manifest: str) -> None:
    port = _free_port()
    server = start_in_thread("127.0.0.1", port, "bi", "bi", manifest, "athena")  # motor por defecto: athena
    try:
        athena, snowflake = _sql(port, "athena"), _sql(port, "snowflake")
        assert "timestamp '2024-01-01'" in athena, athena             # dialecto Athena
        assert "BETWEEN '2024-01-01'" in snowflake and "timestamp" not in snowflake.split("WHERE")[1], snowflake
        assert athena != snowflake
        assert _sql(port, "semantic") == athena                       # dbname=semantic → motor por defecto
        assert _sql(port, "BI") == athena                             # dbname = usuario (default de psql) → por defecto
        print("OK · dbname=athena / snowflake / semantic")

        # dos conexiones simultáneas con motores distintos no se mezclan
        with _connect(port, "snowflake") as c1, _connect(port, "athena") as c2:
            a, b = _norm(c1.execute(QUERY).fetchone()[0]), _norm(c2.execute(QUERY).fetchone()[0])
            assert a == snowflake and b == athena
        print("OK · conexiones simultáneas independientes")

        # el motor también aplica a EXPLAIN y a los filtros del WHERE
        with _connect(port, "snowflake") as c:
            out = _norm(c.execute("EXPLAIN " + QUERY).fetchone()[0])
            assert out == snowflake, out

        # dbname desconocido: conecta y los metadatos funcionan, pero la consulta semántica falla con un mensaje claro
        with _connect(port, "oracle") as c:
            assert c.execute("SELECT count(*) FROM semantic.metrics").fetchone()[0] >= 10
            try:
                c.execute(QUERY)
            except psycopg.Error as e:
                assert "no existe" in str(e) and "snowflake" in str(e), e
            else:
                raise AssertionError("dbname desconocido debía fallar")
        print("OK · dbname desconocido")

        # compatibilidad con drivers JDBC (Metabase): ping ';', SHOW, parámetros de texto y protocolo extendido
        for prepare in (None, 0):
            with psycopg.connect(f"host=127.0.0.1 port={port} user=bi password=bi dbname=athena",
                                 autocommit=True, prepare_threshold=prepare) as c:
                c.execute(";")
                c.execute("")
                assert c.execute("SHOW TRANSACTION ISOLATION LEVEL").fetchone()[0] == "read committed"
                assert c.execute("SELECT %s::int", (5,)).fetchone()[0] == 5   # parámetro tipado (JDBC los tipa)
                assert c.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = %s",
                                 ("semantic",)).fetchone()[0] == 5
                assert c.execute("SELECT 1").fetchone()[0] == 1  # la conexión sigue viva tras todo lo anterior
        print("OK · compatibilidad JDBC (ping, SHOW, parámetros)")

        # sincronización de Metabase: lista de tablas (regnamespace + has_*_privilege) y columnas con tipos PG
        with _connect(port, "athena") as c:
            tablas = c.execute(
                "SELECT c.relnamespace::regnamespace::text AS schema, c.relname AS name FROM pg_catalog.pg_class AS c "
                "WHERE c.relnamespace::regnamespace::text = 'semantic' AND c.relkind IN ('r','p','v','f','m') "
                "AND (pg_catalog.has_schema_privilege(c.relnamespace, 'USAGE') AND "
                "(pg_catalog.has_table_privilege(c.oid, 'SELECT') OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT')))"
            ).fetchall()
            assert {t[1] for t in tablas} == {"events", "items", "metrics", "dimensions", "metric_dimensions"}, tablas
            cols = c.execute(
                "SELECT a.attname, t.typname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class k ON a.attrelid = k.oid "
                "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid WHERE k.relname = 'events' AND a.attnum > 0 "
                "AND NOT a.attisdropped").fetchall()
            assert len(cols) == 18 and dict(cols)["event_count"] == "float8" and dict(cols)["metric_time"] == "date", cols
            assert c.execute("show timezone").fetchone()[0] == "UTC"
            assert c.execute("select schemaname FROM pg_catalog.pg_matviews").fetchall() == []
            assert c.execute("SELECT NULL AS a, 1 AS b").fetchone() == (None, 1)
            filas = c.execute(
                "SELECT n.nspname, c.relname, stat.n_live_tup FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c "
                "ON c.relnamespace = n.oid LEFT JOIN pg_stat_user_tables stat ON (n.nspname = stat.schemaname "
                "AND c.relname = stat.relname) WHERE n.nspname = 'semantic'").fetchall()
            assert len(filas) == 5, filas
            # consulta que sqlglot no puede analizar (palabras reservadas sin comillas): respaldo textual
            n = c.execute("select t.schemaname as schema, t.objectname as table, "
                          "pg_catalog.has_table_privilege(current_user, t.objectname, 'SELECT') as select "
                          "from (select schemaname, matviewname as objectname from pg_catalog.pg_matviews "
                          "union all select schemaname, tablename from pg_catalog.pg_tables) t "
                          "where pg_catalog.has_schema_privilege(current_user, t.schemaname, 'USAGE')").fetchall()
            assert len(n) >= 5, n
            # sondas de Metabase sobre la tabla semántica: no devuelven filas y no pasan por MetricFlow
            assert c.execute('SELECT TRUE AS "_" FROM "semantic"."events" WHERE 1 <> 1 LIMIT 0').fetchall() == []
            assert c.execute("SELECT * FROM semantic.items LIMIT 0").description is not None
            # transacción de solo lectura y claves primarias de JDBC
            c.execute("BEGIN READ ONLY")
            c.execute("COMMIT")
            pk = c.execute("SELECT ct.relname AS TABLE_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ "
                           "FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ct ON i.indrelid = ct.oid")
            assert pk.fetchall() == []
        # parámetros sin tipo con protocolo extendido (como JDBC): no se interpretan como fecha
        with psycopg.connect(f"host=127.0.0.1 port={port} user=bi password=bi dbname=athena", autocommit=True,
                             prepare_threshold=0) as c:
            r = c.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = %s "
                          "AND table_name !~ %s", ("semantic", "^pg_")).fetchone()[0]
            assert r == 5, r
            # parámetros sin tipo en contextos numéricos (LIMIT, oid) y claves primarias de JDBC con 2 parámetros
            assert len(c.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE %s LIMIT %s",
                                 ("%", 2)).fetchall()) == 2
            assert c.execute("SELECT ct.relname, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ "
                             "FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ct ON i.indrelid = ct.oid "
                             "WHERE ct.relname = %s AND ct.relnamespace = %s", ("metrics", "semantic")).fetchall() == []
        print("OK · sincronización de metadatos tipo Metabase")

        # una tabla por modelo semántico: cada una publica solo lo que se puede combinar
        with _connect(port, "athena") as c:
            def cols(t):
                return {r[0] for r in c.execute("SELECT column_name FROM information_schema.columns "
                                                "WHERE table_schema = 'semantic' AND table_name = %s", (t,)).fetchall()}
            ev, it = cols("events"), cols("items")
            assert {"event_count", "event__platform", "metric_time__month"} <= ev and not {"total_item_price", "item__category"} & ev
            assert {"total_item_price", "item__category", "metric_time__month"} <= it and not {"event_count", "event__platform"} & it
            assert dict(c.execute("SELECT metric, table_name FROM semantic.metrics").fetchall())["item_views_per_user"] == "events"
            assert "COUNT(DISTINCT event_id)" in c.execute("SELECT event__platform, event_count FROM semantic.events").fetchone()[0]
            assert "silver_item" in c.execute("SELECT item__category, total_item_price FROM semantic.items").fetchone()[0]
            for sql, msg in [("SELECT item__category, event_count FROM semantic.events", "está en: items"),
                             ("SELECT event__platform, total_item_price FROM semantic.items", "está en: events")]:
                try:
                    c.execute(sql)
                except psycopg.Error as e:
                    assert msg in str(e), e
                else:
                    raise AssertionError(f"debía fallar: {sql}")
        print("OK · una tabla virtual por modelo semántico")

        # navegación de DBeaver: tablas, columnas, índices, triggers, roles, extensiones, bases y tamaños
        with psycopg.connect(f"host=127.0.0.1 port={port} user=bi password=bi dbname=athena", autocommit=True,
                             prepare_threshold=0) as c:
            ns = c.execute("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'semantic'").fetchone()[0]
            tbl = c.execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'events'").fetchone()[0]
            assert len(c.execute(
                "SELECT c.oid, c.*, pg_catalog.pg_get_expr(c.relpartbound, c.oid) AS partition_expr, "
                "pg_catalog.pg_get_partkeydef(c.oid) AS partition_key FROM pg_catalog.pg_class c "
                "WHERE c.relnamespace = %s AND c.relkind NOT IN ('i','I','c')", (ns,)).fetchall()) == 5
            assert len(c.execute(
                "SELECT c.relname, a.*, pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) AS def_value "
                "FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                "LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum "
                "WHERE NOT a.attisdropped AND c.oid = %s ORDER BY a.attnum", (tbl,)).fetchall()) == 18
            assert c.execute("SELECT pg_catalog.pg_relation_size(i.indexrelid), pg_catalog.pg_stat_get_numscans(i.indexrelid) "
                             "FROM pg_catalog.pg_index i WHERE i.indrelid = %s", (tbl,)).fetchall() == []
            for vista in ("pg_trigger", "pg_roles", "pg_extension"):
                assert c.execute(f"SELECT * FROM pg_catalog.{vista}").fetchall() == []
            assert c.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", ("athena",)).fetchall() == [("athena",)]
            assert c.execute("SELECT pg_tablespace_location(t.oid) FROM pg_catalog.pg_tablespace t").fetchall()
            # arranque de DBeaver: search_path (reset_val) y palabras reservadas (pg_get_keywords + arreglo '{...}')
            assert c.execute("SELECT reset_val FROM pg_settings WHERE name = 'search_path'").fetchall() == [("semantic, main",)]
            palabras = c.execute("SELECT LISTAGG(word, ',') FROM pg_catalog.PG_GET_KEYWORDS() "
                                 "WHERE word <> ALL(CAST('{abs,absolute}' AS TEXT[]))").fetchone()[0]
            assert "select" in palabras.split(",") and "abs" not in palabras.split(",")
        print("OK · navegación tipo DBeaver")
    finally:
        server.shutdown()
    print("OK · gateway Postgres wire: motor por conexión")
    probar_ejecucion(manifest)
    probar_npgsql(manifest)
    probar_seguridad(manifest)
    if _SERVIDOR == "server":  # TLS solo existe en el servidor actual (riffq)
        probar_tls_obligatorio(manifest)


def probar_npgsql(manifest: str) -> None:
    """Power BI (Npgsql): catálogo de tipos al conectar y protocolo extendido con error a mitad de lote."""
    import hashlib
    import socket
    import struct

    tipos = ("SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype, CASE WHEN pg_proc.proname='array_recv' "
             "THEN 'a' ELSE a.typtype END AS type, CASE WHEN pg_proc.proname='array_recv' THEN a.typelem ELSE 0 END "
             "AS elemoid, 0 AS ord FROM pg_type AS a JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace) "
             "JOIN pg_proc ON pg_proc.oid = a.typreceive LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)")
    compuestos = ("SELECT typ.oid, att.attname, att.atttypid FROM pg_type AS typ JOIN pg_attribute AS att "
                  "ON (att.attrelid = typ.typrelid) WHERE typ.typtype = 'c' AND attnum > 0")
    enums = "SELECT pg_type.oid, enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid ORDER BY oid"

    port = _free_port()
    server = start_in_thread("127.0.0.1", port, "bi", "bi", manifest, "athena")
    try:
        sock = socket.create_connection(("127.0.0.1", port))

        def leer():
            tipo = sock.recv(1)
            largo = struct.unpack("!i", sock.recv(4))[0]
            datos = b""
            while len(datos) < largo - 4:
                datos += sock.recv(largo - 4 - len(datos))
            return tipo, datos

        def mensaje(tipo, cuerpo):
            return tipo + struct.pack("!i", len(cuerpo) + 4) + cuerpo

        cuerpo = struct.pack("!i", 196608) + b"user\0bi\0database\0athena\0\0"
        sock.sendall(struct.pack("!i", len(cuerpo) + 4) + cuerpo)
        while True:  # autenticación: texto plano (riffq) o MD5 (buenavista)
            tipo, datos = leer()
            if tipo == b"R" and datos[:4] == struct.pack("!i", 3):
                sock.sendall(mensaje(b"p", b"bi\0"))
            elif tipo == b"R" and datos[:4] == struct.pack("!i", 5):
                clave = hashlib.md5(hashlib.md5(b"bibi").hexdigest().encode() + datos[4:8]).hexdigest()
                sock.sendall(mensaje(b"p", b"md5" + clave.encode() + b"\0"))
            elif tipo == b"Z":
                break

        def lote(consultas):
            salida = b""
            for q in consultas:
                salida += mensaje(b"P", b"\0" + q.encode() + b"\0" + struct.pack("!h", 0))
                salida += mensaje(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
                salida += mensaje(b"D", b"P\0")
                salida += mensaje(b"E", b"\0" + struct.pack("!i", 0))
            sock.sendall(salida + mensaje(b"S", b""))
            secuencia = ""
            while True:
                tipo = leer()[0].decode()
                secuencia += tipo
                if tipo == "Z":
                    return secuencia

        r = lote([tipos, compuestos, enums])
        assert "E" not in r and r.count("D") > 20 and r.count("C") == 3, r       # tres resultados, sin error
        assert lote(["SELECT x FROM tabla_que_no_existe", enums]) == "12EZ"      # tras el error: nada hasta Sync
        assert "E" not in lote(["DISCARD ALL", "RESET ALL", "DEALLOCATE ALL", "UNLISTEN *"])  # limpieza del pool
        sock.close()
        with _connect(port, "athena") as c:  # el navegador de Power BI no debe mostrar las vistas auxiliares (main)
            esquemas = {r[0] for r in c.execute("select TABLE_SCHEMA from INFORMATION_SCHEMA.tables").fetchall()}
            assert "main" not in esquemas and "semantic" in esquemas, esquemas
            if _SERVIDOR == "server":  # Metabase sincroniza leyendo pg_class/pg_namespace (solo el servidor actual)
                esquemas = {r[0] for r in c.execute(
                    "SELECT n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                    "ON c.relnamespace = n.oid WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'").fetchall()}
                assert esquemas == {"semantic"}, esquemas
    finally:
        server.shutdown()
    print("OK · Npgsql (Power BI): tipos al conectar y error a mitad de lote")


def probar_seguridad(manifest: str) -> None:
    """Un usuario autenticado no puede tocar archivos de la máquina, alterar el catálogo ni inyectar SQL en el motor."""
    import tempfile
    secreto = tempfile.NamedTemporaryFile("w", suffix=".env", delete=False)
    secreto.write("CLAVE=no-debe-salir\n")
    secreto.close()
    salida = secreto.name + ".copia.csv"
    port = _free_port()
    server = start_in_thread("127.0.0.1", port, "bi", "bi", manifest, "athena")
    ataques = [
        f"SELECT content FROM read_text('{secreto.name}')",            # leer un archivo
        f"SELECT * FROM glob('{os.path.dirname(secreto.name)}/*')",    # listar una carpeta
        f"COPY (SELECT 1 AS x) TO '{salida}'",                         # escribir un archivo
        f"SELECT 1; COPY (SELECT 1 AS x) TO '{salida}'",               # segunda sentencia escondida
        f"ATTACH '{salida}.db' AS otra",                               # abrir otra base
        "LOAD httpfs",                                                 # extensiones
        "DROP VIEW main.gw_pg_settings",                               # romper el catálogo compartido
        "CREATE TABLE main.basura AS SELECT 1 AS x",
    ]
    try:
        with _connect(port, "athena") as c:
            for sql in ataques:
                try:
                    c.execute(sql)
                    raise AssertionError(f"debía rechazarse: {sql}")
                except psycopg.Error:
                    pass
            assert not os.path.exists(salida) and not os.path.exists(salida + ".db")
            assert c.execute("SELECT count(*) FROM pg_settings").fetchone()[0] > 0  # el catálogo sigue intacto
            for valor in ("{{ 7*7 }}", "{%- if true %}x{% endif %}", "{# comentario #}",
                          '{{ "%c"|format(39) }} OR 1=1 OR {{ "%c"|format(39) }}'):  # Jinja de MetricFlow
                try:
                    c.execute("SELECT event__platform, event_count FROM semantic.events WHERE event__platform = '"
                              + valor.replace("'", "''") + "'")
                    raise AssertionError(f"debía rechazarse el filtro {valor!r}")
                except psycopg.Error as e:
                    assert "no pueden contener" in str(e), e
            sql = c.execute("SELECT event__platform, event_count FROM semantic.events "
                            "WHERE event__platform = 'x'' OR ''1''=''1'").fetchone()[0]
            assert "= 'x'' OR ''1''=''1'" in sql, sql                               # la comilla queda escapada
    finally:
        server.shutdown()
        os.unlink(secreto.name)
    print("OK · seguridad: sin acceso a archivos, catálogo de solo lectura y sin inyección por Jinja")


def probar_tls_obligatorio(manifest: str) -> None:
    """--tls-obligatorio: sin SSL la conexión se rechaza con 28000; con SSL todo funciona igual."""
    import shutil
    import subprocess
    import tempfile
    import threading
    import time
    from semantic_gateway.server import build_server

    openssl = shutil.which("openssl")
    if not openssl:
        print("-- TLS obligatorio: se omite (no se encontró openssl para generar un certificado de prueba)")
        return
    carpeta = tempfile.mkdtemp()
    crt, key = os.path.join(carpeta, "server.crt"), os.path.join(carpeta, "server.key")
    subprocess.run([openssl, "req", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-x509", "-days", "1",
                    "-out", crt, "-subj", "/CN=localhost"], check=True, capture_output=True)
    try:
        build_server("127.0.0.1", _free_port(), "bi", "bi", manifest, "athena", tls_obligatorio=True)
        raise AssertionError("--tls-obligatorio sin certificado debía fallar")
    except ValueError:
        pass
    port = _free_port()
    gateway = build_server("127.0.0.1", port, "bi", "bi", manifest, "athena", tls_cert=crt, tls_key=key,
                           tls_obligatorio=True)
    threading.Thread(target=gateway.start, daemon=True).start()
    for _ in range(100):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.1)
    base = f"host=127.0.0.1 port={port} user=bi password=bi dbname=athena"
    try:
        psycopg.connect(base + " sslmode=disable")
        raise AssertionError("una conexión sin SSL debía rechazarse")
    except psycopg.OperationalError as e:
        assert "exige SSL" in str(e), e
    for modo in ("prefer", "require"):  # prefer: libpq intenta SSL primero, así que también entra cifrado
        with psycopg.connect(f"{base} sslmode={modo}", autocommit=True) as c:
            assert c.pgconn.ssl_in_use
            assert c.execute("SELECT count(*) FROM semantic.metrics").fetchone()[0] >= 10   # catálogo
            assert "SELECT" in c.execute(QUERY).fetchone()[0]                                # consulta semántica
        with psycopg.connect(f"{base} sslmode={modo}", autocommit=True, prepare_threshold=0) as c:
            assert c.execute("SELECT %s::int", (5,)).fetchone() == (5,)                     # protocolo extendido
    print("OK · TLS obligatorio: sin SSL se rechaza, con SSL funciona")


_PBI_FECHA = (  # SQL real de Power BI DirectQuery para una tabla con una columna de fecha
    'select "_"."event__event_date" as "c8" from (select "rows"."event__event_date" as "event__event_date", '
    'case when "rows"."o0" is not null then cast("rows"."o0" as timestamp) else timestamp \'1899-12-28 00:00:00\' '
    'end as "t0_0", case when "rows"."o0" is null then 0 else 1 end as "t1_0" from (select "event__event_date" as '
    '"event__event_date", "event__event_date" as "o0" from "semantic"."events" "$Table" group by '
    '"event__event_date","o0") "rows") "_" order by "_"."t0_0", "_"."t1_0" limit 501')


class _EjecutorFalso:
    """Simula Athena: guarda el SQL recibido y devuelve filas fijas (MetricFlow pone dimensiones primero)."""
    motor = "athena"

    def __init__(self) -> None:
        self.sql: list[str] = []

    def ejecutar(self, sql: str):
        import pyarrow as pa
        from semantic_gateway.executor import Resultado
        import datetime as dt
        self.sql.append(sql)
        if "event__event_date__day" in sql:  # dimensión de tiempo sin grano: MetricFlow le agrega __day
            tabla = pa.table({"event__event_date__day": [dt.date(2024, 1, 1)], "event_count": [7]})
        elif "metric_time__day" in sql:  # metric_time y metric_time__day: una sola columna en MetricFlow
            tabla = pa.table({"metric_time__day": [dt.date(2024, 1, 2)], "event_count": [3]})
        else:
            tabla = pa.table({"event__platform": ["ios", "android"], "event_count": [10, 20]})
        return Resultado(tabla=tabla, query_id="q-falsa", segundos=0.1, bytes_escaneados=2048)


def probar_ejecucion(manifest: str) -> None:
    """Con ejecutor: dbname=athena devuelve filas; EXPLAIN y otros motores siguen devolviendo el SQL."""
    falso = _EjecutorFalso()
    port = _free_port()
    server = start_in_thread("127.0.0.1", port, "bi", "bi", manifest, "athena", {"athena": falso})
    try:
        with _connect(port, "athena") as c:
            cur = c.execute("SELECT event_count AS eventos, event__platform FROM semantic.events")
            assert [d.name for d in cur.description] == ["eventos", "event__platform"]  # orden y alias del cliente
            assert cur.fetchall() == [(10, "ios"), (20, "android")]
            assert len(falso.sql) == 1 and "event__platform" in falso.sql[0]           # recibió el SQL compilado
            assert "SELECT" in c.execute("EXPLAIN SELECT event_count FROM semantic.events").fetchone()[0]
            assert len(falso.sql) == 1                                                # EXPLAIN no ejecuta
            assert c.execute("SELECT metric FROM semantic.metrics WHERE metric = 'event_count'").fetchall()
            assert len(falso.sql) == 1                                                # el catálogo no va a Athena
            cur = c.execute("SELECT event__event_date, event_count FROM semantic.events")  # fecha sin grano
            assert [d.name for d in cur.description] == ["event__event_date", "event_count"]
            assert str(cur.fetchall()[0][0]) == "2024-01-01"
            # Power BI al cargar una tabla completa pide todas sus columnas: metric_time y metric_time__day a la vez
            cur = c.execute("SELECT metric_time, metric_time__day, event_count FROM semantic.events ORDER BY metric_time")
            assert [d.name for d in cur.description] == ["metric_time", "metric_time__day", "event_count"]
            fila = cur.fetchone()
            assert [str(fila[0]), str(fila[1])] == ["2024-01-02", "2024-01-02"] and float(fila[2]) == 3, fila
            # Power BI DirectQuery: visual de fecha envuelto en CASE/cast/orden → núcleo en Athena, resto en DuckDB
            n_antes = len(falso.sql)
            cur = c.execute(_PBI_FECHA)
            assert [d.name for d in cur.description] == ["c8"], cur.description
            assert [str(f[0]) for f in cur.fetchall()] == ["2024-01-01"]
            assert len(falso.sql) == n_antes + 1 and "CASE" not in falso.sql[-1].upper()  # a Athena va solo el núcleo
        with psycopg.connect(f"host=127.0.0.1 port={port} user=bi password=bi dbname=athena", autocommit=True,
                             prepare_threshold=0) as c:                              # protocolo extendido
            c.execute("SELECT event__platform, event_count FROM semantic.events WHERE event__platform = 'IOS'").fetchall()
            assert "'IOS'" in falso.sql[-1], falso.sql[-1]                            # el literal conserva mayúsculas
        n = len(falso.sql)
        with _connect(port, "snowflake") as c:                                       # sin ejecutor: devuelve el SQL
            assert c.execute("SELECT event_count FROM semantic.events").description[0].name == "sql"
        assert len(falso.sql) == n
    finally:
        server.shutdown()
    print("OK · ejecución: filas en dbname=athena, SQL en EXPLAIN y en motores sin ejecutor")

    # tiempo máximo y límite de filas del ejecutor de Athena (sin AWS: se reemplaza el cursor)
    import time
    import pyarrow as pa
    from semantic_gateway.executor import ConfigAthena, EjecutorAthena, ExecutionError

    class _Cursor:
        def __init__(self, demora): self.demora = demora; self.data_scanned_in_bytes = 1
        def execute(self, sql, on_start_query_execution=None):
            on_start_query_execution("q-1"); time.sleep(self.demora)
        def as_arrow(self): return pa.table({"x": list(range(5))})

    class _Conexion:
        def __init__(self, demora): self.demora = demora; self.detenidas = []
        def cursor(self): return _Cursor(self.demora)
        @property
        def client(self):
            conexion = self
            class _C:
                def stop_query_execution(self, QueryExecutionId): conexion.detenidas.append(QueryExecutionId)
            return _C()

    ej = EjecutorAthena.__new__(EjecutorAthena)
    ej.config, ej._conexion = ConfigAthena(max_filas=3, timeout=5), _Conexion(0)
    r = ej.ejecutar("select 1")
    assert r.tabla.num_rows == 3 and r.truncado and r.query_id == "q-1"
    ej.config, ej._conexion = ConfigAthena(timeout=0.3), _Conexion(2)
    try:
        ej.ejecutar("select 1")
    except ExecutionError as e:
        assert "tiempo máximo" in str(e) and ej._conexion.detenidas == ["q-1"]
    else:
        raise AssertionError("debía vencer el tiempo")
    print("OK · ejecutor Athena: --max-filas corta y --timeout cancela la consulta")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "semantic_manifests/dbt_glue_lab/semantic_manifest.json")
