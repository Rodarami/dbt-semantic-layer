"""Servidor del gateway semántico por protocolo PostgreSQL, sobre riffq (núcleo Rust: pgwire + catálogo DataFusion).

Reemplaza a `server_old.py` (buenavista, Python puro). El traductor, el motor y el ejecutor no cambian.

Reparto del trabajo:
* riffq (Rust): protocolo PostgreSQL simple y extendido (pgwire), TLS opcional, autenticación, `SHOW` y `DISCARD ALL`
  al ejecutar, consultas vacías. Cada consulta se atiende en un pool de hilos.
* Python (`ConexionSemantica.handle_query`) recibe el resto:
  - consulta sobre una tabla virtual (`semantic.events`…) → traductor → MetricFlow (compila) → ejecutor (Athena,
    con `--ejecutar`) o el SQL compilado en una columna `sql`;
  - catálogo (`pg_catalog`, `information_schema`), `semantic.metrics`… y sondas de los BI → `catalogo_pg.reescribir`
    → DuckDB en memoria (el catálogo emulado de riffq no entiende varias consultas de Metabase/DBeaver/Npgsql);
  - `BEGIN`, `SET`, `RESET`, `DISCARD`… → se aceptan sin efecto.
"""

from __future__ import annotations

import argparse
import hmac
import logging
import os
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa

os.environ.setdefault("RUST_LOG", "warn")  # el núcleo Rust de riffq registra cada consulta en INFO: solo avisos y errores
import riffq  # noqa: E402

from .catalogo_pg import (SCHEMA, _consulta_vacia, _es_sonda, blindar, install_catalog, motor_from_database, reescribir,
                          solo_lectura)
from .engine import MOTORES, SQLGLOT_DIALECT, SemanticEngine
from .tls_obligatorio import PuertaTLS, puerto_libre
from .executor import ConfigAthena, Ejecutor, EjecutorAthena, ExecutionError, formato_bytes, ordenar_columnas
from .translator import NUCLEO, TranslationError, Translator, split_explain

logger = logging.getLogger("semantic_gateway")
logger_sql = logging.getLogger("semantic_gateway.sql")  # cada consulta recibida; se activa con --log-sql
logger_sql.setLevel(logging.WARNING)

# Tipo de cada columna del catálogo semántico → tipo Arrow del resultado (el que se informa al describir)
_TIPO_ARROW = {"DATE": pa.date32(), "VARCHAR": pa.string(), "DOUBLE": pa.float64(), "BIGINT": pa.int64(),
               "INTEGER": pa.int32(), "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}


# ---------------------------------------------------------------------------------------------- sesión
# Comandos de control que los drivers envían y no tienen efecto en el gateway (sin transacciones ni estado de sesión)
_SIN_EFECTO = re.compile(
    r"\s*(begin|start\s+transaction|commit|end|rollback|abort|set|reset|discard|deallocate|unlisten|close|"
    r"savepoint|release|listen)\b", re.I)


# SHOW <variable>: riffq lo responde al ejecutar, pero el describe del protocolo extendido llega a Python
_SHOW = re.compile(r"\s*show\s+(.+?)\s*;?\s*$", re.I)
_VARIABLES = {"transaction isolation level": ("transaction_isolation", "read committed"),
              "transaction_isolation": ("transaction_isolation", "read committed"),
              "timezone": ("TimeZone", "UTC"), "time zone": ("TimeZone", "UTC"),
              "search_path": ("search_path", f"{SCHEMA}, public"), "standard_conforming_strings": ("standard_conforming_strings", "on"),
              "server_version": ("server_version", "14.0"), "client_encoding": ("client_encoding", "UTF8"),
              "datestyle": ("DateStyle", "ISO, MDY"), "max_identifier_length": ("max_identifier_length", "63")}


def _show(sql: str) -> pa.Table | None:
    m = _SHOW.match(sql)
    if not m:
        return None
    nombre = m.group(1).strip().strip('"').lower()
    columna, valor = _VARIABLES.get(nombre, (nombre, ""))
    return pa.table({columna: [valor]})


def _etiqueta(sql: str) -> str:
    """Etiqueta de CommandComplete para un comando de control (`SET`, `BEGIN`…)."""
    primera = sql.strip().split(None, 1)[0].upper().rstrip(";")
    return {"END": "COMMIT", "ABORT": "ROLLBACK", "START": "BEGIN"}.get(primera, primera)


class ConexionSemantica(riffq.BaseConnection):
    """Una instancia por cliente conectado. riffq llama a `handle_auth` y luego a `handle_query` por cada consulta que
    no es de catálogo. Los atributos de clase (`engine`, `translator`, `db`, `ejecutores`, credenciales) los fija
    `build_server`."""

    engine: SemanticEngine
    translator: Translator
    db: duckdb.DuckDBPyConnection
    ejecutores: dict[str, Ejecutor] = {}
    usuario = "bi"
    clave = "bi"

    def __init__(self, conn_id, executor) -> None:
        super().__init__(conn_id, executor)
        self.motor: str = self.engine.motor
        self.motor_error: str | None = None
        # riffq pasa a minúsculas el SQL al ejecutar en el protocolo extendido (no al describir). Se guarda el texto
        # original de cada describe para recuperarlo y no alterar literales como 'F' o nombres de cargos.
        self._originales: dict[str, str] = {}

    # ------------------------------------------------------------------ conexión
    def handle_auth(self, user, password, host, database=None, callback=None, **kwargs):
        if not (hmac.compare_digest(str(user), self.usuario) and hmac.compare_digest(str(password or ""), self.clave)):
            logger.warning("Autenticación rechazada · usuario=%s", user)
            return callback(False, f'password authentication failed for user "{user}"', "FATAL", "28P01")
        self.motor, self.motor_error = motor_from_database(database, user, self.engine.motor)
        logger.info("Conexión · usuario=%s dbname=%s → motor=%s%s", user, database, self.motor,
                    f" (ERROR: {self.motor_error})" if self.motor_error else "")
        callback(True)

    def handle_connect(self, ip, port, server_name=None, callback=None, **kwargs):
        callback(True)

    def handle_disconnect(self, ip, port, callback=None, **kwargs):
        callback(True)

    # ------------------------------------------------------------------ consultas
    def handle_query(self, sql, callback=None, **kwargs):
        # cada consulta en el pool de hilos: una consulta lenta en Athena no bloquea a las demás conexiones
        self.executor.submit(self._responder, sql, callback, kwargs.get("do_describe", False),
                             kwargs.get("query_args"))

    def _responder(self, sql: str, callback, describir: bool, parametros) -> None:
        sql = self._original(sql, describir)
        try:
            resultado = self._resolver(sql, describir, parametros)
        except TranslationError as e:
            # se registra el SQL tal como llegó: así se ve qué genera cada cliente BI (p. ej. Power BI en DirectQuery)
            logger.warning("Consulta rechazada: %s\n---------- SQL recibido ----------\n%s\n----------------------------------",
                           e, sql.strip())
            return callback(("ERROR", "42000", str(e)), is_error=True)
        except ExecutionError as e:
            return callback(("ERROR", "42000", str(e)), is_error=True)
        except duckdb.Error as e:
            return callback(("ERROR", "42000", str(e).splitlines()[0]), is_error=True)
        except Exception as e:  # noqa: BLE001 — cualquier fallo llega al cliente como error SQL
            logger.exception("Error inesperado")
            return callback(("ERROR", "XX000", str(e)), is_error=True)
        if isinstance(resultado, str):
            logger_sql.info("%s%s\n  → etiqueta %s", "[describe] " if describir else "", sql.strip(), resultado or "(vacía)")
            return callback(resultado, is_tag=True)
        logger_sql.info("%s%s\n  → %d filas · columnas: %s", "[describe] " if describir else "", sql.strip(),
                        resultado.num_rows, ", ".join(f"{c.name} {c.type}" for c in resultado.schema))
        self.send_reader(resultado.to_reader(), callback)

    def _original(self, sql: str, describir: bool) -> str:
        clave = sql.strip().lower()
        if describir:
            self._originales[clave] = sql
            if len(self._originales) > 200:  # se conservan solo los recientes
                self._originales.pop(next(iter(self._originales)))
            return sql
        return self._originales.get(clave, sql)

    def _resolver(self, sql: str, describir: bool, parametros) -> pa.Table | str:
        if _consulta_vacia(sql):
            return ""
        if _SIN_EFECTO.match(sql):
            return _etiqueta(sql)
        show = _show(sql)
        if show is not None:
            return show
        if self.translator.is_semantic(sql):
            return self._semantica(sql, describir, parametros)
        return self._duckdb(sql, parametros)

    def _duckdb(self, sql: str, parametros) -> pa.Table:
        """Catálogo (`pg_catalog`, `information_schema`), tablas de metadatos (`semantic.metrics`…) y sondas."""
        cursor = self.db.cursor()
        try:
            cursor.execute(f"SET search_path = '{SCHEMA},main'")
            consulta = reescribir(sql)
            solo_lectura(consulta)
            relacion = cursor.execute(consulta, parametros or None)
            return relacion.fetch_arrow_table() if relacion.description else pa.table({})
        finally:
            cursor.close()

    def _semantica(self, sql: str, describir: bool, parametros) -> pa.Table:
        es_explain, cuerpo = split_explain(sql)
        if not parametros and _es_sonda(cuerpo):
            return self._duckdb(cuerpo, None)  # forma de la tabla virtual, sin filas
        if parametros:
            raise TranslationError("La capa semántica no admite parámetros: envíe la consulta con literales")
        if self.motor_error:
            raise TranslationError(self.motor_error)
        try:
            sq = self.translator.translate(cuerpo, dialect=SQLGLOT_DIALECT[self.motor])
        except TranslationError:
            partes = self.translator.separar_nucleo(cuerpo)
            if partes is None:
                raise
            return self._nucleo_y_resto(*partes, es_explain=es_explain, describir=describir)
        ejecutor = self.ejecutores.get(self.motor)
        devuelve_sql = es_explain or ejecutor is None
        esquema = pa.schema([("sql", pa.string())]) if devuelve_sql else self._esquema(sq)
        if describir:  # describe del protocolo extendido: solo la forma del resultado, sin compilar ni ejecutar
            return esquema.empty_table()

        sql_motor = self.engine.explain(sq, self.motor)
        logger.info(
            "Consulta compilada · motor=%s metrics=%s group_by=%s where=%s time=%s..%s order_by=%s limit=%s\n"
            "---------- SQL %s ----------\n%s\n----------------------------------",
            self.motor, sq.metrics, sq.group_by, sq.where, sq.start, sq.end, sq.order_by, sq.limit,
            self.motor, sql_motor,
        )
        if devuelve_sql:
            return pa.table({"sql": [sql_motor]})
        try:
            resultado = ejecutor.ejecutar(sql_motor)
        except ExecutionError as e:
            logger.error("Falló la ejecución en %s: %s", self.motor, e)
            raise
        tabla = _ajustar_tipos(ordenar_columnas(resultado.tabla, sq.output), esquema)
        logger.info("Ejecutada en %s · query_id=%s · %d filas · %.1f s · %s escaneados%s", self.motor,
                    resultado.query_id or "-", tabla.num_rows, resultado.segundos,
                    formato_bytes(resultado.bytes_escaneados),
                    " · TRUNCADO por --max-filas" if resultado.truncado else "")
        return tabla

    def _nucleo_y_resto(self, sql_nucleo: str, sql_resto: str, es_explain: bool, describir: bool) -> pa.Table:
        """Consultas que los BI envuelven con cálculos propios (Power BI en DirectQuery: casts, CASE, orden de nulos…).
        El núcleo (lo que lee la tabla semántica) va a MetricFlow y al motor; el resto se ejecuta en DuckDB sobre ese
        resultado, que ya viene agregado y es chico."""
        sq = self.translator.translate(sql_nucleo, dialect=SQLGLOT_DIALECT[self.motor])
        ejecutor = self.ejecutores.get(self.motor)
        if es_explain or ejecutor is None:  # sin ejecutor no hay filas sobre las que aplicar el resto
            if describir:
                return pa.schema([("sql", pa.string())]).empty_table()
            return pa.table({"sql": [self.engine.explain(sq, self.motor)]})
        esquema = self._esquema(sq)
        if describir:  # la forma del resultado: el resto aplicado a un núcleo vacío, sin ir al motor
            return _resto_en_duckdb(sql_resto, esquema.empty_table()).schema.empty_table()
        sql_motor = self.engine.explain(sq, self.motor)
        logger.info("Consulta compilada (núcleo; el resto se aplica en DuckDB) · motor=%s metrics=%s group_by=%s "
                    "where=%s\n---------- SQL %s ----------\n%s\n----------------------------------",
                    self.motor, sq.metrics, sq.group_by, sq.where, self.motor, sql_motor)
        resultado = ejecutor.ejecutar(sql_motor)
        nucleo = _ajustar_tipos(ordenar_columnas(resultado.tabla, sq.output), esquema)
        tabla = _resto_en_duckdb(sql_resto, nucleo)
        logger.info("Ejecutada en %s · query_id=%s · núcleo %d filas → %d filas · %.1f s · %s escaneados", self.motor,
                    resultado.query_id or "-", nucleo.num_rows, tabla.num_rows, resultado.segundos,
                    formato_bytes(resultado.bytes_escaneados))
        return tabla

    def _esquema(self, sq) -> pa.Schema:
        """Tipos del resultado según el catálogo (los mismos que se informan al describir)."""
        catalogo = self.translator.catalog
        campos = []
        for columna, publicada in sq.output:
            tipo = catalogo.dimensions[columna].data_type if columna in catalogo.dimensions else "DOUBLE"
            campos.append((publicada, _TIPO_ARROW.get(tipo, pa.string())))
        return pa.schema(campos)


def _resto_en_duckdb(sql_resto: str, nucleo: pa.Table) -> pa.Table:
    """Ejecuta el *resto* de la consulta del BI sobre el resultado del núcleo, en una base DuckDB efímera y blindada
    (sin archivos, red ni extensiones; solo una sentencia SELECT que lee la tabla del núcleo)."""
    con = duckdb.connect()
    try:
        con.register("__nucleo_arrow", nucleo)
        con.execute(f"CREATE TABLE {NUCLEO} AS SELECT * FROM __nucleo_arrow")
        con.unregister("__nucleo_arrow")
        blindar(con)
        consulta = reescribir(sql_resto)
        solo_lectura(consulta)
        return con.execute(consulta).fetch_arrow_table()
    finally:
        con.close()


def _ajustar_tipos(tabla: pa.Table, esquema: pa.Schema) -> pa.Table:
    """Convierte las columnas del resultado de Athena a los tipos informados en el describe: los clientes que leen en
    binario (Npgsql/Power BI) interpretan los bytes según el tipo anunciado."""
    columnas = []
    for i, campo in enumerate(esquema):
        columna = tabla.column(i)
        try:
            columnas.append(columna.cast(campo.type, safe=False))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            columnas.append(columna.cast(pa.string()).cast(campo.type, safe=False) if campo.type != pa.string()
                            else columna.cast(pa.string()))
    return pa.Table.from_arrays(columnas, schema=esquema)


# ---------------------------------------------------------------------------------------------- arranque
class Gateway:
    """Servidor riffq ya configurado. `start()` bloquea hasta Ctrl+C; `start_in_thread` lo usa en segundo plano."""

    def __init__(self, servidor: riffq.RiffqServer, host: str, port: int, tls: bool,
                 puerta: PuertaTLS | None = None) -> None:
        self.servidor, self.host, self.port, self.tls, self.puerta = servidor, host, port, tls, puerta

    def start(self) -> None:
        if self.puerta:  # TLS obligatorio: la puerta cifra y riffq atiende sin TLS en 127.0.0.1
            self.puerta.start_in_thread()
        # catálogo: lo responde catalogo_pg en DuckDB (catalog_emulation=False → todo llega a handle_query)
        self.servidor.start(tls=self.tls, catalog_emulation=False, server_version="14.0")

    def shutdown(self) -> None:
        """riffq no expone una parada desde Python: el hilo es daemon y termina con el proceso."""


def build_server(host: str, port: int, user: str, password: str, manifest: str, motor: str,
                 ejecutores: dict[str, Ejecutor] | None = None, tls_cert: str | None = None,
                 tls_key: str | None = None, hilos: int = 32, tls_obligatorio: bool = False) -> Gateway:
    engine = SemanticEngine(manifest, motor)
    for ruidoso in ("metricflow", "metricflow_semantics", "dbt_metricflow"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)
    logging.getLogger("sqlglot").setLevel(logging.ERROR)
    catalog = engine.catalog(SCHEMA)
    db = duckdb.connect()  # tablas virtuales vacías y tablas de metadatos
    install_catalog(db, catalog)
    blindar(db)  # sin acceso a archivos ni extensiones y configuración bloqueada (ver catalogo_pg.blindar)

    conexion = type("Conexion", (ConexionSemantica,), {
        "engine": engine, "translator": Translator(catalog, warehouse_dialect=SQLGLOT_DIALECT[motor]), "db": db,
        "ejecutores": ejecutores or {}, "usuario": user, "clave": password,
    })
    tls = bool(tls_cert and tls_key)
    if tls_obligatorio and not tls:
        raise ValueError("--tls-obligatorio necesita --tls-cert y --tls-key")
    if host not in ("127.0.0.1", "localhost", "::1"):  # expuesto a la red
        if not tls_obligatorio:
            logger.warning("El gateway escucha en %s sin --tls-obligatorio: riffq pide la clave en texto plano y un "
                           "cliente sin SSL la envía sin cifrar", host)
        if password == "bi":
            logger.warning("El gateway escucha en %s con la clave por defecto 'bi': cámbiela con SEMANTIC_GW_PASSWORD", host)
    puerta = None
    if tls_obligatorio:  # riffq queda en un puerto interno sin TLS; la puerta atiende el puerto público
        interno = puerto_libre()
        puerta = PuertaTLS(host, port, interno, tls_cert, tls_key)
        servidor = riffq.RiffqServer(f"127.0.0.1:{interno}", connection_cls=conexion)
    else:
        servidor = riffq.RiffqServer(f"{host}:{port}", connection_cls=conexion)
        if tls:
            servidor.set_tls(tls_cert, tls_key)
    servidor.executor = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix="consulta")
    logger.info(
        "Gateway listo en %s:%d (riffq%s) · %s.{%s} · motor por defecto=%s (dbname=<motor> lo cambia por conexión) · "
        "%s · %d métricas · %d dimensiones",
        host, port, ", TLS obligatorio" if tls_obligatorio else ", TLS opcional" if tls else "", SCHEMA, ", ".join(catalog.tables), motor,
        f"ejecuta en: {', '.join(sorted(ejecutores))} (el resto solo compila)" if ejecutores else "solo compila y devuelve el SQL",
        len(catalog.metrics), len(catalog.dimensions),
    )
    return Gateway(servidor, host, port, tls and not tls_obligatorio, puerta)


def start_in_thread(host: str, port: int, user: str, password: str, manifest: str, motor: str,
                    ejecutores: dict[str, Ejecutor] | None = None) -> Gateway:
    """Levanta el gateway en un hilo y espera a que acepte conexiones (para pruebas y notebooks)."""
    gateway = build_server(host, port, user, password, manifest, motor, ejecutores)
    threading.Thread(target=gateway.start, name="semantic-gateway", daemon=True).start()
    for _ in range(100):
        with socket.socket() as s:
            if s.connect_ex((host, port)) == 0:
                return gateway
        time.sleep(0.1)
    raise RuntimeError(f"El gateway no abrió el puerto {port}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gateway semántico por protocolo PostgreSQL (riffq)")
    parser.add_argument("--manifest", default=os.environ.get("SEMANTIC_GW_MANIFEST", "semantic_manifests/dbt_glue_lab/semantic_manifest.json"))
    parser.add_argument("--motor", default=os.environ.get("SEMANTIC_GW_MOTOR", "athena"), choices=sorted(MOTORES),
                        help="motor por defecto; cada conexión puede pedir otro con dbname=<motor>")
    parser.add_argument("--host", default=os.environ.get("SEMANTIC_GW_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SEMANTIC_GW_PORT", "5433")))
    parser.add_argument("--user", default=os.environ.get("SEMANTIC_GW_USER", "bi"))
    parser.add_argument("--password", default=os.environ.get("SEMANTIC_GW_PASSWORD", "bi"))
    parser.add_argument("--log-level", default=os.environ.get("SEMANTIC_GW_LOG_LEVEL", "INFO"))
    parser.add_argument("--log-sql", action="store_true", default=os.environ.get("SEMANTIC_GW_LOG_SQL", "") == "1",
                        help="muestra cada consulta que llega (catálogo incluido) y la forma de la respuesta; "
                             "para diagnosticar clientes BI")
    parser.add_argument("--tls-cert", default=os.environ.get("SEMANTIC_GW_TLS_CERT"),
                        help="certificado PEM; con --tls-key activa TLS (los clientes usan sslmode=require)")
    parser.add_argument("--tls-key", default=os.environ.get("SEMANTIC_GW_TLS_KEY"))
    parser.add_argument("--tls-obligatorio", action="store_true",
                        default=os.environ.get("SEMANTIC_GW_TLS_OBLIGATORIO", "") == "1",
                        help="rechaza las conexiones sin SSL (sin esta opción TLS es opcional para el cliente)")
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
    logger_sql.setLevel(logging.INFO if args.log_sql else logging.WARNING)
    ejecutores: dict[str, Ejecutor] = {}
    if args.ejecutar:
        ejecutores["athena"] = EjecutorAthena(ConfigAthena(
            region=args.region, workgroup=args.athena_workgroup, output=args.athena_output, profile=args.aws_profile,
            catalog=args.athena_catalog, timeout=args.timeout, max_filas=args.max_filas))
        logger.info("Ejecución en Athena · workgroup=%s · resultados=%s · región=%s · perfil=%s · timeout=%s · max_filas=%s",
                    args.athena_workgroup, args.athena_output or "(la del workgroup)", args.region or "(por defecto)",
                    args.aws_profile or "(por defecto)", f"{args.timeout:.0f} s" if args.timeout else "sin límite",
                    args.max_filas or "sin límite")
    gateway = build_server(args.host, args.port, args.user, args.password, args.manifest, args.motor, ejecutores,
                           args.tls_cert, args.tls_key, tls_obligatorio=args.tls_obligatorio)
    try:
        gateway.start()  # bloquea hasta Ctrl+C (SIGINT) o SIGTERM
    except KeyboardInterrupt:
        pass
    logger.info("Gateway detenido")


if __name__ == "__main__":
    main()
