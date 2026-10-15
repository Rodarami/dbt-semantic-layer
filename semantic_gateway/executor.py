"""Ejecutores: corren en el warehouse el SQL que compiló MetricFlow y devuelven las filas como tabla Arrow.

MetricFlow solo compila (engine.py); este módulo es el componente que ejecuta, como pide el ADR.
Por ahora hay ejecutor para Athena. Los demás motores siguen devolviendo el SQL compilado.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Protocol

import pyarrow as pa

logger = logging.getLogger("semantic_gateway.executor")


class ExecutionError(Exception):
    """El warehouse rechazó o no terminó la consulta; el mensaje llega tal cual al cliente BI."""


@dataclass
class Resultado:
    tabla: pa.Table
    query_id: str = ""
    segundos: float = 0.0
    bytes_escaneados: Optional[int] = None
    truncado: bool = False  # se cortó en max_filas


class Ejecutor(Protocol):
    motor: str

    def ejecutar(self, sql: str) -> Resultado: ...


@dataclass
class ConfigAthena:
    region: Optional[str] = None  # None: la del perfil o AWS_REGION
    workgroup: str = "primary"
    output: Optional[str] = None  # s3://… ; None: la ubicación de resultados que impone el workgroup
    profile: Optional[str] = None  # None: credenciales por defecto (AWS_PROFILE, variables, rol de la máquina)
    catalog: str = "awsdatacatalog"
    timeout: float = 100.0  # segundos; al vencer se cancela la consulta en Athena (deja de escanear)
    max_filas: int = 0  # 0 = sin límite
    poll: float = 1.0  # cada cuánto se consulta el estado en Athena
    extra: dict = field(default_factory=dict)


class EjecutorAthena:
    """Ejecuta en Athena con PyAthena (ArrowCursor) y devuelve una tabla Arrow."""

    motor = "athena"

    def __init__(self, config: ConfigAthena) -> None:
        try:
            from pyathena import connect  # import diferido: solo hace falta si se ejecuta en Athena
            from pyathena.arrow.cursor import ArrowCursor
        except ImportError as e:  # pragma: no cover
            raise RuntimeError("Falta PyAthena: python -m pip install 'pyathena[arrow]'") from e
        self.config = config
        kwargs = dict(work_group=config.workgroup, catalog_name=config.catalog, poll_interval=config.poll,
                      cursor_class=ArrowCursor, **config.extra)
        if config.region:
            kwargs["region_name"] = config.region
        if config.output:
            kwargs["s3_staging_dir"] = config.output
        if config.profile:
            kwargs["profile_name"] = config.profile
        self._conexion = connect(**kwargs)  # cliente boto3 compartido (es seguro entre hilos); un cursor por consulta

    def ejecutar(self, sql: str) -> Resultado:
        cursor = self._conexion.cursor()
        query_id: dict[str, str] = {}
        salida: dict[str, object] = {}

        def correr() -> None:
            try:
                cursor.execute(sql, on_start_query_execution=lambda qid: query_id.setdefault("id", qid))
                salida["tabla"] = cursor.as_arrow()
            except Exception as e:  # se informa en el hilo principal
                salida["error"] = e

        inicio = time.monotonic()
        hilo = threading.Thread(target=correr, name="athena-query", daemon=True)
        hilo.start()
        hilo.join(self.config.timeout or None)
        if hilo.is_alive():  # venció el tiempo: se cancela en Athena para no seguir escaneando
            qid = query_id.get("id")
            if qid:
                try:
                    self._conexion.client.stop_query_execution(QueryExecutionId=qid)
                except Exception as e:  # pragma: no cover
                    logger.warning("No se pudo cancelar la consulta %s en Athena: %s", qid, e)
            hilo.join(10)
            raise ExecutionError(f"La consulta superó el tiempo máximo de {self.config.timeout:.0f} s en Athena "
                                 f"y se canceló (query_id={qid or 'sin iniciar'})")
        if "error" in salida:
            raise ExecutionError(f"Athena: {salida['error']}") from salida["error"]  # type: ignore[misc]

        tabla: pa.Table = salida["tabla"]  # type: ignore[assignment]
        truncado = bool(self.config.max_filas) and tabla.num_rows > self.config.max_filas
        if truncado:
            tabla = tabla.slice(0, self.config.max_filas)
        return Resultado(tabla=tabla, query_id=query_id.get("id", ""), segundos=time.monotonic() - inicio,
                         bytes_escaneados=getattr(cursor, "data_scanned_in_bytes", None), truncado=truncado)


def ordenar_columnas(tabla: pa.Table, salida: list[tuple[str, str]]) -> pa.Table:
    """Deja las columnas en el orden del SELECT del cliente y con sus nombres (alias incluidos).

    MetricFlow devuelve primero las dimensiones y después las métricas; Athena, en minúsculas.
    """
    por_nombre = {n.lower(): i for i, n in enumerate(tabla.column_names)}
    columnas, nombres = [], []
    for columna_mf, publicada in salida:
        i = por_nombre.get(columna_mf.lower())
        if i is None:
            raise ExecutionError(f"El resultado no trae la columna '{columna_mf}' (columnas: {', '.join(tabla.column_names)})")
        columnas.append(tabla.column(i))
        nombres.append(publicada)
    return pa.Table.from_arrays(columnas, names=nombres)


def formato_bytes(n: Optional[int]) -> str:
    if n is None:
        return "?"
    for unidad in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unidad}" if unidad == "B" else f"{n:.1f} {unidad}"
        n /= 1024
    return f"{n:.2f} TB"
