"""Motor de métricas: MetricFlow como compilador puro sobre `semantic_manifest.json`.

No abre conexiones ni ejecuta nada. El manifest se carga una vez; el dialecto se elige POR LLAMADA (`motor=`), con
`--motor` como valor por defecto. Se crea un MetricFlowEngine por motor la primera vez que se pide. Las rutas físicas
(base.esquema.tabla) salen del manifest tal como está. El resultado es el texto SQL.
"""

from __future__ import annotations

import datetime as dt
import threading
from pathlib import Path

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlPlanRenderer
from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer
from metricflow.sql.render.trino import TrinoSqlPlanRenderer
from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

from metricflow_semantic_interfaces.references import MetricReference

from .model import METRIC_TIME, METRIC_TIME_GRAINS, Catalog, DimensionInfo, MetricInfo, SemanticQuery, TableInfo

RESERVADAS = {"metrics", "dimensions", "metric_dimensions"}  # tablas de catálogo: un modelo no puede usar estos nombres

MOTORES = {
    "athena": (SqlEngine.ATHENA, AthenaSqlPlanRenderer),
    "bigquery": (SqlEngine.BIGQUERY, BigQuerySqlPlanRenderer),
    "databricks": (SqlEngine.DATABRICKS, DatabricksSqlPlanRenderer),
    "duckdb": (SqlEngine.DUCKDB, DuckDbSqlPlanRenderer),
    "postgres": (SqlEngine.POSTGRES, PostgresSQLSqlPlanRenderer),
    "redshift": (SqlEngine.REDSHIFT, RedshiftSqlPlanRenderer),
    "snowflake": (SqlEngine.SNOWFLAKE, SnowflakeSqlPlanRenderer),
    "trino": (SqlEngine.TRINO, TrinoSqlPlanRenderer),
}

# Dialecto sqlglot equivalente, para renderizar los filtros WHERE del usuario
SQLGLOT_DIALECT = {"athena": "athena", "bigquery": "bigquery", "databricks": "databricks", "duckdb": "duckdb",
                   "postgres": "postgres", "redshift": "redshift", "snowflake": "snowflake", "trino": "trino"}


class ClienteSqlSoloDialecto:
    """SqlClient que solo aporta el dialecto: cualquier intento de ejecutar falla."""

    def __init__(self, motor: str) -> None:
        self._engine, renderer = MOTORES[motor]
        self._renderer = renderer()

    @property
    def sql_engine_type(self):
        return self._engine

    @property
    def sql_plan_renderer(self):
        return self._renderer

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return bind_parameter_key

    def _sin_conexion(self, *a, **k):
        raise RuntimeError("Este cliente solo compila SQL; no ejecuta consultas.")

    query = execute = dry_run = _sin_conexion

    def close(self) -> None:
        pass


class SemanticEngine:
    def __init__(self, manifest_path: str | Path, motor: str) -> None:
        self.motor = self._check(motor)  # motor por defecto
        with open(manifest_path, encoding="utf-8") as f:
            manifest = parse_manifest_from_dbt_generated_manifest(manifest_json_string=f.read())
        self._lookup = SemanticManifestLookup(manifest)
        self._engines: dict[str, MetricFlowEngine] = {}
        self._lock = threading.Lock()
        self._engine = self._engine_for(self.motor)  # el catálogo es igual para todos los motores

    @staticmethod
    def _check(motor: str) -> str:
        if motor not in MOTORES:
            raise ValueError(f"Motor desconocido '{motor}'. Disponibles: {', '.join(sorted(MOTORES))}")
        return motor

    def _engine_for(self, motor: str) -> MetricFlowEngine:
        if motor not in self._engines:
            self._engines[motor] = MetricFlowEngine(self._lookup, ClienteSqlSoloDialecto(motor))
        return self._engines[motor]

    # ------------------------------------------------------------------ catálogo
    def catalog(self, schema: str) -> Catalog:
        metrics = {
            m.name: MetricInfo(
                name=m.name,
                label=m.label or m.name,
                type=str(m.type.value if hasattr(m.type, "value") else m.type),
                description=m.description or "",
            )
            for m in sorted(self._engine.list_metrics(include_dimensions=False), key=lambda m: m.name)
        }
        dimensions: dict[str, DimensionInfo] = {
            METRIC_TIME: DimensionInfo(METRIC_TIME, "time", "DATE", "Tiempo de la métrica (día)"),
        }
        for grain in METRIC_TIME_GRAINS:
            name = f"{METRIC_TIME}__{grain}"
            dimensions[name] = DimensionInfo(name, "time", "DATE", f"Tiempo de la métrica ({grain})")

        pairs: list[tuple[str, str]] = []
        for metric in metrics:
            for d in self._engine.simple_dimensions_for_metrics([metric]):
                kind = "time" if "TIME" in str(d.type) else "categorical"
                if d.dunder_name not in dimensions:
                    # Sin conexión no hay tipos físicos: tiempo → DATE, el resto → VARCHAR
                    dimensions[d.dunder_name] = DimensionInfo(
                        d.dunder_name, kind, "DATE" if kind == "time" else "VARCHAR", d.description or ""
                    )
                pairs.append((metric, d.dunder_name))
            pairs.extend((metric, t) for t in dimensions if t.startswith(METRIC_TIME))
        return Catalog(schema, self._tables(metrics, dimensions, pairs), metrics, dict(sorted(dimensions.items())),
                       tuple(sorted(set(pairs))))

    def _tables(self, metrics: dict[str, MetricInfo], dimensions: dict[str, DimensionInfo],
                pairs: list[tuple[str, str]]) -> dict[str, TableInfo]:
        """Una tabla por modelo semántico: sus métricas (las compuestas van donde viven todos sus insumos) y las
        dimensiones comunes a todas ellas, para que cualquier combinación de columnas de una tabla sea consultable.
        Una métrica que mezcla varios modelos va en una tabla propia con el nombre de los modelos unidos por `_`."""
        lookup = self._lookup.metric_lookup
        groups: dict[str, list[str]] = {}
        for metric in metrics:
            models = sorted(m.semantic_model_name for m in lookup.get_derived_from_semantic_models(MetricReference(metric)))
            name = "_".join(models) or metric
            groups.setdefault(f"{name}_sm" if name in RESERVADAS else name, []).append(metric)
        dims_of: dict[str, set[str]] = {}
        for metric, dim in pairs:
            dims_of.setdefault(metric, set()).add(dim)
        tables = {}
        for name, members in sorted(groups.items()):
            common = set.intersection(*(dims_of.get(m, set()) for m in members))
            tables[name] = TableInfo(
                name=name,
                description=f"Métricas del modelo semántico '{name}' y las dimensiones con que se pueden cruzar",
                metrics=tuple(sorted(members)),
                dimensions=tuple(sorted(common)),
            )
        return tables

    # ------------------------------------------------------------------ consultas
    def explain(self, sq: SemanticQuery, motor: str | None = None) -> str:
        """SQL que MetricFlow generaría para `motor` (por defecto el del arranque). No ejecuta nada."""
        motor = self._check(motor or self.motor)
        with self._lock:
            return self._engine_for(motor).explain(self._request(sq)).sql_statement.sql

    @staticmethod
    def _request(sq: SemanticQuery):
        def ts(d: dt.date | None) -> dt.datetime | None:
            return dt.datetime.combine(d, dt.time()) if d else None

        return MetricFlowQueryRequest.create(
            metric_names=list(sq.metrics) or None,
            group_by_names=list(sq.group_by) or None,
            time_constraint_start=ts(sq.start),
            time_constraint_end=ts(sq.end),
            where_constraints=list(sq.where) or None,
            order_by_names=list(sq.order_by) or None,
            limit=sq.limit,
        )
