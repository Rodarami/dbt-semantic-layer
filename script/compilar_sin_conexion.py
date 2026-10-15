#!/usr/bin/env python3
"""Compila una consulta semántica a SQL de un motor SIN conectarse a ningún warehouse.

Usa la librería `metricflow` directamente (no el CLI `mf`, que siempre abre una conexión)
con un cliente SQL "vacío" que sólo aporta el dialecto. Es el comportamiento que pide
el ADR para el metric engine: MetricFlow sólo compila; otro componente ejecuta.

Ejemplos:
  python3 script/compilar_sin_conexion.py --motor athena --metrics item_views \
      --group-by metric_time__month,event__platform
  python3 script/compilar_sin_conexion.py --motor snowflake --manifest semantic_manifests/dbt_glue_lab/semantic_manifest_snowflake.json \
      --metrics item_views_per_user --group-by metric_time__year
"""
import argparse
import sys

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


class ClienteSqlSoloDialecto:
    """SqlClient que aporta el dialecto y NO se conecta: cualquier intento de ejecutar falla."""

    def __init__(self, motor: str):
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
        raise RuntimeError("Este cliente sólo compila SQL; no ejecuta consultas.")

    query = execute = dry_run = _sin_conexion

    def close(self) -> None:
        pass


def compilar(manifest_path: str, motor: str, metrics, group_by, where=None, order_by=None, limit=None) -> str:
    with open(manifest_path, encoding="utf-8") as f:
        manifest = parse_manifest_from_dbt_generated_manifest(manifest_json_string=f.read())
    engine = MetricFlowEngine(SemanticManifestLookup(manifest), ClienteSqlSoloDialecto(motor))
    request = MetricFlowQueryRequest.create(
        metric_names=metrics,
        group_by_names=group_by,
        where_constraints=where,
        order_by_names=order_by,
        limit=limit,
    )
    return engine.explain(request).sql_statement.sql


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--manifest", default="semantic_manifests/dbt_glue_lab/semantic_manifest.json")
    p.add_argument("--motor", required=True, choices=sorted(MOTORES))
    p.add_argument("--metrics", required=True, help="métricas separadas por coma")
    p.add_argument("--group-by", default="", help="dimensiones separadas por coma")
    p.add_argument("--where", action="append", default=None, help="filtro Jinja; repetible")
    p.add_argument("--order", default="", help="orden separado por coma; prefijo - = descendente")
    p.add_argument("--limit", type=int, default=None)
    a = p.parse_args()

    sql = compilar(
        a.manifest,
        a.motor,
        [m for m in a.metrics.split(",") if m],
        [g for g in a.group_by.split(",") if g],
        a.where,
        [o for o in a.order.split(",") if o] or None,
        a.limit,
    )
    print(sql)
    return 0


if __name__ == "__main__":
    sys.exit(main())
