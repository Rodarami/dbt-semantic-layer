"""Modelo interno de la capa semántica: catálogo publicado y consulta semántica canónica.

Todo protocolo de acceso (hoy solo Postgres wire) se traduce a `SemanticQuery` antes de llegar al motor.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

# Granularidades de `metric_time` publicadas como columnas de la tabla virtual (`metric_time` = día)
METRIC_TIME = "metric_time"
METRIC_TIME_GRAINS = ("week", "month", "quarter", "year")


def is_metric_time(name: str) -> bool:
    return name == METRIC_TIME or name.startswith(METRIC_TIME + "__")


@dataclass(frozen=True)
class MetricInfo:
    name: str
    label: str
    type: str
    description: str


@dataclass(frozen=True)
class DimensionInfo:
    name: str  # nombre calificado (dunder), p. ej. business_unit__country_name
    kind: str  # categorical | time
    data_type: str  # tipo SQL (DuckDB) de la columna en la tabla virtual
    description: str = ""


@dataclass(frozen=True)
class TableInfo:
    """Una tabla virtual: las métricas de un modelo semántico y las dimensiones que se pueden cruzar con TODAS ellas."""

    name: str
    description: str
    metrics: tuple[str, ...]
    dimensions: tuple[str, ...]


@dataclass(frozen=True)
class Catalog:
    """Lo que la capa semántica publica: una tabla virtual por modelo semántico (columnas = dimensiones y métricas)."""

    schema: str
    tables: dict[str, TableInfo]
    metrics: dict[str, MetricInfo]
    dimensions: dict[str, DimensionInfo]
    metric_dimensions: tuple[tuple[str, str], ...]  # (métrica, dimensión) compatibles

    def is_metric(self, name: str, table: str | None = None) -> bool:
        return name in (self.tables[table].metrics if table else self.metrics)

    def is_dimension(self, name: str, table: str | None = None) -> bool:
        return name in (self.tables[table].dimensions if table else self.dimensions)

    def tables_with(self, column: str) -> list[str]:
        """Tablas que publican la columna (métrica o dimensión)."""
        return [t.name for t in self.tables.values() if column in t.metrics or column in t.dimensions]

    def table_of_metric(self, metric: str) -> str:
        return next(t.name for t in self.tables.values() if metric in t.metrics)


@dataclass
class SemanticQuery:
    metrics: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    where: list[str] = field(default_factory=list)  # filtros MetricFlow ({{ Dimension('…') }} …)
    start: Optional[dt.date] = None  # restricción de tiempo sobre metric_time (no es un filtro where)
    end: Optional[dt.date] = None
    order_by: list[str] = field(default_factory=list)  # prefijo '-' = descendente
    limit: Optional[int] = None
    # Columnas de salida en el orden del SELECT: (columna de MetricFlow, nombre publicado)
    output: list[tuple[str, str]] = field(default_factory=list)
