"""Pruebas unitarias del traductor SQL → SemanticQuery (sin motor ni base de datos).

Uso (desde la raíz del proyecto dbt):  python -m tests.test_translator
"""

import datetime as dt

from semantic_gateway.model import Catalog, DimensionInfo, MetricInfo, TableInfo
from semantic_gateway.translator import TranslationError, Translator, split_explain

EVENT_METRICS = ("item_views", "unique_users", "item_views_per_user")
EVENT_DIMS = ("metric_time", "metric_time__month", "event__event_date", "event__platform", "event__is_mobile")
CATALOG = Catalog(
    schema="semantic",
    tables={
        "events": TableInfo("events", "", EVENT_METRICS, EVENT_DIMS),
        "items": TableInfo("items", "", ("total_item_price",), ("metric_time", "item__category")),
    },
    metrics={m: MetricInfo(m, m, "simple", "") for m in (*EVENT_METRICS, "total_item_price")},
    dimensions={
        "metric_time": DimensionInfo("metric_time", "time", "DATE"),
        "metric_time__month": DimensionInfo("metric_time__month", "time", "DATE"),
        "event__event_date": DimensionInfo("event__event_date", "time", "DATE"),
        "event__platform": DimensionInfo("event__platform", "categorical", "VARCHAR"),
        "event__is_mobile": DimensionInfo("event__is_mobile", "categorical", "BOOLEAN"),
        "item__category": DimensionInfo("item__category", "categorical", "VARCHAR"),
    },
    metric_dimensions=(),
)
T = Translator(CATALOG)


def test_projection_filters_order_limit() -> None:
    sq = T.translate(
        """SELECT "event__platform" AS plataforma, SUM(item_views) AS vistas, MEASURE(item_views_per_user)
           FROM semantic.events
           WHERE metric_time >= DATE '2024-07-01' AND metric_time <= '2024-07-31'
             AND (event__is_mobile = true AND event__platform IN ('ios', 'android'))
           GROUP BY 1 ORDER BY vistas DESC, plataforma LIMIT 10"""
    )
    assert sq.metrics == ["item_views", "item_views_per_user"], sq
    assert sq.group_by == ["event__platform"], sq
    assert (sq.start, sq.end) == (dt.date(2024, 7, 1), dt.date(2024, 7, 31)), sq
    assert sq.where == [
        "{{ Dimension('event__is_mobile') }} = TRUE",
        "{{ Dimension('event__platform') }} IN ('ios', 'android')",
    ], sq.where
    assert sq.order_by == ["-item_views", "event__platform"], sq
    assert sq.limit == 10
    assert sq.output == [
        ("event__platform", "plataforma"), ("item_views", "vistas"), ("item_views_per_user", "item_views_per_user")
    ]


def test_time_constraints() -> None:
    sq = T.translate("SELECT unique_users FROM events WHERE metric_time__month = '2024-03-01'")
    assert (sq.start, sq.end) == (dt.date(2024, 3, 1), dt.date(2024, 3, 1))
    sq = T.translate("SELECT unique_users FROM events WHERE metric_time BETWEEN '2024-01-01' AND '2024-06-01'")
    assert (sq.start, sq.end, sq.where) == (dt.date(2024, 1, 1), dt.date(2024, 6, 1), [])
    sq = T.translate("SELECT unique_users FROM events WHERE '2024-02-01' <= metric_time")
    assert (sq.start, sq.end) == (dt.date(2024, 2, 1), None)
    # Comparación estricta o sobre otra dimensión de tiempo: filtro MetricFlow, no restricción de tiempo
    sq = T.translate("SELECT unique_users FROM events WHERE metric_time__month > '2024-02-01'")
    assert sq.start is None and sq.where == ["{{ TimeDimension('metric_time', 'month') }} > '2024-02-01'"], sq
    sq = T.translate("SELECT unique_users FROM events WHERE event__event_date >= '2024-02-01'")
    assert sq.where == ["{{ TimeDimension('event__event_date') }} >= '2024-02-01'"], sq


def test_dimension_only_and_explain() -> None:
    sq = T.translate("SELECT DISTINCT event__platform FROM events ORDER BY 1")
    assert (sq.metrics, sq.group_by, sq.order_by) == ([], ["event__platform"], ["event__platform"])
    assert split_explain("  EXPLAIN SELECT 1") == (True, "SELECT 1")
    assert T.is_semantic("explain select unique_users from semantic.events")
    assert not T.is_semantic("select * from semantic.metrics")
    assert not T.is_semantic("SET extra_float_digits = 3")


def test_each_table_translates_its_own_columns() -> None:
    assert T.translate("SELECT event__platform, unique_users FROM events").metrics == ["unique_users"]
    sq = T.translate("SELECT item__category, total_item_price FROM semantic.items")
    assert (sq.metrics, sq.group_by) == (["total_item_price"], ["item__category"])
    assert T.is_semantic("select 1 from semantic.items") and T.is_semantic("select 1 from events")
    assert not T.is_semantic("select 1 from dbt_glue_lab")


def test_envoltorios_de_power_bi() -> None:
    """Power BI envuelve la instrucción SQL nativa: select "_".col … from (CONSULTA) "_" limit 1000."""
    nativa = "SELECT event__platform, item_views FROM semantic.events WHERE metric_time >= '2024-01-01'"
    directa = T.translate(nativa)
    for envuelta in (
        f'select "_"."event__platform", "_"."item_views" from ({nativa}) "_" limit 1000',
        f'select * from ({nativa}) as "$Table" limit 1000',
        f'select * from (select "_"."event__platform", "_"."item_views" from ({nativa}) "_") "rows" limit 1000',
    ):
        sq = T.translate(envuelta)
        assert (sq.metrics, sq.group_by, sq.start, sq.limit) == (directa.metrics, directa.group_by, directa.start, 1000)
        assert sq.output == directa.output
    sq = T.translate(f'select "_"."event__platform" as p from ({nativa}) "_" '
                     f'where "_"."event__platform" <> \'web\' order by "_"."event__platform"')
    assert sq.output == [("event__platform", "p")] and sq.order_by == ["event__platform"] and len(sq.where) == 1
    # DirectQuery: agrupar y sumar afuera = la métrica al nivel de las columnas agrupadas
    sq = T.translate(f'select "rows"."event__platform", sum("rows"."item_views") as "a0" from ({nativa}) "rows" '
                     f'group by "event__platform" limit 1000001')
    assert (sq.metrics, sq.group_by, sq.output) == (["item_views"], ["event__platform"],
                                                    [("event__platform", "event__platform"), ("item_views", "a0")])
    for sql, msg in [  # lo que no es un envoltorio aceptado se sigue rechazando
        (f'select count(*) from ({nativa}) "_"', "subconsultas"),
        (f'select "_"."x" from ({nativa}) "_"', "desconocida"),
        (f'select * from ({nativa}) "_" where "_"."item_views" > 3', "métrica"),
    ]:
        try:
            T.translate(sql)
        except TranslationError as e:
            assert msg in str(e), (sql, str(e))
        else:
            raise AssertionError(f"Se esperaba error: {sql}")


def test_rejections() -> None:
    for sql, msg in [
        ("SELECT * FROM events", "SELECT *"),
        ("SELECT foo FROM events", "desconocida"),
        ("SELECT SUM(event__platform) FROM events", "no se puede agregar"),
        ("SELECT unique_users * 2 FROM events", "no soportada"),
        ("SELECT unique_users FROM events WHERE item_views > 0", "métrica"),
        ("SELECT a.unique_users FROM events a JOIN other b ON a.x = b.x", "una sola tabla semántica"),
        ("SELECT unique_users FROM events ORDER BY event__platform", "debe estar en el SELECT"),
        ("SELECT unique_users FROM events LIMIT 5 OFFSET 5", "OFFSET"),
        ("SELECT item__category, unique_users FROM events", "no está en la tabla 'events'; está en: items"),
        ("SELECT event__platform, total_item_price FROM items", "no está en la tabla 'items'; está en: events"),
        ("SELECT unique_users FROM items", "no está en la tabla 'items'; está en: events"),
        ("SELECT total_item_price FROM items WHERE event__platform = 'ios'", "está en: events"),
        ("SELECT unique_users FROM semantic_otra", "una sola tabla semántica"),
    ]:
        try:
            T.translate(sql)
        except TranslationError as e:
            assert msg in str(e), (sql, str(e))
        else:
            raise AssertionError(f"Se esperaba error: {sql}")


if __name__ == "__main__":
    tests = [f for name, f in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
    print(f"OK · {len(tests)} pruebas del traductor")
