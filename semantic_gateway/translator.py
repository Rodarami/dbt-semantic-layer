"""Traductor SQL (dialecto PostgreSQL) → `SemanticQuery`.

Hay una tabla virtual por modelo semántico; sus columnas son las métricas del modelo y las dimensiones que se pueden
cruzar con todas ellas. Una consulta lee una sola de esas tablas. Reglas:
* Columna de métrica (sola o envuelta en SUM/MAX/MIN/AVG/MEASURE/AGG) → `metrics`. La agregación la decide
  MetricFlow: el envoltorio se ignora, así un ratio nunca se suma.
* Columna de dimensión → `group_by` (GROUP BY es implícito).
* WHERE sobre `metric_time*` con >=, <=, = o BETWEEN y una fecha literal → restricción de tiempo (start/end) y no
  filtro, para que las métricas con offset_window (año anterior) lean sus insumos antes de filtrar.
* Resto del WHERE → filtros MetricFlow, con cada columna reescrita como {{ Dimension('…') }}.
* ORDER BY (por nombre, alias u ordinal) y LIMIT se traspasan. Joins, subconsultas, HAVING y OFFSET se rechazan.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

from .model import METRIC_TIME, Catalog, SemanticQuery, is_metric_time

EXPLAIN_RE = re.compile(r"^\s*explain\s+(.*)$", re.IGNORECASE | re.DOTALL)
AGGREGATES = (exp.Sum, exp.Max, exp.Min, exp.Avg)
MEASURE_FUNCTIONS = {"MEASURE", "AGG"}


class TranslationError(Exception):
    pass


def split_explain(sql: str) -> tuple[bool, str]:
    match = EXPLAIN_RE.match(sql)
    return (True, match.group(1)) if match else (False, sql)


class Translator:
    def __init__(self, catalog: Catalog, read: str = "postgres", warehouse_dialect: str = "duckdb") -> None:
        self.catalog = catalog
        self.read = read
        self.warehouse_dialect = warehouse_dialect

    # ------------------------------------------------------------------ enrutamiento
    def is_semantic(self, sql: str) -> bool:
        """True si la consulta lee una tabla virtual (va al motor); si no, la responde el catálogo."""
        _, body = split_explain(sql)
        try:
            tree = sqlglot.parse_one(body, read=self.read)
        except sqlglot.errors.ParseError:
            return False
        return tree is not None and any(t.name.lower() in self.catalog.tables for t in tree.find_all(exp.Table))

    # ------------------------------------------------------------------ traducción
    def translate(self, sql: str, dialect: Optional[str] = None) -> SemanticQuery:
        """`dialect`: dialecto sqlglot con que se escriben los filtros del WHERE (por defecto el del constructor)."""
        dialect = dialect or self.warehouse_dialect
        try:
            tree = sqlglot.parse_one(sql, read=self.read)
        except sqlglot.errors.ParseError as e:
            raise TranslationError(f"SQL no válido: {e}") from e
        if not isinstance(tree, exp.Select):
            raise TranslationError("Solo se admiten consultas SELECT sobre la tabla semántica")
        tree = _aplanar(tree)
        table = self._check_shape(tree)

        sq = SemanticQuery()
        aliases: dict[str, str] = {}
        for projection in tree.expressions:
            if projection.is_star:
                raise TranslationError("SELECT * no está soportado: elija métricas y dimensiones")
            name, aggregated = self._column(projection.unalias())
            published = projection.alias if isinstance(projection, exp.Alias) else name
            name = _canonico(name)  # metric_time = metric_time__day: MetricFlow no acepta ambas en la misma consulta
            if self.catalog.is_metric(name, table):
                if name not in sq.metrics:
                    sq.metrics.append(name)
            elif self.catalog.is_dimension(name, table):
                if aggregated:
                    raise TranslationError(f"La dimensión '{name}' no se puede agregar")
                if name not in sq.group_by:
                    sq.group_by.append(name)
            else:
                raise self._unknown_column(name, table)
            sq.output.append((name, published))
            aliases[published.lower()] = name

        where = tree.args.get("where")
        if where is not None:
            for condition in self._conjuncts(where.this):
                self._where(condition, sq, dialect, table)

        order = tree.args.get("order")
        if order is not None:
            for ordered in order.expressions:
                name = self._order_name(ordered.this, sq, aliases)
                sq.order_by.append(("-" if ordered.args.get("desc") else "") + name)

        limit = tree.args.get("limit")
        if limit is not None:
            value = limit.args.get("expression")
            if not isinstance(value, exp.Literal) or value.is_string:
                raise TranslationError("LIMIT debe ser un entero")
            sq.limit = int(value.this)
        return sq

    def separar_nucleo(self, sql: str) -> Optional[tuple[str, str]]:
        """Divide una consulta de un BI en *núcleo* y *resto*.

        El núcleo es la única subconsulta que lee una tabla semántica: va a MetricFlow y al motor. El resto es todo lo
        que el BI agrega alrededor (casts, CASE, orden, límites…): se ejecuta en DuckDB sobre el resultado del núcleo,
        leyendo la tabla `NUCLEO`. Devuelve None si la consulta no tiene esa forma.
        """
        try:
            tree = sqlglot.parse_one(sql, read=self.read)
        except sqlglot.errors.ParseError:
            return None
        if tree is None:
            return None
        nucleos = [s for s in tree.find_all(exp.Select) if self._lee_tabla_semantica(s)]
        if len(nucleos) != 1 or nucleos[0] is tree:
            return None
        nucleo = nucleos[0]
        sql_nucleo = nucleo.sql(dialect=self.read)
        nucleo.replace(sqlglot.parse_one(f"SELECT * FROM {NUCLEO}"))
        if any(t.name != NUCLEO for t in tree.find_all(exp.Table)):
            return None  # el resto lee otra tabla: no se acepta
        return sql_nucleo, tree.sql(dialect=self.read)

    def _lee_tabla_semantica(self, select: exp.Select) -> bool:
        origen = select.args.get("from_") or select.args.get("from")
        tabla = origen.this if origen is not None else None
        return isinstance(tabla, exp.Table) and tabla.name.lower() in self.catalog.tables and not select.args.get("joins")

    # ------------------------------------------------------------------ piezas
    def _check_shape(self, tree: exp.Select) -> str:
        """Valida la forma de la consulta y devuelve el nombre de la tabla virtual que lee."""
        tables = list(tree.find_all(exp.Table))
        if len(tables) != 1 or tables[0].name.lower() not in self.catalog.tables:
            raise TranslationError(
                f"La consulta debe leer una sola tabla semántica: {', '.join(sorted(self.catalog.tables))}")
        for arg, label in (("joins", "JOIN"), ("having", "HAVING"), ("offset", "OFFSET"), ("with", "WITH")):
            if tree.args.get(arg):
                raise TranslationError(f"{label} no está soportado en la capa semántica")
        if tree.find(exp.Subquery):
            raise TranslationError("Las subconsultas no están soportadas en la capa semántica")
        return tables[0].name.lower()

    def _unknown_column(self, name: str, table: str) -> TranslationError:
        """Error claro: la columna no existe, o existe pero en otra tabla semántica."""
        others = [t for t in self.catalog.tables_with(name) if t != table]
        if others:
            return TranslationError(
                f"La columna '{name}' no está en la tabla '{table}'; está en: {', '.join(others)}. "
                f"Una consulta solo puede combinar columnas de una misma tabla")
        return TranslationError(f"Columna desconocida en la capa semántica: '{name}'")

    @staticmethod
    def _column(node: exp.Expression) -> tuple[str, bool]:
        aggregated = False
        if isinstance(node, AGGREGATES):
            node, aggregated = node.this, True
        elif isinstance(node, exp.Anonymous) and node.name.upper() in MEASURE_FUNCTIONS and len(node.expressions) == 1:
            node, aggregated = node.expressions[0], True
        if not isinstance(node, exp.Column):
            raise TranslationError(f"Expresión no soportada: {node.sql()} (use columnas de la tabla semántica)")
        return node.name.lower(), aggregated

    @staticmethod
    def _conjuncts(node: exp.Expression) -> list[exp.Expression]:
        node = node.unnest()
        if isinstance(node, exp.And):
            return [c for part in node.flatten() for c in Translator._conjuncts(part)]
        return [node]

    def _where(self, condition: exp.Expression, sq: SemanticQuery, dialect: str, table: str) -> None:
        columns = [c.name.lower() for c in condition.find_all(exp.Column)]
        if not columns:
            raise TranslationError(f"Filtro sin columnas: {condition.sql()}")
        for name in columns:
            if self.catalog.is_metric(name, table):
                raise TranslationError(f"No se puede filtrar por la métrica '{name}' (HAVING no está soportado)")
            if not self.catalog.is_dimension(name, table):
                raise self._unknown_column(name, table)
        if all(is_metric_time(c) for c in columns) and self._time_constraint(condition, sq):
            return
        sq.where.append(self._metricflow_filter(condition, dialect))

    @staticmethod
    def _time_constraint(condition: exp.Expression, sq: SemanticQuery) -> bool:
        start: Optional[dt.date] = None
        end: Optional[dt.date] = None
        if isinstance(condition, exp.Between) and isinstance(condition.this, exp.Column):
            start, end = _date(condition.args.get("low")), _date(condition.args.get("high"))
            if start is None or end is None:
                return False
        elif isinstance(condition, (exp.GTE, exp.LTE, exp.EQ)):
            left, right = condition.this, condition.expression
            op = type(condition)
            if isinstance(right, exp.Column):  # literal op columna → columna op' literal
                left, right = right, left
                op = {exp.GTE: exp.LTE, exp.LTE: exp.GTE}.get(op, op)
            value = _date(right)
            if not isinstance(left, exp.Column) or value is None:
                return False
            start = value if op in (exp.GTE, exp.EQ) else None
            end = value if op in (exp.LTE, exp.EQ) else None
        else:
            return False
        if start is not None:
            sq.start = max(sq.start, start) if sq.start else start
        if end is not None:
            sq.end = min(sq.end, end) if sq.end else end
        return True

    def _metricflow_filter(self, condition: exp.Expression, dialect: str) -> str:
        def to_jinja(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node
            name = node.name.lower()
            if is_metric_time(name):
                grain = name.split("__", 1)[1] if "__" in name else "day"
                return exp.Var(this=f"{{{{ TimeDimension('{METRIC_TIME}', '{grain}') }}}}")
            if self.catalog.dimensions[name].kind == "time":
                return exp.Var(this=f"{{{{ TimeDimension('{name}') }}}}")
            return exp.Var(this=f"{{{{ Dimension('{name}') }}}}")

        # MetricFlow procesa el filtro como plantilla Jinja: un valor del usuario con `{{`, `{%` o `{#` se evaluaría y
        # podría cerrar la comilla del literal e inyectar SQL en la consulta del motor. Se revisa el texto del usuario
        # (sin los `{{ Dimension(…) }}` que agrega el traductor).
        texto_usuario = condition.sql(dialect=dialect)
        if _JINJA.search(texto_usuario):
            raise TranslationError("Los filtros no pueden contener '{{', '{%' ni '{#'")
        return condition.transform(to_jinja).sql(dialect=dialect)

    def _order_name(self, node: exp.Expression, sq: SemanticQuery, aliases: dict[str, str]) -> str:
        if isinstance(node, exp.Literal) and not node.is_string:
            index = int(node.this) - 1
            if not 0 <= index < len(sq.output):
                raise TranslationError(f"ORDER BY {node.this} fuera de rango")
            return sq.output[index][0]
        name, _ = self._column(node)
        name = _canonico(aliases.get(name, name))
        if name not in sq.metrics and name not in sq.group_by:
            raise TranslationError(f"ORDER BY '{name}': la columna debe estar en el SELECT")
        return name


NUCLEO = "__nucleo_semantico"  # tabla con el resultado del núcleo, para ejecutar el resto en DuckDB
_AGREGACIONES = (exp.Sum, exp.Max, exp.Min, exp.Avg)  # aceptadas sobre una métrica (la agregación la define la métrica)


def _salida(proyeccion: exp.Expression) -> str:
    """Nombre con que una columna sale de una consulta: su alias o el nombre de la columna."""
    if isinstance(proyeccion, exp.Alias):
        return proyeccion.alias.lower()
    if isinstance(proyeccion, exp.Column):
        return proyeccion.name.lower()
    raise TranslationError(f"Expresión no soportada: {proyeccion.sql()} (use columnas de la tabla semántica)")


def _aplanar(tree: exp.Select) -> exp.Select:
    """Quita los envoltorios que agregan los BI alrededor de la consulta del usuario.

    Power BI, con una instrucción SQL nativa, envía `select "_"."a", "_"."b" from (CONSULTA) "_" limit 1000` para la
    vista previa y para leer las columnas. Si la consulta externa solo elige columnas de la interna, filtra, ordena o
    limita, se reescribe como una sola consulta sobre la tabla semántica. Cualquier otra cosa (agregar, agrupar, joins)
    se sigue rechazando como subconsulta.
    """
    while True:
        origen = tree.args.get("from_") or tree.args.get("from")  # "from_" en sqlglot recientes
        sub = origen.this if origen is not None else None
        if not (isinstance(sub, exp.Subquery) and isinstance(sub.this, exp.Select)):
            return tree
        for arg in ("joins", "having", "offset", "with"):
            if tree.args.get(arg):
                return tree  # no es un simple envoltorio: _check_shape lo rechaza
        interna = sub.this.copy()
        por_nombre = {_salida(p): p for p in interna.expressions}

        def interna_de(columna: exp.Column) -> exp.Expression:
            nombre = columna.name.lower()
            if nombre not in por_nombre:
                raise TranslationError(f"Columna desconocida en la capa semántica: '{columna.name}'")
            return por_nombre[nombre]

        proyecciones = []
        for p in tree.expressions:
            if p.is_star:
                proyecciones.extend(e.copy() for e in interna.expressions)
                continue
            nombre_salida = p.alias if isinstance(p, exp.Alias) else None
            col = p.unalias()
            if isinstance(col, _AGREGACIONES) and isinstance(col.this, exp.Column):
                # DirectQuery: SUM("_"."metrica") agrupado afuera = la métrica al nivel de las columnas agrupadas
                base = col.__class__(this=interna_de(col.this).unalias().copy())
                proyecciones.append(exp.alias_(base, nombre_salida or col.this.name, quoted=True))
                continue
            if not isinstance(col, exp.Column):
                return tree  # el envoltorio calcula otra cosa (COUNT(*), expresiones…): no se aplana
            base = interna_de(col).unalias().copy()
            nombre = nombre_salida or col.name
            proyecciones.append(base if isinstance(base, exp.Column) and base.name == nombre
                                else exp.alias_(base, nombre, quoted=True))
        interna.set("expressions", proyecciones)

        def a_interna(nodo: exp.Expression) -> exp.Expression:
            if isinstance(nodo, exp.Column):
                return interna_de(nodo).unalias().copy()
            return nodo

        donde = tree.args.get("where")
        if donde is not None:
            condicion = donde.this.transform(a_interna)
            previa = interna.args.get("where")
            interna.set("where", exp.Where(this=exp.and_(previa.this, condicion) if previa else condicion))
        orden = tree.args.get("order")
        if orden is not None:
            interna.set("order", orden.transform(a_interna))
        if tree.args.get("distinct"):
            interna.set("distinct", tree.args["distinct"].copy())
        # GROUP BY de afuera: las dimensiones del SELECT ya definen la agrupación (es implícita en la capa semántica)
        limite = tree.args.get("limit")
        if limite is not None:
            previo = interna.args.get("limit")
            externo, interno = limite.args.get("expression"), previo.args.get("expression") if previo else None
            if (isinstance(interno, exp.Literal) and isinstance(externo, exp.Literal)
                    and int(interno.this) <= int(externo.this)):
                pass  # el límite de adentro ya es menor
            else:
                interna.set("limit", limite.copy())
        tree = interna


def _canonico(name: str) -> str:
    """`metric_time` sin grano es `metric_time__day` (el grano por defecto). Pedir las dos (Power BI lo hace al cargar
    una tabla completa) haría que MetricFlow rechace la consulta por columnas duplicadas."""
    return f"{METRIC_TIME}__day" if name == METRIC_TIME else name


_JINJA = re.compile(r"\{[{%#]")  # inicio de expresión, bloque o comentario Jinja


def _date(node: Optional[exp.Expression]) -> Optional[dt.date]:
    if isinstance(node, exp.Cast):
        node = node.this
    if isinstance(node, exp.Literal) and node.is_string:
        try:
            return dt.date.fromisoformat(node.this[:10])
        except ValueError:
            return None
    return None
