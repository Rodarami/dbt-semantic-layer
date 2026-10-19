"""Gateway de capa semántica — prueba de concepto.

Expone las métricas de un `semantic_manifest.json` a herramientas BI por el protocolo de PostgreSQL. MetricFlow solo
COMPILA; con `--ejecutar` el gateway ejecuta el SQL en Athena y devuelve las filas.

    cliente BI ──Postgres wire──► server (riffq) ──► translator (sqlglot) ──► engine (MetricFlow, compila)
                                    └─ catalogo_pg (DuckDB en memoria)            └─► executor (Athena) o SQL compilado

Módulos: server.py (riffq, actual) · server_old.py (buenavista, anterior) · catalogo_pg.py · translator.py ·
engine.py · executor.py · model.py · tls_obligatorio.py (--tls-obligatorio)

Ejecutar desde la raíz del repo:  python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json
"""
