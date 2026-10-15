"""Gateway de capa semántica para dbt_glue_lab (Glue / Iceberg) — prueba de concepto.

Expone las métricas de `semantic_manifest.json` a herramientas de BI por el protocolo de PostgreSQL.
MetricFlow solo COMPILA (no necesita conexión). El gateway imprime el SQL y no ejecuta nada.

    cliente BI ──Postgres wire──► server (buenavista) ──► translator (sqlglot) ──► engine (MetricFlow, compila)
                                     └─ catálogo (DuckDB en memoria)                 └─► SQL del motor (se imprime)

Ejecutar desde la raíz MetricFlow/:  python -m semantic_gateway --manifest semantic_manifests/dbt_glue_lab/semantic_manifest.json
"""
