#!/usr/bin/env python3
"""Reescribe las rutas de tabla de un semantic_manifest.json generado con el perfil de Glue
para que apunten a tablas de Snowflake (BASE.ESQUEMA.TABLA).

No necesita dbt-snowflake ni conexión: sólo modifica el JSON (bloque `node_relation`
de cada semantic model).

Ejemplos:
  python3 script/manifest_para_snowflake.py --database ANALYTICS
  python3 script/manifest_para_snowflake.py --database ANALYTICS \
      --schema-map lab_data_silver=SILVER,lab_data_gold=GOLD \
      --input semantic_manifests/dbt_glue_lab/semantic_manifest.json --output semantic_manifests/dbt_glue_lab/semantic_manifest_snowflake.json
"""
import argparse
import json
import sys


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--database", required=True, help="Base de Snowflake donde estarán las tablas")
    p.add_argument("--schema-map", default="", help="Reemplazo de esquemas: origen=destino,origen2=destino2")
    p.add_argument("--input", default="semantic_manifests/dbt_glue_lab/semantic_manifest.json")
    p.add_argument("--output", default="semantic_manifests/dbt_glue_lab/semantic_manifest_snowflake.json")
    a = p.parse_args()

    schema_map = dict(kv.split("=", 1) for kv in a.schema_map.split(",") if kv)

    with open(a.input, encoding="utf-8") as f:
        manifest = json.load(f)

    for sm in manifest["semantic_models"]:
        rel = sm["node_relation"]
        schema = schema_map.get(rel["schema_name"], rel["schema_name"])
        rel["database"] = a.database
        rel["schema_name"] = schema
        rel["relation_name"] = f'{a.database}.{schema}.{rel["alias"]}'
        print(f'{sm["name"]:<10} -> {rel["relation_name"]}')

    with open(a.output, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"\nEscrito: {a.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
