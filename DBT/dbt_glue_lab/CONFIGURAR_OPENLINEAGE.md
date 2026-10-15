# Configuración de OpenLineage para SageMaker Unified Studio

## Requisitos Previos

Para que el linaje de datos funcione en SageMaker Unified Studio, necesitas:

1. **JAR de OpenLineage**: El JAR debe estar disponible en la sesión de Glue
2. **Permisos IAM**: El rol `GlueInteractiveSessionRole` debe tener permisos para publicar eventos de linaje a AWS DataZone
3. **Domain ID de DataZone**: Ya configurado (`dzd-b0rxz2ap7djw13`)

## Paso 1: Descargar el JAR de OpenLineage

1. Ve a [Maven Central - OpenLineage Spark](https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/)
2. Descarga la versión **1.22.0 o superior** (compatible con SageMaker Unified Studio)
   - Ejemplo: `openlineage-spark_2.12-1.22.0.jar`
3. Sube el JAR a tu bucket de S3:
   ```bash
   aws s3 cp openlineage-spark_2.12-1.22.0.jar s3://<BUCKET>/jars/
   ```

## Paso 2: Configurar el JAR en profiles.yml

Descomenta y actualiza la línea `extra_jars` en `profiles.yml`:

```yaml
extra_jars: ["s3://<BUCKET>/jars/openlineage-spark_2.12-1.22.0.jar"]
```

## Paso 3: Verificar Permisos IAM

El rol `GlueInteractiveSessionRole` debe tener estos permisos adicionales:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "datazone:PostLineageEvent",
        "datazone:GetLineageEvent"
      ],
      "Resource": "*"
    }
  ]
}
```

## Paso 4: Verificar la Configuración

Los parámetros importantes en `profiles.yml` son:

- ✅ `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`
- ✅ `spark.openlineage.transport.type=amazon_datazone_api`
- ✅ `spark.openlineage.transport.domainId=dzd-b0rxz2ap7djw13`
- ✅ `spark.glue.accountId=<ID_CUENTA>`
- ✅ `spark.openlineage.namespace=test-dbt-glue`
- ✅ `spark.openlineage.applicationName=dbt-glue-lab`
- ✅ `spark.openlineage.columnLineage.datasetLineageEnabled=true`

## Paso 5: Probar el Linaje

1. Ejecuta `dbt run` para ejecutar los modelos
2. Verifica en los logs de Glue que aparezca:
   ```
   INFO SparkContext: Registered listener io.openlineage.spark.agent.OpenLineageSparkListener
   ```
3. Revisa en SageMaker Unified Studio → Data Lineage para ver el linaje generado

## Troubleshooting

### El linaje no aparece en SageMaker Unified Studio

1. **Verifica que el JAR esté disponible**: Revisa los logs de Glue para errores relacionados con OpenLineage
2. **Verifica permisos**: Asegúrate de que el rol tenga permisos para `datazone:PostLineageEvent`
3. **Verifica el Domain ID**: Confirma que `dzd-b0rxz2ap7djw13` sea el ID correcto de tu dominio de DataZone
4. **Verifica el namespace**: El namespace debe coincidir con el configurado en SageMaker Unified Studio

### Error: "Cannot find class io.openlineage.spark.agent.OpenLineageSparkListener"

- El JAR de OpenLineage no está disponible en la sesión de Glue
- Asegúrate de haber configurado `extra_jars` correctamente
- Verifica que el JAR esté accesible desde S3

### El linaje aparece pero está incompleto

- Verifica que `spark.openlineage.columnLineage.datasetLineageEnabled=true` esté configurado
- Asegúrate de que las tablas tengan nombres únicos y correctos
- Verifica que los `sourceIdentifier` coincidan con los assets en DataZone

## Referencias

- [SageMaker Unified Studio - Data Lineage](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/datazone-data-lineage-automate-capture-from-tools.html)
- [OpenLineage Spark Integration](https://openlineage.io/docs/integrations/spark/quickstart/quickstart_glue/)
