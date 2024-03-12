import json
from pyspark.sql.functions import col, monotonically_increasing_id, lit, concat
def run(spark, message_type, slug, alias, previous_join):
  dataset = previous_join.withColumn("message_type", lit(message_type))
  dataset = dataset.withColumn("slug", lit(slug))
  dataset = dataset.withColumn("dataset_id", lit(message_type))
  dataset = dataset.withColumn("message_id", concat(col("message_type"), monotonically_increasing_id().cast("string")))
  dataset = dataset.withColumn("alias", lit(alias))
  catalog = """
    {
        "table": {
            "namespace": \""""+slug + "_db_el"+"""\",
            "name": "Events"
        },
        "rowkey": "key",
        "columns": {
            "message_id":{"cf":"rowkey", "col":"key", "type":"string"},"""        
  result_dataset_schema = {}
  for column in dataset.schema.fields:
    if column.name != "message_id":
      catalog+="""
                \""""+column.name+"""\":{"cf":"messages", "col":\""""+column.name+"""\", "type":"string"},"""
      result_dataset_schema[column.name] = column.dataType.simpleString()
      dataset = dataset.withColumn(column.name, col(column.name).cast("string"))
  catalog=catalog[0:-1]
  catalog+="""
        }
    }"""
  dataset.write.options(catalog=catalog, newtable=5).format("org.apache.spark.sql.execution.datasources.hbase").save()
  print(json.dumps(result_dataset_schema))
  return ""
