import json
from pyspark.sql.functions import col

def run(spark, dataset, renaming, type_cast):
  dataset = dataset.drop("source_id", "dataset_id","alias")
  renaming = json.loads(renaming)
  type_cast = json.loads(type_cast)
  SQL_EXPRESSIONS = []
  for aColumn in renaming.keys():
    SQL_EXPRESSIONS.append("cast(" + aColumn + " as " + type_cast[aColumn] + ") as " + renaming[aColumn])
  dataset = dataset.selectExpr(*SQL_EXPRESSIONS)
  return dataset
