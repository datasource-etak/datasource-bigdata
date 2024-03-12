import json
from pyspark.sql.functions import col
def run(spark, dataset, renaming, type_cast, join_type, join_keys, previous_join):
  dataset = dataset.drop("source_id", "dataset_id","alias")
  renaming = json.loads(renaming)
  type_cast = json.loads(type_cast)
  join_keys = list(json.loads(join_keys))
  SQL_EXPRESSIONS = []
  for aColumn in renaming.keys():
    SQL_EXPRESSIONS.append("cast(" + aColumn + " as " + type_cast[aColumn] + ") as " + renaming[aColumn])
  dataset = dataset.selectExpr(*SQL_EXPRESSIONS)
  new_join = previous_join.join(dataset, on=join_keys, how=join_type)
  return new_join