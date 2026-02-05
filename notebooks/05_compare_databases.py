# Databricks notebook source
# MAGIC %run ./00_functions_python

# COMMAND ----------

# MAGIC %md
# MAGIC #Table list

# COMMAND ----------

left = spark.sql("SHOW TABLES IN samples.bakehouse")
right = spark.sql("SHOW TABLES IN bakehouse2")
keys = ['tableName']
ignore_columns = ['database']
rows_limit = 100

diff_data = compare_data(left, right, keys, ignore_columns, rows_limit)
display(diff_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #Table structure

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG samples;

# COMMAND ----------

left_tables = spark.sql("SHOW TABLES IN samples.bakehouse").select("tableName").collect()
left_views = spark.sql("SHOW VIEWS IN samples.bakehouse").select("viewName").collect()

left_table_names = set([r.asDict()['tableName'] for r in left_tables]) - set([r.asDict()['viewName'] for r in left_views])
left_table_names

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace

# COMMAND ----------

right_tables = spark.sql("SHOW TABLES IN bakehouse2").select("tableName").collect()
right_views = spark.sql("SHOW VIEWS IN bakehouse2").select("viewName").collect()

right_table_names = set([r.asDict()['tableName'] for r in right_tables]) - set([r.asDict()['viewName'] for r in right_views])
right_table_names

# COMMAND ----------

common_table_names = left_table_names.intersection(right_table_names)
common_table_names

# COMMAND ----------

d1 = compare_structures(spark.read.table('samples.bakehouse.sales_customers'), spark.read.table('bakehouse2.sales_customers'))

# COMMAND ----------

display(d1)

# COMMAND ----------

from pyspark.sql.functions import lit

d2 = compare_structures_with_table_name('sales_customers', spark.read.table('samples.bakehouse.sales_customers'), spark.read.table('bakehouse2.sales_customers'))

# COMMAND ----------

display(d2)

# COMMAND ----------

structures_compare_result = [compare_structures_with_table_name(tbl, spark.read.table(f'samples.bakehouse.{tbl}'), spark.read.table(f'bakehouse2.{tbl}')) for tbl in common_table_names]

stats_compare_result = [compare_stats_with_table_name(tbl, spark.read.table(f'samples.bakehouse.{tbl}'), spark.read.table(f'bakehouse2.{tbl}')) for tbl in common_table_names]

# COMMAND ----------

from functools import reduce

structures_compare_result_reduced = reduce(lambda x, y: x.union(y), structures_compare_result)

stats_compare_result_reduced = reduce(lambda x, y: x.union(y), stats_compare_result)

# COMMAND ----------

display(structures_compare_result_reduced)

# COMMAND ----------

display(stats_compare_result_reduced)

# COMMAND ----------

common_table_keys = {
    'sales_customers':['customerID'],
    'sales_franchises':['franchiseID'],
    'sales_suppliers':['supplierID'],
    'sales_transactions':['transactionID']
}

data_compare_result = [
    compare_data(
        spark.read.table(f'samples.bakehouse.{tbl}').withColumn(common_table_keys[tbl][0], col(common_table_keys[tbl][0]).cast(StringType())),
        spark.read.table(f'bakehouse2.{tbl}').withColumn(common_table_keys[tbl][0], col(common_table_keys[tbl][0]).cast(StringType())),
        common_table_keys[tbl],
        ['copied_at'],
        100
    )
    for tbl in common_table_keys.keys()
]

# COMMAND ----------

for df in data_compare_result:
    display(df)
