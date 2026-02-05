# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create first sample DataFrame
data1 = [
    Row(1, "Alice", "White", "New York"),
    Row(2, "Bob", "Black", "San Francisco"),
    Row(5, "Ann", "Yellow", "New York")
]
schema1 = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])
df1 = spark.createDataFrame(data1, schema1)

# Create second sample DataFrame
data2 = [
    Row(3, "Charlie", "Blue", 25, "Detroit"),
    Row(2, "Bob", "Black", 40, "San Francisco"),
    Row(5, "Anna", "yellow", 52, "New York"),
    Row(4, "David", "Green", 30, "Chicago")
]
schema2 = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.createOrReplaceTempView("df1_vw")
df2.createOrReplaceTempView("df2_vw")

# COMMAND ----------

# %sql
# SELECT * FROM df1_vw
# EXCEPT 
# SELECT * FROM df2_vw

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC SELECT id, first_name, last_name, city FROM df1_vw
# MAGIC EXCEPT 
# MAGIC SELECT id, first_name, last_name, city FROM df2_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, first_name, last_name, city FROM df2_vw
# MAGIC EXCEPT 
# MAGIC SELECT id, first_name, last_name, city FROM df1_vw

# COMMAND ----------

# MAGIC %run ./00_functions_python

# COMMAND ----------

left = spark.read.table("df1_vw")
right = spark.read.table("df2_vw")
keys = ['id']
ignore_columns = []
rows_limit = 100

diff_data = compare_data(left, right, keys, ignore_columns, rows_limit)
display(diff_data)

# COMMAND ----------

left = df1
right = df2

diff_stats = compare_stats(left, right)
display(diff_stats)
