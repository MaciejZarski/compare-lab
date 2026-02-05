# Databricks notebook source
# MAGIC %run ./00_functions_python

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# Create first sample DataFrame
data1 = [
    Row(id="1", first_name="Alice", last_name="White", city="New York"),
    Row(id="2", first_name="Bob", last_name="Black", city="San Francisco")
]
schema1 = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])
df1 = spark.createDataFrame(data1, schema1)

# Create second sample DataFrame
data2 = [
    Row(id=3, first_name="Charlie", last_name="Blue", age=25, city="Detroit"),
    Row(id=4, first_name="David", last_name="Green", age=30, city="Chicago")
]
schema2 = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])
df2 = spark.createDataFrame(data2, schema2)

# Compare their structures
df1.printSchema()
df2.printSchema()

# COMMAND ----------

left = df1
right = df2
diff_struct = compare_structures(left, right)
display(diff_struct)
