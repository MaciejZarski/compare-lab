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

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED samples.bakehouse.sales_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED workspace.bakehouse2.sales_customers

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col, when, concat, lit

struct1 = spark.sql("DESCRIBE EXTENDED samples.bakehouse.sales_customers")
struct1 = struct1.withColumn("row_number", monotonically_increasing_id() + 1)
delta_stats_row = struct1.where("col_name = '# Delta Statistics Columns'").collect()[0]

row_number = int(delta_stats_row["row_number"])

struct1 = struct1.withColumn(
    "col_name",
    when(col("row_number") >= row_number, concat(lit("p:"), col("col_name"))).otherwise(concat(lit("c:"), col("col_name")))
).withColumnsRenamed(
    {
        "col_name": "property_name",
        "data_type": "property_value"
    }
).drop("comment").filter(
    (col("col_name") != "c:") &
    (col("col_name") != "p:") &    
    (col("col_name") != "#") &
    (~col("col_name").startswith("p:#"))
)

# COMMAND ----------

display(struct1)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col, when, concat, lit

struct2 = spark.sql("DESCRIBE EXTENDED workspace.bakehouse2.sales_customers")
struct2 = struct2.withColumn("row_number", monotonically_increasing_id() + 1)
delta_stats_row = struct2.where("col_name = '# Delta Statistics Columns'").collect()[0]

row_number = int(delta_stats_row["row_number"])

struct2 = struct2.withColumn(
    "col_name",
    when(col("row_number") >= row_number, concat(lit("p:"), col("col_name"))).otherwise(concat(lit("c:"), col("col_name")))
).withColumnsRenamed(
    {
        "col_name": "property_name",
        "data_type": "property_value"
    }
).drop("comment").filter(
    (col("col_name") != "c:") &
    (col("col_name") != "p:") &    
    (col("col_name") != "#") &
    (~col("col_name").startswith("p:#"))
)

# COMMAND ----------

left = struct1
right = struct2
keys = ['property_name']
ignore_columns = ['row_number']
rows_limit = 100

diff_data = compare_data(left, right, keys, ignore_columns, rows_limit)
display(diff_data)
