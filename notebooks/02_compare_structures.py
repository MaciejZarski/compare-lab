# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# Create first sample DataFrame
data1 = [
    Row("1", "Alice", "White", "New York"),
    Row("2", "Bob", "Black", "San Francisco")
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
    Row(3, "Charlie", "Blue", 25, "Detroit"),
    Row(4, "David", "Green", 30, "Chicago")
]
schema2 = StructType([
    StructField("id", IntegerType(), nullable=False), #the same name and position, different type
    StructField("first_name", StringType(), nullable=False), #the same name, position, and type, different nullability
    StructField("last_name", StringType(), nullable=True), #the same name, position, type, and nullability
    StructField("age", IntegerType(), nullable=True), #new column
    StructField("city", StringType(), nullable=True) #the same name, type, and nullability, different position
])
df2 = spark.createDataFrame(data2, schema2)

# Compare their structures
df1.printSchema()
df2.printSchema()

# COMMAND ----------

df1.schema == df2.schema

# COMMAND ----------

# DBTITLE 1,Compare schema fields: differences
fields1 = set(df1.schema.fields)
fields2 = set(df2.schema.fields)
fields1 - fields2

#Scala: df1.schema.diff(df2.schema)

# COMMAND ----------

df1_diff_df2 = list(set(df1.schema.fields) - set(df2.schema.fields))
df2_diff_df1 = list(set(df2.schema.fields) - set(df1.schema.fields))

print("Columns from df1 that differ from df2:")
[print(f.json()) for f in df1_diff_df2]

print("-" * 100)

print("Columns from df2 that differ from df1:")
[print(f.json()) for f in df2_diff_df1]

# COMMAND ----------

# Create 3rd sample DataFrame
data3 = [
    Row("1", "Alice", "White"),
    Row("2", "Bob", "Black")
]
schema3 = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True)
])
df3 = spark.createDataFrame(data3, schema3)

# Create 4th sample DataFrame
data4 = [
    Row("3", "Blue", "Charlie"),
    Row("4", "Green", "David")
]
schema4 = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("first_name", StringType(), nullable=True)
])
df4 = spark.createDataFrame(data4, schema4)

df3.printSchema()
df4.printSchema()

# COMMAND ----------

df3_diff_df4 = list(set(df3.schema.fields) - set(df4.schema.fields))
df4_diff_df3 = list(set(df4.schema.fields) - set(df3.schema.fields))

print("Columns from df3 that differ from df4:")
[print(f.json()) for f in df3_diff_df4]

print("-" * 100)

print("Columns from df3 that differ from df4:")
[print(f.json()) for f in df4_diff_df3]

# COMMAND ----------

fields3 = [(i, f.name, f.dataType, f.nullable) for i, f in enumerate(df3.schema.fields)]
fields4 = [(i, f.name, f.dataType, f.nullable) for i, f in enumerate(df4.schema.fields)]

df3_diff_df4 = list(set(fields3) - set(fields4))
df4_diff_df3 = list(set(fields4) - set(fields3))

print("Columns from df3 that differ from df4:")
[print(f) for f in df3_diff_df4]

print("-" * 100)

print("Columns from df3 that differ from df4:")
[print(f) for f in df4_diff_df3]

# COMMAND ----------

# %scala
# val fields3 = df3.schema.fields.map(f => (f.name, f.dataType, f.nullable))
# val fields4 = df4.schema.fields.map(f => (f.name, f.dataType, f.nullable))
# val comparison_same_elements = fields3.sameElements(fields4)
# val comparison_as_sets = fields3.toSet == fields4.toSet
