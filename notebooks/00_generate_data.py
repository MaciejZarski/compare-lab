# Databricks notebook source
# Install faker package
%pip install faker

# COMMAND ----------

# DBTITLE 1,Untitled
# 1. Imports ----------------------------------------------------
import builtins
import math
import random

from datetime import date, datetime, timedelta
from faker import Faker

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, LongType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS compare;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS compare.retail_transactions (
# MAGIC   TransactionID STRING,
# MAGIC   CustomerID STRING,
# MAGIC   ProductID STRING,
# MAGIC   Quantity INT,
# MAGIC   Price DOUBLE,
# MAGIC   TransactionDate DATE,
# MAGIC   InsertTimestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Untitled
# 2. Base dates -------------------------------------------------
base_date = date(2025, 12, 1)                     # java.sql.Date equivalent
base_timestamp = datetime(2026, 1, 1, 10, 0, 0)   # java.sql.Timestamp equivalent

# 3. Random offsets (same logic as the Scala version) ----------
#   randomMinutes, randomSeconds, randomMillis are used to build a single offset
random_minutes = random.randint(0, 59)          # nextInt(60)
random_seconds = random.randint(0, 59)          # nextInt(60)
random_millis  = random.randint(0, 998)         # nextInt(999) → 0‑998

# 4. Build the list of rows ------------------------------------
rows = []
for i in range(1, 11):  # (1 to 10).map in Scala
    # Random nanos (same formula as Scala)
    random_nanos = (random_millis + random.randint(0, 1)) * 1000 + random.randint(0, 999_999)
    # Convert nanos → microseconds (Python datetime only supports microseconds)
    random_micros = random_nanos // 1000

    # Build the INSERT timestamp with the generated offset
    insert_ts = base_timestamp + timedelta(
        minutes=random_minutes,
        seconds=random_seconds,
        microseconds=random_micros
    )

    # Transaction date: base_date + i days
    trans_date = base_date + timedelta(days=i)

    # Append a tuple that matches the target schema
    rows.append((
        f"TID{i}",
        f"CUST{100 + i}",
        f"PROD{200 + i}",
        i * 2,                  # Quantity
        10.0 + i,               # Price
        trans_date,             # TransactionDate (date type)
        insert_ts               # InsertTimestamp (timestamp type)
    ))

# 5. Create the DataFrame ---------------------------------------
# Spark can infer the schema from the Python types, but we list it explicitly for clarity
schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("TransactionDate", DateType(), True),
    StructField("InsertTimestamp", TimestampType(), True)]) 

df = spark.createDataFrame(rows, schema)

# 6. Persist as a Delta table ----------------------------------
df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("compare.retail_transactions")

# 7. Optional: Verify the result --------------------------------
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compare.retail_transactions ORDER BY InsertTimestamp

# COMMAND ----------

# MAGIC %sql  
# MAGIC CREATE TABLE IF NOT EXISTS compare.strings_test (
# MAGIC   col1 STRING,
# MAGIC   col2 STRING,
# MAGIC   col3 STRING,
# MAGIC   col4 STRING,
# MAGIC   col5 STRING,
# MAGIC   col6 STRING,
# MAGIC   col7 STRING,
# MAGIC   col8 STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM compare.strings_test;
# MAGIC
# MAGIC INSERT INTO compare.strings_test VALUES 
# MAGIC ('e', 'E', 'E ', 'E\u00A0', '\u00E9', '\u0065\u0301', 'suffix', 'su\ufb03x');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS compare.arrays_test (
# MAGIC   normal_array ARRAY<INT>,
# MAGIC   reverse_array ARRAY<INT>,
# MAGIC   empty_array ARRAY<INT>, 
# MAGIC   null_array  ARRAY<INT>
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compare.arrays_test VALUES (array(1, 2, 3), array(3, 2, 1), array(), CAST(NULL AS ARRAY<INT>))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS compare.maps_test (
# MAGIC   normal_map MAP<INT, STRING>,
# MAGIC   reverse_map MAP<INT, STRING>
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compare.maps_test VALUES (map(1, "first value", 2, "second value"), map(2, "second value", 1, "first value"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'media_customer_reviews' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.media_customer_reviews
# MAGIC UNION ALL 
# MAGIC SELECT 'media_gold_reviews_chunked' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.media_gold_reviews_chunked
# MAGIC UNION ALL 
# MAGIC SELECT 'sales_customers' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.sales_customers
# MAGIC UNION ALL 
# MAGIC SELECT 'sales_franchises' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.sales_franchises
# MAGIC UNION ALL 
# MAGIC SELECT 'sales_suppliers' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.sales_suppliers
# MAGIC UNION ALL 
# MAGIC SELECT 'sales_transactions' AS table_name, COUNT(*) AS row_count FROM samples.bakehouse.sales_transactions

# COMMAND ----------

tables = [row.tableName for row in spark.sql("SHOW TABLES IN bakehouse2").collect()]
for tbl in tables:
    spark.sql(f"DROP TABLE IF EXISTS bakehouse2.{tbl}")

# COMMAND ----------

# DBTITLE 1,Cell 14
# 1. Create bakehouse2 database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS bakehouse2")

# 2. Copy all tables from bakehouse to bakehouse2, modifying schema
tables = [row.tableName for row in spark.sql("SHOW TABLES IN samples.bakehouse").collect()]

for tbl in tables:
    df = spark.table(f"samples.bakehouse.{tbl}")
    # Example modifications:
    # - Cast first Integer column to String (if exists)
    # - Remove last column
    # - Add a new column "copied_at" (timestamp)
    fields = df.schema.fields
    cols = []
    for i, field in enumerate(fields):
        if i == 0 and (isinstance(field.dataType, IntegerType) or isinstance(field.dataType, LongType)):
            cols.append(F.col(field.name).cast(StringType()).alias(field.name))
        elif i == len(fields) - 1:
            continue  # remove last column
        else:
            cols.append(F.col(field.name))
    df_mod = df.select(*cols).withColumn("copied_at", F.current_timestamp())
    df_mod.write.format("delta").mode("overwrite").saveAsTable(f"bakehouse2.{tbl}")

# 3. Delete ~1% of rows (random number) from each table in bakehouse2
for tbl in tables:
    df = spark.table(f"bakehouse2.{tbl}")
    total_rows = df.count()
    num_to_delete = builtins.max(1, math.floor(total_rows * random.uniform(0.009, 0.011)))
    # Add a random column, filter out num_to_delete rows
    df_with_rand = df.withColumn("rand", F.rand())
    rows_to_keep = df_with_rand.orderBy("rand").limit(total_rows - num_to_delete).drop("rand")
    rows_to_keep.write.format("delta").mode("overwrite").saveAsTable(f"bakehouse2.{tbl}")

# 4. Insert similar number of random rows into each table in bakehouse2
fake = Faker()
for tbl in tables:
    df = spark.table(f"bakehouse2.{tbl}")
    total_rows = df.count()
    num_to_insert = builtins.max(1, math.floor(total_rows * random.uniform(0.009, 0.011)))
    schema = df.schema
    # Generate random rows using Faker (basic logic, adjust per table schema as needed)
    random_rows = []
    for _ in range(num_to_insert):
        row = []
        for field in schema:
            if field.dataType == StringType():
                row.append(fake.uuid4())
            elif field.dataType == IntegerType():
                row.append(random.randint(1, 100))
            elif field.dataType == DoubleType():
                row.append(round(random.uniform(1.0, 100.0), 2))
            elif field.dataType == DateType():
                row.append(fake.date_this_decade())
            elif field.dataType == TimestampType():
                row.append(fake.date_time_this_decade())
            else:
                row.append(None)
        random_rows.append(tuple(row))
    random_df = spark.createDataFrame(random_rows, schema)
    random_df.write.format("delta").mode("append").saveAsTable(f"bakehouse2.{tbl}")

#5. Generate a new table
spark.sql(f"""CREATE OR REPLACE TABLE bakehouse2.{tables[0]}_new AS SELECT * FROM samples.bakehouse.{tables[0]}""")
