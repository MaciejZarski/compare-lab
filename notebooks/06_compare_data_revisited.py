# Databricks notebook source
# MAGIC %run ./00_functions_python

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bakehouse2.sales_transactions

# COMMAND ----------

left = spark.sql("SELECT * FROM bakehouse2.sales_transactions VERSION AS OF 1")
right = spark.sql("SELECT * FROM bakehouse2.sales_transactions VERSION AS OF 2")

result = compare_data(
    left,
    right,
    ['transactionID'],
    ['copied_at'],
    100
)

# COMMAND ----------

display(result)
