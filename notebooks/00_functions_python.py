# Databricks notebook source
from pyspark.sql.functions import col, min, max, avg, sum, count, countDistinct, lower, sort_array, lit, when, array, concat, coalesce, uuid

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

def compare_data(left_df, right_df, keys, ignore_columns, rows_limit, show_all=False):
    # Identify common columns, excluding keys and ignored columns
    common_cols = [c for c in left_df.columns if c in right_df.columns and c not in keys and c not in ignore_columns]
    left_renamed = [f"{c}_left" for c in common_cols]
    right_renamed = [f"{c}_right" for c in common_cols]

    # Prepare left DataFrame
    left_sel = [col(k) for k in keys] + [col(c).alias(f"{c}_left") for c in common_cols]
    left_df_mod = left_df.drop(*ignore_columns).select(*left_sel).withColumn("__left", lit(1)).withColumn("left_uuid", uuid())

    # Prepare right DataFrame
    right_sel = [col(k) for k in keys] + [col(c).alias(f"{c}_right") for c in common_cols]
    right_df_mod = right_df.drop(*ignore_columns).select(*right_sel).withColumn("__right", lit(1)).withColumn("right_uuid", uuid())

    # Join DataFrames
    joined = left_df_mod.join(right_df_mod, keys, "outer")

    # Diff column
    diff_col = when(col("__left").isNotNull() & col("__right").isNull(), lit("left only")) \
        .when(col("__left").isNull() & col("__right").isNotNull(), lit("right only")) \
        .otherwise(lit("both")).alias("diff")

    # Changed columns
    change_exprs = [when(col(f"{c}_left").eqNullSafe(col(f"{c}_right")), array()).otherwise(array(lit(c))) for c in common_cols]
    changed_col = when(col("__left").isNull() | col("__right").isNull(), lit(None)) \
        .otherwise(concat(*change_exprs)).alias("changed_columns")

    # Select columns
    select_cols = [diff_col, changed_col, col("left_uuid"), col("right_uuid")] + \
        [coalesce(col(k), col(k)).alias(k) for k in keys] + \
        [col(f"{common_cols[i//2]}_left") if i % 2 == 0 else col(f"{common_cols[i//2]}_right") for i in range(len(common_cols)*2)]

    # Inequality condition
    conds = [
        (col(f"{c}_left").isNull() & col(f"{c}_right").isNotNull()) |
        (col(f"{c}_left").isNotNull() & col(f"{c}_right").isNull()) |
        (col(f"{c}_left") != col(f"{c}_right"))
        for c in common_cols
    ]
    inequality = conds[0]
    for cond in conds[1:]:
        inequality = inequality | cond

    result = joined.select(*select_cols)
    if not show_all:
        result = result.where(inequality)
    result = result.limit(rows_limit)
    return result


# COMMAND ----------

def calculate_stats(df, ignore_columns=None):
    ignore_columns = ignore_columns or []
    schema = df.schema

    numeric_types = ["IntegerType", "LongType", "DoubleType", "FloatType", "ShortType", "DecimalType"]
    string_types = ["StringType"]
    date_types = ["DateType", "TimestampType"]

    numeric_cols = [f.name for f in schema.fields if f.dataType.typeName() in [t.lower().replace("type", "") for t in numeric_types] and f.name not in ignore_columns]
    string_cols = [f.name for f in schema.fields if f.dataType.typeName() in [t.lower().replace("type", "") for t in string_types] and f.name not in ignore_columns]
    date_cols = [f.name for f in schema.fields if f.dataType.typeName() in [t.lower().replace("type", "") for t in date_types] and f.name not in ignore_columns]

    numeric_stats = []
    for c in numeric_cols:
        numeric_stats += [
            min(col(c)).alias(f"{c}_min"),
            avg(col(c)).alias(f"{c}_avg"),
            max(col(c)).alias(f"{c}_max"),
            sum(col(c)).alias(f"{c}_sum"),
            count(col(c)).alias(f"{c}_count"),
            countDistinct(col(c)).alias(f"{c}_countd")
        ]

    string_stats = []
    for c in string_cols:
        string_stats += [
            count(col(c)).alias(f"{c}_count"),
            countDistinct(col(c)).alias(f"{c}_countd")
        ]

    date_stats = []
    for c in date_cols:
        date_stats += [
            min(col(c)).alias(f"{c}_min"),
            max(col(c)).alias(f"{c}_max"),
            count(col(c)).alias(f"{c}_count"),
            countDistinct(col(c)).alias(f"{c}_countd")
        ]

    freq_df = df.stat.freqItems(string_cols)
    freq_df_sorted = freq_df.select([sort_array(col(c)).alias(c) for c in freq_df.columns])

    stats_df = df.select(numeric_stats + string_stats + date_stats)
    result = stats_df.crossJoin(freq_df_sorted)
    return result

def transpose_dataframe(df, table_name):
    columns = df.columns
    exprs = []
    for c in columns:
        exprs += [f"'{table_name}'", f"'{c}'", f"CAST({c} AS STRING)"]
    stack_expr = f"stack({len(columns)}, {', '.join(exprs)}) as (table_name, statistic_name, statistic_value)"
    return df.selectExpr(stack_expr)

def compare_stats(left, right, show_all=False, ignore_columns=None):
    ignore_columns = ignore_columns or []
    s1 = transpose_dataframe(calculate_stats(left, ignore_columns), "left") \
        .withColumnRenamed("statistic_value", "left_statistic_value") \
        .withColumn("statistic_name", lower(col("statistic_name"))) \
        .select("statistic_name", "left_statistic_value")
    s2 = transpose_dataframe(calculate_stats(right, ignore_columns), "right") \
        .withColumnRenamed("statistic_value", "right_statistic_value") \
        .withColumn("statistic_name", lower(col("statistic_name"))) \
        .select("statistic_name", "right_statistic_value")
    joined = s1.join(s2, ["statistic_name"], "outer").orderBy("statistic_name")
    if show_all:
        return joined
    else:
        return joined.where(col("left_statistic_value") != col("right_statistic_value"))

# COMMAND ----------

def compare_structures(left, right, show_all=False):
    # Extract structure as list of tuples: (index, name, type, nullable, metadata)
    left_fields = [(i+1, f.name.lower(), f.dataType.simpleString(), f.nullable, f.metadata) for i, f in enumerate(left.schema)]
    right_fields = [(i+1, f.name.lower(), f.dataType.simpleString(), f.nullable, f.metadata) for i, f in enumerate(right.schema)]

    # Create DataFrames for structure
    schema = StructType([
      StructField("column_index", IntegerType(), True),
      StructField("column_name", StringType(), True),
      StructField("column_type", StringType(), True),
      StructField("nullable", StringType(), True),
      StructField("metadata", StringType(), True)
    ])

    left_struct_df = spark.createDataFrame(left_fields, schema)
    right_struct_df = spark.createDataFrame(right_fields, schema)

    # Compare using compare_data
    result = compare_data(
        left_struct_df, 
        right_struct_df, 
        keys=["column_name"], 
        ignore_columns=[], 
        rows_limit=1000, 
        show_all=show_all
    ).orderBy("column_index_left", "column_index_right")
    return result

# COMMAND ----------

def compare_structures_with_table_name(table_name, left, right, show_all=False):
    compare_result = compare_structures(left, right, show_all)
    cols = ["table_name", "column_name", "diff", "changed_columns", "left_uuid", "right_uuid", "column_index_left", "column_index_right", "column_type_left", "column_type_right", "nullable_left", "nullable_right", "metadata_left", "metadata_right"]
    result = compare_result.withColumn("table_name", lit(table_name)).select(cols)
    return result

# COMMAND ----------

def compare_stats_with_table_name(table_name, left, right, show_all=False, ignore_columns=None):
    compare_result = compare_stats(left, right, show_all, ignore_columns)
    cols = ["table_name", "statistic_name", "left_statistic_value", "right_statistic_value"]
    result = compare_result.withColumn("table_name", lit(table_name)).select(cols)
    return result
