# Databricks notebook source
# MAGIC %md
# MAGIC #Timestamps

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compare.retail_transactions ORDER BY InsertTimestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM compare.retail_transactions WHERE InsertTimestamp = TIMESTAMP '2026-01-01T10:06:07.001+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT InsertTimestamp, COUNT(*) FROM compare.retail_transactions GROUP BY InsertTimestamp ORDER BY InsertTimestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM compare.retail_transactions WHERE InsertTimestamp >= TIMESTAMP '2026-01-01T10:06:07.001+00:00' AND InsertTimestamp < TIMESTAMP '2026-01-01T10:06:08.001+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   InsertTimestamp, 
# MAGIC   to_unix_timestamp(InsertTimestamp) AS InsertTimestampUnix,
# MAGIC   unix_millis(InsertTimestamp) AS InsertTimestampUnixMilli,   
# MAGIC   unix_micros(InsertTimestamp) AS InsertTimestampUnixMicro,
# MAGIC   CAST(InsertTimestamp AS BIGINT) AS InsertTimestampBigInt
# MAGIC   --(CAST(unix_micros(InsertTimestamp) AS BIGINT) * 1000) + (CAST(DATE_FORMAT(InsertTimestamp, 'SSSSSS') AS BIGINT) % 1000) AS InsertTimestampUnixNano
# MAGIC FROM compare.retail_transactions 
# MAGIC ORDER BY InsertTimestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #Floating point numbers, decimals

# COMMAND ----------

a = 0.1
b = 0.2
c = a + b
d = (c == 0.3)

print(d)

# COMMAND ----------

print("%.32f" % (a))
print("%.32f" % (b))
print("%.32f" % (c))

# COMMAND ----------

type(a)

# COMMAND ----------

# MAGIC %md
# MAGIC https://float.exposed/0x3dcccccd

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.python.org/3/library/stdtypes.html#numeric-types-int-float-complex
# MAGIC
# MAGIC Floating-point numbers are usually implemented using double in C; information about the precision and internal representation of floating-point numbers for the machine on which your program is running is available in sys.float_info.

# COMMAND ----------

from sys import float_info
float_info.mant_dig

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL and DataFrames support the following data types:
# MAGIC
# MAGIC Numeric types
# MAGIC - ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
# MAGIC - ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
# MAGIC - IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
# MAGIC - LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
# MAGIC - FloatType: Represents 4-byte single-precision floating point numbers.
# MAGIC - DoubleType: Represents 8-byte double-precision floating point numbers.
# MAGIC - DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal. A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (0.1 + 0.2 = 0.3) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT (0.1 + 0.2 = 0.3) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(0.1 AS FLOAT) + CAST(0.2 AS FLOAT) = CAST(0.3 AS FLOAT)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT (CAST(0.1 AS FLOAT) + CAST(0.2 AS FLOAT) = CAST(0.3 AS FLOAT)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(0.1 AS FLOAT), CAST(0.2 AS FLOAT), CAST(0.3 AS FLOAT), CAST(0.1 AS FLOAT) + CAST(0.2 AS FLOAT),
# MAGIC   0.10000000149011612 
# MAGIC + 0.20000000298023224

# COMMAND ----------

# MAGIC %md
# MAGIC ## IEEE-754 binary32 (4-byte float) addition: 0.1 + 0.2
# MAGIC
# MAGIC ### 1. Input encoding  
# MAGIC Binary32 format: 1 sign bit, 8 exponent bits, 23 fraction bits, bias = 127
# MAGIC
# MAGIC **0.1**
# MAGIC - Normalized ≈ 1.10011001100110011001101 × 2⁻⁴  
# MAGIC - Stored exponent = 123  
# MAGIC - Hex = 3DCCCCCD  
# MAGIC - Exact stored value = 0.100000001490116119384765625  
# MAGIC
# MAGIC **0.2**
# MAGIC - Normalized ≈ 1.10011001100110011001101 × 2⁻³  
# MAGIC - Stored exponent = 124  
# MAGIC - Hex = 3E4CCCCD  
# MAGIC - Exact stored value = 0.20000000298023223876953125  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Exponent alignment  
# MAGIC Larger exponent is −3.  
# MAGIC Shift 0.1 significand right by 1 bit.
# MAGIC
# MAGIC - 0.1 → 0.110011001100110011001101 × 2⁻³  
# MAGIC - 0.2 → 1.10011001100110011001101 × 2⁻³  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Significand addition
# MAGIC
# MAGIC 1.10011001100110011001101  
# MAGIC +0.110011001100110011001101  
# MAGIC =10.011001100110011001100111  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Normalization  
# MAGIC Shift right by 1 bit:
# MAGIC
# MAGIC 1.0011001100110011001100111 × 2⁻²  
# MAGIC
# MAGIC Exponent becomes −2.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Rounding (round-to-nearest, ties-to-even)  
# MAGIC Guard bit = 1, sticky bits ≠ 0 → round up.
# MAGIC
# MAGIC Final fraction (23 bits):  
# MAGIC 00110011001100110011010  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. Final result (binary32)
# MAGIC
# MAGIC - Sign = 0  
# MAGIC - Stored exponent = 125 (−2 + bias)  
# MAGIC - Hex = 3E99999A  
# MAGIC
# MAGIC **Exact decimal value of the float result:**  
# MAGIC 0.300000011920928955078125
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(1000000000 AS FLOAT) + CAST(1 AS FLOAT) = CAST(1000000001 AS FLOAT)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(1000000000 AS FLOAT) + CAST(63 AS FLOAT) = CAST(1000000063 AS FLOAT)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(1000000000 AS FLOAT) + CAST(63 AS FLOAT) = CAST(1000000064 AS FLOAT)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(1000000000 AS FLOAT) + CAST(1 AS FLOAT) = 1000000001) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT (CAST(1000000000 AS FLOAT) + CAST(1 AS FLOAT) = 1000000001) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) = CAST(0.3 AS DOUBLE)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT (CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) = CAST(0.3 AS DOUBLE)) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(0.1 AS DOUBLE), CAST(0.2 AS DOUBLE), CAST(0.3 AS DOUBLE), CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CAST(0.1 AS DOUBLE) = 0.100000000000000005551, --0x3fb999999999999a
# MAGIC   CAST(0.1 AS DOUBLE) = 0.100000000000000006666,
# MAGIC   CAST(0.1 AS DOUBLE) = 0.100000000000000010000,
# MAGIC   CAST(0.1 AS DOUBLE) = 0.100000000000000019429  --0x3fb999999999999b

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(0.3 AS FLOAT) = CAST(0.3 AS DOUBLE) AS col1

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT CAST(0.3 AS FLOAT) = CAST(0.3 AS DOUBLE) AS col1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(cast(0.3 as float) as double), cast(0.3 as double)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(0.3 AS DOUBLE) = CAST(0.3 AS DECIMAL(10, 2)) AS col1

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT CAST(0.3 AS DOUBLE) = CAST(0.3 AS DECIMAL(10, 2)) AS col1;

# COMMAND ----------

# MAGIC %md
# MAGIC #Strings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compare.strings_test

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   col1 = col2 AS col1_vs_col2,
# MAGIC   col2 = col3 AS col2_vs_col3,
# MAGIC   col3 = col4 AS col3_vs_col4,
# MAGIC   col5 = col6 AS col5_vs_col6,
# MAGIC   col7 = col8 AS col7_vs_col8
# MAGIC FROM compare.strings_test

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   col1,
# MAGIC   hex(encode(col1, "UTF-8")) AS col1_hex,
# MAGIC   col2,
# MAGIC   hex(encode(col2, "UTF-8")) AS col2_hex,
# MAGIC   col3,
# MAGIC   hex(encode(col3, "UTF-8")) AS col3_hex,
# MAGIC   col4,
# MAGIC   hex(encode(col4, "UTF-8")) AS col4_hex,
# MAGIC   col5,
# MAGIC   hex(encode(col5, "UTF-8")) AS col5_hex,
# MAGIC   col6,
# MAGIC   hex(encode(col6, "UTF-8")) AS col6_hex,
# MAGIC   col7,
# MAGIC   hex(encode(col7, "UTF-8")) AS col7_hex,
# MAGIC   col8,
# MAGIC   hex(encode(col8, "UTF-8")) AS col8_hex
# MAGIC FROM compare.strings_test

# COMMAND ----------

# %scala
# import java.text.Normalizer
# import org.apache.spark.sql.functions.udf

# val normalizeUDF = udf((s: String) => if (s != null) Normalizer.normalize(s, Normalizer.Form.NFC) else null)

# COMMAND ----------

# DBTITLE 1,Cell 32
# %scala
# import org.apache.spark.sql.functions.col

# val result = spark.read.table("tempmaciejz.strings_test")
#   .withColumn("col5_vs_col6", col("col5") === col("col6"))
#   .withColumn("col5_vs_col6_normalized", normalizeUDF(col("col5")) === normalizeUDF(col("col6")))
#   .withColumn("col7_vs_col8", col("col7") === col("col8"))
#   .withColumn("col7_vs_col8_normalized", normalizeUDF(col("col7")) === normalizeUDF(col("col8")))

# COMMAND ----------

from unicodedata import normalize

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from unicodedata import normalize

def normalize_nfc(text):
    return normalize('NFC', text)

normalize_nfc_udf = udf(normalize_nfc, StringType())

# COMMAND ----------

from pyspark.sql.functions import col

result = spark.read.table("compare.strings_test") \
    .withColumn("col5_vs_col6", col("col5") == col("col6")) \
    .withColumn("col5_vs_col6_normalized", normalize_nfc_udf(col("col5")) == normalize_nfc_udf(col("col6"))) \
    .withColumn("col7_vs_col8", col("col7") == col("col8")) \
    .withColumn("col7_vs_col8_normalized", normalize_nfc_udf(col("col7")) == normalize_nfc_udf(col("col8")))

# COMMAND ----------

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.unicode.org/reports/tr15/tr15-23.html

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from unicodedata import normalize

data = [
    ("é",),                  # U+00E9 composed
    ("e\u0301",),            # U+0065 + U+0301 decomposed
    ("①",),                  # circled number 1
    ("Ⅰ",),                  # Roman numeral 1
    ("½",),                  # one-half
    ("Ⅳ",),                  # Roman numeral 4
    ("㉑",),                  # circled number 21
    ("ᵃ",),                  # superscript a
    ("㎡",),                  # square meter
    ("😀",),                 # emoji
    ("ę",)                   # Polish letter
]

df = spark.createDataFrame(data, ["original"])

def normalize_nfc(s):
    return normalize('NFC', s) if s is not None else None

def normalize_nfkc(s):
    return normalize('NFKC', s) if s is not None else None

def normalize_nfd(s):
    return normalize('NFD', s) if s is not None else None

def normalize_nfkd(s):
    return normalize('NFKD', s) if s is not None else None

def to_hex(s):
    return ' '.join(f"{b:02X}" for b in s.encode("utf-8")) if s is not None else None

normalize_nfc_udf = udf(normalize_nfc, StringType())
normalize_nfkc_udf = udf(normalize_nfkc, StringType())
normalize_nfd_udf = udf(normalize_nfd, StringType())
normalize_nfkd_udf = udf(normalize_nfkd, StringType())
to_hex_udf = udf(to_hex, StringType())

df = df \
    .withColumn("NFC", normalize_nfc_udf(col("original"))) \
    .withColumn("NFKC", normalize_nfkc_udf(col("original"))) \
    .withColumn("NFD", normalize_nfd_udf(col("original"))) \
    .withColumn("NFKD", normalize_nfkd_udf(col("original"))) \
    .withColumn("hex_original", to_hex_udf(col("original"))) \
    .withColumn("hex_NFC", to_hex_udf(col("NFC"))) \
    .withColumn("hex_NFKC", to_hex_udf(col("NFKC"))) \
    .withColumn("hex_NFD", to_hex_udf(col("NFD"))) \
    .withColumn("hex_NFKD", to_hex_udf(col("NFKD")))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Arrays, maps

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC   , normal_array = reverse_array AS normal_vs_reverse
# MAGIC   , empty_array = null_array AS empty_vs_null
# MAGIC FROM compare.arrays_test

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE compare.maps_test
