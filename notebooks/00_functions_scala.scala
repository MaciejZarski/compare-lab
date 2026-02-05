// Databricks notebook source
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Function to compare the data of two DataFrames
def compareData(left: DataFrame, right: DataFrame, keys: Seq[String], ignoreColumns: Seq[String], rowsLimit: Integer, showAll: Boolean = false) : DataFrame = {
  // Get the common columns between the left and right DataFrames, excluding the keys and ignore columns
  val leftColumns: Array[String] = left.columns.intersect(right.columns).filter(col => (!keys.contains(col)) && (!ignoreColumns.contains(col)))
  val leftRenamedColumns: Array[String] = leftColumns.map(col => s"${col}_left")

  // Rename the left DataFrame columns and add a column to indicate existence in the left DataFrame
  val leftRenamedDF: DataFrame = left.drop(ignoreColumns: _*).select(keys.map(col) ++ leftColumns.map(c => col(c).alias(s"${c}_left")): _*).withColumn("__left", lit(1)).withColumn("left_uuid", uuid())

  // Get the common columns between the right and left DataFrames, excluding the keys and ignore columns
  val rightColumns: Array[String] = right.columns.intersect(left.columns).filter(col => (!keys.contains(col)) && (!ignoreColumns.contains(col)))
  val rightRenamedColumns: Array[String] = rightColumns.map(col => s"${col}_right")

  // Rename the right DataFrame columns and add a column to indicate existence in the right DataFrame
  val rightRenamedDF: DataFrame = right.drop(ignoreColumns: _*).select(keys.map(col) ++ rightColumns.map(c => col(c).alias(s"${c}_right")): _*).withColumn("__right", lit(1)).withColumn("right_uuid", uuid())

  // Determine the difference and change columns
  val diffColumn = when(!leftRenamedDF("__left").isNull && rightRenamedDF("__right").isNull, lit("left only")).otherwise(
    when(leftRenamedDF("__left").isNull && !rightRenamedDF("__right").isNull, lit("right only")).otherwise(
      lit("both")
    ) //inner otherwise
  ).alias("diff") //otherwise

  val changeColumn = when(leftRenamedDF("__left").isNull || rightRenamedDF("__right").isNull, lit(null)).otherwise(
      concat(leftColumns.map(c => when(leftRenamedDF(s"${c}_left") <=> rightRenamedDF(s"${c}_right"), array()).otherwise(array(lit(c)))): _*)
    ) //otherwise
    .alias("changed_columns")

  // Select the columns to include in the output DataFrame
  val columnsToSelect = Seq(diffColumn, changeColumn, leftRenamedDF("left_uuid"), rightRenamedDF("right_uuid")) ++ keys.map(k => coalesce(leftRenamedDF(k), rightRenamedDF(k)).alias(k)) ++ leftRenamedColumns.sorted.zip(rightRenamedColumns.sorted).flatMap(pair => Seq(leftRenamedDF(pair._1), rightRenamedDF(pair._2)))

  // Create an inequality condition to filter the rows where there are differences between the left and right DataFrames
  val inequalityCondition = (leftRenamedColumns.sorted.zip(rightRenamedColumns.sorted).map{
    case(leftCol, rightCol) => s"(($leftCol IS NULL AND $rightCol IS NOT NULL) OR ($leftCol IS NOT NULL AND $rightCol IS NULL) OR ($leftCol != $rightCol))"
  } ++ Array("1 = 0")).mkString(" OR ")

  // Join the left and right DataFrames, select the columns, and apply the inequality condition
  if (showAll) {
    leftRenamedDF.join(rightRenamedDF, keys, "outer").select(columnsToSelect: _*).limit(rowsLimit)
  } else {
    leftRenamedDF.join(rightRenamedDF, keys, "outer").select(columnsToSelect: _*).where(inequalityCondition).limit(rowsLimit)
  }
}

// COMMAND ----------

// Function to calculate statistics for a DataFrame
def calculateStats(df: DataFrame, ignoreColumns: Seq[String] = Seq()): DataFrame = {
  // Get the numeric column names from the DataFrame schema
  val numericColumnsNames = df.schema.filter(s => s.dataType.isInstanceOf[NumericType]).map(_.name).filter(col => !ignoreColumns.contains(col)).sorted

  // Calculate numeric column statistics
  val numericColumnsStats = numericColumnsNames.flatMap(col => Seq(
    min(df(col)).alias(s"${col}_min"), 
    avg(df(col)).alias(s"${col}_avg"), 
    max(df(col)).alias(s"${col}_max"),
    sum(df(col)).alias(s"${col}_sum"),    
    count(df(col)).alias(s"${col}_count"),  
    count_distinct(df(col)).alias(s"${col}_countd")  
  ))
  val numericColumnsStatsNames = numericColumnsStats.map(col => col.named.name)

  // Get the character column names from the DataFrame schema
  val characterColumnsNames = df.schema.filter(s => s.dataType.isInstanceOf[CharType] || s.dataType.isInstanceOf[StringType] || s.dataType.isInstanceOf[VarcharType]).map(_.name).filter(col => !ignoreColumns.contains(col)).sorted

  // Calculate character column statistics
  val characterColumnsStats = characterColumnsNames.flatMap(col => Seq(
    count(df(col)).alias(s"${col}_count"),  
    count_distinct(df(col)).alias(s"${col}_countd")  
  ))
  val characterColumnsStatsNames = characterColumnsStats.map(col => col.named.name)  

  // Get the date column names from the DataFrame schema
  val dateTimeColumnsNames = df.schema.filter(s => s.dataType.isInstanceOf[DateType] || s.dataType.isInstanceOf[TimestampType]).map(_.name).filter(col => !ignoreColumns.contains(col)).sorted

  // Calculate date column statistics
  val dateTimeColumnsStats = dateTimeColumnsNames.flatMap(col => Seq(
    min(df(col)).alias(s"${col}_min"), 
    max(df(col)).alias(s"${col}_max"),
    count(df(col)).alias(s"${col}_count"),  
    count_distinct(df(col)).alias(s"${col}_countd")  
  ))
  val dateTimeColumnsStatsNames = dateTimeColumnsStats.map(col => col.named.name)

  // Calculate frequency of character columns and sort the resulting DataFrame
  val freqsDF = df.stat.freqItems(characterColumnsNames)
  val freqsDFSorted = freqsDF.select(freqsDF.columns.map(colName => sort_array(freqsDF(colName)).alias(colName)): _*)
  val characterColumnsStatsNamesWithFreqs = (characterColumnsStatsNames ++ freqsDFSorted.columns).sorted

  // Select the calculated statistics and frequency columns
  df.select(numericColumnsStats ++ characterColumnsStats ++ dateTimeColumnsStats: _*).crossJoin(freqsDFSorted).select((numericColumnsStatsNames ++ characterColumnsStatsNamesWithFreqs ++ dateTimeColumnsStatsNames).map(col): _*)
}

// Function to transpose DataFrame
def transposeDataFrame(df: DataFrame, tableName: String): DataFrame = {
  // Extract column names from the schema
  val columnNames = df.schema.fields.map(_.name)

  // Define expressions to stack columns dynamically
  val stackExprs = columnNames.flatMap { columnName =>
    Seq(s"'${tableName}'", s"'${columnName}'", s"CAST(${columnName} AS STRING)")
  }

  // Transpose the DataFrame to long format
  val transposedDf = df.selectExpr(s"stack(${columnNames.length}, ${stackExprs.mkString(", ")}) as (table_name, statistic_name, statistic_value)")

  // Return the transposed DataFrame
  transposedDf
}

// Function to compare the statistics of two DataFrames
def compareStats(left: DataFrame, right: DataFrame, showAll: Boolean = false, ignoreColumns: Seq[String] = Seq()): DataFrame = {
  // Calculate statistics for both DataFrames
  val s1 = transposeDataFrame(calculateStats(left, ignoreColumns), "left")
    .withColumnRenamed("statistic_value", "left_statistic_value")
    .withColumn("statistic_name", lower($"statistic_name"))
    .select("statistic_name", "left_statistic_value")
  val s2 = transposeDataFrame(calculateStats(right, ignoreColumns), "right")
    .withColumnRenamed("statistic_value", "right_statistic_value")
    .withColumn("statistic_name", lower($"statistic_name"))    
    .select("statistic_name", "right_statistic_value")

  if (showAll) { 
    s1.join(s2, Seq("statistic_name"), "outer").sort("statistic_name")
  } else {
    s1.join(s2, Seq("statistic_name"), "outer").sort("statistic_name").where("left_statistic_value <> right_statistic_value")
  }
}

// COMMAND ----------

def compareStructures(left: DataFrame, right: DataFrame, showAll: Boolean = false): DataFrame = {
  val leftStructureAsList: List[StructField] = left.schema.toList
  val leftListOfTuples: List[(String, String, Boolean, String)] = leftStructureAsList.map{case s:StructField => (s.name.toLowerCase, s.dataType.sql, s.nullable, s.metadata.json)}
  val leftIndexes: List[Int] = List.range(1, leftStructureAsList.size + 1)
  val leftZipped: List[(Int, String, String, Boolean, String)] = leftIndexes.zip(leftListOfTuples).map{case (i, (nm, t, n, m)) => (i, nm, t, n, m)}

  val leftStructureAsDataFrame: DataFrame = sc.parallelize(leftZipped).toDF("column_index", "column_name", "column_type", "nullable", "metadata")

  val rightStructureAsList: List[StructField] = right.schema.toList
  val rightListOfTuples: List[(String, String, Boolean, String)] = rightStructureAsList.map{case s:StructField => (s.name.toLowerCase, s.dataType.sql, s.nullable, s.metadata.json)}
  val rightIndexes: List[Int] = List.range(1, rightStructureAsList.size + 1)
  val rightZipped: List[(Int, String, String, Boolean, String)] = rightIndexes.zip(rightListOfTuples).map{case (i, (nm, t, n, m)) => (i, nm, t, n, m)}

  val rightStructureAsDataFrame: DataFrame = sc.parallelize(rightZipped).toDF("column_index", "column_name", "column_type", "nullable", "metadata")

  compareData(leftStructureAsDataFrame, rightStructureAsDataFrame, Seq("column_name"), Seq(), 1000, showAll).sort("column_index_left", "column_index_right")
}
