package spark.batch

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DataFrameColumnCast {
  def test(spark: SparkSession): Unit = {

    val df: DataFrame = spark.emptyDataFrame
    val columns: Array[String] = df.columns
    val df2: DataFrame = columns.foldLeft(df){
      (currentDF, column) => currentDF.withColumn(column, col(column).cast("String"))
    }

    val arrayColumn: Array[Column] = columns.map(column => col(column).cast("String"))
    df.select(arrayColumn :_*)

  }
}
