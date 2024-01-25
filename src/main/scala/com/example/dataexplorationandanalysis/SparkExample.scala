package com.example.dataexplorationandanalysis

import org.apache.spark.sql.SparkSession

object SparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkExample")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(("John", 25), ("Alice", 30), ("Bob", 22))
    val df = spark.createDataFrame(data).toDF("Name", "Age")

    df.show()

    spark.stop()
  }
}
