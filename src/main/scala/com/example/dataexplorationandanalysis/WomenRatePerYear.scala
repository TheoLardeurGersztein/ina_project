package com.example.dataexplorationandanalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source
import breeze.plot._
import breeze.linalg._

object WomenRatePerYear {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("WomenRatePerYearSpark")
      .master("local[*]")
      .getOrCreate()

    val csvFilePath = "data/20190308-years.csv"

    // Read CSV file into a DataFrame
    val schema = StructType(Seq(
      StructField("media_type", StringType, nullable = true),
      StructField("channel_name", StringType, nullable = true),
      StructField("is_public_channel", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("women_expression_rate", DoubleType, nullable = true),
      StructField("speech_rate", DoubleType, nullable = true),
      StructField("nb_hours_analyzed", DoubleType, nullable = true)
    ))

    val data = spark.read
      .option("delimiter", ",")
      .option("header", "true") // Use the firsreadt row as header
      .schema(schema)
      .csv(csvFilePath)

    import spark.implicits._


    data.show()

    val channel = "TF1"


    val averageByYear = data
      .filter(  $"channel_name" === channel) //Comment to have full infos from all channels
      .groupBy("year")
      .agg(avg("women_expression_rate").as("AverageWomenRate"))
      .orderBy("year")

    averageByYear.show()


    // Convert to Breeze for plotting
    val years = DenseVector(averageByYear.select("year").as[Int].collect())
    val womenRates = DenseVector(averageByYear.select("AverageWomenRate").as[Double].collect())
    val intWomenRates = DenseVector(womenRates.map(_.toInt).toArray)


    // Plot the data using Breeze
    val f = Figure()
    val p = f.subplot(0)

    p += plot(years.toArray, intWomenRates.toArray, name = s"Average Women Expression Rate $channel")
    p.xlabel = "Year"
    p.ylabel = "Average Women Expression Rate"
    p.legend = true

    f.refresh()
  }

}
