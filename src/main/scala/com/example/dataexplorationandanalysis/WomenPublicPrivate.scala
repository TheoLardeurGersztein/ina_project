package com.example.dataexplorationandanalysis

import breeze.linalg.DenseVector
import breeze.plot.{Figure, plot}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.sparkproject.dmg.pmml.True

object WomenPublicPrivate {

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



    //Radio : 2001 - 2019
    //tv : 2010 - 2019
    // Filter data for the years
    val media_type = "tv"
    val filteredData = data.filter("year >= 2010 AND year <= 2019").filter(s"media_type = '$media_type'")


    val averageByYear = filteredData
      .groupBy("year", "is_public_channel")
      .agg(avg("women_expression_rate").as("AverageWomenRate"))
      .orderBy("year")

    averageByYear.show()


    // Separate public and private data
    val publicData = averageByYear.filter("is_public_channel = 'True'")
    publicData.show()
    val privateData = averageByYear.filter("is_public_channel = 'False'")
  privateData.show()

    val publicRates = DenseVector(publicData.select("AverageWomenRate").as[Double].collect())
    val intPublicRates = DenseVector(publicRates.map(_.toInt).toArray)

    val privateRates = DenseVector(privateData.select("AverageWomenRate").as[Double].collect())
    val intPrivateRates = DenseVector(privateRates.map(_.toInt).toArray)

    val years = DenseVector(publicData.select("year").as[Int].collect())


    // Plot the data using Breeze
    val f = Figure()
    val p = f.subplot(0)

    println(intPublicRates)
    println(intPrivateRates)

    println(years)
    p += plot(years, intPublicRates.toArray, name = "Public")
    p += plot(years, intPrivateRates.toArray, name = "Private")

    p.xlabel = "Year"
    p.ylabel = "AverageWomenExpressionRatePublicPrivate"
    p.legend = true

    // Show the plot
    f.refresh()

    // Stop Spark session
    spark.stop()

  }
}
