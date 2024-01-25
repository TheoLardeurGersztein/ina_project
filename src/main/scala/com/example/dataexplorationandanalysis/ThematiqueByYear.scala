package com.example.dataexplorationandanalysis

import breeze.linalg.DenseVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import breeze.plot._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

import scala.Console.println

object ThematiqueByYear {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("Data Exploration And Analysis")
      .master("local[*]") //Run app in local
      .getOrCreate()


    // Specify the path to your CSV file
    val csvFilePath = "data/ina-barometre-jt-tv-donnees-mensuelles-2005-2020-durees.csv"

    // Define the schema based on your data
    val schema = StructType(
      Array(
        StructField("MOIS", StringType, nullable = true),
        StructField("THEMATIQUES", StringType, nullable = true),
        StructField("TF1", StringType, nullable = true),
        StructField("France 2", StringType, nullable = true),
        StructField("France 3", StringType, nullable = true),
        StructField("Canal +", StringType, nullable = true),
        StructField("Arte", StringType, nullable = true),
        StructField("M6", StringType, nullable = true),
        StructField("Totaux", StringType, nullable = true)
      )
    )


    // Read the CSV file into a DataFrame
    val dfRaw: DataFrame = spark.read
      .option("header", "true") // Use the first row as header
      .option("delimiter", ";") // Set the delimiter to semicolon
      .schema(schema) // Specify the schema
      .option("encoding", "UTF-8") // Specify the character encoding
      .option("inferSchema", "true") // Infer the schema of the data
      .csv(csvFilePath)

    // Show the DataFrame
    //dfRaw.show()
    //println("Nombre de lingnes : " + dfRaw.count())


    // Define a UDF to convert time to minutes
    val convertToMinutesUDF: UserDefinedFunction = udf((time: String) => {
      val Array(hours, minutes, seconds) = time.split(":").map(_.toInt)
      hours * 60 + minutes + Math.round(seconds / 60.0).toInt
    })


    // Apply the UDF to each relevant column
    val df: DataFrame = dfRaw
      .withColumn("TF1", convertToMinutesUDF(col("TF1")).cast(IntegerType))
      .withColumn("France_2", convertToMinutesUDF(col("France 2")).cast(IntegerType))
      .withColumn("France_3", convertToMinutesUDF(col("France 3")).cast(IntegerType))
      .withColumn("Canal+", convertToMinutesUDF(col("Canal +")).cast(IntegerType))
      .withColumn("Arte", convertToMinutesUDF(col("Arte")).cast(IntegerType))
      .withColumn("M6", convertToMinutesUDF(col("M6")).cast(IntegerType))
      .withColumn("Totaux", convertToMinutesUDF(col("Totaux")).cast(IntegerType))
      .select("MOIS", "THEMATIQUES", "TF1", "France_2", "France_3", "Canal+", "Arte", "M6", "Totaux")
    // Display the result
    //df.show()


    // Extract the year from the adjusted "MOIS" column
    val dfWithYear = df.withColumn("Year", substring(col("MOIS"), -2, 2))

    // Group by "THEMATIQUES" and "Year", and sum the values for each column
    val totalDF = dfWithYear
      .groupBy("THEMATIQUES", "Year")
      .agg(
        sum("TF1").as("Sum_TF1"),
        sum("France_2").as("Sum_France_2"),
        sum("France_3").as("Sum_France_3"),
        sum("Canal+").as("Sum_Canal"),
        sum("Arte").as("Sum_Arte"),
        sum("M6").as("Sum_M6"),
        sum("Totaux").as("Sum_Totaux")
      )
      .orderBy("Year", "THEMATIQUES")

    // Show the totalDF
    //totalDF.show()


    val thematiques = totalDF.select("THEMATIQUES").distinct().as[String](Encoders.STRING).collect()

    for (thematique <- thematiques) {

      // Choose a channel and thematique for plotting
      val channel = "TF1"

      // Filter the DataFrame for the selected channel and thematique
      val filteredDF = totalDF.filter(s"THEMATIQUES = '$thematique' AND Sum_$channel > 0")

      // Collect the data to the driver program
      val collectedData = filteredDF.collect()

      // Extract years and corresponding values
      val years = DenseVector(collectedData.map(_.getAs[String]("Year")).map(_.toInt))
      val values = DenseVector(collectedData.map(_.getAs[Long](s"Sum_$channel").toDouble).toArray)
      val intValues = DenseVector(values.map(_.toInt).toArray)


      // Create a Breeze-viz plot
      val f = Figure()
      val p = f.subplot(0)

      p += plot(years, intValues, name = s"$thematique - $channel", style = '-')
      p.xlabel = "Year"
      p.ylabel = "Sum Value"
      p.legend = true

      f.refresh()


    }


    // Stop the SparkSession when done
    spark.stop()

  }

}
