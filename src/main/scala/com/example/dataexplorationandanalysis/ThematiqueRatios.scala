package com.example.dataexplorationandanalysis

import breeze.linalg.DenseVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import breeze.plot._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

import scala.Console.println

object ThematiqueRatios {

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


    // Group by "THEMATIQUES"
    val sumDF = df
      .groupBy("THEMATIQUES")
      .agg(
        sum("TF1").as("Sum_TF1"),
        sum("France_2").as("Sum_France_2"),
        sum("France_3").as("Sum_France_3"),
        sum("Canal+").as("Sum_Canal+"),
        sum("Arte").as("Sum_Arte"),
        sum("M6").as("Sum_M6"),
        sum("Totaux").as("Sum_Totaux")
      )
    sumDF.show()


    // Define the columns for which you want to calculate ratios
    val columnsToCalculateRatios = List("Sum_TF1", "Sum_France_2", "Sum_France_3", "Sum_Canal+", "Sum_Arte", "Sum_M6", "Sum_Totaux")


    // Iterate over the columns and calculate ratios in the same column
    val dfWithRatios = columnsToCalculateRatios.foldLeft(sumDF) { (accDF, columnName) =>
      accDF.withColumn(columnName, col(columnName) * 100 / col("Sum_Totaux"))
    }


    // Iterate over the columns and calculate ratios
    val dfWithColumnRatios = columnsToCalculateRatios.foldLeft(dfWithRatios) { (accDF, columnName) =>
      val sumColumn = dfWithRatios.agg(sum(columnName)).first().getDouble(0)
      accDF.withColumn(columnName, col(columnName) * 100 / sumColumn)
    }
    dfWithColumnRatios.show()


    // Perform a query to find rows where "THEMATIQUES" is equal to "Economie"
    val query1: DataFrame = df.filter(col("THEMATIQUES") === "Economie")
    //query1.show()


    // Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("inabarometrejttvdonneesmensuelles20052020durees")
    // Run a Spark SQL query
    val query2: DataFrame = spark.sql(
      "SELECT * FROM inabarometrejttvdonneesmensuelles20052020durees WHERE THEMATIQUES = 'Economie'"
    )
    //query2.show()


    // Stop the SparkSession when done
    spark.stop()

  }

}
