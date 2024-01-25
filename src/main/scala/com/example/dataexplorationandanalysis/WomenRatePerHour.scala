import org.apache.spark.sql.SparkSession
import breeze.plot._
import breeze.linalg.DenseVector
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}


object WomenRatePerHour {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("WomenRatePerHourSpark")
      .master("local[*]")
      .getOrCreate()

    val csvFilePath = "data/20190308-hourstatall.csv"

    // Read CSV file into a DataFrame
    val schema = StructType(Seq(
      StructField("media_type", StringType, nullable = true),
      StructField("channel_name", StringType, nullable = true),
      StructField("is_public_channel", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("hour", IntegerType, nullable = true),
      StructField("women_expression_rate", DoubleType, nullable = true),
      StructField("speech_rate", DoubleType, nullable = true),
      StructField("nb_hours_analyzed", DoubleType, nullable = true)
    ))

    val data = spark.read
      .option("delimiter", ",")
      .option("header", "true") // Use the first row as header
      .schema(schema)
      .csv(csvFilePath)

    import spark.implicits._

    data.show()

    val channel = "TF1"

    val averageByHourDF = data
      .filter($"channel_name" === channel) //Comment to have full infos from all channels
      .groupBy("hour")
      .agg(avg("women_expression_rate").as("AverageWomenRate"))
      .orderBy("hour")

    averageByHourDF.show()

    // Collect the data to local variables for Breeze plotting
    val hours = DenseVector(averageByHourDF.select("hour").as[Int].collect())
    val womenRates = DenseVector(averageByHourDF.select("AverageWomenRate").as[Double].collect())
    val intWomenRates = DenseVector(womenRates.map(_.toInt).toArray)


    // Plot the data using Breeze scatter plot
    val f = Figure()
    val p = f.subplot(0)

    // Use scatter for plotting
    p += plot(hours, intWomenRates, name = s"Average Women Expression Rate - $channel")
    p.xlabel = "Hour"
    p.ylabel = "Average Women Expression Rate"
    p.legend = true

    f.refresh()

    spark.stop()
  }
}