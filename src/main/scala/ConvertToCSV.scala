import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertToCSV extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()


  val df = spark.read.format("parquet")
    .load("src/main/filesInput/metric_value.parquet")


  df.write.mode(SaveMode.Overwrite).format("csv").csv("src/main/filesOutput/")

  spark.close()

}
