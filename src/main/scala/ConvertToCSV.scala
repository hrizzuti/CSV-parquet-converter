import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertToCSV extends App {


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()

  val filename = "part-00000-b7ab4984-4757-480a-81f9-a2b98bf645bf-c000.snappy"

  val df = spark.read
    .format("parquet")
    .load(s"src/main/filesInput/$filename.parquet")

  df.write
    .mode(SaveMode.Append)
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .option("header", "true")
    .csv("src/main/filesOutput/")

}
