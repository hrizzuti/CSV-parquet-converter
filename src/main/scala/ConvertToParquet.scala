import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertToParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()

  val filename = "part-00000-24214a2a-f8d2-491b-ab23-cce86370fe8d-c000"

  val df = spark.read
    .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv(s"src/main/filesInput/$filename.csv")

  spark.close()

}

