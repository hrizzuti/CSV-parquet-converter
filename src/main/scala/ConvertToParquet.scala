import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertToParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()


  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("src/main/filesInput/division.csv")


  df.write.mode(SaveMode.Overwrite).option("compression","none").parquet("src/main/filesOutput/")

  spark.close()

}

