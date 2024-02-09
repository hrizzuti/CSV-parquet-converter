import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

object ReaxysDatasetProcessor extends App {


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()

  val filename = "rx_usage_all_202112_12-2021"

  val schemaNewNames = new StructType()
    .add("Dept-ID", LongType, nullable = true)
    .add("Account-ID", DoubleType, nullable = true)
    .add("Account-Number", StringType, nullable = true)
    .add("Department-Name", StringType, nullable = true)
    .add("SIS-ID", StringType, nullable = true)
    .add("ECR-ID", StringType, nullable = true)
    .add("Country", StringType, nullable = true)
    .add("Rx-Platform", StringType, nullable = true)
    .add("Year", LongType, nullable = true)
    .add("Type", StringType, nullable = true)
    .add("Jan", DoubleType, nullable = true)
    .add("Feb", DoubleType, nullable = true)
    .add("Mar", DoubleType, nullable = true)
    .add("Apr", DoubleType, nullable = true)
    .add("May", DoubleType, nullable = true)
    .add("Jun", DoubleType, nullable = true)
    .add("Jul", DoubleType, nullable = true)
    .add("Aug", DoubleType, nullable = true)
    .add("Sep", DoubleType, nullable = true)
    .add("Oct", DoubleType, nullable = true)
    .add("Nov", DoubleType, nullable = true)
    .add("Dec", DoubleType, nullable = true)

  val df = spark.read
    .format("parquet")
    .schema(schemaNewNames)
    .load(s"src/main/filesInput/$filename.parquet.gz")

  df.show(df.count().toInt)

}
