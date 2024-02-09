import JD.JournalDemand.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.explode

object ScContentSource extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Converter")
    .getOrCreate()


  import spark.implicits._

  val x = spark.read.parquet("/Users/rizzutih/Downloads/sccontent_source/part-00000-6d609bf0-1da7-4f0d-82dc-fb62a34b7f4d-c000.snappy.parquet")
    .select('id, 'sourcetitle, explode('issn).as("sc_issn"), 'eissn, 'asjc)
    .select('id, 'sourcetitle, 'sc_issn, explode('eissn).as("eissn"), 'asjc)
    .select('id, 'sourcetitle, 'sc_issn, 'eissn, explode('asjc).as("asjc"))

    x.limit(100)
      .repartition(1)
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite).parquet("/Users/rizzutih/Downloads/sccontent_source/curated/")
}
