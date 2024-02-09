import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object JournalFamily extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Converter")
    .getOrCreate()

  val journal_families = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("charset", "utf-8")
    .csv("/Users/rizzutih/Downloads/journal_family_full_version.csv")


  val filtered_journal_family = journal_families.where(col("pl3").isin("Cell Press", "Lancet", "Societies", "Clinics", "Masson", "Doyma", "Seminars"))
    .withColumn("issn_new", regexp_replace(col("issn"), "-", "")).drop("issn")

  val rows_to_delete = filtered_journal_family.where(col("issn_new").isin("10454527", "00371963"
    , "00937754"
    , "10407383"
    , "07402570"
    , "02709295"
    , "10431489"
    , "10924450") && filtered_journal_family("pl3") === "Clinics")

  val final_journal_family = filtered_journal_family.join(rows_to_delete, filtered_journal_family("issn_new") === rows_to_delete("issn_new")
    && filtered_journal_family("pl3") === rows_to_delete("pl3"), "left_anti")

  import spark.implicits._

  final_journal_family.select($"Product ID", $"Title", $"PL3", final_journal_family("issn_new").as("ISSN"))
    .repartition(1).write
    .mode(SaveMode.Append)
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .option("header", "true")
    .csv("/Users/rizzutih/Downloads/test/journal_family/")
}
