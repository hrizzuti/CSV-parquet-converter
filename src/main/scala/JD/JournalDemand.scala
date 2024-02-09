package JD

import org.apache.spark.sql.functions.{col, collect_list, explode, lit, substring, sum, udf, upper}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions}

import scala.collection.mutable

object JournalDemand extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Converter")
    .getOrCreate()

  import spark.implicits._

  val scJournal = spark.read.parquet("/Users/rizzutih/Downloads/journalDs/part-00000-0b5bfefc-4ee4-48f2-bd67-e3f034b41c22-c000.snappy.parquet")

  val scJournalExplodeIssn = scJournal.select(explode('issn).as("issn"), 'sourcetitle.as("title")).distinct()
  val scJournalExplodeEissn = scJournal.select(explode('eissn).as("issn"), 'sourcetitle.as("title")).distinct()

  val scJournalUnion = scJournalExplodeIssn.union(scJournalExplodeEissn)

//  scJournalUnion.limit(100)
//    .repartition(1)
//    .write
//    .option("header", true)
//    .mode(SaveMode.Overwrite)
//    .parquet("/Users/rizzutih/Downloads/journalDs/curated/")

  val sdTittleRep: Dataset[SDTitleDetailObj.SDTitRepDetail] = spark.read
    .option("header", value = false)
    .option("sep", "\t")
    .csv("/Users/rizzutih/Downloads/keyfe/")
    .toDF(SDTitleDetailObj.newNames: _*)
    .map(SDTitleDetailObj.mapBase)

  val SDTitRep_untyped = sdTittleRep.drop('date)

  val filterYear = (2023 - 4) * 100

  val inputMonths = ((2023 * 12) + 8) + 1

  val SDTitRep_AccountOnly = SDTitRep_untyped.where(upper('data_type) === "JOURNAL"
    and upper('access_method) === "REGULAR"
    and upper('report_platform_id) =!= 10
    and 'date_month > filterYear)

  val SDTitRep_FullData: DataFrame = SDTitRep_AccountOnly
    .withColumn("fta", 'total_item_requests)
    .withColumn("ta", 'limit_exceeded + 'no_license)
    .withColumn("year", substring('date_month, 1, 4))
    .withColumn("month", substring('date_month, 5, 2))
    .withColumn("months", ('year * 12) + 'month)

//  SDTitRep_FullData.limit(100)
//    .repartition(1)
//    .write
//    .option("header", true)
//    .mode(SaveMode.Overwrite).parquet("/Users/rizzutih/Downloads/SDTitRep_FullData/")


  SDTitRep_FullData.select('account_id, 'issn, 'year, 'months, 'fta, 'ta)

  val SDTitRep_AGG = SDTitRep_FullData
    .groupBy('account_id, 'issn, 'year, 'months)
    .agg(sum('fta).as("total_fta"), sum('ta).as("total_ta"))

  val SDTitRep_MonthsTota = SDTitRep_AGG.withColumn("Month_ta", functions.concat(lit(inputMonths) - col("months").cast(IntegerType), lit(":"), 'total_ta))
    .select('account_id, 'issn, 'Month_ta)

  val SDTitRep_collta = SDTitRep_MonthsTota.groupBy('account_id, 'issn).agg(collect_list('Month_ta).as("Month_ta_List"))

  def indexSum(pStart: Int,
               pEnd: Int,
               pArray: mutable.WrappedArray[String]): Int = {
    var sum = 0
    if (pArray != null) {

      for (pair <- pArray) {
        val colon = pair.indexOf(":")
        if (colon == -1) {
          println("Exception")
        }
        else {
          val index = pair.substring(0, colon).toInt.abs
          val total = pair.substring(colon + 1).toInt
          if (index >= pStart && index <= pEnd) {
            sum = sum + total
          }
        }
      }
    }
    sum
  }

  val udfIndexSum = udf[Int, Int, Int, mutable.WrappedArray[String]](indexSum)

  val SDTitRep_split_ta = SDTitRep_collta
    .withColumn("m6", udfIndexSum(lit("0"), lit(6), 'Month_ta_List)).
    withColumn("m12", udfIndexSum(lit("0"), lit(12), 'Month_ta_List)).
    withColumn("m24", udfIndexSum(lit("0"), lit(24), 'Month_ta_List)).
    withColumn("m36", udfIndexSum(lit("0"), lit(36), 'Month_ta_List))

  SDTitRep_split_ta
    //.where('m6 > 0)
    .show(1000, false)
}
