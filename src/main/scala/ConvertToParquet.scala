import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, from_unixtime, trunc}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.text.SimpleDateFormat
import java.util.Calendar

object ConvertToParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Converter")
    .getOrCreate()

  val filename = "day"

  //  val df = spark.read
  //    .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
  //    .csv(s"src/main/filesInput/$filename.csv")

//    spark.read
//    .csv("src/main/filesOut/sdKeyfe/part-00000-21b3c311-940c-4f7c-9a33-b512e7f337b1-c000.csv")
//    .limit(10000)
//    .repartition(1)
//    .write.option("header", true)
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/filesOut/sdKeyfe/smallerDataset/")
//
//  val credentials = new ProfileCredentialsProvider("epicsuper").getCredentials
//
//  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
//  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
//  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
//  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
//  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
//
//  val sd_keyevents_dir = "s3a://epic-backend-prod/warehouse/keyfe_usage"

  val filepath = List(s"/Users/rizzutih/Downloads/day")
//  val filepath = List(s"$sd_keyevents_dir/year=2023/month=1/day*/type*/day*")

  val download_ke: List[String] = List("ArticleURLrequestPage", "ArticleURL"
    , "MiamiImageURLrequestPage", "ArticleURLchapterView", "MiamiImageURLchapterView", "MiamiImageURL", "ArticleURLrequestExcerpt"
    , "MiamiImageURLrequestPDF", "MiamiImageURLdownloadPDF")

  val invalid_retcodes: List[Int] = List(301, 404)
  val download_articleformat: List[String] = List("pdf", "html")
  val download_objecturl: List[String] = List("ArticleURL", "MiamiImageURL")
  val download_activity: List[String] = List("requestPage", "requestExcerpt", "chapterView", "requestPDF", "downloadPDF")

  import spark.implicits._

  val sd_keyevents = spark.read
    .option("delimiter", "\t")
    .option("header", false)
    .csv(filepath: _*)
    .toDF("timestamp", "pii", "primary_user_id", "primary_account_id", "referrer", "entitlement_reason", "article_format", "ip_location_id",
      "browser_type", "primary_department_id", "zone", "origin", "issn", "isbn", "node", "return_code", "entitlement_account_id", "key_event", "activity",
      "object", "recommended_by")
    .filter(col("pii").isNotNull && !'return_code.isin(invalid_retcodes: _*)
      && 'key_event.isin(download_ke: _*)
      && 'article_format.isin(download_articleformat: _*)
      && 'object.isin(download_objecturl: _*) && 'activity.isin(download_activity: _*)).limit(100)

  sd_keyevents.withColumn("date_month", trunc(from_unixtime(sd_keyevents("timestamp")), "month"))
    .select('timestamp, 'date_month).show(100)
//    .groupBy('date_month)
//    .agg(functions.count('pii).as('total))

//  sd_keyevents.repartition(1)
//    .write
//    .option("header", true)
//    .mode(SaveMode.Overwrite)
//    .csv("src/main/filesOut/sdKeyfe/")

  spark.close()

//  def sparkSession(): SparkSession = {
//    val sparkSession: SparkSession = SparkSession.builder
//      .appName("Converter")
//      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
//      .getOrCreate()
//
//    sparkSession
//  }
}

