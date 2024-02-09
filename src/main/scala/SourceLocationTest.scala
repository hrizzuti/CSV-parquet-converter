import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.LocalDateTime

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
import scala.collection.mutable.ListBuffer

object SourceLocationTest extends App {


  val x = LocalDateTime.now()

  val todayDate = LocalDate.now()
  val toYYYY = todayDate.minusYears(1).getYear
  val fromYYYY = todayDate.minusYears(5).getYear

  val dates = new ListBuffer[String]()

  for (year <- fromYYYY to toYYYY) {
    val yearStr = year.toString
    for (month <- 1 to 12) {
      if (month <= 9) {
        dates += yearStr + String.format("0%d", Integer.valueOf(month))
      } else {
        dates += yearStr + month
      }
    }
  }

  val bucket = "com-elsevier-usage-dataset-prod"
  val prefix = "out/sd/other/fact_journal_article/"
  val sdJbsCustomerUsage = new ListBuffer[String]()

  val credentials = new ProfileCredentialsProvider("prod").getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  dates.foreach(e => sdJbsCustomerUsage += s"s3a://$bucket/$prefix/$e/")

  val entitlementReason: List[String] = List("Author", "AuthorShare", "Condition", "OpenArchive", "Package",
    "Purchased", "SampleCopy", "Subscription", "Unsubscribed", "Promotion", "OpenAccess", "SponsoredAccess", "Manuscript")

  val df = spark.read.parquet(sdJbsCustomerUsage: _*)
    .filter(col("entitlement_reason").isin(entitlementReason: _*)
    && col("report_platform_id") === 1)
    .groupBy("date_month", "account_id", "publisher_item_id")
    .agg(sum("fta_count").alias("fta_count"))

  df.repartition(1)
    .write
    .option("header", true)
    .mode(SaveMode.Overwrite)
    .csv("src/main/filesOut/sdJbsUsage/")

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }
}
