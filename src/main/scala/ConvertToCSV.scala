import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object ConvertToCSV extends App {

  val credentials = new ProfileCredentialsProvider("epicnonprod").getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  //val filename = "sd_fact_journal_article-2023-01-0000_part_00.parquet"

  val a = spark.read.parquet("s3a://epic-backend-dataset-nonprod/warehouse/weekly_dataset/import_audit/*/part-*")

  spark.close()

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }
}
