import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object TestAccToPlatformData extends App {

  val credentials = new ProfileCredentialsProvider("epicenterprise").getCredentials

  sparkSession().sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession().sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession().sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession().sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession().sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  val a = spark.read.parquet("s3a://epic-backend-dataset-prod/warehouse/daily_dataset/account_to_platform/2024-01-21/")

  a.where(col("super_account_id") === 682546).coalesce(1).write
    .mode(SaveMode.Append)
    .option("header", true)
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .csv("/Users/rizzutih/Downloads/")

  spark.close()

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }

}
