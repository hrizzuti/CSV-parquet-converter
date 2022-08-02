
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object TestFiles extends App {

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }

  val sts = AWSSecurityTokenServiceClientBuilder.standard()
    .withRegion("us-east-1")
    .withCredentials(new ProfileCredentialsProvider("epicsuper")).build()

  val role = new STSAssumeRoleSessionCredentialsProvider.Builder(
    "arn:aws:iam::050542495957:role/bos_ss_customer-source-master-data",
    "assumeRole-Session")
    .withStsClient(sts)
    .build()

  val credentials = role.getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  val result = spark.read.parquet("s3a://com-elsevier-ech-masterdata/prod/staging/elsview_customers/live/20220730_225900_00055_z5q85_f944612b-4bc0-4ec0-85dc-c4c4c7600976")
}
