
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
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

  val credentials = new ProfileCredentialsProvider("epicprod").getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  val definitionDataFrame = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("sep", ",")
    .csv("s3a://epic-backend-build-artifacts/appcfg/weekly_dataset_definitions.csv")

  val records = definitionDataFrame.select("id", "table_name", "s3_destination", "writes_to_db", "writes_to_s3", "file_format")
    .collect()
    .map(r => DefinitionRecord(r.getInt(0), r.getString(1), r.getString(2), r.getBoolean(3), r.getBoolean(4),
      r.getString(5))).toList

  records.foreach(r=>println(r.s3Destination))
}

sealed case class DefinitionRecord(id: Int,
                                   tableName: String,
                                   s3Destination: String,
                                   writeToDatabase: Boolean,
                                   writeToS3: Boolean,
                                   fileFormat: String)
