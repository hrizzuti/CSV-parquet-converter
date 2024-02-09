import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestEPSCountryData extends App {

  val credentials = new ProfileCredentialsProvider("epicsuper").getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")


  val spark = sparkSession()

//  val a = spark.read
//    .format("csv")
//    .option("header", true)
//    .load("/Users/rizzutih/Downloads/evs_read_top_countries_all/")

  val a = spark.read
    .format("parquet")
    .load("s3a://epic-backend-dataset-prod/warehouse/weekly_dataset/temp/evs_read_top_countries_all/2023-03-22/")

  a.createTempView("evs_read_top_countries_all")

//  val b = spark.read
//    .format("csv")
//    .option("header", true)
//    .load("/Users/rizzutih/Downloads/accounts_flat_map.csv")
//
//  b.createTempView("accounts_flat_map")
//
//  val c = spark.read
//    .format("csv")
//    .option("header", true)
//    .load("/Users/rizzutih/Downloads/evs_read_top_countries_fc/")
//
//  c.createTempView("evs_read_top_countries_fc")

    val all = spark.sql("select acc.sis_hq as sis_hq, COUNT(distinct ra.reading_countries) as total_all FROM evs_read_top_countries_all ra inner join accounts_flat_map acc on acc.acc_id = ra.account_id AND acc.sis_hq = -866 where ra.content = 'all' group by sis_hq;")

    all.show(false)

  //  val all_notoa = spark.sql("SELECT acc.sis_hq as sis_hq, COUNT(distinct ra.reading_countries) as total_all_not_oa FROM evs_read_top_countries_all ra inner join accounts_flat_map acc on acc.acc_id = ra.account_id AND acc.sis_hq = -866 where ra.content = 'all_notoa' group by sis_hq;")
  //
  //  all_notoa.show(false)

//  val fc = spark.sql("select acc.sis_hq as sis_hq, COUNT(distinct rf.reading_countries) as total_fc FROM evs_read_top_countries_fc rf inner join accounts_flat_map acc on acc.acc_id = rf.account_id AND acc.sis_hq = -866 WHERE rf.content = 'fc' group by sis_hq;")
//
//  fc.show(false)

//  val fc_notoa = spark.sql("SELECT acc.sis_hq as sis_hq, COUNT(distinct rf.reading_countries) as total_fc_not_oa FROM evs_read_top_countries_fc rf inner join accounts_flat_map acc on acc.acc_id = rf.account_id AND acc.sis_hq = -866 WHERE rf.content = 'fc_notoa' group by sis_hq;")
//
//  fc_notoa.show(false)

  spark.close()

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }

}
