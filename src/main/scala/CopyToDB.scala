import ConvertToCSV.{spark, sparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object CopyToDB extends App {

  val spark = sparkSession()

  val account_platform = "/Users/rizzutih/Downloads/account_to_platform/"

  case class acct_to_platform(
                               acct_id: Int,
                               acct_sis: String,
                               acct_els_cust_id: String,
                               acct_name: String,
                               account_sector: String,
                               account_country: String,
                               super_account_id: Int,
                               super_account_sis_id: String,
                               super_account_els_customer_id: String,
                               super_account_name: String,
                               super_account_sector: String,
                               super_account_country: String,
                               parent_super_account_id: Int,
                               parent_super_account_sis_id: String,
                               parent_sa_els_customer_id: String,
                               parent_super_account_name: String,
                               parent_super_account_sector: String,
                               parent_super_account_country: String,
                               product_id: Int,
                               product_name: String,
                               site_name: String,
                               platform_id: Int,
                               platform_name: String,
                               site_primary_flag: String,
                               platform_primary_flag: String
                             )

  val url = s"jdbc:postgresql://dev-epicdb.np.e-pic.systems:5432/epic"

  val props = new java.util.Properties()
  props.setProperty("user", "epic")
  props.setProperty("password", "")
  props.put("driver", "org.postgresql.Driver")
  props.setProperty("ssl", "true")
  props.setProperty("reWriteBatchedInserts", "true")
  props.setProperty("stringtype", "unspecified")

  val Acct_to_Platform_Schema = Encoders.product[acct_to_platform].schema
  val Acct_to_Platform = spark.read.schema(Acct_to_Platform_Schema).option("sep", ",")
    .option("compression", "gzip").csv(account_platform)

  Acct_to_Platform.write.option("truncate", true).mode(SaveMode.Overwrite).jdbc(url, "acct_to_platform", props)

  spark.close()

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }
}
