import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object AccToPlatformApp extends App {
  val spark = sparkSession()

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

  val acctToPlatformSchema = Encoders.product[acct_to_platform].schema

  val props = new java.util.Properties()
  props.setProperty("user", "EPIC_READ_APP")
  props.setProperty("password", "")
  props.setProperty("fetchSize", "100") //Faster downloads the default was 10
  props.setProperty("driver", "oracle.jdbc.driver.OracleDriver")

  val url = s"jdbc:oracle:thin:@//epicutils.e-pic.systems:1521/IDEFIX_ETL_WORK"

  val a = spark.read.jdbc(url, "COMMON_DM_OWN.epic_acct_to_plat_vw", props)
    .select(
      col("ACCOUNT_ID").cast("int").as("acct_id"),
      col("ACCOUNT_SIS_ID").as("acct_sis"),
      col("ACCOUNT_ELS_CUSTOMER_ID").as("acct_els_cust_id"),
      col("ACCOUNT_NAME").as("acct_name"),
      col("account_sector"),
      col("ACCOUNT_COUNTRY_NAME").as("account_country"),
      col("super_account_id").cast("int"),
      col("SUPER_ACCOUNT_SIS_ID").as("super_account_sis_id"),
      col("SUPER_ACCOUNT_ELS_CUSTOMER_ID").as("super_account_els_customer_id"),
      col("SUPER_ACCOUNT_NAME").as("super_account_name"),
      col("super_account_sector"),
      col("super_account_country"),
      col("parent_super_account_id").cast("int"),
      col("parent_SUPER_ACCOUNT_SIS_ID").as("parent_super_account_sis_id"),
      col("parent_sa_els_customer_id").as("parent_sa_els_customer_id"),
      col("parent_SUPER_ACCOUNT_NAME").as("parent_super_account_name"),
      col("parent_super_account_sector"),
      col("parent_super_account_country"),
      col("product_id").cast("int"),
      col("product_name"),
      col("site_name"),
      col("platform_id").cast("int"),
      col("platform_name"),
      col("site_primary_flag"),
      col("platform_primary_flag"))
    .where("account_type = 'Customer'")
//    .withColumn("acct_id", col("acct_id").cast("int"))//
//    .withColumn("PLATFORM_ID", col("PLATFORM_ID").cast("int"))//
//    .withColumn("SUPER_ACCOUNT_ID", col("SUPER_ACCOUNT_ID").cast("int"))//
//    .withColumn("parent_super_account_id", col("parent_super_account_id").cast("int"))//
//    .withColumn("product_id", col("product_id").cast("int"))//
    .withColumn("platform_name", regexp_replace(col("platform_name"), " ", ""))
    .limit(10)
//    .distinct()

  //  val b = spark.createDataFrame(a.rdd, acctToPlatformSchema)
//    b.printSchema()

  a.write.csv("/Users/rizzutih/Downloads/acct_to_platform_test/")

  spark.close()

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }
}
