import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import scala.collection.mutable

object ReadLocally2 extends App {

  case class reference(eid: Option[Long])

  def toReference(n: mutable.WrappedArray[Long]): mutable.WrappedArray[reference] = {

    if (n == null)
      mutable.WrappedArray.empty
    else {
      n.map(x => reference(Some(x)))

    }
  }

  def toArrayString(n: mutable.WrappedArray[Long]): mutable.WrappedArray[String] = {
    if (n == null)
      mutable.WrappedArray.empty
    else {
      n.map(x => x.toString)

    }
  }

  //  val spark: SparkSession = SparkSession.builder()
  //    .master("local[4]")
  //    .appName("Converter")
  //    .getOrCreate()


  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .appName("Converter")
      .config(new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost"))
      .getOrCreate()

    sparkSession
  }

  import spark.implicits._

  val credentials = new ProfileCredentialsProvider("epicprod").getCredentials

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", credentials.asInstanceOf[AWSSessionCredentials].getSessionToken)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  val spark = sparkSession()

  val ani_parsed_ds2 = spark.read.parquet("s3a://epic-backend-transform-prod/cooked/SCANI_cache/20220808/")

    case class Inst_Affil(sis_hq: String, customer_id: String, affil_id: String, affil_name: String)

    val Inst_Schema = Encoders.product[Inst_Affil].schema
    val inst_affil = spark.read.option("header", true).option("sep", "\t").schema(Inst_Schema).csv("/Users/rizzutih/Downloads/institution_affiliations.csv")
    val s1 = spark.read.option("header", true).option("sep", "\t").csv("/Users/rizzutih/Downloads/account_mapping.csv")
    val s2 = spark.read.option("header", true).option("sep", ",").csv("/Users/rizzutih/Downloads/ae_highest_level_superaccount.csv")

    val acc_map = s1.filter(x => x(8).toString.toUpperCase().split(",").contains("SCOPUS")).map(x => x(3).toString).toDF("customer_id")
    val sup_acc = s2.filter(x => x(3).toString.replace("{", "").replace("}", "").toUpperCase().split(",").contains("SCOPUS")).map(x => x(0).toString).toDF("customer_id")
    val acc_sup_map = acc_map.union(sup_acc)

    val inst_affil_scopus_filter = inst_affil.join(acc_sup_map, inst_affil("customer_id") === acc_sup_map("customer_id")).drop(acc_sup_map("customer_id"))

    val ani_results_expl = ani_parsed_ds2
      .where($"pub_year" =!= "")
      .select($"eid"
        , $"source_id"
        , $"pub_year".cast("Int").as("pub_year")
        , explode($"affiliation_ids").as("affiliation")
        , $"references")
      .join(inst_affil_scopus_filter, $"affil_id" === $"affiliation").drop("affil_id", "affiliations_id", "affil_name", "affiliation")
      .distinct()

    val affil_ref = ani_results_expl
      .select($"sis_hq", explode($"references").as("reference"), $"pub_year", $"customer_id")
      .select($"sis_hq", $"reference.eid".as("reference"), $"pub_year", $"customer_id").as("a")
      .join(ani_parsed_ds2, $"eid" === $"reference")
      .select($"a.sis_hq", $"source_id", $"a.pub_year", $"a.customer_id", $"a.reference")
//      .where($"source_id" =!= "")
//      .groupBy($"sis_hq", $"source_id", $"pub_year", $"customer_id")
//      .count().as("count")
//      .select($"sis_hq", $"source_id", $"pub_year", $"count", lit("reference").as("report_type"), $"customer_id")

    affil_ref.write
      .mode(SaveMode.Append)
      .option("header", true)
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .csv("/Users/rizzutih/Downloads/scani/")
}
