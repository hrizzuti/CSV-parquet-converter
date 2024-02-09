import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array_distinct, col, explode, lit, udf, when}

import scala.collection.mutable

object ReadLocally extends App {

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

  import spark.implicits._

  val toReferenceUDF = udf[mutable.WrappedArray[reference], mutable.WrappedArray[Long]](toReference)
  val toArrayStringUDF = udf[mutable.WrappedArray[String], mutable.WrappedArray[Long]](toArrayString)
  val ani_parsed_ds2 = spark.read.parquet("s3a://epic-backend-transform-prod/cooked/SCANI_cache/20220808/")
  //  val ani_parsed_ds = spark.read.parquet("/Users/rizzutih/Downloads/part-01539-10e1261e-0199-4c98-93d7-4a390a3c2556-c000.gz.parquet")
  //
  //
  //  val ani_parsed_ds2 = ani_parsed_ds.select('Eid.as('eid), when($"PII".isNull, "").otherwise($"PII").as('pii),
  //    'pub_year.cast("String").as('pub_year),
  //    when('openaccess.isNull, "")
  //      .when('openaccess === "Repository", "2")
  //      .when('openaccess === "None", "0")
  //      .when('openaccess === "Full", "1")
  //      .as("is_openaccess"),
  //    $"source.srcid".cast("String").as("source_id"),
  //    $"source.publishername".as("publisher"),
  //    $"source.type".as("source_type"),
  //    toArrayStringUDF($"Af.afid").as("affiliation_ids"),
  //    array_distinct($"Af.affiliation_country").as("affiliation_countries"),
  //    toReferenceUDF($"citations").as("references"))

  //  private val ani_parsed_ds3: Dataset[Row] = ani_parsed_ds2.limit(10)
  //  ani_parsed_ds3.coalesce(1).write
  //      .mode(SaveMode.Append)
  //      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  //      .json("/Users/rizzutih/Downloads/")

  val items = Array[String]("60175879", "60175887", "60021895", "60020045")
  //
  //  val ani_parsed_ds3 = ani_parsed_ds2.select($"eid", $"pub_year", $"is_openaccess", $"source_id", $"publisher",
  //    $"source_type", explode($"affiliation_ids").as("affiliation"), $"affiliation_countries", $"references")
  //
  //  ani_parsed_ds3.where($"pub_year" === 2021).filter($"affiliation".isin(items: _*)).coalesce(1).write
  //    .mode(SaveMode.Append)
  //    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  //    .json("/Users/rizzutih/Downloads/")

  //  def array_contains_any(s: Seq[String]): UserDefinedFunction = {
  //    udf((c: mutable.WrappedArray[String]) =>
  //      c.toList.intersect(s).nonEmpty)
  //  }
  //
  //  //val b: Array[String] = Array("60030612", "60005558")
  //  ani_parsed_ds2.where($"pub_year" === 2021)
  //    .where(array_contains_any(items)($"affiliation_ids"))
  //    .coalesce(1).write
  //      .mode(SaveMode.Append)
  //      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  //      .json("/Users/rizzutih/Downloads/")

  val items2 = Array[Long](6328243, 85032005958L, 85064144664L, 85101778683L, 85106749794L, 85116480112L, 85118980038L)

  ani_parsed_ds2.filter(col("eid").isin(items2:_*))
    .select($"eid", $"source_id")
    .coalesce(1).write
    .mode(SaveMode.Append)
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .csv("/Users/rizzutih/Downloads/")

  //  case class Inst_Affil(sis_hq: String, customer_id: String, affil_id: String, affil_name: String)
  //
  //  val Inst_Schema = Encoders.product[Inst_Affil].schema
  //  val inst_affil = spark.read.option("header", true).option("sep", "\t").schema(Inst_Schema).csv("/Users/rizzutih/Downloads/institution_affiliations.csv")
  //  val s1 = spark.read.option("header", true).option("sep", "\t").csv("/Users/rizzutih/Downloads/account_mapping.csv")
  //  val s2 = spark.read.option("header", true).option("sep", ",").csv("/Users/rizzutih/Downloads/ae_highest_level_superaccount.csv")
  //
  //  val acc_map = s1.filter(x => x(8).toString.toUpperCase().split(",").contains("SCOPUS")).map(x => x(3).toString).toDF("customer_id")
  //  val sup_acc = s2.filter(x => x(3).toString.replace("{", "").replace("}", "").toUpperCase().split(",").contains("SCOPUS")).map(x => x(0).toString).toDF("customer_id")
  //  val acc_sup_map = acc_map.union(sup_acc)
  //
  //  val inst_affil_scopus_filter = inst_affil.join(acc_sup_map, inst_affil("customer_id") === acc_sup_map("customer_id")).drop(acc_sup_map("customer_id"))
  //
  //  val ani_results_expl = ani_parsed_ds2
  //    .where($"pub_year" =!= "")
  //    .select($"eid"
  //      , $"source_id"
  //      , $"pub_year".cast("Int").as("pub_year")
  //      , explode($"affiliation_ids").as("affiliation")
  //      , $"references")
  //    .join(inst_affil_scopus_filter, $"affil_id" === $"affiliation").drop("affil_id", "affiliations_id", "affil_name", "affiliation")
  //    .distinct()
  //
  //  ani_results_expl.coalesce(1).write
  //    .mode(SaveMode.Append)
  //    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  //    .json("/Users/rizzutih/Downloads/")
  //
  //  val affil_source = ani_results_expl
  //    .where($"sis_hq" =!= "")
  //    .groupBy($"sis_hq", $"source_id", $"pub_year", $"customer_id")
  //    .count().as("count")
  //    .select($"sis_hq", $"source_id", $"pub_year", $"count", lit("source").as("report_type"), $"customer_id")
  //
  //  val affil_ref = ani_results_expl
  //    //    .where($"customer_id" === "ECR-385720")
  //    .select($"sis_hq", explode($"references").as("reference"), $"pub_year", $"customer_id")
  //    .select($"sis_hq", $"reference.eid".as("reference"), $"pub_year", $"customer_id").as("a")
  //    .join(ani_parsed_ds2, $"eid" === $"reference")
  //    .select($"a.sis_hq", $"source_id", $"a.pub_year", $"a.customer_id")
  //    .where($"source_id" =!= "")
  //    .groupBy($"sis_hq", $"source_id", $"pub_year", $"customer_id")
  //    .count().as("count")
  //    .select($"sis_hq", $"source_id", $"pub_year", $"count", lit("reference").as("report_type"), $"customer_id")
  //
  //  affil_source.union(affil_ref)
  //    .where($"source_id" === "21206")
  //    .where($"pub_year" === 2021)
  //    .coalesce(1).write
  //    .mode(SaveMode.Append)
  //    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  //    .json("/Users/rizzutih/Downloads/")
}
