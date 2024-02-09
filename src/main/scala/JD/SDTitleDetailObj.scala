package JD


import org.apache.spark.sql.Row

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util._

object SDTitleDetailObj {

  val newNames = Seq(
    "date_month",
    "account_id",
    "report_platform_id",
    "issn",
    "isbn",
    "yop",
    "section_type",
    "data_type",
    "access_type",
    "access_method",
    "limit_exceeded",
    "no_license",
    "total_item_investigations",
    "unique_item_investigations",
    "unique_title_investigations",
    "total_item_requests",
    "unique_item_requests",
    "unique_title_requests"
  )

  case class SDTitRepDetail(
                         date: Option[java.sql.Date],
                         date_month:Int,
                         account_id:Int,
                         report_platform_id:Int,
                         issn:String,
                         isbn:String,
                         yop:Int,
                         section_type:String,
                         data_type:String,
                         access_type:String,
                         access_method:String,
                         limit_exceeded:Int,
                         no_license:Int,
                         total_item_investigations:Int,
                         unique_item_investigations:Int,
                         unique_title_investigations:Int,
                         total_item_requests:Int,
                         unique_item_requests:Int,
                         unique_title_requests:Int
                       )


  def toNumber(s:String):Int ={
    Try(s.trim.toInt) match {
      case Success(s) => s
      case Failure(f) => 0
    }
  }
  def toDate(s:String):Option[java.sql.Date] ={
    Try(LocalDate.parse(s,DateTimeFormatter.ofPattern("yyyy-MM-dd"))) match {
      case Success(s) => Some(java.sql.Date.valueOf(s))
      case Failure(f) => None
    }
  }
  def mapBase(row:Row):SDTitRepDetail = {
      def yopInt(yop:String):Int ={
      Try( yop.trim.toUpperCase.toInt) match {
        case Success(s) => s
        case Failure(f) => 9999
      }

    }

val datetoDDMM = (DateString:String) => DateString.replace("-","").trim().substring(0,6).toInt

    SDTitRepDetail(
      toDate(row.getAs[String]("date_month")),
      datetoDDMM(row.getAs[String]("date_month")),
      toNumber(row.getAs[String]("account_id")),
      toNumber(row.getAs[String]("report_platform_id")),
      row.getAs[String]("issn"),
      row.getAs[String]("isbn"),
      yopInt(row.getAs[String]("yop")),
      row.getAs[String]("section_type"),
      row.getAs[String]("data_type"),
      row.getAs[String]("access_type"),
      row.getAs[String]("access_method"),
      toNumber(row.getAs[String]("limit_exceeded")),
      toNumber(row.getAs[String]("no_license")),
      toNumber( row.getAs[String]("total_item_investigations")),
      toNumber( row.getAs[String]("unique_item_investigations")),
      toNumber(row.getAs[String]("unique_title_investigations")),
      toNumber(row.getAs[String]("total_item_requests")),
      toNumber( row.getAs[String]("unique_item_requests")),
      toNumber( row.getAs[String]("unique_title_requests"))
    )
  }
}
