import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}
import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.language.postfixOps

class AmazonS3Utils {

  def constructFullPath(s3Client: AmazonS3,
                        bucketName: String,
                        prefix: String,
                        dateFormatter: String): String = {

    val currentYear = LocalDate.now().getYear

    val commonPrefixes = getLatestCommonPrefixes(currentYear, bucketName, prefix, s3Client)

    val availableDatesList = commonPrefixes.asScala.toList
      .map(str => str.replace(prefix, ""))
      .filter(_.head.isDigit)

    if (availableDatesList.nonEmpty) {
      val sortedAvailableDates = sortDates(availableDatesList, dateFormatter)
      constructPathWithLatestDate(0, prefix, sortedAvailableDates, s3Client, bucketName)
    } else {
      ""
    }
  }

  def getLatestCommonPrefixes(currentYear: Int,
                              bucketName: String,
                              prefix: String,
                              s3Client: AmazonS3): util.List[String] = {

    val maxCapYear = LocalDate.now().minusYears(2).getYear

    val request: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix).withStartAfter(currentYear.toString).withDelimiter("/")

    val commonPrefixes = s3Client.listObjectsV2(request) getCommonPrefixes

    if (!commonPrefixes.isEmpty || currentYear == maxCapYear) {
      commonPrefixes
    } else {
      getLatestCommonPrefixes(currentYear - 1, bucketName, prefix, s3Client)
    }

  }

  def constructPathWithLatestDate(index: Int,
                                  prefix: String,
                                  sortedDatesList: List[String],
                                  s3Client: AmazonS3,
                                  bucketName: String): String = {

    val dateMonth = sortedDatesList.apply(index)
    val commonPrefix = prefix.concat(dateMonth)

    val keys = s3Client.listObjectsV2(bucketName, commonPrefix)
      .getObjectSummaries
      .asScala
      .map(_.getKey)
      .toList

    if (keys.map(key => key.split("/")(key.split("/").length - 1)).contains("_SUCCESS")) {
      s"s3://${bucketName}/${keys.head.split("/").dropRight(1).mkString("/")}/part*"
    } else {
      constructPathWithLatestDate(index + 1, prefix, sortedDatesList, s3Client, bucketName)
    }
  }

  def sortDates(dateList: List[String],
                dateFormat: String): List[String] = {

    val nonDateInformation = dateList.head.substring(dateFormat.length)
    val listToFormat = dateList.map(str => str.replace(nonDateInformation, ""))
    val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
    if (dateFormat.equals("yyyyMM") || dateFormat.equals("yyyy-MM")) {
      return listToFormat.map(x => YearMonth.parse(x, dateTimeFormatter)).reverse
        .map(_.format(dateTimeFormatter).concat(nonDateInformation))
    }
    listToFormat.map(x => LocalDate.parse(x, dateTimeFormatter)).sortBy(_.toEpochDay).reverse
      .map(_.format(dateTimeFormatter).concat(nonDateInformation))
  }
}
