import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest

class EpicS3ClientLoader(credentialsProvider: AWSCredentialsProvider) {

  val bucketRolesMap = Map(
    "sccontent-parsed-ipr-parquet-prod" -> "arn:aws:iam::050542495957:role/bos_ss_institution-profiles-multi-column-parquet",
    "sccontent-parsed-ani-core-parquet-prod" -> "arn:aws:iam::050542495957:role/bos_ss_abstracts-indices-core-multi-column-parquet",
    "com-elsevier-ech-masterdata" -> "arn:aws:iam::050542495957:role/bos_ss_customer-source-master-data",
    "sdcontent-articles-parsed-parquet-prod" -> "arn:aws:iam::050542495957:role/bos_ss_primary-content-articles-parsed-parquet",
    "com-elsevier-bos-live-data-pii" -> "arn:aws:iam::050542495957:role/bos_ss_source-documents",
    "com-elsevier-bos-live-data" -> "arn:aws:iam::050542495957:role/bos_ss_bos-live-data_mendeley-catalogue_canonical-documents")

  def getAmazonS3Client(bucket: String): AmazonS3 = {

    val region = Regions.US_EAST_1

    val s3Client = if (bucketRolesMap.contains(bucket)) {
      getAmazonS3ClientWithAssociatedRole(bucket, region)
    } else {
      AmazonS3Client.builder()
        .withCredentials(credentialsProvider)
        .withRegion(region)
        .build()
    }

    s3Client
  }

  def getAmazonS3ClientWithAssociatedRole(bucket: String,
                                          region: Regions): AmazonS3 = {

    val roleArn = bucketRolesMap(bucket)
    val roleSessionName = bucket

    val stsClient = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withRegion(region)
      .withCredentials(credentialsProvider)
      .build()

    val assumeRoleRequest = new AssumeRoleRequest()
      .withRoleSessionName(roleSessionName)
      .withRoleArn(roleArn)

    val assumedCredentials = stsClient.assumeRole(assumeRoleRequest).getCredentials

    val basicSessionCredentials = new BasicSessionCredentials(
      assumedCredentials.getAccessKeyId,
      assumedCredentials.getSecretAccessKey,
      assumedCredentials.getSessionToken
    )

    val provider = new AWSStaticCredentialsProvider(basicSessionCredentials)

    AmazonS3ClientBuilder.standard().withCredentials(provider).withRegion(region).build()
  }
}
