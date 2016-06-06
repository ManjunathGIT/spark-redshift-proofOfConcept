package org.sparkRedshift.tutorial.test

import cucumber.api.scala.{ScalaDsl, EN}
import org.junit.Assert._
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.words.ShouldVerb
import org.slf4j.{LoggerFactory, Logger}
import scaldi.Injectable


class FromS3ToRedshift extends Matchers with ShouldVerb with ScalaFutures with EN with ScalaDsl with AcceptanceTestFilesUtils with AcceptanceTestRedshiftUtils {

  private val LOG: Logger = LoggerFactory.getLogger("FromS3ToRedshift")

  Given("""^a CSV "([^"]*)" and a bucket "([^"]*)"$"""){ (csvFileName:String, bucketName:String) =>
    assertTrue(s"File $csvFileName Not found!", getFileFromResourceFolder(csvFileName).exists())
    assertTrue(s"Bucket $bucketName Not found!", checkIFS3BucketExist(bucketName))
  }

  Given("""^a CSV "([^"]*)" a bucket "([^"]*)" a redshiftDb "([^"]*)" with a table "([^"]*)" already created$"""){
    (csvFileName:String, bucketName:String, dbName:String, tableName:String) =>
      assertTrue(s"File $csvFileName Not found!", getFileFromResourceFolder(csvFileName).exists())
      assertTrue(s"Bucket $bucketName Not found!", checkIFS3BucketExist(bucketName))
      assertTrue(s"table $tableName Not found!", tableExist(tableName))
  }

  Then("""^I copy file "([^"]*)" in bucket "([^"]*)" into db "([^"]*)" and table "([^"]*)"$"""){
    (csvFileName:String, bucketName:String, dbName:String, tableName:String) =>
      copyCsvIntoTable(s"s3://$bucketName/$csvFileName", tableName)

  }

  When("""^I request to upload CSV "([^"]*)" to bucket "([^"]*)" and keyName "([^"]*)"$"""){ (csvFileName:String, bucketName:String, keyName:String) =>
    val csvFile = getFileFromResourceFolder(csvFileName)
    uploadFileToS3(bucketName, csvFile, keyName)
  }

  Then("""^I check that the file"([^"]*)" in bucket "([^"]*)" exist$"""){ (csvFileName:String, bucketName:String) =>
    assertTrue(s"File $bucketName/$csvFileName Not found!", checkIFS3FileExist(bucketName, csvFileName))
  }

}
