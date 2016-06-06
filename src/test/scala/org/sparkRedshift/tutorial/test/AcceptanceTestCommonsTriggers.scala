package org.sparkRedshift.tutorial.test


import cucumber.api.Scenario
import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers
import org.slf4j.{Logger, LoggerFactory}


class AcceptanceTestCommonsTriggers extends Matchers with ScalaDsl with EN with AcceptanceTestFilesUtils with AcceptanceTestRedshiftUtils  {

  private val LOG: Logger = LoggerFactory.getLogger("AcceptanceTestCommonsTriggers")


  After("@cleanS3csvRawFolder") { scenario: Scenario =>
    s3Client.deleteObject("pjgg-redshift-spark", "csvRaw/category.csv")
  }

  After("@cleanRedshiftRecords") { scenario: Scenario =>
    deleteTable("category")
  }

  def uuid() = java.util.UUID.randomUUID.toString
}
