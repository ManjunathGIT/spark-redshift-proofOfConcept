package org.sparkRedshift.tutorial.test

import cucumber.api.Scenario
import cucumber.api.scala.{ScalaDsl, EN}
import org.apache.spark.sql.SaveMode
import org.junit.Assert._
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.words.ShouldVerb
import org.sparkRedshift.tutorial.{ScaldiModule, RedShiftConnector}
import scaldi.{Injector, Injectable}


class WriteRedshift extends Matchers with Injectable  with ShouldVerb with ScalaFutures with EN with ScalaDsl with AcceptanceTestFilesUtils with AcceptanceTestRedshiftUtils{

  implicit val injector:Injector = new ScaldiModule

  val redShiftConnector = inject[RedShiftConnector]

  Then("""^using RedShiftConnector I copy "([^"]*)" table to "([^"]*)"  and check that has (\d+) records$"""){
    (tableName:String, testTableName:String, expectedResult:Int) =>
      val categoryDF = redShiftConnector.readTable(Some(tableName),None)
      redShiftConnector.createTempTable(categoryDF, "categoryTmp")
      redShiftConnector.writeTable(testTableName,SaveMode.Overwrite,s"SELECT * FROM categoryTmp", None)
      val amount = redShiftConnector.readTable(Some(testTableName),None).count()
      assertTrue(s"temp $testTableName Table should have $expectedResult  but has $amount records ", amount == expectedResult)
  }

  Then("""^using RedShiftConnector I append "([^"]*)" table to "([^"]*)"  and check that has (\d+) records$"""){
    (tmpTableName:String, testTableName:String, expectedResult:Int) =>
      redShiftConnector.writeTable(testTableName,SaveMode.Append,s"SELECT * FROM categoryTmp", None)
      val amount = redShiftConnector.readTable(Some(testTableName),None).count()
      assertTrue(s"temp $testTableName Table should have $expectedResult  but has $amount records ", amount == expectedResult)
  }

  Then("""^using RedShiftConnector I make a write statement with saveMode "([^"]*)" over  "([^"]*)" table to "([^"]*)"  and check that has (\d+) records$"""){
    (saveMode:String, tmpTableName:String, testTableName:String, expectedResult:Int) =>
      val sMode = saveMode match {
        case "append" => SaveMode.Append
        case "errorIfExists" => SaveMode.ErrorIfExists
        case "ignore" => SaveMode.Ignore
        case _ => SaveMode.Overwrite
      }

      redShiftConnector.writeTable(testTableName,sMode,s"SELECT * FROM categoryTmp", None)
      val amount = redShiftConnector.readTable(Some(testTableName),None).count()
      assertTrue(s"temp $testTableName Table should have $expectedResult  but has $amount records ", amount == expectedResult)
  }

  After("@cleanTestCategoryTable") { scenario: Scenario =>
    deleteTable("testCategory")
    redShiftConnector.dropTempTable("categoryTmp")
  }

}
