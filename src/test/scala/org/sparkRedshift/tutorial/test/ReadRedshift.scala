package org.sparkRedshift.tutorial.test

import cucumber.api.scala.{ScalaDsl, EN}
import org.apache.spark.sql.SaveMode
import org.junit.Assert._
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.words.ShouldVerb
import org.sparkRedshift.tutorial.RedShiftConnectorImpl

class ReadRedshift extends Matchers with ShouldVerb with ScalaFutures with EN with ScalaDsl with AcceptanceTestFilesUtils with AcceptanceTestRedshiftUtils{

  Then("""^using RedShiftConnector I read table "([^"]*)" form redshift and check that has (\d+) records$"""){
    (tableName:String, expectedAmount:Int) =>

      val categoryDF = RedShiftConnectorImpl.readTable(Some(tableName),None)
      val amount = categoryDF.count()
      assertTrue(s"category Table should have $expectedAmount  but has $amount records ", amount == expectedAmount)
  }

  Then("""^using RedShiftConnector I make the following "([^"]*)" and check that the amount of records is (\d+)$"""){
    (query:String, expectedAmount:Int) =>
      val categoryDF = RedShiftConnectorImpl.readTable(None,Some(query))
      val amount = categoryDF.count()
      assertTrue(s"category Table should have $expectedAmount  but has $amount records ", amount == expectedAmount)
  }


  Then("""^I create a temp table with "([^"]*)" and "([^"]*)" and check that the amount of records is (\d+)$"""){
    (tableName:String, query:String, expectedAmount:Int) =>
      val categoryDF = RedShiftConnectorImpl.readTable(None,Some(query))
      RedShiftConnectorImpl.createTempTable(categoryDF,tableName)
      val tempCategoryTableDF = RedShiftConnectorImpl.readTable(Some(tableName), None, true)
      val amount = tempCategoryTableDF.count()
      assertTrue(s"temp category Table should have $expectedAmount  but has $amount records ", amount == expectedAmount)
  }

  Then("""^I create a temp table with "([^"]*)" and "([^"]*)" and the I make a "([^"]*)" check that the amount of records is (\d+)$"""){
    (tableName:String, query:String, querytmp:String, expectedAmount:Int) =>
    val categoryDF = RedShiftConnectorImpl.readTable(None,Some(query))
    RedShiftConnectorImpl.createTempTable(categoryDF,tableName)
    val tempCategoryTableDF = RedShiftConnectorImpl.readTable(None, Some(querytmp), true)
    val amount = tempCategoryTableDF.count()
    assertTrue(s"temp category Table should have $expectedAmount  but has $amount records ", amount == expectedAmount)
  }

  Then("""^clean temporalTables "([^"]*)"$"""){ (tableName:String) =>
    RedShiftConnectorImpl.dropTempTable(tableName)
  }
}
