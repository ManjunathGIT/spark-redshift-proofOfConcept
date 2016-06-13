package org.sparkRedshift.tutorial

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{LoggerFactory, Logger}


object DriverExample extends AwsConfigParameters {

  private val LOG: Logger = LoggerFactory.getLogger("DriverExample")

  val sc = init()

  val sparkContextSql = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    val redShiftConnector = new RedShiftConnectorImpl(sparkContextSql)

    //1. Load from a table
    val eventsDF = redShiftConnector.readTable(Some("event"), None)

    eventsDF.show()
    eventsDF.printSchema()

        //2. Load from a query
        val salesQuery = """SELECT salesid, listid, sellerid, buyerid,
                                   eventid, dateid, qtysold, pricepaid, commission
                            FROM sales
                            ORDER BY saletime DESC LIMIT 10000"""

        val salesDF = redShiftConnector.readTable(None, Some(salesQuery))
        salesDF.show()

        val eventQuery = "SELECT * FROM event"
        val eventDF = redShiftConnector.readTable(None,Some(eventQuery))
        eventDF.show()

        /*
        * Register 'event' table as temporary table 'myevent'
        * so that it can be queried via sqlContext.sql
        */
      redShiftConnector.createTempTable(eventDF, "myevent")

     //Save to a Redshift table from a table registered in Spark

     /*
      * Create a new table redshiftevent after dropping any existing redshiftevent table
      * and write event records with event id less than 1000
      */
     redShiftConnector.writeTable("redshiftevent",SaveMode.Overwrite,"SELECT * FROM myevent WHERE eventid<=1000",Some(Map("eventid"->"id")))

     /*
      * Append to an existing table redshiftevent if it exists or create a new one if it does not
      * exist and write event records with event id greater than 1000
      */
     redShiftConnector.writeTable("redshiftevent",SaveMode.Append,"SELECT * FROM myevent WHERE eventid<=1000",Some(Map("eventid"->"id")))

     /* Let's make an aggregation */
     val salesAGGQuery = """SELECT sales.eventid AS id, SUM(qtysold) AS totalqty, SUM(pricepaid) AS salesamt
                            FROM sales
                            GROUP BY (sales.eventid)
                         """
     val salesAGGDF = redShiftConnector.readTable(None, Some(salesAGGQuery))
     redShiftConnector.createTempTable(salesAGGDF, "salesagg")

     /*
      * Join two DataFrame instances. Each could be sourced from any
      * compatible Data Source
      */
    val salesAGGDF2 = salesAGGDF.join(eventsDF, salesAGGDF("id") === eventsDF("eventid"))
       .select("id", "eventname", "totalqty", "salesamt")

     redShiftConnector.createTempTable(salesAGGDF2,"redshift_sales_agg")

     redShiftConnector.writeTable("redshiftsalesagg",SaveMode.Overwrite, "SELECT * FROM redshift_sales_agg", None)
     val redshiftSalesAggDF = redShiftConnector.readTable(Some("redshiftsalesagg"), None)
     redshiftSalesAggDF.show()

    releaseAttachedResources()
  }

  private def init():SparkContext = {
    val initSc = new SparkContext(new SparkConf().setAppName("SparkSQL"))
    initSc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", getAwsAccessKey)
    initSc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", getAwsSecretKey)
    initSc
  }

  def releaseAttachedResources():Unit = {
    sc.stop()
  }

}
