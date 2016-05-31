package org.sparkRedshift.tutorial

import org.apache.spark.sql.SaveMode


object SparkRedshiftTutorialRefactor {

  def main(args: Array[String]): Unit = {

    //1. Load from a table
    val eventsDF = RedShiftConnectorImpl.readTable(Some("event"), None)

    eventsDF.show()
    eventsDF.printSchema()

    //2. Load from a query
    val salesQuery = """SELECT salesid, listid, sellerid, buyerid,
                               eventid, dateid, qtysold, pricepaid, commission
                        FROM sales
                        ORDER BY saletime DESC LIMIT 10000"""

    val salesDF = RedShiftConnectorImpl.readTable(None, Some(salesQuery))
    salesDF.show()

    val eventQuery = "SELECT * FROM event"
    val eventDF = RedShiftConnectorImpl.readTable(None,Some(eventQuery))
    eventDF.show()

    /*
    * Register 'event' table as temporary table 'myevent'
    * so that it can be queried via sqlContext.sql
    */
    RedShiftConnectorImpl.createTempTable(eventDF, "myevent")

    //Save to a Redshift table from a table registered in Spark

    /*
     * Create a new table redshiftevent after dropping any existing redshiftevent table
     * and write event records with event id less than 1000
     */
    RedShiftConnectorImpl.writeTable("redshiftevent",SaveMode.Overwrite,"SELECT * FROM myevent WHERE eventid<=1000",Some(Map("eventid"->"id")))

    /*
     * Append to an existing table redshiftevent if it exists or create a new one if it does not
     * exist and write event records with event id greater than 1000
     */
    RedShiftConnectorImpl.writeTable("redshiftevent",SaveMode.Append,"SELECT * FROM myevent WHERE eventid<=1000",Some(Map("eventid"->"id")))

    /* Let's make an aggregation */
    val salesAGGQuery = """SELECT sales.eventid AS id, SUM(qtysold) AS totalqty, SUM(pricepaid) AS salesamt
                           FROM sales
                           GROUP BY (sales.eventid)
                        """
    val salesAGGDF = RedShiftConnectorImpl.readTable(None, Some(salesAGGQuery))
    RedShiftConnectorImpl.createTempTable(salesAGGDF, "salesagg")

    /*
     * Join two DataFrame instances. Each could be sourced from any
     * compatible Data Source
     */
    val salesAGGDF2 = salesAGGDF.join(eventsDF, salesAGGDF("id") === eventsDF("eventid"))
      .select("id", "eventname", "totalqty", "salesamt")

    RedShiftConnectorImpl.createTempTable(salesAGGDF2,"redshift_sales_agg")

    RedShiftConnectorImpl.writeTable("redshiftsalesagg",SaveMode.Overwrite, "SELECT * FROM redshift_sales_agg", None)
    val redshiftSalesAggDF = RedShiftConnectorImpl.readTable(Some("redshiftsalesagg"), None)
    redshiftSalesAggDF.show()
  }

}
