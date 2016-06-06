package org.sparkRedshift.tutorial

import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import scaldi.{Injector}


class RedShiftConnectorImpl(sqlContext:SQLContext)(implicit injector:Injector) extends AwsConfigParameters with RedShiftConnector {

  def readTable(tableName : Option[String] , query : Option[String], isTmpTable: Boolean = false): DataFrame = {
    if(isTmpTable) readTmpTable(tableName, query)
    else  readRedShiftTable(tableName, query)
  }

  private def readTmpTable(tableName : Option[String] , query : Option[String]): DataFrame = {

    val finalQuery = query.getOrElse(tableName.map(optionalTablename => s"select * from $optionalTablename").get)
    sqlContext.sql(finalQuery)
  }

  private def readRedShiftTable(tableName : Option[String] , query : Option[String]): DataFrame = {

    val queryDF = sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", getConnectionUrl)
      .option("tempdir", getS3TmpFolder)

    query.map(queryStatement=>queryDF.option("query",queryStatement))
    tableName.map(optionalTablename => queryDF.option("dbtable",optionalTablename))
    queryDF.load()
  }

  def writeTable(tableName : String, saveMode: SaveMode ,query : String, columnRename :Option[scala.collection.immutable.Map[String,String]]):Unit = {
    val queryDF = sqlContext.sql(query)

    for ((k,v) <- columnRename.getOrElse(Map.empty)) queryDF.withColumnRenamed(k,v)

    queryDF.write
      .format("com.databricks.spark.redshift")
      .option("url", getConnectionUrl)
      .option("tempdir", getS3TmpFolder)
      .option("dbtable", tableName)
      .mode(saveMode)
      .save
  }

  def createTempTable(data: DataFrame, tempTableName:String):Unit = {
    data.registerTempTable(tempTableName)
  }

  def dropTempTable( tempTableName:String):Unit = {
    sqlContext.dropTempTable(tempTableName)
  }

}
