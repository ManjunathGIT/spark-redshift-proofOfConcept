package org.sparkRedshift.tutorial

import org.apache.spark.sql.{SaveMode, DataFrame}


trait RedShiftConnector {

  def readTable(tableName : Option[String] , query : Option[String], isTmpTable: Boolean = false): DataFrame

  def writeTable(tableName : String, saveMode: SaveMode ,query : String, columnRename :Option[scala.collection.immutable.Map[String,String]]):Unit

  def createTempTable(data: DataFrame, tempTableName:String):Unit

  def dropTempTable( tempTableName:String):Unit

  def releaseAttachedResources():Unit
}
