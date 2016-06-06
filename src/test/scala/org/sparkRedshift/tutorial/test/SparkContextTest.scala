package org.sparkRedshift.tutorial.test

import org.apache.spark.{SparkConf, SparkContext}
import org.sparkRedshift.tutorial.AwsConfigParameters


object SparkContextTest extends AwsConfigParameters{

  val sc = init()

  private def init():SparkContext = {
    val initSc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local"))
    initSc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", getAwsAccessKey)
    initSc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", getAwsSecretKey)
    initSc
  }

  def releaseAttachedResources():Unit = {
    sc.stop()
  }

}
