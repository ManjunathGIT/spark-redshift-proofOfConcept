package org.sparkRedshift.tutorial

import com.typesafe.config.ConfigFactory


trait AwsConfigParameters {

  private val conf = ConfigFactory.load()

  def getAwsAccessKey = conf.getString("awsCredentials.awsAccessKey")

  def getAwsSecretKey = conf.getString("awsCredentials.awsSecretKey")

  def getDbName = conf.getString("awsRedshiftParamaters.dbName")

  def getDbUser = conf.getString("awsRedshiftParamaters.user")

  def getDbPassword = conf.getString("awsRedshiftParamaters.password")

  def getDbhost = conf.getString("awsRedshiftParamaters.host")

  def getDbport = conf.getString("awsRedshiftParamaters.port")

  def getConnectionUrl = s"""jdbc:redshift://$getDbhost:$getDbport/$getDbName?user=$getDbUser&password=$getDbPassword"""

  def getS3TmpFolder = "s3n://" + conf.getString("awsRedshiftParamaters.s3BucketfolderPath")
}
