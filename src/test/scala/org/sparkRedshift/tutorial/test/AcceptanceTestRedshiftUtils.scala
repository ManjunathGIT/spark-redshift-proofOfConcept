package org.sparkRedshift.tutorial.test

import java.sql.{Connection, DriverManager}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.redshift.AmazonRedshiftClient
import com.amazonaws.services.redshift.model.{Cluster, DeleteClusterRequest, CreateClusterRequest}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{LoggerFactory, Logger}
import org.sparkRedshift.tutorial.AwsConfigParameters
import java.util.Properties

import org.sparkRedshift.tutorial.driver.example.SparkRedshiftTutorialRefactor._


trait AcceptanceTestRedshiftUtils extends AwsConfigParameters {

  private val LOG: Logger = LoggerFactory.getLogger("AcceptanceTestRedshiftUtils")

  val awsRedshiftClient = initRedshiftClient()


  def tableExist(tableName:String):Boolean = {
    val redshiftJDBCClient = redshiftConnectionJDBC()

    val q = s"select count(distinct(tablename)) from pg_table_def where schemaname = 'public' and tablename ='$tableName';"
    val stmt = redshiftJDBCClient.createStatement()
    val resultSet = stmt.executeQuery(q)

    resultSet.next()
    val tableExist = resultSet.getInt(1) == 1

    resultSet.close()
    stmt.close()
    redshiftJDBCClient.close()

    tableExist
  }

  def deleteTable(tableName:String):Boolean = {
    val redshiftJDBCClient = redshiftConnectionJDBC()
    val stmt = redshiftJDBCClient.createStatement()
    val q = s"delete from $tableName"
    val status = stmt.execute(q)
    stmt.close()
    redshiftJDBCClient.close()
    status
  }

  def copyCsvIntoTable(csvPath:String, tableName:String):Boolean = {
    val redshiftJDBCClient = redshiftConnectionJDBC()
    val q = s"copy $tableName from '$csvPath' credentials 'aws_access_key_id=$getAwsAccessKey;aws_secret_access_key=$getAwsSecretKey' delimiter '|' region 'eu-west-1';"
    val stmt = redshiftJDBCClient.createStatement()
    val status = stmt.execute(q)
    stmt.close()
    redshiftJDBCClient.close()
    status
  }

  def createCluster(clusterName:String): Cluster = {
    val clusterRequest = new CreateClusterRequest()
      .withClusterIdentifier(clusterName)
      .withMasterUsername(getDbUser)
      .withMasterUserPassword(getDbPassword)
      .withNodeType("dc1.large")
      .withNumberOfNodes(2)

     awsRedshiftClient.createCluster(clusterRequest)
  }

  def deleteCluster(clusterName:String):Unit = {
    val clusterRequest = new DeleteClusterRequest()
      .withClusterIdentifier(clusterName)

    awsRedshiftClient.deleteCluster(clusterRequest)
  }

  private def initRedshiftClient():AmazonRedshiftClient = {
    val tmpClient = new AmazonRedshiftClient(new BasicAWSCredentials(getAwsAccessKey,getAwsSecretKey))
    tmpClient.setEndpoint(getDbhost)
    tmpClient
  }

  private def redshiftConnectionJDBC():Connection = {
    Class.forName("com.amazon.redshift.jdbc41.Driver")
    val props = new Properties()
    props.setProperty("user", getDbUser)
    props.setProperty("password", getDbPassword)
    DriverManager.getConnection(getConnectionUrl, props)
  }
}
