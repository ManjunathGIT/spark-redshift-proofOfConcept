package org.sparkRedshift.tutorial.test

import java.sql.{Connection, DriverManager}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.redshift.AmazonRedshiftClient
import com.amazonaws.services.redshift.model.{DeleteClusterRequest, CreateClusterRequest}
import org.slf4j.{LoggerFactory, Logger}
import org.sparkRedshift.tutorial.AwsConfigParameters
import java.util.Properties


trait AcceptanceTestRedshiftUtils extends AwsConfigParameters{

  private val LOG: Logger = LoggerFactory.getLogger("AcceptanceTestRedshiftUtils")

  val awsRedshiftClient = initRedshiftClient()


  def tableExist(tableName:String):Boolean = {
    val redshiftJDBCClient = redshiftConnectionJDBC()
    val resultSet = redshiftJDBCClient.prepareStatement(s"select count(distinct(tablename)) from pg_table_def where schemaname = 'public' and tablename ='$tableName';").executeQuery()
    val tableExist = resultSet.getInt(1) == 1
    resultSet.close()
    redshiftJDBCClient.close()
    tableExist
  }



  def createCluster(clusterName:String):String = {
    val clusterRequest = new CreateClusterRequest()
      .withClusterIdentifier(clusterName)
      .withMasterUsername(getDbUser)
      .withMasterUserPassword(getDbPassword)
      .withNodeType("dc1.large")
      .withNumberOfNodes(2);

    val createResponse = awsRedshiftClient.createCluster(clusterRequest)
    createResponse.getClusterIdentifier()
  }

  def deleteCluster(clusterName:String):Unit = {
    val clusterRequest = new DeleteClusterRequest()
      .withClusterIdentifier(clusterName)

    awsRedshiftClient.deleteCluster(clusterRequest)
  }

  private def initRedshiftClient():AmazonRedshiftClient = {
    val tmpClient = new AmazonRedshiftClient(new ProfileCredentialsProvider())
    tmpClient.setEndpoint(getDbhost)
    tmpClient
  }

  private def redshiftConnectionJDBC():Connection = {
    Class.forName("com.amazon.redshift.jdbc41.Driver")
    val props = new Properties()
    props.setProperty("user", getDbUser);
    props.setProperty("password", getDbPassword);
    DriverManager.getConnection(getConnectionUrl, props);
  }
}
