package org.sparkRedshift.tutorial.test

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import org.slf4j.{LoggerFactory, Logger}
import org.sparkRedshift.tutorial.AwsConfigParameters


trait AcceptanceTestFilesUtils extends AwsConfigParameters {

  private val LOG: Logger = LoggerFactory.getLogger("AcceptanceTestFilesUtils")

  val s3Client = new AmazonS3Client(new BasicAWSCredentials(getAwsAccessKey,getAwsSecretKey))

  def getFileFromResourceFolder(path: String): File = return new File(this.getClass().getClassLoader().getResource(path).getPath);

  def checkIFS3BucketExist(bucketName:String):Boolean = {
    s3Client.doesBucketExist(bucketName)
  }

  def checkIFS3FileExist(bucketName:String, fileName:String):Boolean = {
    s3Client.doesObjectExist(bucketName, fileName)
  }

  def uploadFileToS3(bucketName:String, file:File, keyName:String):Unit = {
    try {

      s3Client.putObject(new PutObjectRequest(bucketName, keyName, file))

    } catch {
      case ase:AmazonServiceException =>
          System.out.println("Caught an AmazonServiceException, which " +
            "means your request made it " +
            "to Amazon S3, but was rejected with an error response" +
            " for some reason.");
          LOG.error(s"Error Message: ${ase.getMessage}");
          LOG.error(s"HTTP Status Code: ${ase.getStatusCode}");
          LOG.error(s"AWS Error Code:   ${ase.getErrorCode}");
          LOG.error(s"Error Type:       ${ase.getErrorType}");
          LOG.error(s"Request ID:       ${ase.getRequestId}");
      case ace:AmazonClientException =>
          LOG.error("Caught an AmazonClientException, which " +
              "means the client encountered " +
              "an internal error while trying to " +
              "communicate with S3, " +
              "such as not being able to access the network.");
          LOG.error(s"Error Message: ${ace.getMessage}");
    }
  }
}
