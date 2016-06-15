# spark-redshift proofOfConcept

[![Build Status](https://travis-ci.org/pjgg/spark-redshift-proofOfConcept.svg?branch=master)](https://travis-ci.org/pjgg/spark-redshift-proofOfConcept)
[![Coverage Status](https://coveralls.io/repos/github/pjgg/spark-redshift-proofOfConcept/badge.svg?branch=master)](https://coveralls.io/github/pjgg/spark-redshift-proofOfConcept?branch=master)

How to use
----------

1. Open a terminal and run over your pom.xml file the following command

```Maven
mvn clean package 
```

2. Create an application.conf file as the following example, with your custom values

```Text

awsCredentials {
  awsAccessKey = XXXX
  awsSecretKey = XXXX
}

awsRedshiftParamaters{
  dbName = dev
  user = masteruser
  password = XXX
  host = examplecluster.cbofvq1ke8eb.eu-west-1.redshift.amazonaws.com
  port = 5439
  s3BucketfolderPath = pjgg-redshift-spark/temp/
}
```

3. Finally run the executable fatJar

```bash
 java -jar -Dconfig.file=/YOUR_PATH/application.conf target/spark-redshift-0.0.1.jar
```

How to deploy your Spark driver into AWS EMR Spark
--------------------------------------------------

Firstly you should know that spark EMR 4.7.0 runs spark 1.6.1 over scala 2.10, so you must write your driver in a scala version not greater than 2.10.
Secondly be sure that your spark cluster has installed the JVM that your expected to have. In my case I will write down a bootstrap configuration file 

```Text
[
  {
    "Classification": "spark-env",
    "Configurations": [
      {
        "Classification": "export",
        "Configurations": [],
        "Properties": {
          "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
        }
      }
    ],
    "Properties": {}
  }
]
```

And finally launch your cluster, pointing your configuration. This cluster will be deployed over Yarn.
 
``` bash
aws emr create-cluster --release-label emr-4.7.0 --ec2-attributes KeyName=emr --enable-debugging --instance-type r3.xlarge --instance-count 2 --application Name=Spark  --configurations http://pjgg-spark.s3.amazonaws.com/conf/awsEmrConfig.json --use-default-roles --log-uri s3://pjgg-spark/logs/
```

Then upload your driver to the master node and deploy it

``` bash 
scp -i emr.pem /YOUR_PATH/target/spark-redshift-0.0.1.jar ec2-user@ec2-52-18-178-57.eu-west-1.compute.amazonaws.com:/tmp
aws emr add-steps --cluster-id j-1N5EEPM84O5JR --steps Type=Spark,Name="Spark Program",Args=[--class,org.sparkRedshift.tutorial.DriverExample,/tmp/spark-redshift-0.0.1.jar,10]
```

Note: you must package the project with emr profile and also setup your /resources/profile/emr/application.conf in order to deploy into Spark AWS EMR

AWS BI Data-lake
----------------

A data lake could be defined as a repository where you can store a vast amount of data in a raw format. Then you can process it and make queries over this data. 

With the above AWS technologies you could use S3 as HDFS (where the Raw data will be stored) and then you could clean and enrich this data (process data) and stored in an operational repository as Redshift. 

![alt text] [Architecture]

[Architecture]: https://github.com/pjgg/spark-redshift-proofOfConcept/blob/master/src/main/resources/doc/dataLakeExampleArq.png

S3 bucket should be structured in a way that allow to the data analyst decided with months/years load into redshift, and redshift should be created on demand each time. Optionally would be possible to take an snapshot of redshift in case a data analyst required to work more than a day with the same dataset.
An other approach more flexible that I didn't try is setup an HDFS + Hive with EMR and load the required data into a new redshift (created on demand). This choice will allow you to load into redshift, business data more complex than a range of dates.      

