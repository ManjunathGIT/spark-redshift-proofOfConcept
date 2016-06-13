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

