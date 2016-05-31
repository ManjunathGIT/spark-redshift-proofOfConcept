Feature: copy data from s3 to redshift

  As a developer I would like to copy a given CSV to a given redshift table

  @cleanS3csvRawFolder
  Scenario: upload a localhost CSV to S3
    Given a CSV "csv/category.csv" and a bucket "pjgg-redshift-spark"
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist

  #@cleanRedshiftRecords
  #@cleanS3csvRawFolder
  #Scenario: copy CSV to S3
  #  Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
  #  When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
  #  Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
  #  Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"