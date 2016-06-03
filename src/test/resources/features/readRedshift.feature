Feature: read data from redshift

  As a developer I would like to read from redshift

  @cleanRedshiftRecords
  @cleanS3csvRawFolder
  Scenario: read record from Redshift
    Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
    Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"
    Then using RedShiftConnector I read table "category" form redshift and check that has 11 records

  @cleanRedshiftRecords
  @cleanS3csvRawFolder
  Scenario Outline: make a simple queries
    Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
    Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"
    Then using RedShiftConnector I make the following "<Query>" and check that the amount of records is <expectedResult>
    Examples:
    |Query                                                                  | expectedResult            |
    |select * from category                                                 | 11                        |
    |select * from category where catgroup = 'Sports'                       | 5                         |
    |select * from category where catgroup = 'Sports' and catname = 'NBA'   | 1                         |
    |select * from category where catgroup = 'Alekekere'                    | 0                         |


  @cleanRedshiftRecords
  @cleanS3csvRawFolder
  Scenario Outline: create temporal tables
    Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
    Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"
    Then I create a temp table with "<tableName>" and "<Query>" and check that the amount of records is <expectedResult>
    Then clean temporalTables "<tableName>"
    Examples:
      |Query                                                               | tableName         | expectedResult            |
      |select * from category                                              | categoryTemp1     | 11                        |
      |select * from category where catgroup = 'Sports'                    | categoryTemp2     | 5                         |
      |select * from category where catgroup = 'Sports' and catname = 'NBA'| categoryTemp3     | 1                         |
      |select * from category where catgroup = 'Alekekere'                 | categoryTemp4     | 0                         |


  @cleanRedshiftRecords
  @cleanS3csvRawFolder
  Scenario Outline: create temporal tables and make a query over them
    Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
    Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"
    Then I create a temp table with "<tableName>" and "<Query>" and the I make a "<QueryTemp>" check that the amount of records is <expectedResult>
    Then clean temporalTables "<tableName>"
    Examples:
      |Query                                                               | tableName     | QueryTemp                                                                   | expectedResult            |
      |select * from category                                              | categoryTemp1 |  select * from categoryTemp1                                                | 11                        |
      |select * from category where catgroup = 'Sports'                    | categoryTemp2 |  select * from categoryTemp2                                                | 5                         |
      |select * from category where catgroup = 'Sports' and catname = 'NBA'| categoryTemp3 |  select * from categoryTemp3                                                | 1                         |
