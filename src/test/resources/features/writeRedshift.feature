Feature: write data to redshift

  As a developer I would like to write to redshift

  @cleanRedshiftRecords
  @cleanS3csvRawFolder
  @cleanTestCategoryTable
  Scenario: write record to Redshift
    Given a CSV "csv/category.csv" a bucket "pjgg-redshift-spark" a redshiftDb "dev" with a table "category" already created
    When I request to upload CSV "csv/category.csv" to bucket "pjgg-redshift-spark" and keyName "csvRaw/category.csv"
    Then I check that the file"csvRaw/category.csv" in bucket "pjgg-redshift-spark" exist
    Then I copy file "csvRaw/category.csv" in bucket "pjgg-redshift-spark" into db "dev" and table "category"
    Then using RedShiftConnector I copy "category" table to "testCategory"  and check that has 11 records
    Then using RedShiftConnector I make a write statement with saveMode "ignore" over  "categoryTmp" table to "testCategory"  and check that has 11 records
    Then using RedShiftConnector I make a write statement with saveMode "append" over  "categoryTmp" table to "testCategory"  and check that has 22 records
