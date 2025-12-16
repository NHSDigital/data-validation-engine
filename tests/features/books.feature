Feature: Pipeline tests using the books dataset
    Tests for the processing framework which use the books dataset.

    This tests submissions using nested, complex JSON datasets with arrays, and
    introduces more complex transformations that require aggregation.

    Scenario: Validate complex nested XML data (spark)
        Given I submit the books file nested_books.xml for processing
        And A spark pipeline is configured with schema file 'nested_books.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the header entity is stored as a parquet after the file_transformation phase
        And the nested_books entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there is 1 record rejection from the data_contract phase
        And the header entity is stored as a parquet after the data_contract phase
        And the nested_books entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "nested_books" to 3 qualifying records
        And The entity "nested_books" contains an entry for "17.85" in column "total_value_of_books"
        And the nested_books entity is stored as a parquet after the business_rules phase
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 4     |
            | number_record_rejections | 2     |
            | number_warnings          | 0     |

    Scenario: Validate complex nested XML data (duckdb)
        Given I submit the books file nested_books.xml for processing
        And A duckdb pipeline is configured with schema file 'nested_books_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the header entity is stored as a parquet after the file_transformation phase
        And the nested_books entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there is 1 record rejection from the data_contract phase
        And the header entity is stored as a parquet after the data_contract phase
        And the nested_books entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "nested_books" to 3 qualifying records
        And The entity "nested_books" contains an entry for "17.85" in column "total_value_of_books"
        And the nested_books entity is stored as a parquet after the business_rules phase
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 4     |
            | number_record_rejections | 2     |
            | number_warnings          | 0     |

    Scenario: Handle a file with a malformed tag (duckdb)
        Given I submit the books file malformed_books.xml for processing
        And A duckdb pipeline is configured with schema file 'nested_books_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the latest audit record for the submission is marked with processing status failed
        # TODO - handle above within the stream xml reader - specific

    Scenario: Handle a file that fails XSD validation (duckdb)
        Given I submit the books file books_xsd_fail.xml for processing
        And A duckdb pipeline is configured with schema file 'nested_books_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
