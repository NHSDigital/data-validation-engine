Feature: Pipeline tests using the ambsys dataset
    Tests for the processing framework which use a synthetic demographics dataset.

    This tests submissions in CSV format, with configuration in JSON config files.
    The data is simple and not nested, but makes use of custom domain-specific types.

    Scenario: Validate PID data with custom types (spark)
        Given I submit the demographics file basic_demographics.csv for processing
        And A spark pipeline is configured with schema file 'basic_demographics.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the demographics entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are 12 record rejections from the data_contract phase
        And the demographics entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "demographics" to 6 qualifying records
        And At least one row from "demographics" has generated error code "BAD_NHS"
        And the demographics entity is stored as a parquet after the business_rules phase
        And The entity "demographics" does not contain an entry for "FALSE" in column "NHS_Number_Valid"
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 13    |
            | number_record_rejections | 18    |
            | number_warnings          | 1     |

    Scenario: Validate PID data with custom types (duckdb)
        Given I submit the demographics file basic_demographics.csv for processing
        And A duckdb pipeline is configured with schema file 'basic_demographics_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the demographics entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are 12 record rejections from the data_contract phase
        And the demographics entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "demographics" to 6 qualifying records
        And At least one row from "demographics" has generated error code "BAD_NHS"
        And the demographics entity is stored as a parquet after the business_rules phase
        And The entity "demographics" does not contain an entry for "FALSE" in column "NHS_Number_Valid"
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 13    |
            | number_record_rejections | 18    |
            | number_warnings          | 1     |
