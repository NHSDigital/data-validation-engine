Feature: Pipeline tests using the animal dataset
    Test record rejection and ensuring that records are correctly removed from the entity and that
    the correct validation feedback is raised in the error report.

    Scenario: Validate XML data with just record level rejections (duckdb)
        Given I submit the animals file animals.xml for processing
        And A duckdb pipeline is configured with schema file 'animals.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the animals entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no record rejections from the data_contract phase
        And the animals entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode | error_count |
            | record    | ANE01     | 2           |
        And The rules restrict "animals" to 3 qualifying records
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 5     |
            | number_record_rejections | 2     |
            | number_warnings          | 0     |

    Scenario: Validate XML data with a mixture of error types in (duckdb)
        Given I submit the animals file animals_mixture.xml for processing
        And A duckdb pipeline is configured with schema file 'animals.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the animals entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no record rejections from the data_contract phase
        # Then there are errors with the following details and associated error_count from the data_contract phase
        #     | FailureType | Status | ErrorCode  | error_count |
        #     | record      | error  | FieldBlank | 1           |
        And the animals entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | FailureType | Status        | ErrorCode | error_count |
            | record      | error         | ANE01     | 2           |
            | submission  | error         | ANE02     | 1           |
            | record      | informational | ANE03     | 1           |
        And The rules restrict "animals" to 5 qualifying records
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 7     |
            | number_submission_rejections | 1     |
            | number_record_rejections     | 2     |
            | number_warnings              | 1     |
