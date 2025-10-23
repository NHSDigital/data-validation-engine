Feature: Pipeline tests using the movies dataset
    Tests for the processing framework which use the movies dataset.

    This tests submissions in JSON format, with configuration in JSON config files.
    Complex types are tested (arrays, nested structs)

    Some validation of entity attributes is performed: SQL expressions and Python filter
    functions are used, and templatable business rules feature in the transformations.

    Scenario: Validate and filter movies (duckdb)
        Given I submit the movies file movies.json for processing
        And A duckdb pipeline is configured
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the movies entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are 2 record rejections from the data_contract phase
        And there are errors with the following details and associated error_count from the data_contract phase
            | ErrorCode | ErrorMessage                         | error_count |
            | BLANKYEAR | year not provided                    | 1           |
            | DODGYYEAR | year value (NOT_A_NUMBER) is invalid | 1           |
        And the movies entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
