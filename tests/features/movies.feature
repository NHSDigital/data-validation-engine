Feature: Pipeline tests using the movies dataset
    Tests for the processing framework which use the movies dataset.

    This tests submissions in JSON format, with configuration in JSON config files.
    Complex types are tested (arrays, nested structs)

    Some validation of entity attributes is performed: SQL expressions and Python filter
    functions are used, and templatable business rules feature in the transformations.

    Scenario: Validate and filter movies (spark)
        Given I submit the movies file movies.json for processing
        And A spark pipeline is configured
        And I create the following reference data tables in the database movies_refdata
            | table_name | parquet_path                                         |
            | sequels    | tests/testdata/movies/refdata/movies_sequels.parquet |
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the movies entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are 3 record rejections from the data_contract phase
        And there are errors with the following details and associated error_count from the data_contract phase
            | Entity             | ErrorCode | ErrorMessage                              | error_count |
            | movies             | BLANKYEAR | year not provided                         | 1           |
            | movies_rename_test | DODGYYEAR | year value (NOT_A_NUMBER) is invalid      | 1           |
            | movies             | DODGYDATE | date_joined value is not valid: daft_date | 1           |
        And the movies entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "movies" to 4 qualifying records
        And there are errors with the following details and associated error_count from the business_rules phase
            | ErrorCode       | ErrorMessage                                           | error_count |
            | LIMITED_RATINGS | Movie has too few ratings ([6.1])                      | 1           |
            | RUBBISH_SEQUEL  | The movie The Greatest Movie Ever has a rubbish sequel | 1           |
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 5     |
            | number_record_rejections | 4     |
            | number_warnings          | 1     |
        And the error aggregates are persisted

    Scenario: Validate and filter movies (duckdb)
        Given I submit the movies file movies.json for processing
        And A duckdb pipeline is configured with schema file 'movies_ddb.dischema.json'
        And I create the following reference data tables in the database "movies_refdata"
            | table_name | parquet_path                                         |
            | sequels    | tests/testdata/movies/refdata/movies_sequels.parquet |
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the movies entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are 3 record rejections from the data_contract phase
        And there are errors with the following details and associated error_count from the data_contract phase
            | Entity             | ErrorCode | ErrorMessage                              | error_count |
            | movies             | BLANKYEAR | year not provided                         | 1           |
            | movies_rename_test | DODGYYEAR | year value (NOT_A_NUMBER) is invalid      | 1           |
            | movies             | DODGYDATE | date_joined value is not valid: daft_date | 1           |
        And the movies entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "movies" to 4 qualifying records
        And there are errors with the following details and associated error_count from the business_rules phase
            | ErrorCode       | ErrorMessage                                           | error_count |
            | LIMITED_RATINGS | Movie has too few ratings ([6.1])                      | 1           |
            | RUBBISH_SEQUEL  | The movie The Greatest Movie Ever has a rubbish sequel | 1           |
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 5     |
            | number_record_rejections | 4     |
            | number_warnings          | 1     |
        And the error aggregates are persisted

