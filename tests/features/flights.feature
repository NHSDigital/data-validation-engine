Feature: Pipeline tests using the books dataset
    Tests for the processing framework which use the flights dataset.

    These tests are primarily around parquet submissions and ensuring the DVE handles
    them correctly.

    Scenario: Validate a perfect flights parquet submission (duckdb)
        Given I submit the flights file flights.parquet for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the flights entity is stored as a parquet after the file_transformation phase
        And the schema for flights entity matches the following
            | column    | dtype |
            | flight_id | Int64 |
            | plane_id  | Int64 |
        And the passengers entity is stored as a parquet after the file_transformation phase
        # TODO - fix this as it's currently not working as expected
        # And the schema for passengers entity matches the following
        #     | column         | dtype  |
        #     | flight_id      | Int64  |
        #     | passenger_id   | Int64  |
        #     | passenger_name | String |
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no record rejections from the data_contract phase
        And there are no file rejections from the data_contract phase
        When I run the business rules phase
        Then the flights entity is stored as a parquet after the business_rules phase
        And the passengers entity is stored as a parquet after the business_rules phase
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 3     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 0     |
            | number_warnings              | 0     |


    Scenario: Validate a perfect flights parquet submission (spark)
        Given I submit the flights file flights.parquet for processing
        And A spark pipeline is configured with schema file 'flights_spark.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the flights entity is stored as a parquet after the file_transformation phase
        And the schema for flights entity matches the following
            | column    | dtype |
            | flight_id | Int64 |
            | plane_id  | Int64 |
        And the passengers entity is stored as a parquet after the file_transformation phase
        # TODO - fix this as it's currently not working as expected
        # And the schema for passengers entity matches the following
        #     | column         | dtype  |
        #     | flight_id      | Int64  |
        #     | passenger_id   | Int64  |
        #     | passenger_name | String |
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no record rejections from the data_contract phase
        And there are no file rejections from the data_contract phase
        When I run the business rules phase
        Then the flights entity is stored as a parquet after the business_rules phase
        And the passengers entity is stored as a parquet after the business_rules phase
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 3     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 0     |
            | number_warnings              | 0     |
