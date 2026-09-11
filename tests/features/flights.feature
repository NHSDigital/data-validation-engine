Feature: Pipeline tests using the flights dataset
    Test hierarchical record rejection and ensuring that records are removed correctly including
    any "orphan" records generated from record removal in parent entities.

    Scenario: A perfect flights file
        Given I submit the flights file perfect_flights.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are no file rejections from the business_rules phase
        And there are no record rejections from the business_rules phase
        When I run the error report phase
        Then An error report is produced
    # TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
    # And The statistics entry for the submission shows the following information
    #     | parameter                | value |
    #     | record_count             | 1     |
    #     | number_file_rejections   | 0     |
    #     | number_record_rejections | 0     |

    Scenario: A flights submission where the root record is rejected
        Given I submit the flights file missing_country_id.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode | error_count |
            | record    | C1        | 1           |
            | record    | AG1       | 1           |
            | record    | FG1       | 2           |
            | record    | PG1       | 4           |
        When I run the error report phase
        Then An error report is produced
    # TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
    # And The statistics entry for the submission shows the following information
    #     | parameter                | value |
    #     | record_count             | 1     |
    #     | number_file_rejections   | 0     |
    #     | number_record_rejections | 1     |

    Scenario: A flights submission where a child primary key is rejected
        Given I submit the flights file missing_flight_id.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode | error_count |
            | record    | F1        | 1           |
            | record    | PG1       | 2           |
        When I run the error report phase
        Then An error report is produced
    # TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
    # And The statistics entry for the submission shows the following information
    #     | parameter                | value |
    #     | record_count             | 1     |
    #     | number_file_rejections   | 0     |
    #     | number_record_rejections | 1     |

    Scenario: A flights submission with a mixture of group and record rejections
        Given I submit the flights file mixture_of_group_rej_and_bi_rej.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode | error_count |
            | record    | F1        | 1           |
            | record    | PG1       | 2           |
            | record    | P1        | 1           |
        When I run the error report phase
        Then An error report is produced
    # TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
    # And The statistics entry for the submission shows the following information
    #     | parameter                | value |
    #     | record_count             | 1     |
    #     | number_file_rejections   | 0     |
    #     | number_record_rejections | 1     |

    Scenario: A flights submission with no valid airports record on submission
        Given I submit the flights file only_country_id.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType  | ErrorCode | error_count |
            | submission | C2        | 1           |
        When I run the error report phase
        Then An error report is produced
    # TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
    # And The statistics entry for the submission shows the following information
    #     | parameter                | value |
    #     | record_count             | 1     |
    #     | number_file_rejections   | 0     |
    #     | number_record_rejections | 1     |

    Scenario: A flights submission with a mixture of group and orphan record rejections
        Given I submit the flights file invalid_flight_destination.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | Status        | ErrorCode | error_count |
            | record    | error         | F2        | 2           |
            | record    | error         | PG1       | 4           |
            | record    | informational | A1        | 1           |
        When I run the error report phase
        Then An error report is produced
# TODO - fix the stats calculations as they're currently incorrect for hiearchical datasets
# And The statistics entry for the submission shows the following information
#     | parameter                | value |
#     | record_count             | 1     |
#     | number_file_rejections   | 0     |
#     | number_record_rejections | 1     |
