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
        And the staff entity is stored as a parquet after the file_transformation phase
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
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 0     |
            | number_warnings              | 0     |

    Scenario: A flights submission where the root record is rejected
        Given I submit the flights file missing_country_id.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the staff entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there is 1 record rejection from the data_contract phase
        And there are errors with the following details and associated error_count from the data_contract phase
            | FailureType | ErrorCode          | error_count |
            | record      | CountryIdIsMissing | 1           |
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode            | error_count |
            | record    | AirportHasNoCountry  | 3           |
            | record    | StaffHasNoAirport    | 15          |
            | record    | FlightHasNoAirport   | 10          |
            | record    | PassengerHasNoFlight | 25          |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 54    |
            | number_warnings              | 0     |

    Scenario: A flights submission where a child primary key is rejected
        Given I submit the flights file missing_flight_id.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the staff entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | ErrorCode            | error_count |
            | record    | FlightIDMissing      | 1           |
            | record    | PassengerHasNoFlight | 3           |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 4     |
            | number_warnings              | 0     |

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
            | ErrorType | ErrorCode           | error_count |
            | record    | CountryHasNoAirport | 1           |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 1     |
            | number_warnings              | 0     |

    Scenario: A flights submission with a rejection on a node with one mandatory node
        Given I submit the flights file singular_node_rejections.xml for processing
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
            | ErrorType | Status | ErrorCode                | error_count |
            | record    | error  | InvalidFlightDestination | 2           |
            | record    | error  | PassengerHasNoFlight     | 4           |
            | record    | error  | AirportHasNoStaff        | 1           |
            | record    | error  | CountryHasNoAirport      | 1           |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 8     |
            | number_warnings              | 0     |

    Scenario: A flights submission with a rejection on a node with two mandatory nodes
        Given I submit the flights file multi_node_file_rejection.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are no file rejections from the data_contract phase
        And there are no record rejections from the data_contract phase
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | Status | ErrorCode            | error_count |
            | record    | error  | StaffIDMissing       | 7           |
            | record    | error  | AirportHasNoStaff    | 1           |
            | record    | error  | FlightHasNoAirport   | 1           |
            | record    | error  | PassengerHasNoFlight | 1           |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 10    |
            | number_warnings              | 0     |

    Scenario: A flights submission with many types of rejections in a single submission
        Given I submit the flights file flights_full_regression.xml for processing
        And A duckdb pipeline is configured with schema file 'flights.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the country entity is stored as a parquet after the file_transformation phase
        And the airport entity is stored as a parquet after the file_transformation phase
        And the flights entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the passengers entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there are errors with the following details and associated error_count from the data_contract phase
            | FailureType | ErrorCode          | error_count |
            | record      | AirportIdIsMissing | 1           |
        When I run the business rules phase
        Then there are errors with the following details and associated error_count from the business_rules phase
            | ErrorType | Status | ErrorCode                | error_count |
            | record    | error  | InvalidFlightDestination | 1           |
            | record    | error  | PassengerNameMissing     | 1           |
            | record    | error  | StaffIDMissing           | 4           |
            | record    | error  | PassengerHasNoFlight     | 3           |
            | record    | error  | StaffHasNoAirport        | 1           |
            | record    | error  | FlightHasNoAirport       | 2           |
            | record    | error  | AirportHasNoStaff        | 1           |
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                    | value |
            | record_count                 | 1     |
            | number_submission_rejections | 0     |
            | number_record_rejections     | 14    |
            | number_warnings              | 0     |
