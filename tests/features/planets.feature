Feature: Pipeline tests using the planets dataset
    Tests for the processing framework which use the planets dataset.

    This tests submissions in CSV format, with configuration in JSON config files.
    Mutiple entities and complex transformations are tested, but the data is flat
    (i.e. not nested schemas/arrays are present).

    Some validation of entity attributes is performed: SQL expressions and Python filter
    functions are used, and templatable business rules feature in the transformations.

    # Scenario: Validate and filter planets (spark)
    #     Given I submit the planets file planets_demo.csv for processing
    #     And A spark pipeline is configured
    #     And I add initial audit entries for the submission
    #     Then the latest audit record for the submission is marked with processing status file_transformation
    #     When I run the file transformation phase
    #     Then the planets entity is stored as a parquet after the file_transformation phase
    #     And the latest audit record for the submission is marked with processing status data_contract
    #     When I run the data contract phase
    #     Then there is 1 record rejection from the data_contract phase
    #     And the planets entity is stored as a parquet after the data_contract phase
    #     And the latest audit record for the submission is marked with processing status business_rules
    #     When I run the business rules phase
    #     Then The rules restrict "planets" to 1 qualifying record
    #     And At least one row from "planets" has generated error code "HIGH_DENSITY"
    #     And At least one row from "planets" has generated error code "WEAK_ESCAPE"
    #     And the planets entity is stored as a parquet after the business_rules phase
    #     And the latest audit record for the submission is marked with processing status error_report
    #     When I run the error report phase
    #     Then An error report is produced
    #     And The entity "planets" does not contain an entry for "Jupiter" in column "planet"
    #     And The entity "planets" contains an entry for "Neptune" in column "planet"
    #     And The statistics entry for the submission shows the following information
    #         | parameter                | value |
    #         | record_count             | 9     |
    #         | number_record_rejections | 18    |
    #         | number_warnings          | 0     |

    Scenario: Handle a file with no extension provided (spark)
        Given I submit the planets file planets_no_extension for processing
        And A spark pipeline is configured
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the latest audit record for the submission is marked with processing status failed

    Scenario: Handle a file with duplicated extension provided (spark)
        Given I submit the planets file planets.csv.csv for processing
        And A spark pipeline is configured
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there is 0 record rejection from the data_contract phase
        And the planets entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced

    Scenario: Validate and filter planets (duckdb)
        Given I submit the planets file planets_demo.csv for processing
        And A duckdb pipeline is configured with schema file 'planets_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the planets entity is stored as a parquet after the file_transformation phase
        And the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then there is 1 record rejection from the data_contract phase
        And the planets entity is stored as a parquet after the data_contract phase
        And the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "planets" to 1 qualifying record
        And At least one row from "planets" has generated error code "HIGH_DENSITY"
        And At least one row from "planets" has generated error code "WEAK_ESCAPE"
        And the planets entity is stored as a parquet after the business_rules phase
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The entity "planets" does not contain an entry for "Jupiter" in column "planet"
        And The entity "planets" contains an entry for "Neptune" in column "planet"
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 9     |
            | number_record_rejections | 18    |
            | number_warnings          | 0     |

    Scenario: Handle a file with malformed header provided (duckdb)
        Given I submit the planets file malformed_planets.csv for processing
        And A duckdb pipeline is configured with schema file 'planets_ddb.dischema.json'
        And I add initial audit entries for the submission
        Then the latest audit record for the submission is marked with processing status file_transformation
        When I run the file transformation phase
        Then the latest audit record for the submission is marked with processing status data_contract
        When I run the data contract phase
        Then the latest audit record for the submission is marked with processing status business_rules
        When I run the business rules phase
        Then The rules restrict "planets" to 0 qualifying records
        And At least one row from "planets" has generated error code "STRONG_GRAVITY"
        And the latest audit record for the submission is marked with processing status error_report
        When I run the error report phase
        Then An error report is produced
        And The statistics entry for the submission shows the following information
            | parameter                | value |
            | record_count             | 1     |
            | number_record_rejections | 2     |
            | number_warnings          | 0     |
