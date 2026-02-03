## v0.5.2 (2026-02-02)

### Refactor

- allow passing of custom loggers into pipeline objects
- ensure traceback in broad exceptions
- improve the logging around dve processing errors and align reporting to module name rather than legacy name
- add sense check for text based file (#32)

## v0.5.1 (2026-01-28)

### Fix

- deal with pathing assumption that file had been moved to processed_file_path during file transformation

## v0.5.0 (2026-01-16)

### Feat

- added entity name override option in data contract error details to align with business rules

### Fix

- Amend relation to python dictionaries approach as using polars (… (#25)
- fix issue where reporting_entity resulted in key fields being removed from error reports (#23)

### Refactor

- added reporting_period_start and end attribute to submission_info model (#28)
- rename "Grouping" to "Group"
- rename the column headers for elements of the error report

## v0.4.0 (2025-12-17)

### Feat

- add persistance of error aggregates to pipeline
- add Foundry pipeline

### Fix

- issue where templated error messages would not correctly format when passing in parameter values

### Refactor

- include submission status for services passthrough

## v0.3.0 (2025-11-19)

### Feat

- new domain type formattedtime for time only data

### Refactor

- small tweak to allow use of dynamic fields in select rules

## v0.2.0 (2025-11-12)

### Refactor

- ensure dve working on python 3.10
- ensure dve working on python 3.11

### BREAKING CHANGE

- Numerous typing updates that will make this codebase unusable below python 3.9

note - this does not mean the package will work on python 3.9. Minimum working version is 3.10.

### Feat

- added functionality to allow error messages in business rules t… (#8)

### Refactor

- bump pylint to work correctly with py3.11 and fix numerous linting issues

## 0.1.0 (2025-11-10)

*NB - This was previously v1.0.0 and v1.1.0 but has been rolled back into a 0.1.0 release to reflect lack of package stability.*

### Feat

- Added ability to define custom error codes and templated messages for data contract feedback messages
- Added new JSON readers
- Added SparkCSVReader
- Added PolarsToDuckDBCSVReader and DuckDBCSVRepeatingReader
- Added quotechar option to DuckDBCSVReader

### Fix

- Fixed issues with refdata loader table implementations
- Fixed duckdb try_cast statements in data contract phase
- Allowed use of entity type in file transformation

### Refactor

- release initial dve source code
