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
