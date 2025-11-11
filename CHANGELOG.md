## 0.1.0 (2025-11-10)

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
