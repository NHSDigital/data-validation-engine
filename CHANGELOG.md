## v1.1.0 (2025-10-28)

### Feat

- tweaked json schemas for contract error details. Fixed failing unit test
- sorted linting. Added json schema docs
- improved movies dataset test coverage. Added testing for spark and duckdb refdata loaders when table config specified.
- further refinement of movies test dataset.
- small fixes and movies dataset working up to end of data contract
- bug fixes with readers and base pipeline. Added spark csv reader. Added new dischema files for tests
- added support for nested fields when configuring custom error details for data contract. Includes accessing nested error values in error messages.
- initial work to add custom error messages to data contract

## 1.0.0 (2025-10-09)

### Refactor

- release initial dve source code
