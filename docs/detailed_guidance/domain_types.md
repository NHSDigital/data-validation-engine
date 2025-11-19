# Domain Types

Domain types are custom defined pydantic types that solve common problems with usual datasets or schemas defined in [Data contract](./data_contract.md).
This might include Postcodes, NHS Numbers, dates with specific formats etc.

Below is a list of defined types, their output type and any contraints. Nested beneath them are any constraints that area allowed and their default values if there are any.
| Defined Type | Output Type | Contraints & Defaults | Supported Implementations |
| ------------ | ----------- | --------------------- | ------------------------- |
| NHSNumber | str | | Spark, DuckDB |
| permissive_nhs_number | str | <li> warn_on_test_numbers = False </li> | Spark, DuckDB |
| Postcode | str | | Spark, DuckDB |
| OrgId | str | | Spark, DuckDB |
| conformatteddate | date | <li>date_format: str</li><li>ge: date</li><li>le: date</li><li>gt: date</li><li>lt: date</li> | Spark, DuckDB |
| formatteddatetime | datetime | <li>date_format: str </li><li>timezone_treatment: one_of ["forbid", "permit", "require] = "permit"</li> | Spark, DuckDB |
| formattedtime | time | <li>time_format: str </li><li>timezone_treatment: one_of ["forbid", "permit", "require"] = "permit" | DuckDB |
| reportingperiod | date | <li>reporting_period_type: one_of ["start", "end"]</li><li>date_format: str = "%Y-%m-%d"</li> | Spark, DuckDB |
| alphanumeric | str | <li>min_digits : NonNegativeInt = 1</li><li>max_digits: PositiveInt = 1</li> | Spark, DuckDB |
| identifier | str | <li>min_digits : NonNegativeInt = 1</li><li>max_digits: PositiveInt = 1</li> | Spark, DuckDB |

**Other types that are allowed include:**
- str
- int
- date
- datetime
- Decimal
- float
- Any types that are included in [pydantic version 1.10](https://docs.pydantic.dev/1.10/usage/types/#pydantic-types)
