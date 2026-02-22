
## Mixing backend implementations

The examples shown above are using the Spark Backend. DVE also has a DuckDB backend found at [core_engine.backends.implementations.duckdb](https://github.com/NHSDigital/data-validation-engine/tree/main/src/dve/core_engine/backends/implementations/duckdb). In order to mix the two you will need to convert from one type of entity to the other. For example from a spark `Dataframe` to DuckDB `relation`. The easiest way to do this is to use the `write_parquet` method from one backend and use `read_parquet` from another backend. 

Currently the configuration isn't backend agnostic for applying business rules. So if you want to swap between spark and duckdb, the business rules need to be written using only features that are common to both backends. For example, a regex check in spark would be something along the lines of...
```sql
nhsnumber rlike '^\d{10}$'
```
...but in duckdb it would be...
```sql
regexp_matches(nhsnumber, '^\d{10}$')
```
Failures in parsing the expressions lead to failure messages such as 
```python
FeedbackMessage(
    entity=None,
    record=None,
    failure_type='integrity',
    is_informational=False,
    error_type=None,
    error_location=None,
    error_message="Unexpected error (AnalysisException: Undefined function: 'regexp_matches'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 5) in transformations (rule: root; step: 0; id: None)",
    error_code=None,
    reporting_field=None,
    reporting_field_name=None,
    value=None,
    category=None
)
```