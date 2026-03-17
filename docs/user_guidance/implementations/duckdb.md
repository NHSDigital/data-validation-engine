!!! quote
    DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs, maps), and several extensions designed to make SQL easier to use.

    DuckDB is available as a standalone CLI application and has clients for Python, R, Java, Wasm, etc., with deep integrations with packages such as pandas and dplyr.

You can read more about DuckDB with the following links:

- [Official Documentation :material-file-document-arrow-right:](https://duckdb.org/docs/stable/) 
- [GitHub :material-github:](https://github.com/duckdb/duckdb)

## Setting up a DuckDB Connection

To be able to use DuckDB with the DVE you first need to create a DuckDB connection object. You can simply do this with the following code:

=== "Persist Database on memory"
    ```py
    import duckdb as ddb

    db_path = ":memory:"
    db_con = ddb.connect(db_path)
    ```

=== "Persist Database on disk"
    ```py
    import duckdb as ddb

    db_path = "path/to/my_database.duckdb"
    db_con = ddb.connect(db_path)
    ```

!!! note
    You will need to close the db_con object with `db.close()`. Alternatively, you could build a custom [context manager](https://docs.python.org/3/library/contextlib.html) object to open and close the connection without needing to explicitly close the connection.


Now you have the DuckDB connection object setup, you are ready to setup the required DVE objects.

## Generating SubmissionInfo objects

Before we utilise the DVE, we need to generate an iterable object containing `SubmissionInfo` objects. These objects effectively contain the necessery metadata for the DVE to work with a given submission. Here is an example function used to generate SubmissionInfo objects from a given path:

```py
import glob
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from dve.core_engine.models import SubmissionInfo


def generate_sub_infos_from_submissions_path(
    submission_path: Path,
    dataset_id: Optional[str] = "example",
    submitting_org: Optional[str] = None,
    submission_method: Optional[str] = "local_test",
    reporting_period_start_date: Optional[date | datetime] = None,
    reporting_period_end_date: Optional[date | datetime] = None,
) -> list[SubmissionInfo]:
    sub_infos: list[SubmissionInfo] = []
    for f in glob.glob(str(submission_path) + "/*.*"):
        file_path = Path(f)
        file_stats = file_path.stat()
        metadata = {
            "dataset_id": dataset_id,
            "file_name": file_path.stem,
            "file_extension": file_path.suffix,
            "submission_method": submission_method,
            "file_size": file_stats.st_size,
            "datetime_received": datetime.now(),
        }
        if submitting_org:
            metadata["submitting_org"] = submitting_org
        if reporting_period_start_date:
            metadata["reporting_period_start"] = str(reporting_period_start_date)
        if reporting_period_end_date:
            metadata["reporting_period_end"] = str(reporting_period_end_date)

        sub_infos.append(SubmissionInfo(submission_id=uuid4().hex, **metadata))
    return sub_infos


submissions = generate_sub_infos_from_submissions_path(Path("path", "to", "my", "submissions"))
```

!!! note
    If you have a large number of submissions, it may be worth converting the above into a [generator](https://docs.python.org/3/reference/expressions.html#generator-expressions). Using the example above, you can do this by simply removing the sub_infos object and yield the SubmissionInfo object per file returned from the glob iterator.

## DuckDB Audit Table Setup

The first object you must setup is an "Audit Manager Object". This can be done with the following code:

```py
from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager

audit_manager = DDBAuditingManager(db_path, connection=db_con)  # type: ignore
```

The "Audit Manager" object within the DVE is used to keep track of the status of your submission. A submission for instance could fail during the File Transformation section, so it's important that we have something to keep track of the submission. The Audit Manager object has a number of methods that can be used to read/write information to tables being stored within the duckdb connection setup in the previous step.

You can learn more about the Auditing Objects [here](../auditing.md).

Once you have setup your "Audit Manager" object, we can move onto setting up the DuckDB reference data loader (if required) and then setting up the DuckDB DVE Pipeline object.

## DuckDB Reference Data Setup (Optional)
If your business rules are reliant on utilising reference data, you will need to write the following code to ensure that reference data can be loaded during the application of those rules:

```py
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader

DuckDBRefDataLoader.connection = db_con
DuckDBRefDataLoader.dataset_config_uri = Path("path", "to", "my", "rules").as_posix()
```

The connection passed into the `DuckDBRefDataLoader` object will then be able to use various DuckDB readers to load data from an existing table on the connection OR loading data from reference data persisted in either `parquet` or `pyarrow` format.

If you want to learn more about the reference data loaders, you can view the advanced user guidance [here](../../advanced_guidance/package_documentation/refdata_loaders.md).

Now we can move onto setting up the DuckDB DVE Pipeline object.

## DuckDB Pipeline Setup

To setup a DuckDB Pipeline, you can use the following example below:

=== "Without Rules"

    ```py

    from dve.pipeline.duckdb_pipeline import DDBDVEPipeline


    dve_pipeline = DDBDVEPipeline(
        processed_files_path=Path("location_to_store", "dve_outputs").as_posix(),
        audit_tables=audit_manager,
        connection=db_con,
        submitted_files_path=Path("submissions", "path").as_posix(),
        reference_data_loader=DuckDBRefDataLoader,
    )
    ```

=== "With Rules"

    ```py
    from dve.pipeline.duckdb_pipeline import DDBDVEPipeline


    dve_pipeline = DDBDVEPipeline(
        processed_files_path=Path("location_to_store", "dve_outputs").as_posix(),
        audit_tables=audit_manager,
        connection=db_con,
        rules_path=Path("to", "my", "rules").as_posix(),
        submitted_files_path=Path("submissions", "path").as_posix(),
        reference_data_loader=DuckDBRefDataLoader,
    )
    ```

!!! note
    If using remote resources, then you will want to use `as_uri` for your paths.

    E.g.
    ```py
    Path("remote", "path").as_uri()
    ```

Once your Pipeline object is defined, you can simply run the `cluster_pipeline_run` method. E.g.

```py
error_reports = dve_pipeline.cluster_pipeline_run()
```


## Further documentation
For further details on the objects referenced above, you can use the following links to read more about the objects:

- [Pipeline Docs](../../advanced_guidance/package_documentation/pipeline.md)
- [Reference Data Docs](../../advanced_guidance/package_documentation/refdata_loaders.md)
