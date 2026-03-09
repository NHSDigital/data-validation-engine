!!! quote
    Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

You can read more about Spark here with the following links:

- [Official Documentation :material-file-document-arrow-right:](https://spark.apache.org/) 
- [GitHub :material-github:](https://github.com/apache/spark)


## Setting up a Spark Session

For a basic Spark Session setup, you can use the following snippet of code:
```py
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
```

You can learn more about setting up a Spark Session [here](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html).

!!! warning

    If you need to load XML data and the version of spark you're running is <4.0.0, you'll need the `spark-xml` extension. You can read more about it [here](https://github.com/databricks/spark-xml).


## Generating SubmissionInfo Objects

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

## Spark Audit Table Setup

The first object you must setup is an "Audit Manager Object". This can be done with the following code:

```py
from dve.core_engine.backends.implementations.spark.auditing import SparkAuditingManager

db_name = "test_dve"
spark.sql(f"CREATE DATABASE {db_name};")

audit_manager = SparkAuditingManager(db_name, spark)
```

!!! note

    `spark` session is optional for the `SparkAuditingManager`. If not provided a spark session will be generated.

The "Audit Manager" object within the DVE is used to keep track of the status of your submission. A submission for instance could fail during the File Transformation section, so it's important that we have something to keep track of the submission. The Audit Manager object has a number of methods that can be used to read/write information to tables being stored within the duckdb connection setup in the previous step.

You can learn more about the Auditing Objects [here](../auditing.md).

Once you have setup your "Audit Manager" object, we can move onto setting up the Spark reference data loader (if required) and then setting up the Spark DVE Pipeline object.

## Spark Reference Data Setup (Optional)
If your business rules are reliant on utilising reference data, you will need to write the following code to ensure that reference data can be loaded during the application of those rules:

```py
from pathlib import Path

from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader

SparkRefDataLoader.spark = spark
SparkRefDataLoader.dataset_config_uri = Path("path", "to", "my", "rules").as_posix()
```

## Spark Pipeline Setup

To setup a Spark Pipeline, you can use the following example below:

=== "Without Rules"

    ```py

    from dve.pipeline.spark_pipeline import SparkDVEPipeline


    dve_pipeline = SparkDVEPipeline(
        processed_files_path=Path("location_to_store", "dve_outputs").as_posix(),
        audit_tables=audit_manager,
        submitted_files_path=Path("submissions", "path").as_posix(),
        reference_data_loader=SparkRefDataLoader,
        spark=spark,
    )
    ```

=== "With Rules"

    ```py
    from dve.pipeline.spark_pipeline import SparkDVEPipeline


    dve_pipeline = SparkDVEPipeline(
        processed_files_path=Path("location_to_store", "dve_outputs").as_posix(),
        audit_tables=audit_manager,
        rules_path=Path("to", "my", "rules").as_posix(),
        submitted_files_path=Path("submissions", "path").as_posix(),
        reference_data_loader=SparkRefDataLoader,
        spark=spark,
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
