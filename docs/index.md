---
title: Data Validation Engine
tags:
    - Home
---

# Data Validation Engine

The Data Validation Engine (DVE) is a configuration driven data validation library.

There are 3 core steps within the DVE:

1. [File transformation](user_guidance/file_transformation.md){ data-preview } - Parsing files from their submitted format into a common format.
2. [Data contract](user_guidance/data_contract.md){ data-preview } - Validating the types that have been submitted and casting them.
3. [Business rules](user_guidance/business_rules.md){ data-preview } - Performing more complex validations such as comparisons between fields and tables.

with a 4th step being important but more variable depending on platform and users:

4. [Error reports](user_guidance/feedback_messages.md){ data-preview } - Compiles the errors generated from the previous stages and presents them within an Excel report. However, this could be reconfigured to meet the needs of your users.

Each of these steps produce a list of [Feedback message](user_guidance/feedback_messages.md){ data-preview } objects which can be reported back to the user for them to fix any issues.

DVE configuration can be instantiated from a json (dischema) file which might be structured like this:

```json
{
    "contract": {
        "cache_originals": true,
        "error_details": null,
        "types": {},
        "schemas": {},
        "datasets": {
            "CWTHeader": {
                "fields": {
                    "version": {
                        "description": null,
                        "is_array": false,
                        "callable": "constr",
                        "constraints": {
                            "regex": "\\d{1,2}\\.\\d{1,2}"
                        }
                    },
                    "periodStartDate": {
                        "description": null,
                        "is_array": false,
                        "callable": "conformatteddate",
                        "constraints": {
                            "date_format": "%Y-%m-%d"
                        }
                    }
                },
                "mandatory_fields": [
                    "version",
                    "periodStartDate"
                ],
                "reporting_fields": [],
                "key_field": null,
                "reader_config": {
                    ".xml": {
                        "reader": "XMLStreamReader",
                        "kwargs": {
                            "record_tag": "Header",
                            "n_records_to_read": 1
                        },
                        "field_names": null
                    }
                },
                "aliases": {}
            }
        }
    },
    "transformations": {
        "rule_stores": [],
        "reference_data": {},
        "parameters": {},
        "rules": [],
        "filters": [
            {
                "name": "version is at least 1.0",
                "entity": "CWTHeader",
                "expression": "version >= '1.0'",
                "failure_type": "submission",
                "failure_message": "version is not at least 1.0",
                "error_code": "CWT000101",
                "reporting_field": "version",
                "category": "Bad value"
            }
        ],
        "post_filter_rules": [],
        "complex_rules": []
    }
}
```
"Contract" is where [Data Contract](user_guidance/data_contract.md) and [File Transformation](user_guidance/file_transformation.md) (in the reader configs) are configured, and (due to legacy naming) transformations are where [Business rules](user_guidance/business_rules.md) are configured.

## Quick start
In the code example shared above we have a json file named `cwt_example.dischema.json` and an xml file with the following structure:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CWT>
    <Header>
        <version>1.1</version>
        <periodStartDate>2025-01-01</periodStartDate>
    </Header>
</CWT>
```

### Data contract
We can see in `config.contract.datasets` that there is a `CWTHeader` entity declared. This entity has 2 fields, `version` and `periodStartDate`. 

`version` is declared to be a `constr` which is the constrained string type from the Pydantic library. Therefore, any keyword arguments `constr` can be passed as `constraints` here. In this case we are constraining it to a regex 1-2 digits, followed by a literal period followed by 1-2 digits. This should match an `max n2` data type.

`periodStartDate` on the other hand is a `conformatteddate`, this type is one that's defined in the DVE library as a `domain_type` see [Domain types](user_guidance/domain_types.md). The output of a `conformatteddate` is a date type. 

This means that after the data contract step the resulting data will have the types: `version::string` and `periodStartDate::date`. 

We can also see that the `CWTHeader` entity has both `version` and `periodStartDate` set as mandatory fields. That means that if they are missing from the file or the value is null an error will be created.

### File transformation
Within the `CWTHeader` entity we can see a `reader_config` object. This should have a key for every expected file extension that is being submitted for the given dataset. In this case just `".xml"`. We declare which reader is being used `XMLStreamReader` and any kwargs that get passed to it when it's instantiated. Stream reader expects a tag where the record exists in the file (`Header` in this case) and how many records to read. Stream reader is written to be able to quickly pull out singular records such as headers. it will stop parsing once it has hit the maximum number of records, which can save time compared to traversing the whole file.

### Code
Lets bring together those first 2 steps in code. We want to first parse the file into a spark dataframe with all string types, then apply data contract to the dataframe to get a typed dataframe.

> **note in the version that comes from gitlab, the dve library is spread across a number of modules. We are looking to put this in a top level `dve` module**

```python
import os
from pyspark.sql import SparkSession 
# The spark tools require the current active spark session
from dve.core_engine.backends.implementations.spark.readers.xml import SparkXMLStreamReader 
# we're using the spark stream reader, this uses the xmlstream reader but outputs a dataframe
from dve.core_engine.backends.implementations.spark.contract import SparkDataContract
# Applies the data contract over a spark dataframe
from dve.core_engine.configuration.v1 import V1EngineConfig
# the engine configuration for the current DVE version
from dve.core_engine.backends.utilities import stringify_model 
# this takes the types of the datacontract and converts them to strings with the same structure.
```

Here we have all the imports from DVE we need, the stream reader, data contract, configuration object and utility.

we've also imported `os` so we can set some spark args to make sure [SparkXML](https://github.com/databricks/spark-xml) is included, and spark session which will be needed.

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(
    [
        "--packages",
        "com.databricks:spark-xml_2.12:0.16.0",
        "pyspark-shell",
    ]
)
spark = SparkSession.builder.getOrCreate()

config = V1EngineConfig.load("cwt_example.dischema.json")

data_contract_config = config.get_contract_metadata()
reader_configs = data_contract_config.reader_metadata

readers = {"XMLStreamReader": SparkXMLStreamReader}

# File transformation step here
entities = {}
for entity in data_contract_config.schemas:
    # get config based on file type you're parsing
    ext_config = reader_configs[entity][".xml"] 
    reader = readers[ext_config.reader](**ext_config.parameters)
    df = reader.read_to_dataframe(
        "cwt_example.xml", entity, stringify_model(data_contract_config.schemas[entity])
    )
    entities[entity] = df

# Data contract step here
data_contract = SparkDataContract(spark_session=spark)
entities, validation_messages, success = data_contract.apply_data_contract(
    entities, data_contract_config
)
```

from the top down we 
- set some spark arguments to make sure we have spark-xml present
- get a spark session
- load the configuration
- get the data contract config specifically
**file transformation**
- get the reader configurations from the data contract config
- create a mapping of reader_names to their concrete class. (This allows us to refer to a more abstract name in the config and decide what backend we're using in the code)
- create an empty entity dictionary
- iterate over each of the entities defined in the config
- get the reader configuration for the file type we're reading (xml in this case)
- get the concrete reader and instantiate it with the parameters we set in the config
- read the file with a stringified model, this maintains the structure of the datacontract but makes sure everything is kept as strings.
- add the dataframe to the entities dictionary
**data contract**
- instatiate the SparkDataContract class with a spark session
- apply the data contract to the dict of entities returning the entities in the correct types. any validation messages and a success bool

### Business rules

Now we have typed entities we can apply business rules to them. We need a step implementation. we'll import that from the spark rules backend.

```python
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations

business_rules = SparkStepImplementations(spark_session=spark)
business_rule_config = config.get_rule_metadata()

messages = business_rules.apply_rules(entities, business_rule_config)
```

There we go. Messages is a list of [Feedback message](user_guidance/feedback_messages.md) for every failed rule.

### Utilising the Pipeline objects to run the DVE
Within the DVE package, we have also created the ability to build pipeline objects to help orchestrate the running of the DVE from start to finish. We currently have an implementation for `Spark` and `DuckDB`. These pipeline objects abstract some of the complexity described above and only requires you to supply a few objects to run the DVE from start (file transformation) to finish (error reports). These can be read in further detail [here](https://github.com/NHSDigital/data-validation-engine/tree/main/src/dve/pipeline) and we have tests [here](https://github.com/NHSDigital/data-validation-engine/tree/main/tests/test_pipeline) to ensure they are working as expected. Furthermore, if you have a situation where maybe you only want to run the Data Contract, then you can utilise the pipeline objects in a way that only runs the specific stages that you want. Below will showcase an example where the full e2e pipeline is run and how you can  trigger the stages that you want.

> **note in the version that comes from gitlab, the dve library is spread across a number of modules. We are looking to put this in a top level `dve` module**

```python
# Imports for a spark setup
from pyspark.sql import SparkSession

from core_engine.backends.implementations.spark.auditing import SparkAuditingManager
from pipeline.spark_pipeline import SparkDVEPipeline

# Local Spark Setup
os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(
    [
        "--packages",
        "com.databricks:spark-xml_2.12:0.16.0",
        "pyspark-shell",
    ]
)

spark = SparkSession.builder.getOrCreate()

# Setting up the audit manager
audit_manager = SparkAuditingManager(
    database=spark_test_database,
    pool=ThreadPoolExecutor(1),
    spark=spark,
)

# Setting up the Pipeline (in this case the Spark implemented one)
pipeline = SparkDVEPipeline(
    processed_files_path="path/where/my/processed_files/should_go/",
    audit_tables=audit_manager,
    job_run_id=1,
    rules_path="path/to/my_dischema",
    submitted_files_path="path/to/my/cwt_files/",
    reference_data_loader=SparkParquetRefDataLoader,
    spark=spark
)
```

Once you have setup the Pipeline object, audit object and your environment - you are ready to use the pipeline in whatever way works for you. You can simply utilise the `cluster_pipeline_run` method which will run all the stages of dve (from file transformation to error reports) or you can run the stages that you specifically need. For instance...

```python
# this will run all stages of the dve
dve_pipeline.cluster_pipeline_run(max_workers=2)
```

**OR**

```python
submitted_files = dve_pipeline._get_submission_files_for_run()
submitted_file_infos = []

for submission in submitted_files:
    submitted_file_infos.append(dve_pipeline.audit_received_file(sub_id, *subs))

dve_pipeline.data_contract_step(
    pool=ThreadPoolExecutor(2),
    file_transform_results=submitted_file_infos 
)
```

For the Data Contract step you may have noticed that you will need to provide a list of `SubmissionInfo` objects. These are pydantic models which contain metadata for a given Submission. Within this example we are using the `_get_submission_files_for_run` method to get a tuple of URI's where the Submission URI and Metadata URI exist for a given submission. We then pass them through the `audit_received_file_step` method to audit the submissions and in return get a SubmissionInfo object that we can then utilise within the `data_contract_step` method.

If you'd rather not rely on needing a `metadata.json` associated with your submitted files you can build your own bespoke process for building a list of `SubmissionInfo` objects.

### Mixing backends

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

# Extra information
Thanks for reading the documentation and looking into utilising the DVE. If you need more information on any of the steps you can find the following guidance below. If you need additional support, please raise an issue ([see guidance here](https://github.com/NHSDigital/data-validation-engine/blob/main/CONTRIBUTE.md)) and we will try and respond to you as quickly as possible.
