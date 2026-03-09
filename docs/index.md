---
title: Data Validation Engine
tags:
    - Introduction
    - File Transformation
    - Data Contract
    - Business Rules
    - Spark
    - DuckDB
---

# Data Validation Engine

The Data Validation Engine (DVE) is a configuration driven data validation library written in [Python](https://www.python.org/), [Pydantic](https://docs.pydantic.dev/latest/) and a SQL backend currently consisting of [DuckDB](https://duckdb.org/) or [Spark](https://spark.apache.org/sql/). The configuration to run validations against a dataset are defined and written in a json document, which we will be referring to as the "dischema". The rules written within the dischema are designed to be run against all incoming data in a given submission - as this allows the DVE to capture all possible issues with the data without the submitter having resubmit the same data repeatedly which is burdensome and time consuming for the submitter and receiver of the data. Additionally, the rules can be configured to have the following behaviour:

- **File Rejection** - The entire submission will be rejected if the given rule triggers one or more times.
- **Row Rejection** - The row that triggered the rule will be rejected. Rows that pass the validation will be flowed through into a validated entity.
- **Warning** - The rule will still trigger and be listed as a feedback message, but the record will still flow through into the validated entity. 

Certain scenarios prevent all validations from being executed. For more details, see the [File Transformation](user_guidance/file_transformation.md) section.

The DVE has 3 core components:

1. [File Transformation](user_guidance/file_transformation.md) - Parsing submitted files into a "stringified" (all fields casted to string) parquet format.

    ???+ tip
        If your files are already in a parquet format, you do not need to use the file transformation and you can move straight onto the Data Contract.

2. [Data Contract](user_guidance/data_contract.md) - Validates submitted data against a specified datatype and casts successful records to that type.

3. [Business rules](user_guidance/business_rules.md) - Performs simple and complex validations such as comparisons between fields, entities and/or lookups against reference data.

For each component listed above, a [feedback message](user_guidance/feedback_messages.md) is generated whenever a rule is violated. These [feedback messages](user_guidance/feedback_messages.md) can be integrated directly into your system given you can consume `JSONL` files. Alternatively, we offer a fourth component called the [Error Reports](user_guidance/error_reports.md). This component will load the [feedback messages](user_guidance/feedback_messages.md) into an `.xlsx` (Excel) file which could be sent back to the submitter of the data. The excel file is compatible with services that offer spreadsheet reading such as [Microsoft Excel](https://www.microsoft.com/en/microsoft-365/excel), [Google Docs](https://docs.google.com/), [Libre Office Calc](https://www.libreoffice.org/discover/calc/) etc.

To be able to run the DVE out of the box, you will need to choose and install one of the supported Backend Implementations such as [DuckDB](user_guidance/implementations/duckdb.md) or [Spark](user_guidance/implementations/spark.md). If you to need a write a custom backend implementation, you may want to look at the [Advanced User Guidance](advanced_guidance/backends.md) section.

Feel free to use the Table of Contents on the left hand side of the page to navigate to sections of interest or to use the "Next" and "Previous" buttons at the bottom of each page if you want to read through each page in sequential order.

If you have questions or need additional support with the DVE, then please raise an issue on our GitHub page [here](https://github.com/NHSDigital/data-validation-engine/issues).
