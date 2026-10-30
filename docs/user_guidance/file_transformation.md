---
title: File Transformation
tags:
    - Contract
    - Data Contract
    - File Transformation
    - Readers
---

The File Transformation stage within the DVE is used to convert submitted files to stringified parquet format. This is critical as the rest of the stages within the DVE are reliant on the data being in parquet format. [Parquet was chosen as it's a very efficient column oriented format](https://www.databricks.com/glossary/what-is-parquet). When specifying which formats you are expecting, you will define it in your dischema like this:

=== "DuckDB"

    ```json
    {
        "contract": {
            "datasets": {
                "<entity_name>": {
                    "fields": {
                        ...
                    },
                },
                "reader_config": {
                    ".json": {
                        "reader": "DuckDBJSONReader",
                        "kwargs": {
                            ...
                        }
                    },
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            ...
                        }
                    }
                }
            }
        }
    }
    ```

=== "Spark"

    ```json
    {
        "contract": {
            "datasets": {
                "<entity_name>": {
                    "fields": {
                        ...
                    },
                },
                "reader_config": {
                    ".csv": {
                        "reader": "SparkCSVReader",
                        "kwargs": {
                            ...
                        }
                    },
                    ".json": {
                        "reader": "SparkJSONReader",
                        "kwargs": {
                            ...
                        }
                    }
                }
            }
        }
    }
    ```

The secondary use of the File Transformation stage is the ability to normalise your data into multiple entities. Imagine you had something like Hospital and Patient data in a single submission. You could split this out into seperate entities so that the validated outputs of the data could be loaded into seperate  tables (parquet). For example:

=== "DuckDB"

    ```json
        {
            "contract": {
                "datasets": {
                    "hospital": {
                        "fields": {
                            "hospital_id": "int",
                            "hospital_name": "string"
                        },
                        "reader_config": {
                            ".json": {
                                "reader": "DuckDBJSONReader",
                                "kwargs": {
                                    "encoding": "utf-8",
                                    "multi_line": true,
                                }
                            }
                        }
                    },
                    "patients": {
                        "fields": {
                            "patient_id": "int",
                            "patient_name": "string"
                        },
                        "reader_config": {
                            ".json": {
                                "reader": "DuckDBJSONReader",
                                "kwargs": {
                                    "encoding": "utf-8",
                                    "multi_line": true,
                                }
                            }
                        }
                    }
                }
            }
        }
    ```


=== "Spark"

    ```json
        {
            "contract": {
                "datasets": {
                    "hospital": {
                        "fields": {
                            "hospital_id": "int",
                            "hospital_name": "string"
                        },
                        "reader_config": {
                            ".json": {
                                "reader": "SparkJSONReader",
                                "kwargs": {
                                    "encoding": "utf-8",
                                    "multi_line": true,
                                }
                            }
                        }
                    },
                    "patients": {
                        "fields": {
                            "patient_id": "int",
                            "patient_name": "string"
                        },
                        "reader_config": {
                            ".json": {
                                "reader": "SparkJSONReader",
                                "kwargs": {
                                    "encoding": "utf-8",
                                    "multi_line": true,
                                }
                            }
                        }
                    }
                }
            }
        }
    ```

!!! abstract ""
    You can read more about the readers and kwargs [here](../advanced_guidance/package_documentation/data_contract/readers.md).

## Reader additional checks

The file transformation section supports additional reader checks which will apply across all reader configs for a given entity. These are the checks you can currently perform:

### Check Empty

The `check empty` feature allows you check whether an entity is empty on submission. When this error is raised, the DVE will generate a feedback message and report. This won't process any further entities as you are effectively saying this entity cannot be empty on submission.

Here is an example of this being implemented in a dischema document:

```json
{
    "contract": {
        "datasets": {
            "my_entity": {
                "fields": {...},
                "reader_config": {
                    ".csv": {...},
                    ".xml": {...}
                },
                "reader_additional_checks": {
                    "check_empty": {
                        "error_code": "Empty entity",
                        "error_message": "'my_entity' is empty. Please resolve"
                    }
                }
            }
        }
    }
}
```

## Supported Formats

| Format  | DuckDB             | Spark              | Version Available |
| ------- | ------------------ | ------------------ | ----------------- |
| `.csv`  | :white_check_mark: | :white_check_mark: | >= 0.1.0 |
| `.json` | :white_check_mark: | :white_check_mark: | >= 0.1.0 |
| `.xml`  | :white_check_mark: | :white_check_mark: | >= 0.1.0 |
