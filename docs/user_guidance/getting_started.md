---
title: Getting Started
tags:
    - Introduction
    - Data Contract
    - Business Rules
---


## Rules Configuration Introduction

To use the DVE you will need to create a dischema document. The dischema document describes how the DVE should validate your data. It's divided into two primary parts. The first part is the `contract` (data contract) - this describes the structure of your data and controls how the data should be typecasted. For example, here is a dischema document describing how the DVE might validate data about a movies dataset:

!!! example "Example `movies.dischema.json`"

    ```json
    {
        "contract": {
            "schemas": {
                "cast": {
                    "fields": {
                        "name": "str",
                        "role": "str",
                        "date_joined": "date"
                    }
                }
            },
            "datasets": {
                "movies": {
                    "fields": {
                        "title": "str",
                        "year": "int",
                        "genre": {
                            "type": "str",
                            "is_array": true
                        },
                        "duration_minutes": "int",
                        "ratings": {
                            "type": "NonNegativeFloat",
                            "is_array": true
                        },
                        "cast": {
                            "model": "cast",
                            "is_array": true
                        }
                    },
                },
                "mandatory_fields": [
                    "title",
                    "year"
                ],
                "reader_config": {
                     ".json": {
                        "reader": "SparkJSONReader"
                    }
                }
            }
        }
    }
    ```

Within the example above, there are two parent keys - `schemas` and `datasets`.

`schemas` allow you to define custom complex data types. So, in the example above, the field `cast` would be expecting an array of structs containing the actors name, role and the date they joined the movie.

`datasets` describe the actual models for the entities you want to load. In the example above, we only want to load a single entity called `movies` which contains the fields `title, year, genre, duration_minutes, ratings and cast`. However, you could load the complex type `cast` into a seperate entity if you wanted to split your data into seperate entities. This can be useful in situations where a given entity has all the information you need to perform a given validation rule against, making the performance of rule faster & more efficient as there's less data to scan in a given entity.

!!! note
    The "splitting" of entities is considerably more useful in situtations where you want to normalise/de-normalise your data. If you're unfamiliar with this concept, you can read more about it [here](https://en.wikipedia.org/wiki/Database_normalization). However, you should keep in mind potential performance impacts of doing this. If you have rules that requires fields from different entities, you will have to perform a `join` between the split entities to be able to perform the rule.

For each dataset definition, you will need to provide a `reader_config` which describes how to load the data during the [File Transformation](file_transformation.md) stage. So, in the example above, we expect `movies` to come in as a `JSON` file. However, you can add more readers if you have the same data in different data formats (e.g. `csv`, `xml`, `json`). Regardless, of what they submit, the [File Transformation](file_transformation.md) stage will turn their submissions into a "stringified" parquet format which is a requirement for the subsequent stages.

To learn more about how you can construct your Data Contract please read [here](data_contract.md).

The second part of the dischema are the `business_rules` *or* `tranformations`. This section describes the validation rules you want to apply to entities defined within the `contract`. For example, with our `movies` dataset above, we may want to check that movies in this dataset are less than 4 hours long. The expression to write this check is written in SQL and that syntax may change slightly depending on the SQL backend you've choosen (we currently support [DuckDB](implementations/duckdb.md) and [Spark SQL](implementations/spark.md)).
!!! example "Example `movies.dischema.json`"

    ```json
    {
        "transformations": {
            "filters":{
                {
                    "entity": "movies",
                    "name": "Ensure movie is less than 4 hours long",
                    "expression": "duration_minutes > 240",
                    "error_code": "MOVIE_TOO_LONG",
                    "failure_message": "Movie must be less than 4 hours long."
                }
            }
        }
    }
    ```
You may look at the expression above and think "Hang on! That's the opposite of what you want! You're only getting movies less than 4 hours!", however, all validation rules are wrapped inside a `NOT` expression. So, you write the rules as though you are looking for non problematic values.

We also offer a feature called `complex_rules`. These are rules where you need to transform the data before you can apply the rule. For instance, you may want to perform a join, aggregate the data, or perform a filter. The complex rules allow you to combine "pre-steps" before you perform the validation.

To learn more about how to write your validation rules and complex validation rules, please follow the guidance [here](business_rules.md).


## Utilising the Pipeline objects to run the DVE
Within the DVE package, we have created the ability to build pipeline objects to help orchestrate the running of the DVE from start to finish. We currently have an implementation for `Spark` and `DuckDB` ready for users to use out of the box. The links below will direct you to detailed guidance on how you can setup a DVE pipeline.

<div class="grid cards" markdown>

-   :material-duck:{ .lg .middle } __Set up with DuckDB__

    ---

    [:octicons-arrow-right-24: Setup a DuckDB pipeline here](implementations/duckdb.md)

-   :material-shimmer:{ .lg .middle } __Set up with Spark__

    ---

    [:octicons-arrow-right-24: Setup a Spark pipeline here](implementations/spark.md)

</div>

<br>
