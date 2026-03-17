---
title: Business Rules
tags:
    - Business Rules
    - dischema
    - Rule Store
    - Reference Data
---

The Business Rules section contain the rules you want to apply to your dataset. Rule logic might include...

- Checking if two or more fields are equivalent
- Aggregating data to check if it matches a given value
- Joining against other entities to compare values

All rules are written in `SQL`. Depending on which [backend implementation](./implementations/) you have choosen, the syntax might be different between implementations.

When writing the rules, you need to be aware that the expressions are wrapped in `NOT` expression. So, you should write the rules as though you are looking for non problematic values.

When rules are being applied, [Complex Rules](./business_rules.md#complex-rules) are always applied before [Rules](./business_rules.md#rules) and [Filters](./business_rules.md#filters).

This page is meant to give you greater details on how you can write your Business Rules. If you want a summary of how the Business Rules work, then please refer to the [Getting Started](./getting_started.md#rules-configuration-introduction) page.

## Filters

For the simplest rules, you can write them in the filters section. For example, if you had a movies dataset where you wanted to check the length of the movie had a realistic duration then you could write a rule like this...


=== "Record Rejection"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "filters": [
                {
                    "entity": "movies",
                    "name": "Ensure movie is less than 4 hours long",
                    "expression": "duration_minutes > 240",
                    "failure_type": "record",
                    "error_code": "MOVIE_TOO_LONG",
                    "failure_message": "Movie must be less than 4 hours long.",
                    "category": "Bad Value"
                }
            ]
        }
    }
    ```

=== "File Rejection"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "filters": [
                {
                    "entity": "movies",
                    "name": "Ensure movie is less than 4 hours long",
                    "expression": "duration_minutes > 240",
                    "failure_type": "submission",
                    "error_code": "MOVIE_TOO_LONG",
                    "failure_message": "Movie must be less than 4 hours long.",
                    "category": "Bad Value"
                }
            ]
        }
    }
    ```

=== "Warning"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "filters": [
                {
                    "entity": "movies",
                    "name": "Ensure movie is less than 4 hours long",
                    "expression": "duration_minutes > 240",
                    "failure_type": "record",
                    "is_informational": true,
                    "error_code": "MOVIE_TOO_LONG",
                    "failure_message": "Movie must be less than 4 hours long.",
                    "category": "Bad Value",
                }
            ]
        }
    }
    ```

The rule above can be written directly into the filters section because we do not need to perform any complex pre-step(s) such as filtering, aggregation(s), join(s) etc. We can simply select the fields of interest and perform the check.

If you need to perform more complex rules, with pre-steps, then see the [Complex Rules](./business_rules.md#complex-rules) section further down this page.



### Types of rejections

You may have noticed the field "failure_type" in the example above. For any given rule (filter) you can reject a record, the whole file (submission) or just raise a warning. Here are the details around the currently supported Rejection Types:

| Rejection Type | Behaviour | How to set in the rule |
| -------------- | --------- | ---------------------- |
| `submission` | Rejects the entire file. Even if it triggers once, no data will be projected in the final asset. | Set `failure_type` to `submission` |
| `record` | Rejects the record that failed the check. Any records that fail will not be projected in the final asset | Set `failure_type` to `record` |
| `warning` | Raises a warning that the record failed the check. This has no impact on whether the file/record is rejected. | Set `is_informational` to `true` |

## Rules

The `rules` section allows you to perform pre-steps to entities. For example, if you wanted to derive a new column, apply filters, aggregations,  joins etc.

With pre-steps, you can either modify an existing `entity` or create a new entity from an existing one. For example, here is a pre-step showing both modifying an existing entity or creating a new one:

=== "Existing Entity"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "rules": [
                {
                    "name": "add duration_hours as a new column",
                    "operation": "add",
                    "entity": "movies",
                    "column_name": "duration_hours",
                    "expression": "(duration_minutes / 60)"
                }
            ]
        }
    }
    ```

=== "New Entity"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "rules": [
                {
                    "name": "add duration_hours as a new column",
                    "operation": "add",
                    "entity": "movies",
                    "column_name": "duration_hours",
                    "expression": "(duration_minutes / 60)",
                    "new_entity_name": "movies_modified"
                }
            ]
        }
    }
    ```

The difference between modifiying the existing entity and adding a new one is simply adding `"new_entity_name": "<new_name>"`.

!!! warning

    If you add columns to an existing entity defined within the contract, that column will be written out with the projected entity. To get around this, you will either need to create new entities *or* you can see the [post rule logic](./business_rules.md#post-rule) section to remove the column.

### Operations

For a full list of operations that you can perform during the pre-steps see [Advanced User Guidance: Operations](../advanced_guidance/package_documentation/operations.md).

## Post Rule

When a Business Rule has been finished, "post step rules" can be run. This is useful in situtations where you've created lots of new entities *or* you have added lots of new columns to existing entities.

For new entities, it's advised that you always remove them. In instances where you have derived new columns for existing entities you may not want them to persist the columns in the projected assets. The code snippets below showcases how you can remove columns and new entities:

=== "New Column Removal"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "rules": [
                {
                    "name": "add duration_hours as a new column",
                    "operation": "add",
                    "entity": "movies",
                    "column_name": "duration_hours",
                    "expression": "(duration_minutes / 60)",
                }
            ],
            "filters": [
                ...
            ],
            "post_filter_rules": [
                {
                    "operation": "remove",
                    "entity": "movies",
                    "column_name": "duration_hours"
                }
            ]
        }
    }
    ```


=== "New Entity Removal"

    ```json title="movies.dischema.json"
    {
        "contract": {
            "datasets": {
                "movies": {
                    "fields": {
                        "duration_minutes": "int",
                        ...
                    },
                    ...
                }
            }
        },
        "transformations": {
            "rules": [
                {
                    "name": "add duration_hours as a new column",
                    "operation": "add",
                    "entity": "movies",
                    "column_name": "duration_hours",
                    "expression": "(duration_minutes / 60)",
                    "new_entity_name": "movies_modified"
                }
            ],
            "filters": [
                ...
            ],
            "post_filter_rules": [
                {
                    "operation": "remove_entity",
                    "entity": "movies_modified"
                }
            ]
        }
    }
    ```


## Reference Data

If your Business Rules are reliant on reference data, then you can add the `"reference_data"` key to the `"transformations"` section. The snippet below shows various formats of reference data that you might want to add:

=== "Parquet source"

    ```json title="movies.dischema.json"
    {
        "transformations": {
            "reference_data": {
                "movie_genre_lookup":{
                    "type": "filename",
                    "filename": "path/to/my/movie_genre_lookup.parquet"
                },
                ...
            }
        }
    }
    ```

=== "Arrow source"

    ```json title="movies.dischema.json"
    {
        "transformations": {
            "reference_data": {
                "movie_genre_lookup":{
                    "type": "filename",
                    "filename": "path/to/my/movie_genre_lookup.arrow"
                },
                ...
            }
        }
    }
    ```

=== "Database source"

    ```json title="movies.dischema.json"
    {
        "transformations": {
            "reference_data": {
                "movie_genre_lookup": {
                    "type": "table",
                    "database": "my_database",
                    "table_name": "movie_genre_lookup"
                },
                ...
            }
        }
    }
    ```

!!! note

    - When a new reference data entity is created, it will always be prefixed with `refdata_`

    !!! warning
        
        - Refdata entities are also immutable. So, if you need to modify them in any way, you will always need to create a new entity from it

For latest supported reference data types, see [Advanced User Guidance: Reference Data Types](../advanced_guidance/package_documentation/refence_data_types.md).

## Complex Rules

Complex Rules are recommended when you need to perform a number of "pre-step" operations before you can apply a business rule (filter). For instance, if you needed to add a column, filter and then join you would need to add all these steps into your [Rules](./business_rules.md#rules) section. This might be ok, if you only need a small number of pre-steps or only have a couple of rules. However, when you have lots of rules and more than 1 have a number of operations required, it's best to place these into a [Rulestore](./business_rules.md#rule-stores) and reference them within the complex rules. Otherwise, you could start to make the dischema document completely unmaintainable.

Here is an example of defining a complex rule:

=== "dischema"

    ```json title="movies.dischema.json"
    {
        "transformations": {
            "parameters": {"entity": "movies"},
            "reference_data": {
                "sequels": {
                    "type": "table",
                    "database": "movies_refdata",
                    "table_name": "sequels"
                }
            },
            "complex_rules": [
                {
                    "rule_name": "ratings_count"
                },
                {
                    "rule_name": "poor_sequel_check",
                    "parameters": {
                        "sequel_entity": "refdata_sequels"
                    }
                }
            ]
        }
    }
    ```

=== "rulestore"

    ```json title="movies_rulestore.json"
    {
        "ratings_count": {
            "description": "Ensure more than 1 rating",
            "type": "complex_rule",
            "parameter_descriptions": {
                "entity": "The entity to apply the workflow to."
            },
            "parameter_defaults": {},
            "rule_config": {
                "rules": [
                    {
                        "name": "Get count of ratings",
                        "operation": "add",
                        "entity": "{{entity}}",
                        "column_name": "no_of_ratings",
                        "expression": "length(ratings)"
                    }
                ],
                "filters": [
                    {
                        "name": "filter_too_few_ratings",
                        "entity": "{{entity}}",
                        "expression": "no_of_ratings > 1",
                        "error_code": "LIMITED_RATINGS",
                        "reporting_field": "title",
                        "failure_message": "Movie has too few ratings ({{ratings}})"
                    }
                ],
                "post_filter_rules": [
                    {
                        "name": "Remove the no_of_ratings field",
                        "operation": "remove",
                        "entity": "{{entity}}",
                        "column_name": "no_of_ratings"
                    }
                ]
            }
        },
        "poor_sequel_check": {
            "description": "check if bad sequel exists",
            "type": "complex_rule",
            "parameter_descriptions": {
                "entity": "The entity to apply the workflow to.",
                "sequel_entity": "The entity containing sequel data"
            },
            "parameter_defaults": {},
            "rule_config": {
                "rules": [
                    {
                        "name": "Join sequel data",
                        "operation": "inner_join",
                        "entity": "{{entity}}",
                        "target": "{{sequel_entity}}",
                        "join_condition": "{{entity}}.title = {{sequel_entity}}.sequel_to",
                        "new_entity_name": "with_sequels",
                        "new_columns": {
                            "{{sequel_entity}}.ratings": "sequel_rating"
                        }
                    },
                    {
                        "name": "Get median sequel rating",
                        "operation": "group_by",
                        "entity": "with_sequels",
                        "group_by": "title",
                        "agg_columns": {
                            "list_aggregate(sequel_rating, 'median')": "median_sequel_rating"
                        }
                    }

                ],
                "filters": [
                    {
                        "name": "filter_rubbish_sequel",
                        "entity": "with_sequels",
                        "expression": "median_sequel_rating > 5",
                        "error_code": "RUBBISH_SEQUEL",
                        "reporting_entity": "derived",
                        "reporting_field": "title",
                        "failure_message": "The movie {{title}} has a rubbish sequel",
                        "is_informational": true
                    }
                ],
                "post_filter_rules": [
                    {
                        "name": "Remove the with_sequel entity",
                        "operation": "remove_entity",
                        "entity": "with_sequels"
                    }
                ]
            }
        }
    }
    ```

For all complex rules, you must set the key `"type"` to "complex_rule". Description is optional but future you will thank you when there is a quick explantation explaining what the rule is doing.

After that, you define the `"rule_config"` key which defines the [Rules](./business_rules.md#rules), [Filters](./business_rules.md#) and [Post Rule steps](./business_rules.md#post-rule) to be applied.

The sections below will cover the unique elements in a complex rule not already covered in the previous sections.

### Parameters

Parameters have two scopes. "Global" and "local".

"Global" parameters can be defined as a new key under the `"transformations"` section. These can contain variables accessible by every single rule and filter.

"Local" parameters are defined during the setup of a [Complex Rule](./business_rules.md#complex-rules).

Below is an example showing how you would define them:

=== "Global Example"

    ```json title="movies.dischema.json"
    {
        "contract": {
            ...
        },
        "transformations": {
            "parameters": {
                "param_name": "value",
                "param_name2": "value",
                ...
            },
            ...
        }
    }
    ```

=== "Local Example"

    === "dischema"

        ```json title="movies.dischema.json"
            {
                "contract": {
                    ...
                },
                "transformations": {
                    "complex_rules": [
                        {
                            "rule_name": "my_complex_rule",
                            "parameters": {
                                "param_key1": "value",
                                "param_key2": "value",
                                ...
                            }
                        }
                    ],
                    ...
                }
            }
        ```

    === "Rulestore"

        ```json title="movies_rulestore.json"
        {
            "my_complex_rule": {
                "parameter_descriptions": {
                    "param_key1": "required for x,y,z reason",
                    "param_key2": "lorem ipsum",
                },
                "parameter_defaults": {
                    "param_key2": "hello world"
                },
                "rule_config": {
                    ...
                }
            }
        }
        ```

### Rule Stores

Rule stores are seperate JSON documents that you can load into the dischema document. The benefit of building rulestores are that you can reutilise them across multiple dischema documents.

To add a new rulestore simply add a new key called `"rule_stores"` under the transformation section. For example:

```json title="movies.dischema.json"
    {
        "contract": {
            ...
        },
        "transformations": {
            "rulestores": [
                {
                    "store_type": "json",
                    "filename": "<name_of_rulestore>.json"
                },
                ...
            ]
            ...
        }
    }
```
