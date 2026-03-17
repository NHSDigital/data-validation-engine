---
title: Data Contract
tags:
    - Contract
    - Data Contract
    - Domain Types
---

The Data Contract defines the structure (models) of your data and controls how it is typecast. We use [Pydantic](https://docs.pydantic.dev/1.10/) to generate and validate the models. This page is meant to give you greater details on how you should write your Data Contract. If you want a summary of how the Data Contract works, please refer to the [Getting Started](./getting_started.md#rules-configuration-introduction) page.

!!! Note

    We plan to migrate to Pydantic v2+ in a future release. This page currently reflects what is available through Pydantic v1.

## Models

The models within the Data Contract are written under the `datasets` key. For example, this is how you might define a model for a movies dataset:

=== "movies.dischema.json"

    ```json
    {
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": "str",
                    "year_released": "conformatteddate",
                    "genres": {
                        "type": "str",
                        "is_array": true
                    }
                }
            },
            "cast": {
                "fields": {
                    "actor_id": "int",
                    "actor_forename": "str",
                    "actor_surname": "str",
                    "character_name": "",
                    "movies_acted": {
                        "type": "int",
                        "is_array": true
                    }
                }
            }
        }
    }
    ```

=== "movies.json"

    ```json
    {
        "movie_uuid": 1,
        "movie_name": "John Doe & The Giant Peach",
        "year_released": "1964-01-01",
        "genres": [
            "thriller",
            "action",
            "horror"
        ],
        "cast": {
            "actor_forename": "John",
            "actor_surname": "Doe",
            "character_name": "John Doe",
            "movies_acted": [
                1
            ]
        }
    }
    ```

From the example above, we've built two models from the source data which in turn will provide two seperated entities to work with in the business rules and how the data will be written out at the end of the process. Those models being `"movie"` and `"cast"` with `fields` specifying the name of the columns and the data type they should be casted to. We will look into [data types later in this page](data_contract.md#types).


### Mandatory Fields

Within the Data Contract you can also specify `mandatory fields`. These are fields that must be present in the submitted data or a [Feedback Message](./feedback_messages.md) will be generated stating that the field is missing. You can define `mandatory fields` like this...

```json title="movies.dischema.json"
{
    "contract": {
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": "str",
                    "year_released": "conformatteddate",
                    "genres": {
                        "type": "str",
                        "is_array": true
                    }
                },
                "required_fields": [
                    "movie_uuid",
                    "movie_name"
                ]
            },
            "cast": {
                "fields": {
                    "actor_id": "int",
                    "actor_forename": "str",
                    "actor_surname": "str",
                    "character_name": "",
                    "movies_acted": {
                        "type": "int",
                        "is_array": true
                    }
                },
                "required_fields": [
                    "actor_id",
                    "actor_forename",
                    "actor_surname"
                ]
            }
        }
    }
}
```

### Key Fields

You can define a `key_field` or `key_fields` within a given entity. These represent the unique identifiers within your dataset. `key_field` represents a single unique identifier, whereas `key_fields` allows a combination of fields to represent a unique record.

This can be defined within the dischema like...

```json title="movies.dischema.json"
{
    "contract": {
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": "str",
                    ...
                },
                "key_fields": [
                    "movie_uuid",
                    "movie_name"
                ]
            },
            "cast": {
                "fields": {
                    "actor_id": "int",
                    ...
                },
                "key_field": "actor_id"
            }
        }
    }
}
```

### Readers

You can define a reader for each specific model. You can have multiple readers if your incoming data is in multiple formats (e.g. csv & json). Here is an example of adding readers to our movie dataset example:

```json title="movies.dischema.json"
{
    "contract": {
        "datasets": {
            "movie": {
                "fields": {
                    ...
                },
                "mandatory_fields": {
                    ...
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
            "cast": {
                "fields": {
                    ...
                },
                "mandatory_fields": {
                    ...
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

If you want to read more about the readers, please see the [File Transformation](./file_transformation.md) page.


## Types

Within the `fields` section of the contract you must define what data type a given field should be. Depending on how strict/lenient you want your types to be, a number of types are available to use. The types available are:

- [Built-in standard library](https://docs.python.org/3.11/library/stdtypes.html) types (such as `int`, `str`, `date`) available with your version of Python installed for the DVE.
- [Pydantic v1 types](https://docs.pydantic.dev/1.10/usage/types/)
- [Custom Types](./data_contract.md#custom-types)
- [Domain types](./data_contract.md#domain-types)

### Constraints

Given the DVE supports Pydantic types, you can use any of the [constrained types available](https://docs.pydantic.dev/1.10/usage/types/#constrained-types). The docs will also show you what `kwarg` arguments are available for each constraint such as min/max length, regex patterns etc.

For example, if you wanted to use a `constr` type for a field, you would define it like this:

```json title="movies.dischema.json"
{
    "contract": {
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": {
                        "callable": "constr",
                        "constraints": {
                            "min_length": 1,
                            "max_length": 20
                        }
                    },
                    ...
                }
            }
        }
    }
}
```

In the example above we would be ensuring that the movie name is between 1 & 20 characters. If it is less than 1, or more than 20, a [Feedback Message](./feedback_messages.md) will be produced.

### Custom Types

As shown in the [Constraints](./data_contract.md#constraints) section above, you may want to apply the same constraints to many fields. A better way to define this rather than rewriting the constraints repeatedly for each field, is to define a custom type under the `types` key within the `contract` section. You can define a custom type like this:

```json title="movies.dischema.json"
{
    "types": {
        "MyConstrainedString": {
            "callable": "constr",
            "constraints": {
                "min_length": 1,
                "max_length": 20
            }
        }
    },
    "contract": {
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": "MyConstrainedString",
                    ...
                }
            },
            "cast": {
                "fields": {
                    "actor_id": "int",
                    "actor_forename": "MyConstrainedString",
                    "actor_surname": "MyConstrainedString",
                    ...
                }
            }
        }
    }
}
```

As you can see, we can set the "type" for several fields to `MyConstrainedString` which has a min & max length constraint.

#### Domain Types

Domain types are custom Pydantic model types available with the DVE. Current Domain types available are `Postcode`, `NHSNumber`, `FormattedDatetime` etc. You can find the full list of Domain Types [here](../advanced_guidance/package_documentation/domain_types.md).

### Complex Types

DVE supports the ability to define complex types such as `arrays`, `structs`, arrays of structs etc.

To define a struct type, you would add it to the `types` section like this...

```json title="movies.dischema.json"
{
    "contract": {
        "types": {
            "Actor": {
                "actor_id": "int",
                "actor_forename": "str",
                "actor_surname": "str",
                "character_name": "",
                "movies_acted": {
                    "type": "int",
                    "is_array": true
                }
            }
        },
        "datasets": {
            ...
        }
    }
}
```

... and then you can simply add a new field with `model` set to the new type and `is_array` equal to `true` (or `false` if you just want a struct).

```json title="movies.dischema.json"
{
    "contract": {
        "types": {
            "Actor": {
                "actor_id": "int",
                "actor_forename": "str",
                "actor_surname": "str",
                "character_name": "",
                "movies_acted": {
                    "type": "int",
                    "is_array": true
                }
            }
        },
        "datasets": {
            "movie": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": {
                        "callable": "constr",
                        "constraints": {
                            "min_length": 1,
                            "max_length": 20
                        }
                    },
                    "actors": {
                        "model": "Actor",
                        "is_array": true
                    }
                }
            }
        }
    }
}
```

If you just want to turn a simple type into an array, simply set `is_array` to `true`. E.g.

```json title="movies.dischema.json"
{
    "contract": {
        "datasets": {
            "cast": {
                "fields": {
                    "movies_acted": {
                        "type": "int",
                        "is_array": true
                    }
                }
            }
        }
    }
}
```

## Error Categories

As mentioned earlier, when a field...

- cannot be correctly type casted
- breaks the constraints of the type
- is missing when mandatory

... a [Feedback Message](./feedback_messages.md) will be produced. Each error raised, will be categorised into one of...

| Category | Meaning |
| -------- | ------- |
| Blank | The value is missing |
| Wrong format | The value could not be casted into the defined type. <br><br> I.e. str -> date, str -> int etc |
| Bad value | The value broke one of the constraints |

## Custom Error Details

When a [Feedback Message](./feedback_messages.md) is produced during the contract a number of default error codes and messages are utilised. If you need to overhaul the error code and error message, you can create a custom contract error details `JSON` document. It can be setup in the following way:

=== "movie.dischema.json"

    ```json
    {
        "contract": {
            "error_details": "movie_data_contract_details.json",
            "datasets": {
                "fields": {
                    "movie_uuid": "int",
                    "movie_name": "str",
                    ...
                }
            }
        }
    }
    ```

=== "movie_data_contract_details.json"

    ```json
    {
        "movie_uuid": {
            "Blank": {
                "error_code": "MOVIE_UUID_01",
                "error_message": "File Rejected - movie_uuid is blank."
            },
            "Bad Value": {
                "error_code": "MOVIE_UUID_02",
                "error_message": "File Rejected - movie_uuid has an incorrect data format. movie_uuid={{ movie_uuid }}."
            }
        },
        "movie_name": {
            "Bad Value": {
                "error_code": "MOVIE_NAME_01",
                "error_message": "File Rejected - movie_name has an incorrect data format. movie_name={{ movie_name }}."
            }
        }
    }
    ```

!!! Warning

    The contract details document must be in the same directory as the dischema document.


## Cache Originals

This setting allows you to retain a copy of the original entities (as defined within the dischema) before the business rules are applied. This is set to `False` by default. To enable it, simply add the following to your dischema document:

```json title="movies.dischema.json"
{
    "contract": {
        "cache_originals": true,
        ...
    }
}
```
