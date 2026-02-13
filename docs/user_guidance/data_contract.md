Lets look at the data contract configuration from [Introduction to DVE](https://github.com/NHSDigital/data-validation-engine/blob/main/README.md) more closely, with a few more fields added:

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
                    },
                    "periodEndDate": {
                        "description": null,
                        "is_array": false,
                        "callable": "conformatteddate",
                        "constraints": {
                            "date_format": "%Y-%m-%d"
                        }
                    },
                },
                "mandatory_fields": [
                    "version",
                    "periodStartDate",
                    "periodEndDate"
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
            },
			"CWTActivity": {
				"fields": {
					"activityStartDate":{
					"is_array": false,
                        "callable": "conformatteddate",
                        "constraints": {
                            "date_format": "%Y-%m-%d"
                        }
					}
				}
			}
        }
    }
}
```

### Types

Here we have only filled out datasets. We've added a few more fields such as `PeriodEndDate` and `activityStartDate` and we're starting to see a fair amount of duplication. Lets refactor this to remove that. For this we use `types`. This allows us to pre-configure a type and re-use it across the different datasets.

```json
{
    "contract": {
        "cache_originals": true,
        "error_details": null,
        "types": {
            "isodate": {
                "description": "an isoformatted date type",
                "callable": "conformatteddate",
                "constraints": {
                    "date_format": "%Y-%m-%d"
                }
            }
        },
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
                    "periodStartDate": "isodate",
                    "periodEndDate": "isodate"
                },
                "mandatory_fields": [
                    "version",
                    "periodStartDate",
                    "periodEndDate"
                ],
                "reporting_fields": [],
                "key_field": null,
                "reader_config": {
                    ".xml": {
                        "reader": "XMLStreamReader",
                        "kwargs": {
                            "record_tag": "Header",
                            "n_records_to_read": 1
                        }
                    }
                },
                "aliases": {}
            },
            "CWTActivity": {
                "fields": {
                    "activityStartDate": "isodate"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "SparkXMLReader",
                        "kwargs": {
                            "record_tag": "Activity"
                        }
                    }
                }
            }
        }
    }
}
```

Now we've added an `isodate` type in the `types` object. We can now use this pre-configured type elsewhere.

### Schemas

Schemas are used when a dataset has another nested dataset within. An example in XML would be:

```xml
<Activity>
	<startdate>2025-01-02</startdate>
	<enddate>2025-01-31</enddate>
	<nhsnumber>1111111111</nhsnumber>
	<nationalcode>01</nationalcode>
	<CstActivity>
		<cstCode>somecode</cstCode>
		<number>100</number>
		<resource>
			<resource_id>abcd</resource_id>
			<cost>10.10</cost>
		</resource>
		<resource>
			<resource_id>defg</resource_id>
			<cost>20.20</cost>
		</resource>
	</CstActivity>
</Activity>
```

We can see here that the Activity has a number of fields. `startdate`, `enddate` etc. However, `CstActivity` has its own fields. Including `resource` which has it's own fields. This is a use case for Schemas. 

```json
{
    "contract": {
        "cache_originals": true,
        "error_details": null,
        "types": {
            "isodate": {
                "description": "an isoformatted date type",
                "callable": "conformatteddate",
                "constraints": {
                    "date_format": "%Y-%m-%d"
                }
            }
        },
        "schemas": {
            "resource": {
                "fields": {
                    "resource_id": "str",
                    "cost": {
                        "callable": "condecimal",
                        "constraints": {
                            "max_digits": 18,
                            "decimal_places": 8
                        }
                    }
                },
				"mandatory_fields": [
				    "cost",
                    "resource_id"
				]
            },
            "CstActivity": {
                "fields": {
                    "cstCode": "str",
                    "number": "int",
                    "resource": {
                        "model": "resource",
                        "is_array": true
                    }
                }
            }
        },
        "datasets": {
            "CWTActivity": {
                "fields": {
                    "startdate": "isodate",
                    "enddate": "isodate",
                    "nhsnumber": "str",
                    "nationalcode": "str",
                    "CstActivity": {
                        "model": "CstActivity",
                        "is_array": true
                    }
                },
                "reader_config": {
                    ".xml": {
                        "reader": "SparkXMLReader",
                        "kwargs": {
                            "record_tag": "Activity"
                        }
                    }
                }
            }
        }
    }
}
```

There's a lot going on here. We've set the `CstActivity` to a `model` and set the `is_array` parameter to `true`. This builds it as an array of that model. 

The same is true for resource in `CstActivity`. In Spark this would create a schema that has an array of structs of `CstActivities` with an array of Structs of Resources.

You can define as many schemas as you need to model your domain. This is particularly useful when the nested schemas don't have linkage IDs, so they can't be parsed as separate entities because the hierarchy would be lost.

Schemas can have `mandatory_fields` but don't require reader configurations.

### Field types

Fields can have a type defined as a string, either a base type like `date`, `str`, a [Domain type](./domain_types.md), or a defined [type](#types):

```json
{
	"startdate" : "date",
	"enddate" : "isodate",
	"numberofactivities": "NonNegativeInt"
}
```

If the type is an array then it needs to be defined as an object rather than short hand with just a string.

```json
{
	"startdates" : {
		"type" : "date",
		"is_array" : true
	}
}
```

It can be a model type defined in [schemas](#schemas), which can be also be an array or not.

```json
{
	"schemas" : {
		"APCCstActivity" : {
			"fields" : {
			...
			}
		}
	},
    ...
	{
		"CstActivity" : {
			"model" : "APCCstActivity"
			},
		"Resources" : {
			"model" : "APCResources",
			"is_array" : true
			}
	}
}
```

Finally callables. These are functions that return a type. Like `constr` from pydantic or `conformatteddate` in DVE [Domain types](./domain_types.md). Any keyword arguments that go to these callables are passed in as `constraints`.

```json
{
	"ID": {
		"callable" : "constr",
		"constraints" : {
			"min_length" : 5,
			"max_length": 20,
			"regex" : "^ABC\w+"
		}
	},
	"nhsnumber" : {
		"callable" : "permissive_nhs_number",
		"constraints" : {
			"warn_on_test_numbers" : true
		}
	}
}
```

In the example above, I've defined an `ID` field that is a constrained string type that should be between 5 and 20 characters in length and start with `ABC`. Additionally, I have also defined an `nhsnumber` field that raises warning when a test number is submitted (palindromes, or starts with 9).
