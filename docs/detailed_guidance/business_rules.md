# Business Rules
Business rules are defined in the `transformations` section of the config. There are 6 keys within the json document that we will discuss in more detail throughout this document.

## Keys
| Key | Purpose |
| --- | ------- |
| `parameters` | For setting globally available variables. |
| `reference_data` | For bringing in reference data tables. |
| `rules_store` | For referring to other configuration files that contain shared rules. |
| `filters` | Simple rules that don't require much or any transformation. |
| `complex_rules` | Series of transformations that end in a filter. Such as joining or aggregating before performing a check. |
| `post_filter_rules` | For clearing down created entities that are no longer needed after validation. |

## Filters 
These are the most simple of the business rules. These are defined as a json object with the following structure:
```json
{
"entity": "APCActivity",
"name": "EpiNo_is_valid",
"expression": "EpiNo IS NULL OR EpiNo RLIKE '^(0[1-9]|[1-7][0-9]|8[0-7]|9[89])$'",
"failure_type": "submission",
"failure_message": "is invalid",
"error_code": "1203",
"reporting_field": "EpiNo",
"is_informational" : false,
"category": "Bad value"
}
```
This rule checks that EpiNo must be present and that the value is 01-87 or 98 or 99. If EpiNo is missing this rule doesnt fire (to prevent double dinging a missing value). Any EpiNo that are present but not one of the values expected will raise a 1203 error with the message "is invalid". 
Lets break it down:
| Key | Purpose |
| --- | ------- |
| `entity` | This is the name of the entity to perform the filter on. In this Case the `APCActivity` dataframe |
| `name` | This should be a descriptive name for the rule. |
| `expression` | The SQL expression that evaluates to a bool. Any row that evaluates to False will be filtered out. This is so that you can define the rules as they are written in the ETOS rather than inverting the conditions |
| `failure_type` | The type of failure. There are three types of failures. Submission, record or integrity. <li>"submission" means the whole submission is invalidated by a failure in this rule and should be rejected (though this is for you to implement).</li><li>"record" means that this row of data is invalid, and will be excluded from the output.</li><li>"integrity" means that some constraint on the data has failed and no further processing can occur. This is normally raised when there is a parsing error in the expression but can be used to quickly reject data that doesn't meet a basic check</li> |
| `failure_message` | The message you wish for the user to receive |
| `error_code` | the code that links back to the specification. For example CHC had error codes like `CHC0010021` for the second field in the CHC001 tables first validation. This allows for collection of metrics for which rules have fired (again for you to implement) and allows the user to go back to the specification if the error message isn't clear enough |
| `reporting_field` | This is the field to report back to the user as having failed. The expression could be more complex and be something like `if(NhsNumber is null, NHSStatus = '05', True)`, where is NHSNumber isn't null we short circuit the rule and everything else passes. otherwise the result of the expression is the `NHSStaus` being 05. In this case you may wish to have reporting field be `NHSStatus` and report back which status triggered the check. or `['NHSNumber', 'NHSStatus']` to report them both back. |
| `is_informational` | This bool signals that this is a warning rather than an error
| `category` | Optional literal. Used more in metrics to give an idea of how many things fail due to a values being wrong, formatting, nulls or file parsing. Below is an example of categorical error types... <li> "bad value" - The value(s) in the check were wrong</li><li>"wrong format" - The formatting of the field is incorrect</li><li>"blank" - the value is missing when it shouldn't be </li><li>"bad file" - usually used when the file fails to parse due to bad formatting</li>

## Parameters
Parameters are globally available parameters that can be templated in to a rule using jinja2 syntax.

Lets say we have an example that compares several fields against the start of the financial year.

we could implement it like this:

```json
{
	"filters" : [
		{
			"entity": "APCActivity",
			"name": "StartDate_is_valid",
			"expression": "StartDate >= '2025-04-01'",
			"failure_type": "submission",
			"failure_message": "start date is before the start of the financial year",
			"error_code": "1203",
			"reporting_field": "StartDate",
			"is_informational" : false,
			"category": "Bad value"
		},
		{
			"entity": "APCActivity",
			"name": "EndDate_is_valid",
			"expression": "EndDate >= '2025-04-01'",
			"failure_type": "submission",
			"failure_message": "EndDate is before the start of the financial year",
			"error_code": "1203",
			"reporting_field": "EndDate",
			"is_informational" : false,
			"category": "Bad value"
		},
		...
	]
}
```
This is fine for just 2 rules, but what if all dates need to be after the start of the financial year? What if a requirement comes in that it should be the 6th of april not the 1st of april?

This is what parameters are for.

```json
{
	"parameters" : {
		"financial_year_start_date" : "'2025-04-01'"
	},
	"filters" : [
		{
			"entity": "APCActivity",
			"name": "StartDate_is_valid",
			"expression": "StartDate >= {{ financial_year_start_date }}",
			"failure_type": "submission",
			"failure_message": "start date is before the start of the financial year",
			"error_code": "1203",
			"reporting_field": "StartDate",
			"is_informational" : false,
			"category": "Bad value"
		},
		{
			"entity": "APCActivity",
			"name": "EndDate_is_valid",
			"expression": "EndDate >= {{ financial_year_start_date }}",
			"failure_type": "submission",
			"failure_message": "EndDate is before the start of the financial year",
			"error_code": "1204",
			"reporting_field": "EndDate",
			"is_informational" : false,
			"category": "Bad value"
		},
		...
	]
}
```
Now we have the financial year start date parameterized. Any rule that needs to use it uses the same version. If we change the value in the parameter, all of the rules that use the parameter are updated too.

These rules are quite repetitive. Using the same set up, similar error message. The only difference is the reporting field and error code in essence. 

## Complex rules

Complex rules are pre-configured rules that can have multiple steps and accept parameters. These need to be defined in another file and then brought in using the rule store.

The complex rule key in the main configuration refers to externally defined complex rules, and passes any parameters into them. So lets look at a simple rule, refactoring the example from above.

> `complex_rules.rulestore.json`
```json
{
	"date_is_ge_financial_year" : {
		"description" : "checks the passed date is after or equal to the passed in date",
		"type" : "complex_rule",
		"parameter_descriptions" : {
			"error_code" : "code for the raised error",
			"financial_year_start_date" : "the date that the financial year starts",
			"field" : "the field to check", 
			"entity" : "the entity the field exist on"
		},
		"parameter_defaults" : {},
		"rule_config": {
			"rules" : [],
			"filters" : [
				{
					"entity": "{{ entity }}",
					"name": "{{ field }}_is_valid",
					"expression": "{{ field }} >= {{ financial_year_start_date }}",
					"failure_type": "submission",
					"failure_message": "{{ field }} is before the start of the financial year",
					"error_code": "{{ error_code }}",
					"reporting_field": "{{ field }}",
					"is_informational" : false,
					"category": "Bad value"
				},
			]
		}
	}
}
```
Now that we have those rules define, we can use them in our regular configuration file.

First we need to include the file in our rule_stores
>`example.json`
```json
{
	"parameters" : {
		"financial_year_start_date" : "'2025-04-01'"
	},
	"rule_stores": [
		{
			"store_type": "json",
			"filename": "complex_rules.dischema.json"
		},
	],
	"filters" : [],
	"complex_rules" : [
		{
			"rule_name" : "date_is_ge_financial_year",
			"parameters" : {
				"field" : "StartDate",
				"error_code" : "1203",
				"entity" : "APCActivity",
			}
		},
		{
			"rule_name" : "date_is_ge_financial_year",
			"parameters" : {
				"field" : "EndDate",
				"error_code" : "1204",
				"entity" : "APCActivity",
			}
		},
		...
	]
}
```
Note we've replaced the filters from the parameters section with complex rules. This requires that we pull in a rule_store. There are no limits to the number that can be included, and they can be shared across multiple versions of the specification. Now we have a rule that's defined once, and called multiple times with different parameters. 

> Note that `financial_year_start_date` isn't passed explicitly, that's because it's set as a parameter. Parameters are implicitly passed, you can be explicit if you prefer.

## Rules
We've covered adding filters to complex rules, but we can add rules to them aswell. This may be a bit of a misnomer, these are transformations on the data that get executed before filters. These operations include
- select
- takes an entity and performs a select for either adding new columns, removing columns.
- remove
- remove a given column
- add column
- adds a new column
- group_by
- perform an aggregation on an entity
- filter_without_notifying
- filter things without raising an error message, to do things like remove nulls before doing a regular filter
- Joins
- left
- inner
- anti_join
- join to another table, any row that doesn't have a match in the other table will remain
- more performant than doing a join then a null check
- semi_join
- join to another table, any row that doesn't have in the other table with be removed
- join_header
- joins a table with a single row onto every row. will raise an error if the header table has more than a single row.
- used for things like checking submitting all dates in a file match the header 
- one_to_one_join
- join to another entity expecting no change in the number of rows. integrity check can be toggled off
> see [json_schemas/transformations](../json_schemas/transformations/) for expected fields for each operation

Rules are executed in the order they are put into the array. So a join then select should be implemented in that order.

```json
{
	"rule_name" : {
		...
	},
	"rules": [
		{
			"name": "Get CareId counts",
			"operation": "group_by",
			"entity": "{{ feed_type }}Activity",
			"new_entity_name": "{{ feed_type }}CareIdCounts",
			"group_by": "CareId",
			"agg_columns": {
				"COUNT(1)": "CareIdFreq"
			}
		},
		{
			"name": "Filter to keep only CareIds occuring more than once",
			"operation": "filter_without_notifying",
			"entity": "{{ feed_type }}CareIdCounts",
			"filter_rule": "CareIdFreq > 1"
		},
		{
			"name": "Inner join the activities onto the CareId counts",
			"operation": "inner_join",
			"entity": "{{ feed_type }}CareIdCounts",
			"target": "{{ feed_type }}Activity",
			"join_condition": "{{ feed_type }}CareIdCounts.CareId == {{ feed_type }}Activity.CareId",
			"new_columns": "{{ feed_type }}Activity.*"
		}
	],
	"filters": [
		{
			"entity": "{{ feed_type }}CareIdCounts",
			"expression": "FALSE",
			"failure_type": "submission",
			"failure_message": "cannot be duplicate",
			"error_code": "1500",
			"reporting_entity": "{{ feed_type }}Activity",
			"reporting_field": "CareId",
			"category": "Bad value"
		}
	],
	"post_filter_rules": [
		{
			"name": "Remove temporary entities",
			"operation": "remove_entity",
			"entity": "{{ feed_type }}CareIdCounts"
		}
	],
	"dependencies" : []
}
```
Above is an example taken from a [PLICS](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/patient-level-information-and-costing-system-plics-data-collections) rule. We start with a `group_by`, that creates a new entity. 

> ⚠️ If you don't set a new entity, it will override the entity that's been used ⚠️

We can then filter out any that don't have a count greater than 1. We then join back the activity date so it can be included in the error.

The filter acts on the newly created entity, and since we've already filtered out any that's less than 1, all of the remaining are failures. So we raise a 1500 error for all of them. 

Then post-filter rules runs, and clears up the created entity. 

Finally we see the `dependencies` key, this is a list of rule names that this rule depends on. In this case it doesn't have any dependencies. But lets say we wanted to explode out an array, and that exploded version is used for many rules. We can make that explode a rule without any filters. Then other rules can depend on it. 

> Any dependencies need to be included in the complex rules of the `dischema.json` file

## Reference data

Reference data can be included, it's on object that takes the name you want to refer to the data as a key and the specification as a value:

```json
{
	"reference_data": {
		"allowed_submitters": {
			"type": "table",
			"database": "dve",
			"table_name": "refdata_plics_organisation_submitting_id"
		},
		"collection_activity": {
			"type": "table",
			"database": "dve",
			"table_name": "refdata_plics_int_collection_activity"
		},
		"collection_resource": {
			"type": "table",
			"database": "dve",
			"table_name": "refdata_plics_int_collection_resource"
		}
	}
}
```
This allows us to refer to `refdata_plics_organisation_submitting_id` as `allowed_submitters` when we do things like anti-joins to it. The type is a table, it's in the dve database and the table name is `refdata_plics_organisation_submitting_id`. If we use the `SparkRefDataLoader` from `core_engine/backends/implementations/spark/reference_data.py` as our loader then this will lazily include the tables when they are used. There are other ways to specify reference data than just database objects - we can also specify relative file paths (from the location of the dischema location) or absolute uris.

When using reference data we recommend using the `EntityManager` class, this prevents reference data from being mutated. 

an example in code for the parquet reader would be...
```python
ref_data_config = config.get_reference_data_config()
rules = config.get_rule_metadata()
SparkRefDataLoader.spark = spark
SparkRefDataLoader.dataset_config_uri = "/path/to/folder/containing/dischema"

ref_data = SparkRefDataLoader(
	ref_data_config,
)

entities = {...}
entity_manager = EntityManager(entities, ref_data)

business_rules.apply_rules(entity_manager, rules)
```
For the table loader it would be...
```python
ref_data_config = config.get_reference_data_config()
rules = config.get_rule_metadata()
ref_data = SparkTableRefDataLoader(ref_data_config)

entities = {...}
entity_manager = EntityManager(entities, ref_data)

business_rules.apply_rules(entity_manager, rules)
```

...This can then be used in rules for refdata comparison:

```json
{
	"name": "Get the activities violating 1029",
	"operation": "anti_join",
	"entity": "{{ feed_type }}Activity",
	"target": "refdata_allowed_submitters",
	"join_condition": "{{ feed_type }}Activity.OrgId <=> refdata_allowed_submitters.Org_ID",
	"new_entity_name": "{{ feed_type }}1029Violators"
}
```
> Note the prefix `refdata_` acts as an alias and allows for explicit join between entities.
