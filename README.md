<h1 style="display: flex; align-items: center; gap: 10px;">
    <img src="https://raw.githubusercontent.com/NHSDigital/data-validation-engine/616b55890306db4546177f7effac48ca241857ec/overrides/.icons/nhseng.svg" alt="" width="5%" height="100%" align="left">
    Data Validation Engine
</h1>

![License](https://img.shields.io/github/license/NHSDigital/data-validation-engine)
![PyPi](https://img.shields.io/pypi/v/data-validation-engine)
![Conda](https://anaconda.org/nhs/data-validation-engine/badges/version.svg)
[![CI Unit Tests](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_testing.yml/badge.svg)](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_testing.yml)
[![CI Formatting & Linting](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_linting.yml/badge.svg)](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_linting.yml)

The Data Validation Engine (DVE) is a configuration-driven data validation library created and used by NHS England. It lets users define validation rules once and apply them across multiple dataset collections - supporting consistent, accurate data checks.

__The DVE offers__:

- SQL configuration-based validations
- Format normalization to Parquet for a unified data representation
- Data modelling and typecasting
- Business-rule validations executed on supported backends such as Spark and DuckDB, with the option to add custom backends
- Clear validation reporting, including summary insights and record-level error messages

As mentioned above, the DVE is "configuration driven" which means the majority of development for you as a user will be building a JSON document to describe how the data will be validated. The JSON document is known as a `dischema` (data ingest schema) file and example files can be accessed [here ↗️](https://github.com/NHSDigital/data-validation-engine/tree/main/tests/testdata). If you'd like to learn more about JSON document and how to build one from scratch, then please read the documentation [here ↗️](https://nhsdigital.github.io/data-validation-engine/).

## Installation and usage

Please see the documentation [here ↗️](https://nhsdigital.github.io/data-validation-engine/user_guidance/install/).

## Requesting new features and raising bug reports
**Before creating new issues, please check to see if the same bug/feature has been created already. Where a duplicate is created, the ticket will be closed and referenced to an existing issue.**

If you have spotted a bug with the DVE then please raise an issue [here ↗️](https://github.com/nhsengland/Data-Validation-Engine/issues) using the "bug template". 

If you have feature request then please follow the same process whilst using the "Feature request template".

## Upcoming features
Below is a list of features that we would like to implement or have been requested.
| Feature                                                                         | Release Version   | Released? |
| ------------------------------------------------------------------------------- | ----------------- | --------- |
| Open source release                                                             | 0.1.0             | Yes       |
| Uplift to Python 3.11                                                           | 0.2.0             | Yes       |
| Uplift Pyspark to 3.5                                                           | 0.8.0             | Yes       |
| Allow DVE to run on Python 3.12+                                                | 0.8.0             | Yes       |
| Upgrade to Pydantic 2.0                                                         | 0.9.0             | Yes        |
| Uplift Pyspark to 4.0+                                                          | TBA               | No        |
| Polars upgrade to v1+ | TBA | No |
| DuckDB upgrade to v1.5+ | TBA | No |
| Python 3.13 & 3.14 upgrade | TBA | No |
| Create a more user friendly interface for building and modifying dischema files | TBA | No        |

If you are interested in getting any of the unreleased features listed above available, then please read the [Contributing](#Contributing) section and then submit us a pull request.

## Contributing
Please see guidance [here ↗️](https://github.com/NHSDigital/data-validation-engine/blob/main/CONTRIBUTE.md).

## Legal
This codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation.

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
