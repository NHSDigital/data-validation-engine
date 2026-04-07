<h1 style="display: flex; align-items: center; gap: 10px;">
    <img src="https://github.com/NHSDigital/data-validation-engine/blob/616b55890306db4546177f7effac48ca241857ec/overrides/.icons/nhseng.svg" alt="" width="5%" height="100%" align="left">
    Data Validation Engine
</h1>

![License](https://img.shields.io/github/license/NHSDigital/data-validation-engine)
![Version](https://img.shields.io/github/v/release/NHSDigital/data-validation-engine)
[![CI Unit Tests](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_testing.yml/badge.svg)](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_testing.yml)
[![CI Formatting & Linting](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_linting.yml/badge.svg)](https://github.com/NHSDigital/data-validation-engine/actions/workflows/ci_linting.yml)

The Data Validation Engine (DVE) is a configuration driven data validation library built and utilised by NHS England. Currently the package has been reverted from v1.0.0 release to a 0.x as we feel the package is not yet mature enough to be considered a 1.0.0 release. So please bear this in mind if reading through the commits and references to a v1+ release when on v0.x.

As mentioned above, the DVE is "configuration driven" which means the majority of development for you as a user will be building a JSON document to describe how the data will be validated. The JSON document is known as a `dischema` file and example files can be accessed [here](https://github.com/NHSDigital/data-validation-engine/tree/main/tests/testdata). If you'd like to learn more about JSON document and how to build one from scratch, then please read the documentation [here](https://nhsdigital.github.io/data-validation-engine/).

Once a dischema file has been defined, you are ready to use the DVE. The DVE is typically orchestrated based on four key "services". These are...

|    | Service | Purpose |
| -- | ------- | ------- |
| 1. | File Transformation | This service will take submitted files and turn them into stringified parquet file(s) to ensure that a consistent data structure can be passed through the other services. |
| 2. | Data Contract | This service will validate and perform type casting against a stringified parquet file using [pydantic models](https://docs.pydantic.dev/1.10/). |
| 3. | Business Rules | The business rules service will perform more complex validations such as comparisons between fields and tables, aggregations, filters etc to generate new entities. |
| 4. | Error Reports | The error reports service will take all the errors raised in previous services and surface them into a readable format for a downstream users/service. Currently, this implemented to be an excel spreadsheet but could be reconfigured to meet other requirements/use cases. |

If you'd like more detailed documentation around these services the please read the extended documentation [here](https://nhsdigital.github.io/data-validation-engine/).

The DVE has been designed in a way that's modular and can support users who just want to utilise specific "services" from the DVE (i.e. just the file transformation + data contract). Additionally, the DVE is designed to support different backend implementations. As part of the base installation of DVE, you will find backend support for `Spark` and `DuckDB`. So, if you need a `MySQL` backend implementation, you can implement this yourself. Given our organisations requirements, it will be unlikely that we add anymore specific backend implementations into the base package beyond Spark and DuckDB. So, if you are unable to implement this yourself, I would recommend reading the guidance on [requesting new features and raising bug reports here](#requesting-new-features-and-raising-bug-reports).

Additionally, if you'd like to contribute a new backend implementation into the base DVE package, then please look at the [Contributing](#Contributing) section.

## Installation and usage

The DVE is a Python package and can be installed using package managers such as [pip](https://pypi.org/project/pip/). As of the latest release we support Python 3.10 & 3.11, with Spark v3.4 and DuckDB v1.1. In the future we will be looking to upgrade the DVE to working on a higher versions of Python, DuckDB and Spark.

If you're planning to use the Spark backend implementation, you will also need OpenJDK 11 installed.

Python dependencies are listed in `pyproject.toml`.

To install the DVE package you can simply install using a package manager such as [pip](https://pypi.org/project/pip/).

```
pip install data-validation-engine
```

*Note - Only versions >=0.6.2 are available on PyPi. For older versions please install directly from the git repo or build from source.*

Once you have installed the DVE you are ready to use it. For guidance on how to create your dischema JSON document (configuration), please read the [documentation](https://nhsdigital.github.io/data-validation-engine/).

Version 0.0.1 does support a working Python 3.7 installation. However, we will not be supporting any issues with that version of the DVE if you choose to use it. __Use at your own risk__.

## Requesting new features and raising bug reports
**Before creating new issues, please check to see if the same bug/feature has been created already. Where a duplicate is created, the ticket will be closed and referenced to an existing issue.**

If you have spotted a bug with the DVE then please raise an issue [here](https://github.com/nhsengland/Data-Validation-Engine/issues) using the "bug template". 

If you have feature request then please follow the same process whilst using the "Feature request template".

## Upcoming features
Below is a list of features that we would like to implement or have been requested.
| Feature                                                                         | Release Version   | Released? |
| ------------------------------------------------------------------------------- | ----------------- | --------- |
| Open source release                                                             | 0.1.0             | Yes       |
| Uplift to Python 3.11                                                           | 0.2.0             | Yes       |
| Uplift Pyspark to 3.5                                                           | TBA               | No        |
| Allow DVE to run on Python 3.12+                                                | TBA               | No        |
| Upgrade to Pydantic 2.0                                                         | TBA               | No        |
| Uplift Pyspark to 4.0+                                                          | TBA               | No        |
| Create a more user friendly interface for building and modifying dischema files | Not yet confirmed | No        |

Beyond the Python and Pydantic upgrade, we cannot confirm the other features will be made available anytime soon. Therefore, if you have the interest and desire to make these features available, then please read the [Contributing](#Contributing) section and get involved.

## Contributing
Please see guidance [here](https://github.com/NHSDigital/data-validation-engine/blob/main/CONTRIBUTE.md).

## Legal
This codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation.

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
