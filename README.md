# Data Validation Engine

The Data Validation Engine (DVE) is a configuration driven data validation library built and utilised by NHS England. Currently the package has been reverted from v1.0.0 release to a 0.x as we feel the package is not yet mature enough to be considered a 1.0.0 release. So please bear this in mind if reading through the commits and references to a v1+ release when on v0.x.

As mentioned above, the DVE is "configuration driven" which means the majority of development for you as a user will be building a JSON document to describe how the data will be validated. The JSON document is known as a `dischema` file and example files can be accessed [here](./tests/testdata/). If you'd like to learn more about JSON document and how to build one from scratch, then please read the documentation [here](./docs/).

Once a dischema file has been defined, you are ready to use the DVE. The DVE is typically orchestrated based on four key "services". These are...

|    | Service | Purpose |
| -- | ------- | ------- |
| 1. | File Transformation | This service will take submitted files and turn them into stringified parquet file(s) to ensure that a consistent data structure can be passed through the other services. |
| 2. | Data Contract | This service will validate and perform type casting against a stringified parquet file using [pydantic models](https://docs.pydantic.dev/1.10/). |
| 3. | Business Rules | The business rules service will perform more complex validations such as comparisons between fields and tables, aggregations, filters etc to generate new entities. |
| 4. | Error Reports | The error reports service will take all the errors raised in previous services and surface them into a readable format for a downstream users/service. Currently, this implemented to be an excel spreadsheet but could be reconfigured to meet other requirements/use cases. |

If you'd like more detailed documentation around these services the please read the extended documentation [here](./docs/).

The DVE has been designed in a way that's modular and can support users who just want to utilise specific "services" from the DVE (i.e. just the file transformation + data contract). Additionally, the DVE is designed to support different backend implementations. As part of the base installation of DVE, you will find backend support for `Spark` and `DuckDB`. So, if you need a `MySQL` backend implementation, you can implement this yourself. Given our organisations requirements, it will be unlikely that we add anymore specific backend implementations into the base package beyond Spark and DuckDB. So, if you are unable to implement this yourself, I would recommend reading the guidance on [requesting new features and raising bug reports here](#requesting-new-features-and-raising-bug-reports).

Additionally, if you'd like to contribute a new backend implementation into the base DVE package, then please look at the [Contributing][#Contributing] section.

## Installation and usage

The DVE is a Python package and can be installed using `pip`. As of release v0.1.x we currently only supports Python 3.7, with Spark version 3.2.1 and DuckDB version of 1.1.0. We are currently working on upgrading the DVE to work on Python 3.10-3.11 and this will be made available with version v0.2.x release.

In addition to a working Python 3.7+ installation you will need OpenJDK 11 installed if you're planning to use the Spark backend implementation.

Python dependencies are listed in `pyproject.toml`.

To install the DVE package you can simply install using a package manager such as [pip](https://pypi.org/project/pip/).

```
pip install git+https://github.com/NHSDigital/data-validation-engine.git@v0.1.0
```

Once you have installed the DVE you are ready to use it. For guidance on how to create your dischema JSON document (configuration), please read the [documentation](./docs/).

Please note - The long term aim is to make the DVE available via PyPi and Conda but we are not quite there yet. Once available this documentation will be updated to contain the new installation options.

## Requesting new features and raising bug reports
**Before creating new issues, please check to see if the same bug/feature has been created already. Where a duplicate is created, the ticket will be closed and referenced to an existing issue.**

If you have spotted a bug with the DVE then please raise an issue [here](https://github.com/nhsengland/Data-Validation-Engine/issues) using the "bug template". 

If you have feature request then please follow the same process whilst using the "Feature request template".

## Upcoming features
Below is a list of features that we would like to implement or have been requested.
| Feature | Release Version | Released? |
| ------- | --------------- | --------- |
| Open source release | 0.1.0 | Yes |
| Uplift to Python 3.11 | 0.2.0 | Yes |
| Upgrade to Pydantic 2.0 | Not yet confirmed | No |
| Create a more user friendly interface for building and modifying dischema files | Not yet confirmed | No |

Beyond the Python upgrade, we cannot confirm the other features will be made available anytime soon. Therefore, if you have the interest and desire to make these features available, then please read the [Contributing](#contributing) section and get involved.

## Contributing
Please see guidance [here](./CONTRIBUTE.md).

## Legal
This codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation.

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
