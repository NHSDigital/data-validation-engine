# Data Validation Engine

The Data Validation Engine (DVE) is a configuration driven data validation library built and utilised by NHS England.

As mentioned above, the DVE is "configuration driven" which means the majority of development for you as a user will be building a JSON document to describe how the data will be validated. The JSON document is also typically known as a `dischema` file and example files can be accessed [here](./tests/testdata/). If you'd like to learn more about JSON document and how to build one from scratch, then please read the documentation [here](./docs/).

Once a dischema file has been defined, you are ready to use the DVE. The DVE is typically orchestrated based on the four key "services". These are...

| Order | Service | Purpose |
| ----- | ------- | ------- |
| 1. | File Transformation | This service will take ingest submitted files and turn them into stringified parquet files to ensure that a consistent data structure can be passed through the DVE. |
| 2. | Data Contract | This service will validate and cast a stringified parquet submission against a [pyantic model](https://docs.pydantic.dev/latest/). |
| 3. | Business Rules | The business rules service will perform more complex validations such as comparisons between fields and tables, aggregations, filters etc to generate new entities. |
| 4. | Error Reports | The error reports service will take all the errors raised in previous services and surface them into a readable format for a downstream users/service. Currently, this implemented to be an excel spreadsheet but could be reconfigure to meet other requirements/use cases. |

We have more detailed documentation around these services [here](./docs/).

## Installation and usage

The DVE is a Python package and can be installed using `pip`. As of release (version 1+) only supports Python 3.7, with Spark version 3.2.1 and DuckDB version of 1.1.0. We are currently working on upgrading the DVE to work on Python 3.11+ and this will be made available asap with version 2.0.0 release.

In addition to a working Python 3.7+ installation you will need OpenJDK 11 installed.

Python dependencies are listed in `pyproject.toml`.

To install the DVE package you can simply install using a package manager such as [pip](https://pypi.org/project/pip/).

```
pip install https://github.com/nhsengland/Data-Validation-Engine
```

Once you have installed the DVE you are ready to use it. For guidance on how to create your dischema json document (configuration), please read the [documentation](/docs/).

The long term aim is to make the DVE available via PyPi and Conda but we are not quite there yet. Once available this documentation will be updated to reflect the new installation options.

## Requesting new features and raising bug reports
If you have spotted a bug with the DVE then please raise an issue [here](https://github.com/nhsengland/Data-Validation-Engine/issues). Same for any feature requests.

## Upcoming features
Below is a list of features that we would like to implement or have been requested.
| Feature | Release Version | Released? |
| ------- | --------------- | --------- |
| Open source release | 1.0.0 | Yes |
| Uplift to Python 3.11 | 2.0.0 | No |
| Upgrade to Pydantic 2.0 | Not yet confirmed | No |
| Create a more user friendly interface for building and modifying dischema files | Not yet confirmed | No |

Beyond the Python upgrade, we cannot confirm the other features will be made available any time soon. Therefore, if you have the interest and desire to make these features available, then please feel free to read the [contributing section](#contributing) and get involved.

## Contributing
Please see guidance [here](./CONTRIBUTE.md).

## Legal
This codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation.

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
