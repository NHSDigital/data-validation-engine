---
title: Installing the Data Validation Engine
tags:
    - Introduction
    - Installation
---

!!! warning
    **DVE is currently an unstable package. Expect breaking changes between every minor patch**. We intend to follow semantic versioning of `major.minor.patch` more strictly after a 1.0 release. Until then, we recommend that you pin your install to the latest version available and keep an eye on [future releases](https://github.com/NHSDigital/data-validation-engine/releases).

    **Please note that we only support Python runtimes of 3.10 and 3.11.** In the future we will look to add support for Python versions greater than 3.11, but it's not an immediate priority.

    If working on Python 3.7, the `0.1` release supports this (and only this) version of Python. However, we have not been updating that version with any bugfixes, performance improvements etc. There are also a number of vulnerable dependencies on version `0.1` release due to [Python 3.7 being depreciated](https://devguide.python.org/versions/) and a number of packages dropping support. **If you choose to install `0.1`, you accept the risks of doing so and additional support will not be provided.**

You can install the DVE package through python package managers such as [pip](https://pypi.org/project/pip/), [pipx](https://github.com/pypa/pipx), [uv](https://docs.astral.sh/uv/) and [poetry](https://python-poetry.org/).

=== "pip"

    ```sh
    pip install data-validation-engine
    ```

=== "pipx"

    ```sh
    pipx install data-validation-engine
    ```

=== "uv"

    Add to your existing `uv` project...
    ```sh
    uv add data-validation-engine
    ```

    ...or you can add via your `pyproject.toml`...

    ```toml
    dependencies = [
        data-validation-engine
    ]
    ```

    ```sh
    uv lock
    ```

    ```sh
    uv sync
    ```

=== "poetry"

    Add to your existing `poetry` project...
    ```sh
    poetry add data-validation-engine
    ```

    ...or you can add via your `pyproject.toml`...

    ```toml
    [tool.poetry.dependencies]
    data-validation-engine = "*"
    ```

    ```sh
    poetry lock
    ```

    ```sh
    poetry install
    ```

!!! info
    We are working on getting the DVE available via Conda. We will update this page with the relevant instructions once this has been successfully setup.

Python dependencies are listed in the [`pyproject.toml`](https://github.com/NHSDigital/data-validation-engine/blob/main/pyproject.toml). Many of the dependencies are locked to quite restrictive versions due to complexity of this package. Core packages such as Pydantic, Pyspark and DuckDB are unlikely to receive flexible version constraints as changes in those packages could cause the DVE to malfunction. For less important dependencies, we have tried to make the contraints more flexible. Therefore, we would advise you to install the DVE into a seperate environment rather than trying to integrate it into an existing Python environment.

Once you have installed the DVE you are almost ready to use it. To be able to run the DVE, you will need to choose one of the supported pipeline runners (see Backend implementations here - [DuckDB](user_guidance/implementations/duckdb.md) *or* [Spark](user_guidance/implementations/spark.md)) and you will need to create your own dischema document to configure how the DVE should validate incoming data. You can read more about this in the [Getting Started](getting_started.md) page.


## DVE Version Compatability Matrix

| DVE Version  | Python Version | DuckDB Version | Spark Version | Pydantic Version |
| ------------ | -------------- | -------------- | ------------- | ---------------- |
| >=0.6        | >=3.10,<3.12   | 1.1.*          | 3.4.*         | 1.10.15          |
| >=0.2,<0.6   | >=3.10,<3.12   | 1.1.0          | 3.4.4         | 1.10.15          |
| 0.1          | >=3.7.2,<3.8   | 1.1.0          | 3.2.1         | 1.10.15          |
