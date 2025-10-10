# DVE Contributing guidelines

# Developer information

## Getting started

To begin with I would recommend that users read all the documentation available within the [docs](./docs/). It gives an overview of how the DVE works and how to work with the dischema json document.

## General requirements
To start contributing to the DVE project you will need the following tooling available:
| Tool | Version | Reason |
| ---- | ------- | ------ |
| `Python` | 3.7.17 | Currently supported version of `Python` for the DVE. |
| `Poetry` | 1.4.2 | Build and venv tool used for the DVE. |
| `Java` | java liberica-1.8.0 | `Java` version required for `PySpark`. |
| `pre-commit` | 2.21.0 | Currently installed as part of the `poetry` venv but seperate installation is fine. |
| `commitizen` | 3.9.1 | Like `pre-commit`, installed as part of the `poetry` venv but seperate installation is fine. This is used to manage commits and automated semantic versioning. |
| `git-secrets` | Latest | Utilised as part of the `pre-commit` to ensure that no secrets are commited to the repository. There is a helper installation script within [scripts](/scripts/git-secrets/). |

Additionally, we have created a [asdf support](.tool-versions) and [mise-en-toml](.mise.toml) for those utilising `asdf` or `mise-en-toml` software.

## Testing Requirements

Testing requirements are given in [pyproject.toml](./poetry.toml#48) under `tool.poetry.group.test.dependencies`. These are always pinned versions for consistency, but should be updated regularly if new versions are released. The following core packages are used for testing:
 - [pytest](https://docs.pytest.org/en/stable/): Used for Python unit tests, and some small e2e tests which check coverage.
 - [behave](https://github.com/behave/behave): Used for full, business-driven end-to-end tests.
 - [coverage](https://coverage.readthedocs.io/en/7.10.7/): Used to get coverage for `pytest` tests.

## Linting/Formatting/Type Checking Requirements

Additional dependencies for linting, type checking, and formatting are listed within the [pyproject.toml](./poetry.toml#58) under `tool.poetry.group.lint.dependencies`. Like the testing requirements, we use pinned versions for these.

This mostly breaks down to:
- [black](https://github.com/psf/black): a tool to format Python, which ensures consistency of formatting across the project. We use a line length of 100.
- [isort](https://pycqa.github.io/isort/): a tool to organise imports in a consistent way across the project.
- [mypy](https://github.com/python/mypy): a type checker for Python. This ensures that our function signatures remain accurate and is very useful for spotting type-related issues (a significant category of bugs).
- [pylint](https://pylint.readthedocs.io/en/stable/): a (very aggressive) linter for Python, which prevents errors and code style violations across the project. This pushes us to document the code and is often quite good at highlighting overly complex code.

We use these tools to ensure that code quality is not excessively compromised, even when working at pace.

## Installation for Development

We are utilising Poetry for build dependency management and packaging. If you're on a system that has `Make` available, you can simply run `make install` to setup a local virtual environment with all the dependencies installed (this won't install Poetry for you).

## Testing

Tests should be run after installing the package for development as outlined above.
- To run unit tests without coverage, run `poetry run pytest tests/`
- To run the unit tests with coverage, run `poetry run coverage run`
- To check the coverage run `poetry run coverage report -m`
- To run the behave tests, run `poetry run behave tests/features` (these are not included in coverage calculations)

## Submitting a pull request

If you want to contribute to the DVE then please follow the steps below:
1. Fork the repository.
2. Configure and install the dependencies.
3. Ensure that new changes are fully tested and that you are reguarlly checking for changes within the DVE repository to sort any potential merge conflicts.
4. Ensure linting passes.
5. Push and then create a pull request from your fork to our repository.

Your pull request will then be reviewed. You may receive some feedback and suggested changes before it can be approved and merged.
