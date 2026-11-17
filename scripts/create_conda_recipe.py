"""Helper script to update conda recipe file when releasing"""

import logging
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import jinja2
import urllib3
import yaml

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


def get_pypi_conda_mapping():
    """Generate mapping between pypi and conda-forge package names using grayskull pypi mapping"""
    resp = urllib3.request(
        "GET",
        "https://raw.githubusercontent.com/conda-forge/conda-forge-bot-data/refs/heads/main/mappings/pypi/grayskull_pypi_mapping.yaml",  # pylint: disable=C0301
    )
    return {
        val["pypi_name"]: val["conda_name"]
        for val in filter(
            lambda x: x["conda_name"] != x["pypi_name"], yaml.safe_load(resp.data).values()
        )
    }


def get_conda_recipe_template_values(toml_file: Path) -> dict[str, Any]:
    """Generate template values for supplying to meta.yaml template"""
    with open(toml_file, "rb") as fle:
        pyproj = tomllib.load(fle)

    specific_release = re.compile(r"^\d+\.\d+\.\d+$")

    deps = pyproj["tool"]["poetry"]["dependencies"]
    return {
        "version": os.environ["VERSION"],
        "sha_release": os.environ["RELEASE_SHA"],
        "python_version": deps["python"],
        "dependencies": [
            f"{PYPI_TO_CONDA_MAPPING.get(k.lower(),k.lower())} {v if not re.match(specific_release, v) else v + '.*'}"  # pylint: disable=C0301
            for k, v in deps.items()
            if k
        ],
    }


TEMPLATE = """schema_version: 1

context:
  version: {{version}}

package:
  name: data-validation-engine
  version: ${{'{{'}}version{{'}}'}}

source:
  url: https://files.pythonhosted.org/packages/source/d/data-validation-engine/data_validation_engine-${{'{{'}}version{{'}}'}}.tar.gz
  sha256: {{sha_release}}

build:
  number: 0
  noarch: python
  script: ${{ PYTHON }} -m pip install . -vv

requirements:
  host:
    - python >=3.11,<3.13
    - poetry-core
    - pip
  run:
    - python >=3.11,<3.13
    - boto3 >=1.34.162,<1.36
    - botocore >=1.34.162,<1.36
    - delta-spark >=3.0.0,<=3.2.0
    - python-duckdb >=1.4,<1.4.5
    - jinja2 3.1.6.*
    - lxml 6.1.1.*
    - numpy 1.26.4.*
    - openpyxl 3.1.5.*
    - pandas 2.3.3.*
    - polars 0.20.31.*
    - pyarrow 23.0.1.*
    - pydantic 2.13.4.*
    - pyspark >=3.5.0,<=3.5.5
    - typing_extensions 4.16.0.*
    - urllib3 2.8.0.*

tests:
  - python:
      imports:
        - dve
        - duckdb
      pip_check: true

about:
  summary: "`nhs data validation engine` is a framework used to validate data"
  license: MIT
  license_file: LICENSE
  homepage: https://github.com/NHSDigital/data-validation-engine
  documentation: https://nhsdigital.github.io/data-validation-engine/

extra:
  recipe-maintainers:
    - georgeRobertson
    - stevenhsd

"""

if __name__ == "__main__":
    logger.info("Aquiring mapping of pypi packages to conda packages")
    PYPI_TO_CONDA_MAPPING: dict[str, str] = get_pypi_conda_mapping()
    env = jinja2.Environment(trim_blocks=True, autoescape=True)
    logger.info("Creating meta.yaml file")
    with open(
        Path(__file__).parent.joinpath("../conda_recipe/meta.yaml"), mode="w", encoding="utf-8"
    ) as recipe:
        recipe.write(
            env.from_string(TEMPLATE).render(
                **get_conda_recipe_template_values(
                    Path(__file__).parent.joinpath("../pyproject.toml")
                )
            )
        )
