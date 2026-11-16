import tomllib
import jinja2
from pathlib import Path
import os
import re
import yaml
import urllib3
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

from typing import Any

def get_pypi_conda_mapping():
    resp = urllib3.request("GET", "https://raw.githubusercontent.com/conda-forge/conda-forge-bot-data/refs/heads/main/mappings/pypi/grayskull_pypi_mapping.yaml")
    return dict(
        map(
            lambda x: (x["pypi_name"], x["conda_name"]),
            filter(
                lambda x: x["conda_name"] != x["pypi_name"],
                   yaml.safe_load(resp.data).values()
                   )
            )
        )

def get_conda_recipe_template_values(toml_file: Path) -> dict[str, Any]:
    with open(toml_file, "rb") as fle:
        pyproj = tomllib.load(fle)
        
    
    specific_release = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
    
    deps = pyproj["tool"]["poetry"]["dependencies"]
    return {
        "version": os.environ["VERSION"],
        "sha_release": os.environ["RELEASE_SHA"],
        "python_version": deps["python"],
        "dependencies": [ f"{PYPI_TO_CONDA_MAPPING.get(k.lower(),k.lower())} {v if not re.match(specific_release, v) else v + '.*'}"
                         for k, v in deps.items() if k ]
    }

TEMPLATE = """schema_version: 1

package:
  name: data-validation-engine
  version: {{version}}

source:
  url: https://files.pythonhosted.org/packages/source/d/data-validation-engine/data_validation_engine-{{version}}.tar.gz
  sha256: {{sha_release}}

requirements:
  host:
    - python {{ python_version }}
    - poetry-core
    - pip

build:
  number: 0
  noarch: python
  script: python -m pip install . -vv

  run:
{% for item in dependencies %}
    - {{ item }}
{% endfor %}

tests:
  - python:
      imports:
        - dve
      pip_check: true

about:
  summary: "`nhs data validation engine` is a framework used to validate data"
  license: "MIT"
  license_file: "LICENSE"
  homepage: "https://github.com/NHSDigital/data-validation-engine"
  documentation: "https://nhsdigital.github.io/data-validation-engine/"

extra:
  recipe-maintainers:
    - georgeRobertson
    - stevenhsd

"""

if __name__ == "__main__":
    logger.info("Aquiring mapping of pypi packages to conda packages")
    PYPI_TO_CONDA_MAPPING: dict[str, str] = get_pypi_conda_mapping()
    env = jinja2.Environment(trim_blocks=True)
    logger.info("Creating meta.yaml file")
    with open(Path(__file__).parent.joinpath("../conda_recipe/meta.yaml"), mode="w") as recipe:
        recipe.write(
            env.from_string(TEMPLATE).render(
            **get_conda_recipe_template_values(Path(__file__).parent.joinpath("../pyproject.toml"))
        ))