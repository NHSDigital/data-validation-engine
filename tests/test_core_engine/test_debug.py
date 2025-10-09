import re
from pathlib import Path

import pytest

from dve.core_engine.engine import CoreEngine

from ..conftest import get_test_file_path
from ..fixtures import temp_dir


# TODO: Update these tests.
@pytest.mark.skip(reason="Outdated expectations about debug logging")
def test_debug_disabled(temp_dir: str, caplog):
    with caplog.at_level("INFO"):
        test_instance = CoreEngine.build(
            dataset_config_path=get_test_file_path("planets/planets.dischema.json"),
            output_prefix=Path(temp_dir),
        )

        with test_instance:
            test_instance.run_pipeline(
                entity_locations={
                    "planets": get_test_file_path("planets/planets_demo.csv").as_uri(),
                    "satellites": get_test_file_path("planets/satellites_demo.csv").as_uri(),
                },
                method="dataframe",
            )

        assert "Debug mode: False" in caplog.messages
        assert not re.search(pattern=r"Converted to Dataframe: \(\d+ rows\)", string=caplog.text)


@pytest.mark.skip(reason="Outdated expectations about debug logging")
def test_debug_enabled(temp_dir: str, caplog):
    with caplog.at_level("INFO"):
        test_instance = CoreEngine.build(
            dataset_config_path=get_test_file_path("planets/planets.dischema.json"),
            output_prefix=Path(temp_dir),
            debug=True,
        )

        with test_instance:
            test_instance.run_pipeline(
                entity_locations={
                    "planets": get_test_file_path("planets/planets_demo.csv").as_uri(),
                    "satellites": get_test_file_path("planets/satellites_demo.csv").as_uri(),
                },
                method="dataframe",
            )

        print(caplog.messages)
        assert "Debug mode: True" in caplog.messages
        assert re.search(pattern=r"Converted to Dataframe: \(\d+ rows\)", string=caplog.text)
