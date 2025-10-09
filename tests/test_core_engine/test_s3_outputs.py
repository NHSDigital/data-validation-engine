import warnings

import pytest

from dve.core_engine.engine import CoreEngine
from dve.parser.file_handling.service import get_content_length, get_resource_exists

from ..conftest import get_test_file_path
from ..fixtures import temp_s3_prefix

# pylint: disable=protected-access,unused-import,redefined-outer-name

# TODO: Update these tests


@pytest.mark.skip(reason="Outdated expectations about S3 output")
class TestCoreEngine:
    def test_dummy_planet_s3_err_output(self, temp_s3_prefix: str):
        """Test that we can output the error report to mock s3.
        Assumption spark will cope with s3 writes - testing would likely require localstack integration
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_instance = CoreEngine.build(
                dataset_config_path=get_test_file_path("planets/planets.dischema.json"),
                output_prefix=temp_s3_prefix,
            )

            entity_locations = {
                "planets": get_test_file_path("planets/planets_demo.csv").as_uri(),
                "satellites": get_test_file_path("planets/satellites_demo.csv").as_uri(),
            }
            with test_instance:
                entities, messages = test_instance._data_contract_validate(entity_locations)
                entities, messages = test_instance._apply_rules(entities, messages)
                test_instance._write_exception_report(messages)

        error_uri = temp_s3_prefix + "/pipeline.errors"
        assert get_resource_exists(error_uri)
        assert get_content_length(error_uri) > 0
