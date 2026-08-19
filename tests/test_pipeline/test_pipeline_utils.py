from dve.core_engine.backends.exceptions import MessageBearingError
from dve.core_engine.configuration.v1 import _ModelConfig, _ReaderConfig
from dve.pipeline.utils import load_reader

import pytest


class TestLoadReader:
    test_model_config = _ModelConfig(
        fields={"test": "str"},
        reporting_fields=["test"],
        key_field="test",
        reader_config={
            ".csv": _ReaderConfig(reader="TestCsvReader"),
        }
    )

    def test_invalid_load_reader_with_file_ext(self):
        with pytest.raises(MessageBearingError) as exc_info:
            load_reader(
                {"test": self.test_model_config},
                "test_model",
                "jpeg"
            )

        assert exc_info.value.messages[0].error_message == "The supplied file extension `jpeg` is not a supported file format for test_model."

    def test_invalid_load_reader_missing_file_ext(self):
        with pytest.raises(MessageBearingError) as exc_info:
            load_reader(
                {"test": self.test_model_config},
                "test_model",
                ""
            )

        assert exc_info.value.messages[0].error_message == "No supplied file extension. Unable to parse file without a file extension."
