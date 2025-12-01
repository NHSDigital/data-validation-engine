"""Implement XML linting for files. Please note that xml linting requires xmllint to be installed
onto your system."""

import shutil
import tempfile
from collections.abc import Sequence
from contextlib import ExitStack
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Union
from uuid import uuid4

from dve.core_engine.message import FeedbackMessage
from dve.parser.file_handling import (
    copy_resource,
    get_file_name,
    get_resource_exists,
    open_stream,
)
from dve.parser.file_handling.implementations.file import file_uri_to_local_path
from dve.parser.type_hints import URI

ErrorMessage = str
"""Error message for xml issues"""
ErrorCode = str
"""Error code for xml feedback errors"""

FIVE_MEBIBYTES = 5 * (1024**2)
"""The size of 5 binary megabytes, in bytes."""


def _ensure_schema_and_resources(
    schema_uri: URI, schema_resources: Sequence[URI], temp_dir: Path
) -> Path:
    """Given the schema and schema resource URIs and a temp dir, if the resources
    are remote or exist in different directories, copy them to the temp dir.

    Return the local schema path.

    """
    if not get_resource_exists(schema_uri):
        raise IOError(f"No resource accessible at schema URI {schema_uri!r}")

    missing_resources = list(
        filter(lambda resource: not get_resource_exists(resource), schema_resources)
    )
    if missing_resources:
        raise IOError(f"Some schema resources missing: {missing_resources!r}")

    all_resources = [schema_uri, *schema_resources]

    schemas_are_files = all(map(lambda resource: resource.startswith("file:"), all_resources))
    if schemas_are_files:
        paths = list(map(file_uri_to_local_path, all_resources))
        all_paths_have_same_parent = len({path.parent for path in paths}) == 1

        if all_paths_have_same_parent:
            schema_path = paths[0]
            return schema_path

    for resource_uri in all_resources:
        local_path = temp_dir.joinpath(get_file_name(resource_uri))
        copy_resource(resource_uri, local_path.as_uri())

    schema_path = temp_dir.joinpath(get_file_name(schema_uri))
    return schema_path


def run_xmllint(
    file_uri: URI,
    schema_uri: URI,
    *schema_resources: URI,
    error_code: ErrorCode,
    error_message: ErrorMessage,
) -> Union[None, FeedbackMessage]:
    """Run `xmllint`, given a file and information about the schemas to apply.

    The schema and associated resources will be copied to a temporary directory
    for validation, unless they are all already in the same local folder.

    Args:
     - `file_uri`: the URI of the file to be streamed into `xmllint`
     - `schema_uri`: the URI of the XSD schema for the file.
     - `*schema_resources`: URIs for additional XSD files required by the schema.
     - `error_code`: The error_code to use in FeedbackMessage if the linting fails.
     - `error_message`: The error_message to use in FeedbackMessage if the linting fails.

    Returns a deque of messages produced by the linting.

    """
    if not shutil.which("xmllint"):
        raise OSError("Unable to find `xmllint` binary. Please install to use this functionality.")

    if not get_resource_exists(file_uri):
        raise IOError(f"No resource accessible at file URI {file_uri!r}")

    # Ensure the schema and resources are local file paths so they can be
    # read by xmllint.
    # Lots of resources to manage here.
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        schema_path = _ensure_schema_and_resources(schema_uri, schema_resources, temp_dir)
        message_file_path = temp_dir.joinpath(uuid4().hex)

        with ExitStack() as linting_context:
            # Need to write lint output to a file to avoid deadlock. Kinder to mem this way anyway.
            message_file_bytes = linting_context.enter_context(message_file_path.open("wb"))

            # Open an `xmllint` process to pipe into.
            command = ["xmllint", "--stream", "--schema", str(schema_path), "-"]
            process = linting_context.enter_context(
                Popen(command, stdin=PIPE, stdout=message_file_bytes, stderr=STDOUT)
            )
            # This should never trigger, bad typing in stdlib.
            if process.stdin is None:
                raise ValueError("Unable to pipe file into subprocess")

            # Pipe the XML file contents into xmllint.
            block = b""
            try:
                with open_stream(file_uri, "rb") as byte_stream:
                    while True:
                        block = byte_stream.read(FIVE_MEBIBYTES)
                        if not block:
                            break
                        process.stdin.write(block)
            except BrokenPipeError:
                pass
            finally:
                # Close the input stream and await the response code.
                # Output will be written to the message file.
                process.stdin.close()
                return_code = process.wait(10)

        if return_code == 0:
            return None

        return FeedbackMessage(
            entity="xsd_validation",
            record={},
            failure_type="submission",
            is_informational=False,
            error_type="xsd check",
            error_location="Whole File",
            error_message=error_message,
            error_code=error_code,
        )
