"""Implement XML linting for files."""

import argparse
import re
import shutil
import sys
import tempfile
from collections.abc import Iterable, Sequence
from contextlib import ExitStack
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Optional
from uuid import uuid4

from typing_extensions import Literal

from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import Messages
from dve.parser.file_handling import copy_resource, get_file_name, get_resource_exists, open_stream
from dve.parser.file_handling.implementations.file import file_uri_to_local_path
from dve.parser.type_hints import URI

Replacement = str
"""A replacement for a regex pattern."""
Stage = Literal["Pre-validation", "Post-validation"]
"""The stage at which the XML linting is being performed."""
ErrorMessage = str
"""Error message for xml issues"""
ErrorCode = str
"""Error code for xml feedback errors"""

FIVE_MEBIBYTES = 5 * (1024**3)
"""The size of 5 binary megabytes, in bytes."""

# Patterns/strings for xmllint message sanitisation.
IGNORED_PATTERNS: list[re.Pattern] = [re.compile(r"^Unimplemented block at")]
"""Regex patterns for messages that should result in their omission."""
ERRONEOUS_PATTERNS: list[tuple[re.Pattern, Replacement]] = [
    (
        re.compile(r"^XSD schema .+/(?P<file_name>.+) failed to compile$"),
        r"Missing required component of XSD schema '\g<file_name>'",
    )
]
"""
Patterns for messages that should trigger errors. Replacement messages will be
raised as `ValueError`s.

"""
INCORRECT_NAMESPACE_PATTERN = re.compile(
    r"Element .*?{(?P<Namespace>.*?)}.*? "
    r"No matching global declaration available for the validation root."
)
"""# 1 capture
Pattern to match incorrect namespace found in the xmllint errors. Captures incorrect namespace
"""
# match literal Element followed by anything up to opening curly brace. capture the contents
# of the braces if the literal "No matching global declaration available..." is present in the
# message. This is raised when the namespace doesn't match between the schema and the file

MISSING_OR_OUT_OF_ORDER_PATTERN = re.compile(
    r"Element .*?{.*?}(?P<Found>.*?): This element is not expected\. "
    r"Expected is .*?}(?P<Expected>.*?)\)"
)
"""# 2 captures
Pattern to match out of order elements found in the xmllint errors. Captures incorrect found
and expected fields (in that order)
"""
# match literal Element followed by anything up to opening curly brace. capture the field that
# comes after the braces if the literal "this element is not expected" is present in the
# message. capture the expected value too
# This is raised when the xml fields are out of order or a field is missing.

MISSING_CHILD_ELEMENTS_PATTERN = re.compile(
    r"Element .*?{.*?}.*?: Missing child element\(s\). Expected is.*?}(?P<Expected>.*?)\)"
)
"""# 1 capture
Pattern to match when a tag has missing child elements. Captures the expected element
"""
FAILS_TO_VALIDATE_PATTERN = re.compile(r"fails to validate")
"""# 0 captures
Pattern to match the fails to validate xmllint message, doesn't capture anything"""

UNEXPECTED_FIELD_PATTERN = re.compile(
    r"Element .*?{.*?}(?P<Unexpected_Field>.*?): This element is not expected\.$"
)
"""# 1 capture
Pattern to match unexpected fields rather than out of order fields. Captures incorrect field
"""

REMOVED_PATTERNS: list[re.Pattern] = [
    re.compile(r"\{.+?\}"),
    re.compile(r"[\. ]+$"),
]
"""Regex patterns to remove from the xmllint output."""
REPLACED_PATTERNS: list[tuple[re.Pattern, Replacement]] = [
    (re.compile(r":(?P<line_number>\d+):"), r" on line \g<line_number>:"),
]
"""Regex patterns to replace in the xmllint output."""
REPLACED_STRINGS: list[tuple[str, Replacement]] = [
    (
        "No matching global declaration available for the validation root",
        "Incorrect namespace version, please ensure you have the most recent namespace",
    ),
    (
        "fails to validate",
        "Whole file has failed schema validation - please ensure your data matches the expected "
        + "structure",
    ),
]
"""Strings to replace in the xmllint output."""


def _sanitise_lint_issue(issue: str, file_name: str) -> Optional[str]:
    """Sanitise an xmllint lint message. If the message should be ignored,
    this function will return `None` instead of a string.

    If messages are considered erroneous, a `ValueError` will be raised.

    """
    for pattern in IGNORED_PATTERNS:
        if pattern.match(issue):
            return None
    for pattern, error_message in ERRONEOUS_PATTERNS:
        if pattern.match(issue):
            raise ValueError(pattern.sub(error_message, issue))

    # Line will start with "-" because that's the file name for streamed XML files.
    if issue.startswith("-"):
        issue = "".join((file_name, issue[1:]))

    for pattern in REMOVED_PATTERNS:
        issue = pattern.sub("", issue)
    for pattern, replacement in REPLACED_PATTERNS:
        issue = pattern.sub(replacement, issue)
    for string, replacement in REPLACED_STRINGS:
        issue = issue.replace(string, replacement)

    return issue


def _parse_lint_messages(
    lint_messages: Iterable[str],
    error_mapping: dict[re.Pattern, tuple[ErrorMessage, ErrorCode]],
    stage: Stage = "Pre-validation",
    file_name: Optional[str] = None,
) -> Messages:
    """Parse a sequence of messages from `xmllint` into a deque of feedback messages."""
    messages: Messages = []
    for issue in lint_messages:
        for regex, (error_message, error_code) in error_mapping.items():
            match_ = regex.search(issue)
            if match_:
                groups = {key: group.strip(",' ") for key, group in match_.groupdict().items()}
                if not groups:
                    reporting_field = "Whole file"
                    groups[reporting_field] = file_name
                elif len(groups) > 1:
                    reporting_field = list(groups)  # type: ignore
                else:
                    reporting_field = list(groups)[0]

                messages.append(
                    FeedbackMessage(
                        entity=stage,
                        record={"Wholefile": groups},
                        failure_type="submission",
                        is_informational=False,
                        error_type=None,
                        error_location="Whole file",
                        error_code=error_code,
                        error_message=error_message.format(*groups.values()),
                        reporting_field=reporting_field,
                        category="Bad file",
                    )
                )
    return list(dict.fromkeys(messages))  # remove duplicate errors but preserve order


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
    error_mapping: dict[re.Pattern, tuple[ErrorMessage, ErrorCode]],
    stage: Stage = "Pre-validation",
) -> Messages:
    """Run `xmllint`, given a file and information about the schemas to apply.

    The schema and associated resources will be copied to a temporary directory
    for validation, unless they are all already in the same local folder.

    Args:
     - `file_uri`: the URI of the file to be streamed into `xmllint`
     - `schema_uri`: the URI of the XSD schema for the file.
     - `*schema_resources`: URIs for additional XSD files required by the schema.
     - `stage` (keyword only): One of `{'Pre-validation', 'Post-validation'}`

    Returns a deque of messages produced by the linting.

    """
    if not shutil.which("xmllint"):
        raise OSError("Unable to find `xmllint` binary")

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
                file_name = get_file_name(file_uri)
                return [
                    FeedbackMessage(
                        entity=stage,
                        record={"Whole file": file_name},
                        failure_type="submission",
                        is_informational=False,
                        error_type=None,
                        error_location="Whole file",
                        error_message="failed schema check",
                        category="Bad file",
                    ),
                    FeedbackMessage(
                        entity=stage,
                        record={"Whole file": block[:50].decode(errors="replace")},
                        failure_type="submission",
                        is_informational=False,
                        error_type=None,
                        error_location="Whole file",
                        error_message="Failed xml validation",
                        reporting_field="Whole file",
                        category="Bad file",
                    ),
                ]

            # Close the input stream and await the response code.
            # Output will be written to the message file.
            process.stdin.close()
            # TODO: Identify an appropriate timeout.
            return_code = process.wait()

        if return_code == 0:
            return []
        with message_file_path.open("r", encoding="utf-8") as message_file:
            lint_messages = (line for line in map(str.rstrip, message_file) if line)
            file_name = get_file_name(file_uri)
            messages = _parse_lint_messages(lint_messages, error_mapping, stage, file_name)

            # Nonzero exit code without messages _shouldn't_ happen, but it's possible
            # if `xmllint` is killed (and presumably possible if it runs out of mem)
            # so we should handle that possibility.
            if not messages:
                messages.append(
                    FeedbackMessage(
                        entity=stage,
                        record={"Whole file": file_name},
                        failure_type="submission",
                        is_informational=False,
                        error_type=None,
                        error_location="Whole file",
                        error_message="failed schema check",
                        category="Bad file",
                    )
                )

            return messages


def _main(cli_args: list[str]):
    """Command line interface for XML linting. Useful for testing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("xml_file_path", help="The path to the XML file to be validated")
    parser.add_argument(
        "xsd_file_paths", help="The path to the XSD schemas (primary schema first)", nargs="+"
    )
    args = parser.parse_args(cli_args)

    xml_path = Path(args.xml_file_path).resolve()
    xsd_uris = list(map(lambda path_str: Path(path_str).resolve().as_uri(), args.xsd_file_paths))
    try:
        messages = run_xmllint(xml_path.as_uri(), *xsd_uris, error_mapping={})
    except Exception as err:  # pylint: disable=broad-except
        print(f"Exception ({type(err).__name__}) raised in XML linting.", file=sys.stderr)
        print(err, file=sys.stderr)
        sys.exit(2)

    if not messages:
        print(f"File {xml_path.name!r} validated successfully\n", file=sys.stderr)
        sys.exit(0)
    else:
        print(f"File {xml_path.name!r} validation failed\n", file=sys.stderr)

        print("|".join(FeedbackMessage.HEADER))
        for message in messages:
            print("|".join(map(lambda col: str(col) if col else "", message.to_row())))
        sys.exit(1)


if __name__ == "__main__":
    _main(sys.argv[1:])
