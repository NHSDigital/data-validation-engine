"""Tests for the file parser's file handling routines."""

# pylint: disable=line-too-long,redefined-outer-name,unused-import
import logging
import platform
import re
import uuid
from pathlib import Path
from typing import Tuple

import boto3
import pytest
from typing_extensions import Literal

from dve.parser.exceptions import FileAccessError, LogDataLossWarning
from dve.parser.file_handling import (
    ResourceHandler,
    TemporaryPrefix,
    copy_prefix,
    copy_resource,
    get_content_length,
    get_resource_digest,
    get_resource_exists,
    iter_prefix,
    move_prefix,
    move_resource,
    open_stream,
    parse_uri,
    remove_prefix,
    remove_resource,
    resolve_location,
)
from dve.parser.file_handling.implementations import S3FilesystemImplementation
from dve.parser.file_handling.service import _get_implementation
from dve.parser.type_hints import Hostname, Scheme, URIPath

from ..fixtures import temp_dbfs_prefix, temp_prefix, temp_s3_prefix


@pytest.mark.parametrize(
    ["uri", "expected"],
    [
        ("file:///home/user/file.txt", ("file", None, "/home/user/file.txt")),
        (
            "s3://bucket/path/within/bucket/file.csv",
            ("s3", "bucket", "/path/within/bucket/file.csv"),
        ),
        (
            "/home/user/some%20path/some%20file.txt",
            ("file", None, "/home/user/some%20path/some%20file.txt"),
        ),
    ],
)
def test_parse_uri(uri: str, expected: Tuple[Scheme, Hostname, URIPath]):
    """Test the happy path for URI parsing."""
    assert parse_uri(uri) == expected


def test_s3_uri_raises_missing_bucket():
    """Test a sad path for URI parsing: missing bucket."""
    uri = "s3:///path/to/file.csv"
    impl = _get_implementation(uri)
    assert isinstance(impl, S3FilesystemImplementation)
    with pytest.raises(FileAccessError):
        impl._parse_s3_uri(uri)


@pytest.mark.parametrize(
    "prefix",
    [
        pytest.lazy_fixture("temp_prefix"),
        pytest.lazy_fixture("temp_s3_prefix"),
        pytest.lazy_fixture("temp_dbfs_prefix"),
    ],  # type: ignore
)
class TestParametrizedFileInteractions:
    """Tests which involve S3 and local filesystem."""

    def test_open_stream(self, prefix: str):
        """Test that `open_stream` works as expected."""
        uri = prefix + "/file.txt"
        uri_2 = prefix + "/file2.txt"

        with open_stream(uri, "w", "utf-8") as stream:
            stream.write("Hello, world!")

        with open_stream(uri, "r", "utf") as stream:
            assert stream.read() == "Hello, world!"

        # Ensure we can 'append' to empty files.
        with open_stream(uri_2, "a", "utf-8") as stream:
            stream.write("Hello, world!")

        with open_stream(uri_2, "r", "utf") as stream:
            assert stream.read() == "Hello, world!"

        with open_stream(uri, "ba") as byte_stream:
            byte_stream.write(b" And again!")

        with open_stream(uri, "a") as stream:
            stream.write(" And again!")

        with open_stream(uri, "r") as stream:
            assert stream.read() == "Hello, world! And again! And again!"

        with open_stream(uri, "bw") as byte_stream:
            byte_stream.write(b"overwritten")

        with open_stream(uri, "br") as stream:
            assert stream.read() == b"overwritten"

        with pytest.raises(FileAccessError):
            with open_stream(uri + ".another", "r") as stream:
                stream.read()

    def test_invalid_mode(self, prefix: str):
        """Test that invalid modes raise an error."""
        with pytest.raises(FileAccessError):
            with open_stream(prefix + "/file.txt", "g"):  # type: ignore
                pass

    def test_get_content_length(self, prefix: str):
        """Test that content length can be fetched and is correct."""
        uri = prefix + "/file.txt"
        with open_stream(uri, "bw") as byte_stream:
            byte_stream.write(b"This byte string contains 39 characters")

        assert get_content_length(uri) == 39

        with open_stream(uri, "bw") as byte_stream:
            byte_stream.write(b"")

        assert get_content_length(uri) == 0

    def test_get_content_length_empty_file(self, prefix: str):
        """Test that getting the content length of a missing file raises an error."""
        with pytest.raises(FileAccessError):
            get_content_length(prefix + "/file.txt")

    def test_resource_exists(self, prefix: str):
        """
        Test that it's possible to check if files exist (this should be false
        for directories).

        """
        uri = prefix + "/file.txt"
        assert not get_resource_exists(uri)

        with open_stream(uri, "w") as file:
            file.write("I exist now")

        assert get_resource_exists(uri)
        assert not get_resource_exists(prefix)

    def test_remove_resource(self, prefix: str):
        """Test that it's possible to remove a resource."""
        uri = prefix + "/file.txt"
        assert not get_resource_exists(uri)

        with open_stream(uri, "w") as file:
            file.write("I exist now")

        assert get_resource_exists(uri)
        remove_resource(uri)
        assert not get_resource_exists(uri)
        # Call a second time to ensure removing already-deleted resources
        # works okay.
        remove_resource(uri)

    def test_iter_prefix(self, prefix: str):
        """
        Create a directory structure within the prefix and ensure it can be
        listed.

        """
        # ├── test_file.txt
        # ├── test_sibling.txt
        # └── test_prefix  # Shouldn't see stuff in this folder.
        #     ├── test_file.txt
        #     └── test_sibling.txt
        structure = [
            "/test_file.txt",
            "/test_sibling.txt",
            "/test_prefix/test_file.txt",
            "/test_prefix/test_sibling.txt",
        ]

        resource_uris = [prefix + uri_path for uri_path in structure]
        for uri in resource_uris:
            with open_stream(uri, "w") as file:
                file.write("")

        actual_nodes = sorted(iter_prefix(prefix, recursive=False))
        expected_nodes = sorted(
            [
                (prefix + "/test_file.txt", "resource"),
                (prefix + "/test_sibling.txt", "resource"),
                (prefix + "/test_prefix/", "directory"),
            ]
        )
        assert actual_nodes == expected_nodes

    def test_iter_prefix_recursive(self, prefix: str):
        """
        Create a directory structure within the prefix and ensure it can be
        listed (recursively).

        """
        # ├── test_file.txt
        # └── test_prefix
        #     ├── another_level
        #     │   ├── nested_sibling.txt
        #     │   └── test_file.txt
        #     ├── test_file.txt
        #     └── test_sibling.txt
        structure = [
            "/test_file.txt",
            "/test_prefix/test_file.txt",
            "/test_prefix/test_sibling.txt",
            "/test_prefix/another_level/test_file.txt",
            "/test_prefix/another_level/nested_sibling.txt",
        ]
        resource_uris = [prefix + uri_path for uri_path in structure]
        directory_uris = [prefix + "/test_prefix/", prefix + "/test_prefix/another_level/"]

        for uri in resource_uris:
            with open_stream(uri, "w") as file:
                file.write("")

        actual_nodes = sorted(iter_prefix(prefix, recursive=True))
        expected_nodes = sorted(
            [(uri, "directory") for uri in directory_uris]
            + [(uri, "resource") for uri in resource_uris]
        )
        assert actual_nodes == expected_nodes

    def test_remove_prefix(self, prefix: str):
        """Test that it's possible to remove a prefix (non-recursively)."""
        # ├── test_file.txt
        # └── test_sibling.txt
        structure = [
            "/test_file.txt",
            "/test_sibling.txt",
        ]

        resource_uris = [prefix + uri_path for uri_path in structure]
        for uri in resource_uris:
            with open_stream(uri, "w") as file:
                file.write("")

        remove_prefix(prefix, recursive=False)
        assert not list(iter_prefix(prefix))

    def test_remove_prefix_raises_nested(self, prefix: str):
        """
        Test that attempting to remove a prefix raises an error if
        `recursive` is not specified and the directory is nested.

        """
        # ├── test_file.txt
        # ├── test_sibling.txt
        # └── test_prefix
        #     ├── test_file.txt
        #     └── test_sibling.txt
        structure = [
            "/test_file.txt",
            "/test_sibling.txt",
            "/test_prefix/test_file.txt",
            "/test_prefix/test_sibling.txt",
        ]
        resource_uris = [prefix + uri_path for uri_path in structure]
        for uri in resource_uris:
            with open_stream(uri, "w") as file:
                file.write("")

        with pytest.raises(FileAccessError):
            remove_prefix(prefix, recursive=False)

    def test_remove_prefix_recursive(self, prefix: str):
        """Test that it is possible to remove a nested prefix recursively."""
        # ├── test_file.txt
        # ├── test_sibling.txt
        # └── test_prefix
        #     ├── test_file.txt
        #     └── test_sibling.txt
        structure = [
            "/test_file.txt",
            "/test_sibling.txt",
            "/test_prefix/test_file.txt",
            "/test_prefix/test_sibling.txt",
        ]
        resource_uris = [prefix + uri_path for uri_path in structure]
        for uri in resource_uris:
            with open_stream(uri, "w") as file:
                file.write("")

        remove_prefix(prefix, recursive=True)
        assert not list(iter_prefix(prefix, recursive=True))

    def test_TemporaryPrefix(self, prefix: str):  # pylint: disable=invalid-name
        """Ensure that TemporaryPrefix can clear files under its control."""
        temp_prefix_obj = TemporaryPrefix(prefix)
        with pytest.raises(ValueError):
            # Shouldn't be able to access this when not in context
            temp_prefix_obj.prefix  # pylint: disable=pointless-statement

        with temp_prefix_obj as temp_prefix_uri:
            structure = [
                "test_file.txt",
                "test_prefix/test_file.txt",
                "test_prefix/test_sibling.txt",
                "test_prefix/another_level/test_file.txt",
                "test_prefix/another_level/nested_sibling.txt",
            ]
            resource_uris = [temp_prefix_uri + uri_path for uri_path in structure]
            for uri in resource_uris:
                with open_stream(uri, "w") as file:
                    file.write("")

            assert len(list(iter_prefix(temp_prefix_uri, recursive=True))) == 7
        assert len(list(iter_prefix(temp_prefix_uri, recursive=True))) == 0

    def test_ResourceHandler(self, prefix: str):  # pylint: disable=invalid-name
        """Ensure that we can log to a resource."""
        log_resource = prefix + "/log.txt"
        with open_stream(log_resource, "w") as file:
            file.write("This should be overwritten\n")

        handler = ResourceHandler(logging.WARNING, resource=log_resource)

        # Creating a 'new' handler with a lower log level should lower it.
        # This will actually be the same handler.
        with pytest.warns(LogDataLossWarning):
            handler2 = ResourceHandler(logging.INFO, resource=log_resource)
        assert handler2 is handler
        assert handler.level == logging.INFO

        # Creating a 'new' handler with a higher log level should do nothing.
        with pytest.warns(LogDataLossWarning):
            handler3 = ResourceHandler(logging.WARNING, resource=log_resource)
        assert handler3 is handler
        assert handler.level == logging.INFO

        logger = logging.getLogger(uuid.uuid4().hex)
        logger.propagate = False
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        messages = [
            "Some debug message",
            "Some informational message",
            "A scary warning!",
            "A scarier error!!",
        ]
        for log_level, message in zip(log_levels, messages):
            logger.log(logging.getLevelName(log_level), message)

        handler.close()
        with open_stream(log_resource) as file:
            actual_messages = list(filter(bool, file.read().split("\n")))
            # Shouldn't see debug message due to log level.
            expected_messages = messages[1:]
            assert actual_messages == expected_messages

    def test_get_resource_digest(self, prefix: str):
        """Ensure that we can get a digest for a remote file."""
        temp_uri = prefix + "/file.txt.txt"
        with open_stream(temp_uri, "w") as file:
            file.write("Hello, world!\n")

        assert get_resource_digest(temp_uri, "md5") == "746308829575e17c3331bbcb00c0898b"
        assert get_resource_digest(temp_uri, "sha1") == "09fac8dbfd27bd9b4d23a00eb648aa751789536d"


def test_cursed_s3_keys_supported(temp_s3_prefix: str):
    """Ensure that S3 prefixes with a `/` prefix for the key are supported."""
    s3_uri = temp_s3_prefix + "/file.txt"
    parsed_uri = re.match("^s3://(?P<bucket>.+?)/(?P<key>.+)$", s3_uri)
    assert parsed_uri, "Temp S3 prefix wrong"
    # S3 is a key-value database.
    # S3 URIs with a slash at the start of the key _are_ strictly valid.
    cursed_s3_uri = f"s3://{parsed_uri.group('bucket')}//{parsed_uri.group('key')}"

    with open_stream(s3_uri, "w") as stream:
        stream.write("Hello, world!")

    assert get_resource_exists(s3_uri)
    assert not get_resource_exists(cursed_s3_uri)

    remove_resource(s3_uri)
    assert not get_resource_exists(s3_uri)

    with open_stream(cursed_s3_uri, "w") as stream:
        stream.write("Hello, world!")

    assert get_resource_exists(cursed_s3_uri)
    assert not get_resource_exists(s3_uri)

    remove_resource(cursed_s3_uri)
    assert not get_resource_exists(cursed_s3_uri)


@pytest.mark.skipif(platform.system() == "Windows", reason="Resolver tests are platform specific.")
@pytest.mark.parametrize(
    ["uri", "expected"],
    # fmt: off
    [
        (
            Path("abc/samples/planet_test_records.xml"),
            Path("abc/samples/planet_test_records.xml").resolve().as_uri(),
        ),
        ("file:///home/user/file.txt", Path("/home/user/file.txt").as_uri()),
        ("s3://bucket/path/within/bucket/file.csv", "s3://bucket/path/within/bucket/file.csv"),
        (
            "file:///abc/samples/planet_test_records.xml",
            "file:///abc/samples/planet_test_records.xml",
        ),
        (
            "/abc/samples/planet_test_records.xml",
            Path("/abc/samples/planet_test_records.xml").as_uri(),
        ),
        (
            Path("/abc/samples/planet_test_records.xml"),
            Path("/abc/samples/planet_test_records.xml").as_uri(),
        ),
    ],
    # fmt: on
)
def test_filename_resolver_linux(uri, expected):
    """Ensure that the filename resolver works as expected on Linux."""
    assert resolve_location(uri) == expected


@pytest.mark.parametrize("action", ["copy", "move"])
@pytest.mark.parametrize(
    ["source_prefix", "target_prefix"],
    [
        (pytest.lazy_fixture("temp_prefix"), pytest.lazy_fixture("temp_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_s3_prefix"), pytest.lazy_fixture("temp_s3_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_prefix"), pytest.lazy_fixture("temp_s3_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_s3_prefix"), pytest.lazy_fixture("temp_prefix")),  # type: ignore
    ],
)
def test_copy_move_resource(
    source_prefix: str, target_prefix: str, action: Literal["copy", "move"]
):
    """Test that resources can be copied and moved."""
    if action == "copy":
        func = copy_resource
    else:
        func = move_resource

    source_uri = source_prefix + "/file.txt"
    target_uri = target_prefix + "/some/nested/path/to/file2.txt"

    with open_stream(source_uri, "w", "utf-8") as stream:
        stream.write("Hello, world!")
    func(source_uri, target_uri, overwrite=False)

    # Ensure target can be overwritten
    with open_stream(source_uri, "w", "utf-8") as stream:
        stream.write("Hello, world!")
    with pytest.raises(FileAccessError):
        func(source_uri, target_uri, overwrite=False)
    func(source_uri, target_uri, overwrite=True)

    with open_stream(target_uri, "r", "utf-8") as stream:
        assert stream.read() == "Hello, world!"

    if action == "move":
        assert not get_resource_exists(source_uri)


@pytest.mark.parametrize("action", ["copy", "move"])
@pytest.mark.parametrize(
    ["source_prefix", "target_prefix"],
    [
        (pytest.lazy_fixture("temp_prefix"), pytest.lazy_fixture("temp_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_s3_prefix"), pytest.lazy_fixture("temp_s3_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_prefix"), pytest.lazy_fixture("temp_s3_prefix")),  # type: ignore
        (pytest.lazy_fixture("temp_s3_prefix"), pytest.lazy_fixture("temp_prefix")),  # type: ignore
    ],
)
def test_copy_move_prefix(source_prefix: str, target_prefix: str, action: Literal["copy", "move"]):
    """Test that resources can be copied and moved."""
    source_prefix += "/source"
    target_prefix += "/target"

    if action == "copy":
        func = copy_prefix
    else:
        func = move_prefix

    paths_within_prefix = ("/file.txt", "/path/to/nested/file.txt", "/sibling.txt")

    for file_path in paths_within_prefix:
        with open_stream(source_prefix + file_path, "w", "utf-8") as stream:
            stream.write("Hello, world!")

    func(f"{source_prefix}/", f"{target_prefix}/", overwrite=False)

    # Ensure target can be overwritten
    for file_path in paths_within_prefix:
        with open_stream(source_prefix + file_path, "w", "utf-8") as stream:
            stream.write("Hello, world!")

    with pytest.raises(FileAccessError):
        func(source_prefix, target_prefix, overwrite=False)
    func(f"{source_prefix}/", f"{target_prefix}/", overwrite=True)

    for file_path in paths_within_prefix:
        with open_stream(target_prefix + file_path, "r", "utf-8") as stream:
            assert stream.read() == "Hello, world!"

    if action == "move":
        assert not any(iter_prefix(source_prefix))


def test_iter_prefix_ignores_dir_listing_files(temp_s3_prefix: str):
    """
    Ensure that zero-sized files are excluded by iter_prefix if they can't be
    reached by `head_object`.

    """

    def create_directory_marker(target_prefix: str) -> None:
        """Create an empty S3 object indicating a directory marker."""
        _, bucket, prefix = S3FilesystemImplementation()._parse_s3_uri(target_prefix)
        if not prefix.endswith("/"):
            prefix += "/"
        boto3.client("s3").put_object(Bucket=bucket, Key=prefix)

    # folder/ (dir)
    # ├── another/ (dir)
    # │   └── file.txt (resource)
    # └── sibling/ (dir)
    #     └── nested/ (dir)

    folder = temp_s3_prefix + "/folder/"
    create_directory_marker(folder)
    nested = folder + "another/"
    create_directory_marker(nested)
    actual_file = nested + "file.txt"
    with open_stream(actual_file, "w", encoding="utf-8") as file:
        file.write("Hello, world!")

    sibling = folder + "sibling/"
    create_directory_marker(sibling)
    sibling_nested = sibling + "nested/"
    create_directory_marker(sibling_nested)

    assert not get_resource_exists(folder)
    assert not get_resource_exists(nested)
    assert get_resource_exists(actual_file)
    assert not get_resource_exists(sibling)
    assert not get_resource_exists(sibling_nested)

    actual = sorted(iter_prefix(temp_s3_prefix, True))
    expected = sorted(
        [
            (folder, "directory"),
            (nested, "directory"),
            (actual_file, "resource"),
            (sibling, "directory"),
            (sibling_nested, "directory"),
        ]
    )
    assert actual == expected


@pytest.mark.parametrize("file_size", [1024, 1 * 1024**2, 5 * 1024**2, 20 * 1024**2])
def test_s3_multipart_copy(temp_s3_prefix: str, file_size: int):
    """Test that resources can be copied using a multipart copy in S3."""
    source_uri = temp_s3_prefix + "/file.txt"
    target_uri = temp_s3_prefix + "/some/nested/path/to/file2.txt"

    n_megs, remainder = divmod(file_size, 1024**2)
    with open_stream(source_uri, "wb") as stream:
        for _ in range(n_megs):
            stream.write(b"\0" * (1024**2))
        if remainder:
            stream.write(b"\0" * remainder)

    S3FilesystemImplementation().copy_resource(
        source_uri, target_uri, overwrite=False, _force_multipart=True
    )

    with open_stream(target_uri, "rb") as stream:
        for _ in range(n_megs):
            assert stream.read(1024**2) == (b"\0" * (1024**2))
        if remainder:
            assert stream.read() == (b"\0" * remainder)
