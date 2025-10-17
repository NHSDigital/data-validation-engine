# mypy: disable-error-code="attr-defined"
"""XML parsers for the Data Validation Engine."""

import re
from typing import IO, Any, Collection, Dict, Iterator, List, Optional, Type, Union, overload

import polars as pl
from lxml import etree  # type: ignore
from pydantic import BaseModel, create_model
from typing_extensions import Annotated, Protocol, get_args, get_origin

from dve.core_engine.backends.base.reader import BaseFileReader
from dve.core_engine.backends.exceptions import EmptyFileError
from dve.core_engine.backends.utilities import stringify_model, get_polars_type_from_annotation
from dve.core_engine.loggers import get_logger
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import NonClosingTextIOWrapper, get_content_length, open_stream
from dve.parser.file_handling.implementations.file import (
    LocalFilesystemImplementation,
    file_uri_to_local_path,
)
from dve.parser.file_handling.service import _get_implementation

XMLType = Union[Optional[str], List["XMLType"], Dict[str, "XMLType"]]  # type: ignore
"""The definition of a type within XML."""
XMLRecord = Dict[str, XMLType]  # type: ignore
"""A record within XML."""
TemplateElement = Union[None, List["TemplateElement"], Dict[str, "TemplateElement"]]  # type: ignore
"""The base types used in the template row."""
TemplateRow = Dict[str, "TemplateElement"]  # type: ignore
"""The type of a template row."""


def _strip_annotated(annotation: Any) -> Any:
    """Strip the 'Annotated' wrapper from a type."""
    origin = get_origin(annotation)
    if origin is None or origin is not Annotated:
        return annotation
    return get_args(annotation)[0]


def create_template_row(schema: Type[BaseModel]) -> Dict[str, Any]:
    """Create a template row from a schema. A template row is essentially the
    shape of the record that would be populated by the reader (i.e. contains
    default values), except lists are pre-populated with a single 'empty'
    record as a hint to the reader about the data structure.

    """
    template_row: Dict[str, Any] = {}
    for field_name, model_field_def in schema.__fields__.items():
        field_type = _strip_annotated(model_field_def.annotation)

        if not model_field_def.is_complex():
            template_row[field_name] = None
            continue

        if isinstance(field_type, type):
            if issubclass(field_type, BaseModel):
                template_row[field_name] = create_template_row(field_type)
                continue
            raise TypeError(f"Cannot read arbitrary complex type from XML, got {field_type!r}")

        origin = get_origin(field_type)
        if origin is list:
            list_type = _strip_annotated(get_args(field_type)[0])

            # This is a quick and dirty hack to avoid implementing our own logic
            # to check complex types...
            list_type_field_spec = create_model("", lt=(list_type, ...)).__fields__["lt"]
            if not list_type_field_spec.is_complex():
                template_row[field_name] = [None]
                continue

            if isinstance(list_type, type) and issubclass(list_type, BaseModel):
                template_row[field_name] = [create_template_row(list_type)]
                continue

        raise TypeError(f"Cannot read arbitrary complex type from XML, got {field_type!r}")
    return template_row


class XMLElement(Protocol):
    """A description of an element in an XML document.

    This is used because we could in theory use `lxml` or the standard library
    `ElementTree` interchangeably and while these have the same strucure the
    type hints for `lxml` are rubbish.

    """

    tag: Optional[str]
    """The XML element's tag."""

    text: Optional[str]
    """The text inside the XML element's tags."""

    def clear(self) -> None:
        """Clear the element, removing children/attrs/etc."""

    def __iter__(self) -> Iterator["XMLElement"]:
        ...


class BasicXMLFileReader(BaseFileReader):
    """A reader for XML files built atop LXML."""

    def __init__(
        self,
        *,
        record_tag: str,
        root_tag: Optional[str] = None,
        trim_cells: bool = True,
        null_values: Collection[str] = frozenset({"NULL", "null", ""}),
        sanitise_multiline: bool = True,
        encoding: str = "utf-8-sig",
        n_records_to_read: Optional[int] = None,
        **_,
    ):
        """Init function for the base XML reader.

        Args:
         - `record_tag`: a required string indicating the tag of each 'record'
           in the XML document.
         - `root_tag`: a string indicating the tag to find the records in within
           the XML document. If `None`, assume that the records are under the root
           node.
         - `trim_cells`: a boolean value indicating whether to strip whitespace
           from elements in the XML document. Default: `True`
         - `null_values`: a container of values to replace with null if encountered
           in an element. Default: `{'', 'null', 'NULL'}`
         - `sanitise_multiline`: whether to sanitise (remove newlines and multiple
           spaces) from multiline fields.
         - `encoding`: encoding of the XML file. Default: `utf-8-sig`
         - `n_records_to_read`: the maximum number of records to read from a document.

        """
        self.record_tag = record_tag
        """The name of a 'record' tag in the XML document."""
        self.root_tag = root_tag
        """The name of the 'root' tag in the XML document."""
        self.trim_cells = trim_cells
        """A boolean value indicating whether to strip whitespace from fields."""
        self.null_values = null_values
        """A container of values to replace with null if encountered in an element."""
        self.sanitise_multiline = sanitise_multiline
        """A boolean value indicating whether to sanitise multiline fields."""
        self.encoding = encoding
        """Encoding of the XML file."""
        self.n_records_to_read = n_records_to_read
        """The maximum number of records to read from a document."""
        super().__init__()
        self._logger = get_logger(__name__)

    def _strip_namespace(self, element: XMLElement) -> None:
        """Mutate an element and strip the namespace."""
        if element.tag is not None:
            element.tag = re.sub(r"^(\{.+\}|.+:)", "", element.tag)

    def _strip_namespaces_recursively(self, element: XMLElement) -> None:
        """Mutate an element and its children and strip their namespaces."""
        self._strip_namespace(element)

        for child in element:
            self._strip_namespaces_recursively(child)

    def _sanitise_field(self, value: Optional[str]) -> Optional[str]:
        """Sanitise a field value from an XML document."""
        if value is not None:
            if self.trim_cells:
                value = value.strip()

            if self.sanitise_multiline:
                value = re.sub("\\s*\n\\s*", " ", value, flags=re.MULTILINE)

            if value in self.null_values:
                value = None
        return value

    @overload
    def _parse_element(self, element: XMLElement, template: TemplateRow) -> XMLRecord:
        ...

    @overload
    def _parse_element(self, element: XMLElement, template: TemplateElement) -> XMLType:
        ...

    def _parse_element(self, element: XMLElement, template: Union[TemplateElement, TemplateRow]):
        """Parse an XML element according to a template."""
        if template is None:
            return self._sanitise_field(element.text)

        if isinstance(template, list):
            return [self._parse_element(element, template[0])]

        record: XMLRecord = {}
        for child in element:
            tag = child.tag
            if tag is None or tag not in template:
                continue

            template_element = template[tag]
            if isinstance(template_element, list):
                if tag in record:
                    current_value = record[tag]
                    if isinstance(current_value, list):
                        source_list = current_value
                    else:
                        source_list = [current_value]
                        record[tag] = source_list
                else:
                    source_list = []
                    record[tag] = source_list
                source_list.append(self._parse_element(child, template_element[0]))
            else:
                record[tag] = self._parse_element(child, template_element)

        for missing_key in template.keys() - record.keys():
            record[missing_key] = None
        return record

    def _get_elements_from_stream(self, stream: IO[bytes]) -> Iterator[XMLElement]:
        """Get an iterator of records from the tree as XML elements."""
        encoding = self.encoding if self.encoding != "utf-8-sig" else "utf-8"
        parser = etree.XMLParser(
            encoding=encoding,
            remove_pis=True,
            remove_comments=True,
            dtd_validation=False,
            resolve_entities=False,
        )

        tree: etree._ElementTree = etree.parse(stream, parser)
        root: etree._Element = tree.getroot()

        elements: List[XMLElement]
        if self.root_tag:
            elements = root.xpath(
                f"//*[local-name()='{self.root_tag}']/*[local-name()='{self.record_tag}']"
            )
        else:
            elements = root.xpath(f"//*[local-name()='{self.record_tag}']")

        element_count = 0
        for element in elements:
            self._strip_namespaces_recursively(element)
            yield element
            element_count += 1
            if self.n_records_to_read and element_count == self.n_records_to_read:
                break

    def _parse_xml(
        self, stream: IO[bytes], schema: Type[BaseModel]
    ) -> Iterator[Dict[str, XMLType]]:
        """Coerce a parsed record into the intended shape, nulling values
        which are expected to be parsed as nulls.

        """
        elements = self._get_elements_from_stream(stream)
        template_row = create_template_row(schema)

        for element in elements:
            yield self._parse_element(element, template_row)

    def read_to_py_iterator(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: Type[BaseModel],
    ) -> Iterator[Dict[str, Any]]:
        """Iterate through the contents of the file at URI, yielding rows
        containing the data.

        Field names can be slightly more complex for XML: to indicate a
        level of nesting, use a dot to separate levels.
        For arrays (which may occur none, 1, or many times), enclose the
        whole field name in square brackets. See
        `parser.utilities.parse_default_row`

        """
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource!r} is empty")

        with open_stream(resource, "rb") as stream:
            yield from self._parse_xml(stream, schema)

    def write_parquet(  # type: ignore
        self,
        entity: Iterator[Dict[str, Any]],
        target_location: URI,
        schema: Optional[Type[BaseModel]] = None,
        **kwargs,
    ) -> URI:
        """Writes the data of the given entity out to a parquet file"""
        # polars misinterprets local file schemes and creates a file: folder.
        # parse it as Path and uri to resolve
        if isinstance(_get_implementation(target_location), LocalFilesystemImplementation):
            target_location = file_uri_to_local_path(target_location).as_posix()
        if schema:
            polars_schema: Dict[str, pl.DataType] = {  # type: ignore
                fld.name: get_polars_type_from_annotation(fld.type_)
                for fld in stringify_model(schema).__fields__.values()
            }
            pl.LazyFrame(data=entity, schema=polars_schema).sink_parquet(
                path=target_location, compression="snappy", **kwargs
            )
        else:
            pl.LazyFrame(data=entity).sink_parquet(
                path=target_location, compression="snappy", **kwargs
            )

        return target_location


class XMLStreamReader(BasicXMLFileReader):
    """An XML parser which 'streams' the file, parsing only specific records.
    This means it requires more configuration, but should be much kinder on memory.

    """

    def _get_elements_from_stream(self, stream: IO[bytes]) -> Iterator[XMLElement]:
        parser = etree.XMLPullParser(
            events=(
                "start",
                "end",
            ),
            remove_pis=True,
            remove_comments=True,
            dtd_validation=False,
            resolve_entities=False,
        )

        container_contexts = 1 if not self.root_tag else 0
        record_contexts = 0
        emitted_element_count = 0

        self._logger.debug(
            f"Starting to parse XML stream, state is {'closed' if stream.closed else 'open'}"
        )

        element: XMLElement
        with NonClosingTextIOWrapper(stream, encoding=self.encoding) as text_stream:
            self._logger.debug(
                f"Starting text stream, state is {'closed' if text_stream.closed else 'open'}"
            )
            while not text_stream.closed:
                text_block = text_stream.read(500_000)
                if not text_block:
                    break

                parser.feed(text_block)

                for action, element in parser.read_events():
                    self._strip_namespace(element)

                    if action == "start":
                        if self.root_tag and element.tag == self.root_tag:
                            container_contexts += 1
                        if element.tag == self.record_tag:
                            record_contexts += 1
                        continue
                    if action != "end":
                        continue

                    if not container_contexts:
                        continue

                    if self.root_tag and element.tag == self.root_tag:
                        container_contexts -= 1
                    if element.tag == self.record_tag:
                        self._strip_namespaces_recursively(element)
                        yield element
                        emitted_element_count += 1
                        record_contexts -= 1

                    if not record_contexts:
                        element.clear()

                    if (
                        self.n_records_to_read is not None
                        and emitted_element_count == self.n_records_to_read
                    ):
                        break
                else:
                    continue
                break

            try:
                parser.close()
            # We don't care if the XML is incomplete.
            except Exception:  # pylint: disable=broad-except
                pass

    def write_parquet(  # type: ignore
        self,
        entity: Iterator[Dict[str, Any]],
        target_location: URI,
        schema: Optional[Type[BaseModel]] = None,
        **kwargs,
    ) -> URI:
        """Writes the given entity data out to a parquet file"""
        return super().write_parquet(
            entity=entity, target_location=target_location, schema=schema, **kwargs
        )
