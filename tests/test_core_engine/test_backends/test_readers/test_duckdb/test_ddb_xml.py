
from pathlib import Path
import duckdb
from duckdb import DuckDBPyRelation

from dve.core_engine.backends.implementations.duckdb.readers.xml import DuckDBXMLStreamReader
from dve.core_engine.constants import RECORD_INDEX_COLUMN_NAME
from tests.test_core_engine.test_backends.test_readers.fixtures import (
    temp_dir,
    temp_xml_file,
    temp_xml_file_w_null_recs
)

def test_ddb_xml_reader_all_str(temp_xml_file):
    uri, header_model, header_data, class_data_model, class_data = temp_xml_file
    ddb_conn = duckdb.connect()
    header_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="Header"
    )
    class_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="ClassData"
    )
    header_rel: DuckDBPyRelation = header_reader.read_to_relation(
        uri.as_uri(), "header", header_model
    )
    class_rel: DuckDBPyRelation = class_reader.read_to_relation(
        uri.as_uri(), "class_data", class_data_model
    )
    expected_header = [{**recs, RECORD_INDEX_COLUMN_NAME: idx} for idx, recs in enumerate(header_data, start=1)]
    expected_class = [{**recs, RECORD_INDEX_COLUMN_NAME: idx} for idx, recs in enumerate(class_data, start=1)]
    assert header_rel.count("*").fetchone()[0] == 1
    assert header_rel.df().to_dict("records") == expected_header
    assert class_rel.count("*").fetchone()[0] == 1
    assert class_rel.df().to_dict("records") == expected_class


def test_ddb_xml_reader_write_parquet(temp_xml_file):
    uri, header_model, header_data, class_data_model, class_data = temp_xml_file
    ddb_conn = duckdb.connect()
    header_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="Header"
    )
    class_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="ClassData"
    )
    header_rel: DuckDBPyRelation = header_reader.read_to_relation(
        uri.as_uri(), "header", header_model
    )
    class_rel: DuckDBPyRelation = class_reader.read_to_relation(
        uri.as_uri(), "class_data", class_data_model
    )
    target_header_loc: Path = uri.parent.joinpath("header_parquet.parquet").as_posix()
    target_class_loc: Path = uri.parent.joinpath("class_parquet.parquet").as_posix()
    header_reader.write_parquet(entity=header_rel, target_location=target_header_loc)
    class_reader.write_parquet(entity=class_rel, target_location=target_class_loc)
    header_parquet_rel: DuckDBPyRelation = header_reader._connection.read_parquet(
        target_header_loc
    )
    class_parquet_rel: DuckDBPyRelation = class_reader._connection.read_parquet(target_class_loc)
    assert header_parquet_rel.df().to_dict(orient="records") == header_rel.df().to_dict(
        orient="records"
    )
    assert class_parquet_rel.df().to_dict(orient="records") == class_rel.df().to_dict(
        orient="records"
    )

def test_ddb_xml_reader_remove_null_recs(temp_xml_file_w_null_recs):
    uri, header_model, _, class_data_model, _ = temp_xml_file_w_null_recs
    ddb_conn = duckdb.connect()
    header_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="Header"
    )
    class_reader = DuckDBXMLStreamReader(
        connection=ddb_conn, root_tag="root", record_tag="ClassData"
    )
    header_rel: DuckDBPyRelation = header_reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_uri(), "header", header_model
    )
    class_rel: DuckDBPyRelation = class_reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_uri(), "class_data", class_data_model
    )
    assert header_rel.count("*").fetchone()[0] == 1
    assert class_rel.count("*").fetchone()[0] == 2
    assert class_rel.select("year_group").pl().to_dict(as_series=False).get("year_group") == ["1", "2"]
