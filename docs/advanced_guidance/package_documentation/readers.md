## CSV

=== "Base"

    ::: src.dve.core_engine.backends.readers.csv.CSVFileReader
        options:
            heading_level: 3
            merge_init_into_class: true
            members: false

=== "DuckDB"

    ::: src.dve.core_engine.backends.implementations.duckdb.readers.csv.DuckDBCSVReader
        options:
            heading_level: 3
            members:
                - __init__

    ::: src.dve.core_engine.backends.implementations.duckdb.readers.csv.PolarsToDuckDBCSVReader
        options:
            heading_level: 3
            members:
                - __init__

    ::: src.dve.core_engine.backends.implementations.duckdb.readers.csv.DuckDBCSVRepeatingHeaderReader
        options:
            heading_level: 3
            members:
                - __init__

=== "Spark"

    ::: src.dve.core_engine.backends.implementations.spark.readers.csv.SparkCSVReader
        options:
            heading_level: 3
            members:
                - __init__

## JSON

=== "DuckDB"

    ::: src.dve.core_engine.backends.implementations.duckdb.readers.json.DuckDBJSONReader
        options:
            heading_level: 3
            members:
                - __init__

=== "Spark"

    ::: src.dve.core_engine.backends.implementations.spark.readers.json.SparkJSONReader
        options:
            heading_level: 3
            members:
                - __init__

## XML

=== "Base"

    ::: src.dve.core_engine.backends.readers.xml.BasicXMLFileReader
        options:
            heading_level: 3
            merge_init_into_class: true
            members: false

=== "DuckDB"

    ::: src.dve.core_engine.backends.implementations.duckdb.readers.xml.DuckDBXMLStreamReader
        options:
            heading_level: 3
            members:
                - __init__

=== "Spark"

    ::: src.dve.core_engine.backends.implementations.spark.readers.xml.SparkXMLStreamReader
        options:
            heading_level: 3
            members:
                - __init__

    ::: src.dve.core_engine.backends.implementations.spark.readers.xml.SparkXMLReader
        options:
            heading_level: 3
            members:
                - __init__
