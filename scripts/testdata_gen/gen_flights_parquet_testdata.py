import polars as pl
from pathlib import Path


def main():
    data = [
        {
            "flight_id": 1,
            "plane_id": 1,
            "passengers": [
                {"flight_id": 1, "passenger_id": 1, "passenger_name": "Alice"},
                {"flight_id": 1, "passenger_id": 2, "passenger_name": "Bob"}
            ]
        },
        {
            "flight_id": 2,
            "plane_id": 1,
            "passengers": [
                {"flight_id": 2, "passenger_id": 3, "passenger_name": "Charlie"},
                {"flight_id": 2, "passenger_id": 4, "passenger_name": "Diana"}
            ]
        },
        {
            "flight_id": 3,
            "plane_id": 2,
            "passengers": [
                {"flight_id": 3, "passenger_id": 5, "passenger_name": "Eve"},
                {"flight_id": 3, "passenger_id": 6, "passenger_name": "Frank"}
            ]
        }
    ]

    df = pl.DataFrame(data)
    df.write_parquet(
        Path(Path(__file__).parent.parent.parent, "tests", "testdata", "flights", "flights.parquet")
    )

if __name__ == "__main__":
    main()
