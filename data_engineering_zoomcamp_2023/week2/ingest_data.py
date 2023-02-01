#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
from urllib.parse import urlparse
from pathlib import PurePosixPath


def main(params):
    # Get the CSV file name, taking care to preserve the .gz suffix if present.
    url_parts = urlparse(params.csv_url)
    csv_file_name = PurePosixPath(url_parts.path).name

    os.system(f"wget {params.csv_url} -O {csv_file_name}")

    # Get the types.
    # Note read_csv() automatically decompresses the CSV file
    df = pd.read_csv(f"{csv_file_name}", nrows=10000)
    my_dtypes = df.dtypes.apply(lambda x: x.name).to_dict()

    engine = create_engine(f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db_name}")
    engine.connect()

    # Insert the green_taxi_trips table.
    df_iter = pd.read_csv(f"{csv_file_name}", iterator=True, chunksize=100000, dtype=my_dtypes)
    if_exists_mode = "replace"
    for df in df_iter:
        start_time = time.time()
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=params.db_table, con=engine, if_exists=if_exists_mode)
        if_exists_mode = "append"
        print("Block: {:.2f} seconds".format(time.time() - start_time))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Ingest CSV data for NYC taxi trips')
    parser.add_argument('--user', help='DB user name')
    parser.add_argument('--password', help='DB password')
    parser.add_argument('--host', help='DB host')
    parser.add_argument('--port', help='DB port')
    parser.add_argument('--db_name', help='DB name')
    parser.add_argument('--db_table', help='DB table name')
    parser.add_argument('--csv_url', help='URL of the csv file')
    args = parser.parse_args()
    main(args)
