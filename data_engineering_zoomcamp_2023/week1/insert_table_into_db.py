#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    engine = create_engine(f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db_name}")
    engine.connect()
    csv_name = "download.csv"

    os.system(f"wget {params.csv_url} -O {csv_name}.gz")
    os.system(f"gunzip {csv_name}.gz")

    # Get the types.
    df = pd.read_csv(f"{csv_name}", nrows=10000)
    my_dtypes = df.dtypes.apply(lambda x: x.name).to_dict()
    # Insert the green_taxi_trips table.
    df_iter = pd.read_csv(f"{csv_name}", iterator=True, chunksize=100000, dtype=my_dtypes)
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
