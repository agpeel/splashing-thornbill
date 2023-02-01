#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
from urllib.parse import urlparse
from pathlib import PurePosixPath
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url: str):
    """Download the data, determine the schema and create an iterator."""
    # Get the CSV file name, taking care to preserve the .gz suffix if present.
    url_parts = urlparse(csv_url)
    csv_file_name = PurePosixPath(url_parts.path).name

    os.system(f"wget {csv_url} -O {csv_file_name}")

    # Get the types.
    # Note read_csv() automatically decompresses the CSV file
    df = pd.read_csv(f"{csv_file_name}", nrows=100000)
    data_types = df.dtypes.apply(lambda x: x.name).to_dict()
    return csv_file_name, data_types

@task(log_prints=True)
def transform_data(df):
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # Extra transform from the example in video 2.2.2: remove rows with passenger_count == 0.
    # df['passenger_count'].isin(0) returns a pandas.Series with True or False corresponding to each row.
    # sum() calculates the sum of the items in the series, converting False to 1, so this is a nifty
    # way of counting the number of rows that met the condition.
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    # df['passenger_count'] is a pandas.Series.
    # Series.ne() returns a Series with each item containing the comparison result for that item.
    # df[Series.ne()] is doing pandas.DataFrame.loc(<a boolean series>) where the argument is a 
    # Series containing booleans, and only rows containing True are selected and included in the 
    # returned DataFrame.
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def load_data(table_name, df, engine, replace_table=False):
    if replace_table:
        if_exists_mode = "replace"
    else:
        if_exists_mode = "append"
    df.to_sql(name=table_name, con=engine, if_exists=if_exists_mode)

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Data")
def main_flow(params):
    engine = create_engine(f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db_name}")
    engine.connect()

    log_subflow(params.db_table)
    csv_file_name, data_types = extract_data(params.csv_url)
    # Cannot put this in extract_data() as a generator cannot be pickled to be returned from the task.
    df_iter = pd.read_csv(f"{csv_file_name}", iterator=True, chunksize=100000, dtype=data_types)
    first_df = True
    for df in df_iter:
        data = transform_data(df)
        load_data(params.db_table, data, engine, replace_table=first_df)
        first_df = False


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
    main_flow(args)
