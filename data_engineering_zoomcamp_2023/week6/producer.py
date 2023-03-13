#!/usr/bin/env python

from random import choice
import gzip
import csv
import json
from datetime import datetime
import time
import threading
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer


def produce_taxi_data(taxi_csv_file: str, topic: str, cluster_config: dict):
    if taxi_csv_file.endswith('.gz'):
        csv_file = gzip.open(taxi_csv_file, 'rt')
    else:
        csv_file = open(taxi_csv_file, 'rt')
    try:
        print(f"{topic} starting.")
        csv_reader = csv.DictReader(csv_file)
        producer = Producer(cluster_config)
        count = 1
        for row in csv_reader:
            # Fixes in FHV data
            if 'PUlocationID' in row:
                row['pickup_location_id'] = row.pop('PUlocationID')
            if 'DOlocationID' in row:
                row['dropoff_location_id'] = row.pop('DOlocationID')
            if 'pickup_datetime' in row:
                row['pickup_datetime'] = datetime.fromtimestamp(time.time() - (20 * 60)) \
                    .isoformat(sep=' ', timespec='seconds')
            if 'dropOff_datetime' in row:
                row['dropoff_datetime'] = datetime.now() \
                    .isoformat(sep=' ', timespec='seconds')
                del row['dropOff_datetime']
            # Fixes in Green data
            if 'PULocationID' in row:
                row['pickup_location_id'] = row.pop('PULocationID')
            if 'DOLocationID' in row:
                row['dropoff_location_id'] = row.pop('DOLocationID')
            if 'lpep_pickup_datetime' in row:
                row['pickup_datetime'] = datetime.fromtimestamp(time.time() - (20 * 60)) \
                    .isoformat(sep=' ', timespec='seconds')
                del row['lpep_pickup_datetime']
            if 'lpep_dropoff_datetime' in row:
                row['dropoff_datetime'] = datetime.now() \
                    .isoformat(sep=' ', timespec='seconds')
                del row['lpep_dropoff_datetime']
            message = json.dumps(row)
            key = row['pickup_location_id']
            if not key:
                # Discard rows where the pickup location is not specified.
                continue
            print(f"{topic} count={count} sending key={key}, message={message}", flush=True)
            producer.produce(topic, message, key, callback=delivery_callback)
            count += 1
            if count > 10:
                break
            time.sleep(5)
        # Block until the messages are sent.
        print(f"{topic} Finishing.")
        producer.poll(10000)
        producer.flush()
    except Exception as exc:
        print(f"Topic {topic} go an exception: {exc}")
    finally:
        csv_file.close()
    

def delivery_callback(err, msg):
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        pass
        # This does not flush until the end.
        # print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
        #    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


if __name__ == '__main__':
    # Run from bash command line with
    #   (venv) $ python producer.py cluster_config.ini

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    green_thread = threading.Thread(
        target=produce_taxi_data,
        name='green_thread',
        kwargs={
            'taxi_csv_file': '/home/apeel/my_data/projects/data_eng_Zoomcamp/data_taxis/green/green_tripdata_2019-01.csv.gz',
            'topic': 'rides_green',
            'cluster_config': config})
    fhv_thread = threading.Thread(
        target=produce_taxi_data,
        name='fhv_thread',
        kwargs={
            'taxi_csv_file': '/home/apeel/my_data/projects/data_eng_Zoomcamp/data/fhv_tripdata_2019-01.csv.gz',
            'topic': 'rides_fhv',
            'cluster_config': config})
    green_thread.start()
    fhv_thread.start()
    green_thread.join()
    fhv_thread.join()