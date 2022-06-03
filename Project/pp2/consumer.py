#!/usr/bin/env python
from confluent_kafka import Consumer
import ccloud_lib
import json
import time
from datetime import date
import pandas as pd
import numpy as np
import psycopg2
from val_trans import validate, transform
import os

DBname = "postgres"
DBuser = "postgres"
DBpass = "dataeng"
TripTable = "trip"
BreadCrumbTable = "breadcrumb"

def connect_db():
    print("connect_db starting...")
    connection = psycopg2.connect(
        host='localhost',
        database=DBname,
        user=DBuser,
        password=DBpass,
    )
    connection.autocommit = False;
    print("connect_db done")
    return connection

def setup_db(connection):
    print("setup_db starting...")
    with connection.cursor() as cursor:
        cursor.execute(f"""
            drop table if exists {TripTable} cascade;
            drop table if exists {BreadCrumbTable} cascade;
            drop table if exists staging;
            drop type if exists service_type cascade;
            drop type if exists tripdir_type cascade;
            create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
            create type tripdir_type as enum ('Out', 'Back');
            create table if not exists {TripTable} (
                trip_id integer,
                route_id integer,
                vehicle_id integer,
                service_key service_type,
                direction tripdir_type,
                PRIMARY KEY (trip_id)
            );
            create table if not exists {BreadCrumbTable} (
                tstamp timestamp,
                latitude float,
                longitude float,
                direction integer,
                speed float,
                trip_id integer,
                FOREIGN KEY (trip_id) REFERENCES {TripTable}
            );
        """)
        connection.commit()
    print("setup_db done")
    return connection

def load_db(connection, df):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            create unlogged table if not exists staging (
                trip_id integer,
                tstamp timestamp,
                vehicle_id integer,
                speed float,
                direction integer,
                longitude float,
                latitude float,
                service_key service_type
            );
        """)
    with connection.cursor() as cursor:
        print(f"load_db {len(df['trip_id'])} records...")
        start = time.perf_counter()
        TEMP_FILE = "temp.csv"
        df.to_csv(TEMP_FILE, index=False, header=False)
        f = open(TEMP_FILE, 'r')
        cursor.copy_from(f, 'staging', sep=",", columns=['trip_id', 'tstamp', 'vehicle_id', 'speed', 'direction', 'longitude', 'latitude', 'service_key'])
        f.close()
        os.remove(TEMP_FILE)

        print("Copied temp.csv into staging table")

        cursor.execute(f"""
                insert into {TripTable} (trip_id, vehicle_id, service_key) 
                select distinct on (trip_id) trip_id, vehicle_id, service_key from staging
                on conflict (trip_id) do nothing;
        """)
        print("upserted into trip table")
        cursor.execute(f"""
                insert into {BreadCrumbTable} (tstamp, latitude, longitude, direction, speed, trip_id) 
                select tstamp, latitude, longitude, direction, speed, trip_id from staging;
        """)
        print("upserted into breadcrumb table")
        cursor.execute(f"drop table staging cascade;")
        print("dropped staging table")
        connection.commit()
        elapsed = time.perf_counter() - start
        print(f"finished loading. Elapsed Time: {elapsed:0.4} seconds")

if __name__ == '__main__':
    FILE_DATE = date.today().strftime("%Y-%m-%d")

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    setup = args.setup
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    data_array = []
    total_count = 0

    def push_db():
        global setup, data_array, total_count
        df = pd.DataFrame(data_array)
        df = validate(df)
        df = transform(df)
        connection = connect_db()
        if setup is True:
            connection = setup_db(connection)
            setup = False
        load_db(connection, df)
        print(f"{total_count} records uploaded to database")
        data_array.clear()
        total_count = 0

    try:
        while True:
            # Check for Kafka message
            msg = consumer.poll(1.0)
            if msg is None:
                if total_count > 0:
                    push_db()
                print(f"Waiting for message or event/error in poll() from {topic}")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                data_array.append(data)
                total_count += 1
                if total_count == 150000:
                    push_db()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close() # Leave group and commit final offsets
