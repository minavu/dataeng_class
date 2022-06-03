#!/usr/bin/env python
# File: se-consumer.py

from confluent_kafka import Consumer
import ccloud_lib
import json
import time
from datetime import date
import pandas as pd
import numpy as np
import psycopg2
from se_val_trans import validate, transform
import os

DBname = "postgres"
DBuser = "postgres"
DBpass = "dataeng"
TripTable = "trip"

def connect_db():
    connection = psycopg2.connect(
        host='localhost',
        database=DBname,
        user=DBuser,
        password=DBpass,
    )
    connection.autocommit = False;
    return connection

def load_db(connection, df):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            drop table if exists stopevents;
            create table if not exists stopevents (
                trip_id integer,
                route_id integer,
                vehicle_id integer,
                service_key service_type,
                direction tripdir_type
            );
        """)
    with connection.cursor() as cursor:
        print(f"load_db {len(df['trip_id'])} records...")
        start = time.perf_counter()
        TEMP_FILE = "se-temp.csv"
        df.to_csv(TEMP_FILE, index=False, header=False)
        f = open(TEMP_FILE, 'r')
        cursor.copy_from(f, 'stopevents', sep=",", columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
        f.close()
        os.remove(TEMP_FILE)
        print("Copied se-temp.csv into stopevents table")

        cursor.execute(f"""
                insert into {TripTable} (trip_id, route_id, vehicle_id, service_key, direction)
                select distinct on (trip_id) trip_id, route_id, vehicle_id, service_key, direction from stopevents
                on conflict (trip_id) do update set
                route_id = excluded.route_id, 
                vehicle_id = excluded.vehicle_id, 
                service_key = excluded.service_key, 
                direction = excluded.direction;
        """)
        print("upserted into trip table")
        cursor.execute(f"drop table stopevents cascade;")
        print("dropped stopevents table")

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
        load_db(connection, df)
        print(f"{len(df['trip_id'])} stop event records uploaded to database")
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
