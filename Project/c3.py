#!/usr/bin/env python
from confluent_kafka import Consumer
import ccloud_lib
import json
import os
import time
from datetime import date
import pandas as pd
import numpy as np
import psycopg2

DBname = "postgres"
DBuser = "postgres"
DBpass = "dataeng"
TripTable = "trip"
BreadCrumbTable = "breadcrumb"

# data validations
def validate(df):
    try:
        assertion0 = "EVENT_NO_TRIP field is a 9 digit integer for all records."
        df['EVENT_NO_TRIP'] = df['EVENT_NO_TRIP'].astype(int)
        assert df['EVENT_NO_TRIP'].between(100000000,999999999).all(), f"FAILED {assertion0}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion0}")
        
    try:
        assertion1 = "EVENT_NO_STOP field is a 9 digit integer for all records."
        df['EVENT_NO_STOP'] = df['EVENT_NO_TRIP'].astype(int)
        assert df['EVENT_NO_STOP'].between(100000000,999999999).all(), f"FAILED {assertion1}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion1}")
        
    try:
        assertion2 = "OPD_DATE field is a date in 2020 for all records."
        assert df['OPD_DATE'].str.match('.*-20$').all() == True, f"FAILED {assertion2}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion2}")
        
    try:    
        assertion3 = "VEHICLE_ID field is a positve integer for all records."
        df['VEHICLE_ID'] = df['VEHICLE_ID'].astype(int)
        assert df['VEHICLE_ID'].all() > 0, f"FAILED {assertion3}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion3}")
        
    try:    
        assertion4 = "ACT_TIME field is in seconds between midnight up to at most 2am the next day."
        df['ACT_TIME'] = df['ACT_TIME'].astype(int)
        seconds_in_day = (60 * 60 * 24) + (60 * 60 * 2)
        assert df['ACT_TIME'].between(0,seconds_in_day).all(), f"FAILED {assertion4}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion4}")
        
    try:    
        assertion5 = "VELOCITY field exists and is greater than or equal to 0 mph for all records."
        df['VELOCITY'] = df['VELOCITY'].replace("", "0")
        df['VELOCITY'] = df['VELOCITY'].astype(float)
        assert df['VELOCITY'].all() >= 0, f"FAILED {assertion5}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion5}")
        
    try:    
        assertion6 = "The OPD_DATE field is in the format DD-MMM-YY."
        assert df['OPD_DATE'].str.match('\d\d-[A-Z]{3}-20').all(), f"FAILED {assertion6}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion6}")
        
    try:    
        assertion7 = "RADIO_QUALITY field is empty for all records."
        assert df['RADIO_QUALITY'].all() == False, f"FAILED {assertion7}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion7}")
        
    try:    
        assertion8 = "DIRECTION field will be an integer within the range [0,360)."
        df['DIRECTION'] = df['DIRECTION'].replace("", "0")
        df['DIRECTION'] = df['DIRECTION'].astype(int)
        assert df['DIRECTION'].between(0,360,inclusive="left").all(), f"FAILED {assertion8}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion8}")
        
    try:    
        assertion9 = "GPS_HDOP field is a positive floating point number."
        df['GPS_HDOP'] = df['GPS_HDOP'].replace("", "0")
        assert df['GPS_HDOP'].all() > 0, f"FAILED {assertion9}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion9}")
        
    try:    
        assertion10 = "GPS_LONGITUDE field exists for all records."
        df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].replace("", "0")
        df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].astype(float)
        assert df['GPS_LONGITUDE'].all() >= 0, f"FAILED {assertion10}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion10}")
        
    try:    
        assertion11 = "GPS_LATITUDE field exists for all records."
        df['GPS_LATITUDE'] = df['GPS_LATITUDE'].replace("", "0")
        df['GPS_LATITUDE'] = df['GPS_LATITUDE'].astype(float)
        assert df['GPS_LATITUDE'].all() >= 0, f"FAILED {assertion11}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion11}")

    return df

# data transformations
def transform(df):
    df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'])
    df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='S', errors='ignore')
    df['OPD_DATE'] = df['OPD_DATE'] + df['ACT_TIME']

    df.rename(columns={'OPD_DATE': 'tstamp'}, inplace=True)
    df.rename(columns={'GPS_LATITUDE': 'latitude'}, inplace=True)
    df.rename(columns={'GPS_LONGITUDE': 'longitude'}, inplace=True)
    df.rename(columns={'DIRECTION': 'direction'}, inplace=True)
    df.rename(columns={'VELOCITY': 'speed'}, inplace=True)
    df.rename(columns={'EVENT_NO_TRIP': 'trip_id'}, inplace=True)
    df.rename(columns={'VEHICLE_ID': 'vehicle_id'}, inplace=True)

    df = df.drop(columns=['EVENT_NO_STOP', 'METERS', 'ACT_TIME', 'GPS_SATELLITES', 'GPS_HDOP', 'RADIO_QUALITY', 'SCHEDULE_DEVIATION'])
    df['service_key'] = df['tstamp'].dt.dayofweek.apply(lambda day: 'Weekday' if day <= 4 else ('Saturday' if day == 5 else 'Sunday'))
    print(df.columns, len(df['trip_id']))
    return df

def connect_db():
    connection = psycopg2.connect(
        host='localhost',
        database=DBname,
        user=DBuser,
        password=DBpass,
    )
    connection.autocommit = True
    #return connection

#def setup_db(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TripTable} CASCADE;
            DROP TABLE IF EXISTS {BreadCrumbTable} CASCADE;
            DROP TYPE IF EXISTS service_type;
            DROP TYPE IF EXISTS tripdir_type;
            create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
            create type tripdir_type as enum ('Out', 'Back');
            CREATE TABLE IF NOT EXISTS {TripTable} (
                trip_id integer,
                route_id integer,
                vehicle_id integer,
                service_key service_type,
                direction tripdir_type
            );
            CREATE TABLE IF NOT EXISTS {BreadCrumbTable} (
                tstamp timestamp,
                latitude float,
                longitude float,
                direction integer,
                speed float,
                trip_id integer
            );
        """)
        cursor.execute(f"""
            DROP TABLE IF EXISTS staging;
            CREATE TABLE IF NOT EXISTS staging (
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
        #connection.commit()
    #return connection

#def load_db(connection, df):
    #with connection.cursor() as cursor:
        #print(f"loading {len(df['trip_id'])} records")
        start = time.perf_counter()
        TEMP_FILE = "temp.csv"
        #df.to_csv(TEMP_FILE, index=False, header=False)
        #df.to_csv(TEMP_FILE, index=False)
        #f = open(TEMP_FILE, "r")
        #cursor.copy_from(f, "staging", sep=',')
        #cursor.copy_expert('COPY staging(trip_id, tstamp, vehicle_id, speed, direction, longitude, latitude, service_key) FROM STDIN WITH HEADER CSV', f)
        #f.close()
        #os.remove(TEMP_FILE)
                #select distinct trip_id, vehicle_id, service_key from staging
                #with trip_data as (select distinct on (trip_id) trip_id, vehicle_id, service_key from staging order by trip_id, vehicle_id)
                #on conflict (trip_id) do nothing;
#        cursor.execute(f"""
#                insert into {TripTable} (trip_id, vehicle_id, service_key) 
#                select distinct on (trip_id) trip_id, vehicle_id, service_key from staging;
#        """)
#        print("upserted into trip table")
#        cursor.execute(f"""
#                insert into {BreadCrumbTable} (tstamp, latitude, longitude, direction, speed, trip_id) 
#                select tstamp, latitude, longitude, direction, speed, trip_id from staging;
#        """)
#        print("upserted into breadcrumb table")
#        cursor.execute(f"drop table staging cascade;")
#        print("dropped staging table")
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
    try:
        while True:
            # Check for Kafka message
            msg = consumer.poll(1.0)
            if msg is None:
                print("connect_db")
                connection = connect_db()
                #print("setup_db")
                #connection = setup_db(connection)
                if total_count > 0:
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
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close() # Leave group and commit final offsets
