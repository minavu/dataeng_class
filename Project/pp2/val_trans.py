#!/usr/bin/env python
import pandas as pd
import numpy as np

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
    return df

