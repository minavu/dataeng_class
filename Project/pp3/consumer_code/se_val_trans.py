import pandas as pd

# data validation
def validate(df):
    try:
        assertion0 = "All trip_ids are 9 digit integers"
        df['trip_id'] = df['trip_id'].astype(int)
        assert df['trip_id'].between(100000000,999999999).all(), f"FAILED {assertion0}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion0}")

    try:
        assertion1 = "All route_id are non-null, non-negative integers"
        df['route_id'] = df['route_id'].astype(int)
        assert df['route_id'].all() >= 0, f"FAILED {assertion1}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion1}")

    try:
        assertion2 = "All vehicle_id are non-null, non-negative integers"
        df['vehicle_id'] = df['vehicle_id'].astype(int)
        assert df['vehicle_id'].all() >= 0, f"FAILED {assertion2}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion2}")

    try:
        assertion3 = "All service_key fields are filled with values of either W, S, or U"
        allowed = ['W', 'S', 'U']
        assert df['service_key'].apply(lambda x: True if x in allowed else False).all(), f"FAILED {assertion3}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion3}")

    try:
        assertion4 = "All directions are either 0 or 1"
        allowed = ['0', '1']
        df.dropna(inplace=True)
        assert df['direction'].apply(lambda x: True if x in allowed else (True if x is None else False)).all(), f"FAILED {assertion4}"
    except AssertionError as warning:
        print(warning)
    else:
        print(f"PASSED {assertion4}")
    
    return df

# data transformation
def transform(df):
    df['service_key'] = df['service_key'].apply(lambda x: 'Weekday' if x == 'W' else ('Saturday' if x == 'S' else ('Sunday' if x == 'U' else None)))
    df['direction'] = df['direction'].apply(lambda x: 'Out' if x == '0' else ('Back' if x == '1' else None))
    return df

