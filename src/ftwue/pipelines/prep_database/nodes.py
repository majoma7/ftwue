"""
This is a boilerplate pipeline 'prep_database'
generated using Kedro 0.19.9
"""

import pandas as pd
import typing as t
import yaml
import os
import psycopg2
import json


### Prep geolocation


def split_geolocation(x, type):
    return x.apply(lambda x: x[type])


def clean_columns(df):
    df.drop(columns=["geo_point_2d", "id", "city"], inplace=True)
    return df


def convert_to_json(series: pd.Series):
    series = series.apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)



def preprocess_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    df["lat"] = split_geolocation(df["geo_point_2d"], "lat")
    df["lon"] = split_geolocation(df["geo_point_2d"], "lon")
    df["geo_shape"] = convert_to_json(df["geo_shape"])
    df = clean_columns(df)
    return df


### Prep holidays
 
def preprocess_holidays(x):
    
    # remove frist 4 digits from string in column Datum
    x["Datum"] = x["Datum"].str[4:]
    # rename Datum to date
    x.rename(columns={"Datum": "date"}, inplace=True)
    x.drop(columns=["Feiertag"], inplace=True)
    x["date"] = pd.to_datetime(x["date"], dayfirst=True)
    # change back to string:
    x["date"] = x["date"].astype(str)
    return x

### Prep main dataset


def convert_to_int(x: pd.Series):
    return x.astype(pd.Int64Dtype())


def create_full_date(x: pd.DataFrame):
    df = x.copy()
    df["date"] = df["date"].astype(str)
    df["hour"] = df["hour"].astype(str)
    df["full_date"] = df["date"] + " " + df["hour"]
    df["full_date"] = pd.to_datetime(df["full_date"])
    date_column = df["full_date"]
    return date_column


def clean_columns_data_all(df):
    df.drop(columns=["date", "hour", "city", "weekday"], inplace=True)
    return df


def remove_duplicate_ids(df):
    # Find duplicate rows based on 'id' column
    duplicate_ids = df[df.duplicated(subset="id", keep=False)]
    
    # Separate rows to drop where 'collection_type' is 'no_information'
    to_drop = duplicate_ids[duplicate_ids['collection_type'] == 'no_information']
    
    # Keep rows that are not in the drop list
    df = df[~df.index.isin(to_drop.index)]
    
    return df

def preprocess_data_all(train: pd.DataFrame, test: pd.DataFrame) -> pd.DataFrame:

    data_all = pd.concat([train, test], sort=False)
    data_all = remove_duplicate_ids(data_all)
    data_all["n_pedestrians"] = convert_to_int(data_all["n_pedestrians"])
    data_all["n_pedestrians_towards"] = convert_to_int(
        data_all["n_pedestrians_towards"]
    )
    data_all["n_pedestrians_away"] = convert_to_int(data_all["n_pedestrians_away"])

    data_all["full_date"] = create_full_date(data_all)

    data_all = clean_columns_data_all(data_all)

    return data_all


### Separate weather data


def get_weather_df(x):
    return x[["full_date", "temperature", "weather_condition"]].copy()


def rm_weather_columns(x):
    x.drop(columns=["temperature", "weather_condition"], inplace=True)
    return x


def drop_duplicates(x):
    x.drop_duplicates(inplace=True)
    return x


def preprocess_weather(data_all: pd.DataFrame) -> t.Tuple:
    weather = get_weather_df(data_all)
    weather = drop_duplicates(weather)
    data_all = rm_weather_columns(data_all)
    return weather, data_all


### Inserting data into the database

# def update_database(weather, data_all, geolocation):
#     db_credentials = get_db_credentials()

#     try:
#         conn = psycopg2.connect(db_credentials['con'])
#         cursors = conn.cursor()


#     except Exception as e:
#         print(f"Error updating database: {e}")
    
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

def update_database(*args):
    return args # logic handeled by dataset