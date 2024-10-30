"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.9
"""

import pandas as pd

### Nodes for processing

def add_engineered_datetime_features(df_data: pd.DataFrame, df_holidays: pd.DataFrame, ) -> pd.DataFrame:
    
    df_data["date"] = pd.to_datetime(df_data["full_date"]).dt.date
    df_holidays["date"] = pd.to_datetime(df_holidays["date"]).dt.date
    df_data["holiday"] = df_data["date"].isin(df_holidays["date"])

    df_data["weekday"] = pd.to_datetime(df_data["full_date"]).dt.weekday
    df_data["month"] = pd.to_datetime(df_data["full_date"]).dt.month
    df_data["year"] = pd.to_datetime(df_data["full_date"]).dt.year
    df_data["calendar_week"] = pd.to_datetime(df_data["full_date"]).dt.isocalendar().week
    df_data["quarter"] = pd.to_datetime(df_data["full_date"]).dt.quarter

    df_data["hour"] = pd.to_datetime(df_data["full_date"]).dt.hour

    return df_data

def feature_processing(df):
    df = melt_pedestrian_dataset(df)
    df = encode_categorical_features(df)
    df = remove_date(df)
    return df

### Helper nodes

def melt_df(df, columns_to_melt):
    
    # determine columns to melt
    value_vars = columns_to_melt
    id_vars = [col for col in df.columns if col not in value_vars]

    # perform melting
    df_melted = pd.melt(
    df,
    id_vars=id_vars,
    value_vars=value_vars,
    var_name = "traffic_type",
    value_name = "pedestrian_count",
    
    )

    # rename columns
    df_melted['traffic_type'] = df_melted['traffic_type'].replace({
        'n_pedestrians': 'total',
        'n_pedestrians_towards': 'towards',
        'n_pedestrians_away': 'away'
    })

    # sort by row_id
    df_melted = df_melted.sort_values(by=["row_id", "traffic_type"])

    # delete old id column
    df_melted.drop(columns=["id"], inplace=True)

    return df_melted

def melt_pedestrian_dataset(df):
    return melt_df(df, ["n_pedestrians", "n_pedestrians_towards", "n_pedestrians_away"])

def encode_categorical_features(df):

    features_on_hot_encoding = [
        # covariates
        "incidents",
        "collection_type",
        "weather_condition",
        "weekday",
        "month",
        "calendar_week",
        "quarter",
        "hour",
        
        # target categories
        "streetname",
        "traffic_type"
    ]

    df = pd.get_dummies(df, columns=features_on_hot_encoding)
    return df

def remove_date(df):
    return df.drop(columns=["date"])

    