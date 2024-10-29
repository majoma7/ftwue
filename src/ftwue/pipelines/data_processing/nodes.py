"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.9
"""

import pandas as pd

def retrieve_dataset(*args): # need placeholder args
    # This node doesn't perform any operation.
    # It ensures that reading happens after writing.
    flag = args[-1]
    return tuple(args[:-1])

def add_engineered_datetime_features(df_data: pd.DataFrame, df_holidays: pd.DataFrame, ) -> pd.DataFrame:
    
    df_data["date"] = pd.to_datetime(df_data["full_date"]).dt.date
    df_holidays["date"] = pd.to_datetime(df_holidays["date"]).dt.date
    df_data["holiday"] = df_data["date"].isin(df_holidays["date"])

    df_data["weekday"] = pd.to_datetime(df_data["full_date"]).dt.weekday
    df_data["month"] = pd.to_datetime(df_data["full_date"]).dt.month
    df_data["year"] = pd.to_datetime(df_data["full_date"]).dt.year
    df_data["calendar_week"] = pd.to_datetime(df_data["full_date"]).dt.isocalendar().week
    df_data["quarter"] = pd.to_datetime(df_data["full_date"]).dt.quarter

    return df_data


    