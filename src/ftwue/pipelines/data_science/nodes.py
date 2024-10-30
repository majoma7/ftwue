"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.9
"""

import pandas as pd
import typing as t

# import random forest
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor

import logging
logger = logging.getLogger(__name__)

def data_splitting(data: pd.DataFrame, split: pd.DataFrame, split_db_flag: bool) -> t.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    """
    Split the data into train, validation and test sets.
    """

    # merge data and split with row_id in data corresponding to datapoint_id in split
    data = data.merge(split, left_on="row_id", right_on="datapoint_id")

    train = data[data["split_column"] == "train"]
    val = data[data["split_column"] == "val"]
    test = data[data["split_column"] == "test"]
    
    return train, val, test

def remove_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    
    columns_to_remove = [
        # ID columns
        "row_id",
        "datapoint_id",

        # Date columns
        "full_date",

        # Data_type columns
        "split_column",

        # Target column
        "pedestrian_count",
        ]

    return df.drop(columns=columns_to_remove)

def train_model (train: pd.DataFrame) -> RandomForestRegressor:

    y_train = train["pedestrian_count"]

    X_train = remove_unwanted_columns(train)

    model = RandomForestRegressor()
    # model = DecisionTreeRegressor()
    
    logger.info("Starting training")
    model.fit(X_train, y_train)
    return model

def make_predictions(model, X: pd.DataFrame) -> pd.Series:

    # for column in X:
    #     print(column)

    y_pred = X[["row_id", "traffic_type_away", "traffic_type_total", "traffic_type_towards"]].copy()

    X_train = remove_unwanted_columns(X)

    y_pred["predictions"] = model.predict(X_train)

    return y_pred

def bring_predictions_into_submission_format(predictions: pd.DataFrame) -> pd.DataFrame:

    # Create a new dataframe with unique values from 'row_id'
    result_df = pd.DataFrame(predictions['row_id'].unique(), columns=['id'])

    # Add columns for different pedestrian counts based on conditions
    result_df['n_pedestrians'] = result_df['id'].apply(
        lambda x: predictions.loc[(predictions['row_id'] == x) & (predictions['traffic_type_total']), 'predictions'].sum()
    )

    result_df['n_pedestrians_away'] = result_df['id'].apply(
        lambda x: predictions.loc[(predictions['row_id'] == x) & (predictions['traffic_type_away']), 'predictions'].sum()
    )

    result_df['n_pedestrians_towards'] = result_df['id'].apply(
        lambda x: predictions.loc[(predictions['row_id'] == x) & (predictions['traffic_type_towards']), 'predictions'].sum()
    )

    return result_df