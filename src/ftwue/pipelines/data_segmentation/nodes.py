"""
This is a boilerplate pipeline 'data_segmentation'
generated using Kedro 0.19.9
"""

import typing as t
import pandas as pd


def split_data(df: pd.DataFrame, parameters: t.Dict) -> t.Tuple:

    split_name = parameters["split_name"]
    val_start = parameters["val_start"]
    test_start = parameters["test_start"]

    val_start = pd.to_datetime(val_start)
    test_start = pd.to_datetime(test_start)

    df[split_name] = df["full_date"].apply(lambda x: "train" if x < val_start else "val" if x < test_start else "test")

    df.drop(columns=["full_date", "streetname"], inplace=True)
    
    return [df], True