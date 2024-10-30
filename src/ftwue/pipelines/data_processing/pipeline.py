"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.9
"""

from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import Pipeline, node
# from kedro.pipeline import Pipeline, pipeline, node

from .nodes import add_engineered_datetime_features, feature_processing
from ...general_nodes import retrieve_dataset

import os

# print cwd
print(os.getcwd())

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # this is a dummy node to link the previous pipeline to this one
        node(
            func=retrieve_dataset,
            inputs=["ftwue_db_multi_series", "ftwue_db_holidays", "db_write_complete"],
            outputs=["ftwue_db_multi_series_df", "ftwue_db_holidays_df"],
            name="retrieve_dataset_node",
        ),
        node(
            func=add_engineered_datetime_features,
            inputs=["ftwue_db_multi_series_df", "ftwue_db_holidays_df"],
            outputs="combined_dataset_df",
            name="add_engineered_datetime_features_node",
        ),
        node(
            func=feature_processing,
            inputs="combined_dataset_df",
            outputs="combined_dataset",
            name="feature_processing_node",
        )
    ])