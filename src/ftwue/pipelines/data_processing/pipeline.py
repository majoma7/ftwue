"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.9
"""

from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import Pipeline, node
# from kedro.pipeline import Pipeline, pipeline, node

from .nodes import add_engineered_datetime_features

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=add_engineered_datetime_features,
            inputs=["ftwue_db_multi_series", "ftwue_db_holidays"],
            outputs="all_data",
            name="add_engineered_datetime_features_node",
        ),
    ])