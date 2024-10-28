"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.9
"""

from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import Pipeline, node
# from kedro.pipeline import Pipeline, pipeline, node

from .nodes import add_engineered_datetime_features, db_read_dependency_node

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # this is a dummy node to link the previous pipeline to this one
        node(
            func=db_read_dependency_node,
            inputs="db_write_complete",
            outputs=None,
            name="db_read_dependency_node",
        ),
        node(
            func=add_engineered_datetime_features,
            inputs=["ftwue_db_multi_series", "ftwue_db_holidays"],
            outputs="all_data",
            name="add_engineered_datetime_features_node",
        ),
    ])