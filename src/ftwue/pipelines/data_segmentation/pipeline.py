"""
This is a boilerplate pipeline 'data_segmentation'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import split_data
from ...general_nodes import retrieve_dataset


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
            node(
            func=retrieve_dataset,
            inputs=["ftwue_db_datapoints", "db_write_complete"],
            outputs=["ftwue_db_datapoints_df"],
            name="retrieve_dataset_for_splitting_node",
        ),
        node(
            func=split_data,
            inputs=["ftwue_db_datapoints_df", "params:data_split"],
            outputs=["ftwue_db_datasplit", "db_write_split_complete"],
            name="split_data_node",
        ),
    ])