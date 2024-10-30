"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import data_splitting, train_model, make_predictions, bring_predictions_into_submission_format


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=data_splitting,
            inputs=["combined_dataset", "data_split", "db_write_split_complete"],
            outputs=["train_df", "val_df", "test_df"],
            name="data_splitting_node",
        ),
        node(
            func=train_model,
            inputs="train_df",
            outputs="model",
            name="train_model_node",
        ),
        node(
            func=make_predictions,
            inputs=["model", "test_df"],
            outputs="test_predictions",
            name="make_final_predictions_node",
        ),
        node(
            func=bring_predictions_into_submission_format,
            inputs=["test_predictions"],
            outputs="submission",
            name="bring_predictions_into_submission_format_node",
        )
    ])
