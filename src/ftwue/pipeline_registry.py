"""Project pipelines."""

from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from kedro.pipeline.modular_pipeline import pipeline

from .pipelines.prep_database.pipeline  import create_pipeline as create_prep_database_pipeline
from .pipelines.data_processing.pipeline import create_pipeline as create_data_processing_pipeline



def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # pipelines = find_pipelines()

    prep_database_pipeline = create_prep_database_pipeline()
    # data_processing_pipeline = pipeline(
    #     create_data_processing_pipeline(),
    #     inputs = {"ftwue_db_multi_series": "ftwue_db"},
    # )
    data_processing_pipeline = create_data_processing_pipeline()

    return{"__default__": Pipeline([]) + prep_database_pipeline + data_processing_pipeline,
            "prep_database": prep_database_pipeline,
            "data_processing": data_processing_pipeline,}




# # db_input_output_mapping_pipeline = pipeline(pipe=create_prep_database_pipeline(), outputs={"ftwue_db": "ftwue_db_multi_series"})

# def create_pipeline(**kwargs) -> Pipeline:
#     # Create the data processing pipeline
#     data_processing_pipeline = create_data_processing_pipeline()
    
#     # Wrap the pipeline to map "all_data" to "processed_data"
#     wrapped_data_pipeline = pipeline(
#         pipe=data_processing_pipeline,
#         outputs={"all_data": "processed_data"}
#     )

#     # Create the prep_database pipeline
#     prep_database_pipeline = create_prep_database_pipeline()

#     # Combine the wrapped pipeline with the prep_database pipeline
#     combined_pipeline = wrapped_data_pipeline + prep_database_pipeline

#     return combined_pipeline
