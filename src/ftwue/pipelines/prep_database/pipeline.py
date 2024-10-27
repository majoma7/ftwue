"""
This is a boilerplate pipeline 'prep_database'
generated using Kedro 0.19.9
"""

from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import Pipeline, node
# from kedro.pipeline import Pipeline, pipeline, node


from .nodes import preprocess_geolocation, preprocess_data_all, preprocess_weather, update_database, preprocess_holidays


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_geolocation,
                inputs="counterGeoLocations",
                outputs="preprocessed_geolocation",
                name="preprocess_geolocation_node",
            ),
            node(
                func=preprocess_data_all,
                inputs=["train", "test"],
                outputs="preprocessed_combined_data_intermediate",
                name="preprocess_data_all_node",
            ),

            node(
                func=preprocess_holidays,
                inputs="holidays",
                outputs="preprocessed_holidays",
                name="preprocess_holidays_node",
            ),

            node(
                func=preprocess_weather,
                inputs="preprocessed_combined_data_intermediate",
                outputs=["preprocessed_weather", "preprocessed_foot_traffic"],
                name="preprocess_weather_node",
            ),

            node(
                func=update_database,
                inputs= ["preprocessed_weather", "preprocessed_foot_traffic", "preprocessed_geolocation", "preprocessed_holidays"],
                outputs= "ftwue_db",
                name = "update_database_node",
            )
        ]
    )
