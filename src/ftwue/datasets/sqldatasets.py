from kedro.io import AbstractDataset
from kedro.io.core import get_filepath_str, get_protocol_and_path


from pathlib import Path

from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings

import psycopg2
from psycopg2.extras import execute_values

import pandas as pd

import json

import logging

from typing import Dict, Any

logger = logging.getLogger(__name__)

class FtwueDataset(AbstractDataset):
    def __init__(self,
                 table_names,
                 unique_columns=None,
                 load_args=None,
                 save_args=None):
        # conf_path = str(Path("ftwue") / settings.CONF_SOURCE)
        conf_path = str(Path(settings.CONF_SOURCE))
        conf_loader = OmegaConfigLoader(conf_source=conf_path)
        self.table_names = table_names
        self.unique_columns = unique_columns
        self.db_credentials = conf_loader["credentials"]["postgres"]
        self.save_args = save_args or {}
        self.load_args = load_args or {}

        self.set_up_tables()

    def set_up_tables(self):
        logger.info("Setting up tables in database")

        try:
            with psycopg2.connect(self.db_credentials['con']) as conn:
                with conn.cursor() as cursor:
                    for table_name in self.table_names:
                        # Ensure the table exists
                        cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY
                        );
                        """)
        except Exception as e:
            print(f"Error setting up tables: {e}")
            raise  # Re-raise the exception

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(
            table_names=self.table_names,
            unique_columns=self.unique_columns,
        )

    def _load(self) -> str:
        pass
        # raise ValueError("This dataset type is write-only")
    
    
    def _save(self, data: str) -> None:
        # Ensure data has the same length as table_names
        if len(data) != len(self.table_names):
            raise ValueError(
                f"Number of tables ({len(self.table_names)}) does not match the number of DataFrames ({len(data)})"
            )

        try:
            with psycopg2.connect(self.db_credentials['con']) as conn:
                with conn.cursor() as cursor:
                    for table_name, df, unique_cols in zip(self.table_names, data, self.unique_columns):

                        logger.info(f"Saving table {table_name}")

                        # # Convert numpy.datetime64 columns to native Python datetime
                        # for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                        #     df[col] = pd.to_datetime(df[col]).dt.to_pydatetime()

                        # Rename the "id" column to "row_id" if it exists
                        if "id" in df.columns:
                            df = df.rename(columns={"id": "row_id"})

                        # Replace NaN with None for database insertion
                        df = df.map(lambda x: None if pd.isna(x) else x)

                        # Ensure the unique columns are present in the DataFrame
                        if not set(unique_cols).issubset(set(df.columns)):
                            raise ValueError(
                                f"Unique columns {unique_cols} are not all present in DataFrame columns {df.columns}"
                            )

                        # Ensure columns exist in the table
                        for column in df.columns:
                            # Add the column if it doesn't exist
                            cursor.execute(f"""
                            DO $$
                            BEGIN
                                IF NOT EXISTS (
                                    SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_name='{table_name}' AND column_name='{column}'
                                ) THEN
                                    ALTER TABLE {table_name} ADD COLUMN {column} {self._get_column_type(df[column])};
                                END IF;
                            END $$;
                            """)

                        # Create a unique constraint on the unique columns
                        constraint_name = f"unique_{table_name}_constraint"

                        # Check if the unique constraint already exists
                        cursor.execute("""
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = %s AND constraint_name = %s
                        """, (table_name, constraint_name))
                        exists = cursor.fetchone()

                        # If the constraint doesn't exist, create it
                        if not exists:
                            unique_columns_str = ', '.join(unique_cols)
                            cursor.execute(f"""
                            ALTER TABLE {table_name}
                            ADD CONSTRAINT {constraint_name} UNIQUE ({unique_columns_str})
                            """)

                        # Prepare the columns and values for insertion
                        columns = ', '.join(df.columns)

                        # Prepare the unique columns for the ON CONFLICT clause
                        unique_columns_str = ', '.join(unique_cols)

                        # Prepare the SET clause for the UPDATE action, excluding unique columns
                        update_columns = [col for col in df.columns if col not in unique_cols]
                        if update_columns:
                            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                        else:
                            update_clause = None

                        # Prepare the full INSERT statement with ON CONFLICT
                        if update_clause:
                            insert_statement = f"""
                            INSERT INTO {table_name} ({columns}) VALUES %s
                            ON CONFLICT ({unique_columns_str}) DO UPDATE SET {update_clause};
                            """
                        else:
                            # If no update columns, just ignore conflicts
                            insert_statement = f"""
                            INSERT INTO {table_name} ({columns}) VALUES %s
                            ON CONFLICT ({unique_columns_str}) DO NOTHING;
                            """

                        # Create list of data tuples
                        data_tuples = [tuple(row) for row in df.to_numpy()]

                        # Use execute_values for bulk insertion
                        execute_values(cursor, insert_statement, data_tuples)

        except Exception as e:
            print(f"Error updating database: {e}")
            raise  # Re-raise the exception

    def _get_column_type(self, series: pd.Series) -> str:
        """Dynamically infer the SQL column type based on the pandas Series dtype."""
        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(series):
            return "DOUBLE PRECISION"
        elif pd.api.types.is_bool_dtype(series):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        elif isinstance(series.iloc[0], dict):  # Check if the first element is a dictionary
            return "JSONB"
        else:
            return "TEXT"  # Default to TEXT for non-numeric columns


# class FtwueDatasetSingleSeries(AbstractDataset):
#     def __init__(self,
#                  table_names,
#                  unique_columns=None,
#                  load_args=None,
#                  save_args=None):
#         # conf_path = str(Path("ftwue") / settings.CONF_SOURCE)
#         conf_path = str(Path(settings.CONF_SOURCE))
#         conf_loader = OmegaConfigLoader(conf_source=conf_path)
#         self.table_names = table_names
#         self.db_credentials = conf_loader["credentials"]["postgres"]
#         self.load_args = load_args or {}

#     def _describe(self) -> Dict[str, Any]:
#         """Returns a dict that describes the attributes of the dataset."""
#         return dict(
#             table_names=self.table_names,
#         )

#     def _load(self) -> pd.DataFrame:
#         """Fetches merged data from the 'foot_traffic' and 'weather' PostgreSQL tables using an SQL join."""
#         try:
#             # Connect to the database
#             with psycopg2.connect(self.db_credentials['con']) as conn:
#                 with conn.cursor() as cursor:
#                     # Query the merged data using an SQL JOIN
#                     cursor.execute("""
#                         SELECT *
#                         FROM foot_traffic
#                         LEFT JOIN weather
#                         ON foot_traffic.full_date = weather.full_date;
#                     """)
                    
#                     # Fetch all rows from the joined query
#                     rows = cursor.fetchall()
                    
#                     # Get column names from the cursor description
#                     columns = [desc[0] for desc in cursor.description]
                    
#                     # Create a DataFrame from the fetched data
#                     merged_df = pd.DataFrame(rows, columns=columns)
                    
#                     # Return the merged DataFrame
#                     return merged_df

#         except Exception as e:
#             print(f"Error loading or merging data from 'foot_traffic' and 'weather' tables: {e}")
#             raise
        

#         return data

#     def _save(self, data: str) -> None:
#         raise ValueError("This dataset type is read-only")