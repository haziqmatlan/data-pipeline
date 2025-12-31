import uuid
from datetime import datetime
from typing import Text, List

from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame

from we.pipeline.core.util.configuration_util import to_database_name, to_s3_location
from we.pipeline.validation.task.validation_context import ValidationContext
from we.pipeline.core.validation.load_view_into_table import load_view_into_table
from we.pipeline.core.validation.read_data import read_data


class SmokeTestResultGeneration:
    """
    A utility class for generating and managing test result DataFrames 
    which will be append to validation report table: altrata_data_lake_*.*_validation_database.*

    Attributes:
        headers (List[str]): List of column names from the input DataFrame.
        schema (StructType): Schema of the input DataFrame.
        rows (List[dict]): List of row data as dictionaries from the input DataFrame.
        df (DataFrame): The most recently generated test result DataFrame.

    Args:
        data (DataFrame): The input DataFrame used as a template for schema and columns.

    Methods:
        new_test_result(**kwargs):
            - Creates a new test result DataFrame with values provided via keyword arguments.
            - Any missing columns will be filled with None.

        to_dataframe():
            - Returns the most recently generated test result DataFrame.
    """
    def __init__(self, data: DataFrame):
        self.spark = SparkSession.builder.getOrCreate()
        self.headers = data.columns
        self.schema = data.schema
        self.rows = [row.asDict() for row in data.collect()]
   
    def new_test_result(self, **kwargs):
        new_row = {col: kwargs.get(col, None) for col in self.headers}
        self.df = self.spark.createDataFrame([new_row], schema=self.schema)
    
    def to_dataframe(self):
        return self.df