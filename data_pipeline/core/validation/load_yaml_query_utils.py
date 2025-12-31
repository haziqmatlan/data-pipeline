import yaml
import importlib.resources
from datetime import datetime
from typing import Text, List, Union
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from importlib.resources import files, as_file
from we.pipeline.core.validation.load_view_into_table import load_view_into_table
from we.pipeline.validation.task.validation_context import ValidationContext, CrossPlatformValidationContext
from we.pipeline.validation.task.smoke_validation_context import SmokeValidationContext
from we.pipeline.core.validation.jira_integration_lib import TestCreationAndExecution
from we.pipeline.core.validation.generate_test_result import generate_cross_platform_val_test_result
# from we.pipeline.core.validation.read_yml_query_lib import get_yaml_files, replace_placeholders
from we.pipeline.core.validation.pg_val_lib import read_pg_data_from_query, get_pg_ddp_audit_batch_id
from we.pipeline.core.util.sns_util import SnsUtil


def get_yaml_files(job_type, zone=None, test_type=None, check_type=None, pos_type=None) -> List[str]:
    """
    Read all files formatted: .yml & .yaml.

    Return:
        All yaml_files in that particular folder assigned as job_type
    """
    if job_type == 'pop':
        package_path = f'we.pipeline.core.validation.yml_query.{job_type}.{pos_type}.{test_type}.{check_type}'
        target_filename = f'{zone}_check.yml'
    else:
        package_path = f'we.pipeline.core.validation.yml_query.{job_type}.{test_type}.{check_type}'
        target_filename = f'{zone}_check.yml'
    
    yaml_files = []

    try:       
        # Step 1: Get a Traversable for the package
        package_dir = files(package_path)

        for entry in package_dir.iterdir():
            if entry.name in target_filename and entry.is_file():
                with as_file(entry) as actual_file:
                    yaml_files.append(str(actual_file))
    except ModuleNotFoundError:
        raise FileNotFoundError(f'The package {package_path} could not be found.')
    return yaml_files


def replace_placeholders(query_str: str, catalog, space):
    """
    Replaces placeholders in a list of YAML files.

    Args:
        yaml_files (List[str]): List of paths to the YAML files.
        catalog (str): The value to replace the '${catalog}' placeholder.
        space (str): The value to replace the '${space}' placeholder.
    """
    query = query_str.replace('${catalog}', catalog).replace('${space}', space)
    return query


class LoadYamlQueryUtils():
    """
    Utility class for loading and executing SQL queries defined in YAML files for data validation workflows.

    Args:
        validation_context (dict): Contextual information for validation, including the rank to select specific queries.
        catalog (Text): The catalog name.
        space (Text): The space name.
        job_type (Text): The type of job.
        zone (Text): The data zone (e.g., 'raw', 'silver').
        test_type (Text): The type of test being performed (e.g., 'smoke', 'regression').
        check_type (Text): The type of check (e.g., 'software', 'data_quality').
        source_dataframe (DataFrame): The source Spark DataFrame to be used as a temporary SQL view.

    Methods:
        load_yaml_query():
            - Loads SQL queries from YAML files based on the provided context and executes the query based on specified rank.
            - Returns the result DataFrame and the query name.
    """
    def __init__(
        self,
        spark: SparkSession,
        validation_context: dict,
        catalog: Text,
        space: Text,
        job_type: Text,
        zone: Text,
        test_type: Text,
        check_type: Text,
        pos_type: Text,
        source_dataframe: DataFrame
    ):
        self.spark = spark
        self.validation_context = validation_context
        self.catalog = catalog
        self.space = space
        self.job_type = job_type
        self.zone = zone
        self.test_type = test_type
        self.check_type = check_type
        self.pos_type = pos_type
        self.source_dataframe = source_dataframe


    def load_yaml_query(self):
        ctx = self.validation_context
        catalog = self.catalog
        space = self.space
        job_type = self.job_type
        zone = self.zone
        test_type = self.test_type
        check_type = self.check_type
        pos_type = self.pos_type
        source_dataframe = self.source_dataframe

        if source_dataframe is not None:
            source_dataframe.createOrReplaceTempView("source_dataframe")
        else:
            print("source_dataframe is None — skipping temp view registration.")

        # Trigger get_yaml_files to retrieve YAML file paths
        yaml_files = get_yaml_files(job_type, zone, test_type, check_type, pos_type)
        if not yaml_files:
            raise FileNotFoundError("No YAML files found.")

        # Process each YAML file
        for file_path in yaml_files:
            with open(file_path, 'r') as file:
                queries = yaml.safe_load(file)

            # Iterate through all queries in the YAML file
            for query_name, query_details in queries['queries'].items():
                query_str = query_details.get('query')
                # For purpose to run selected rank only
                ranked = query_details.get('rank')
                if ranked == ctx.rank:
                    # Replace placeholders and run the query
                    query = replace_placeholders(query_str, catalog, space)
                    result = self.spark.sql(query)
                    return result, query_name
                else:
                    continue

                