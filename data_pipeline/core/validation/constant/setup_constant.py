import json
from pyspark.shell import spark
from pyspark.sql.types import StructType
from importlib import resources as pkg_resources
from data_pipeline.core.util.spark_util import load_dataframe_from_package
from data_pipeline.core.validation.load_yaml_query_utils import LoadYamlQueryUtils

def config_file(job_type, test_type=None):
    if test_type is not None:
        return "qa_config_" + job_type + "_" + test_type + ".csv"
    else:
        return "qa_config_" + job_type + ".csv"
    

def load_config(qa_config, job_type=None):
    if job_type == 'cross_platform_count':
        schema = StructType.fromJson(
            json.loads(pkg_resources.read_text('we.pipeline.core.validation.schema', 'qa_cross_platform_config_schema.json')))
    else:
        schema = StructType.fromJson(
            json.loads(pkg_resources.read_text('we.pipeline.core.validation.schema', 'qa_config_schema.json')))

    df = load_dataframe_from_package(spark,
                                    'we.pipeline.core.validation.config',
                                    qa_config,
                                    'csv',
                                    schema,
                                    header=True, sep=',')
    return df


def yaml_query_load_utils(val_context, catalog, space, job_type, smoke_zone, test_type, check_type, source_dataframe, pos_type=None):
    yaml_query_load_utils_lib = LoadYamlQueryUtils(spark=spark, 
                                                   validation_context=val_context,
                                                   catalog=catalog, 
                                                   space=space, 
                                                   job_type=job_type, 
                                                   zone=smoke_zone, 
                                                   test_type=test_type, 
                                                   check_type=check_type,
                                                   pos_type=pos_type,
                                                   source_dataframe=source_dataframe)
    query_result = yaml_query_load_utils_lib.load_yaml_query()
    return query_result


def query_result_assignment(result):
    """
    Processes the result of a query and assigns success_count and failure_count based on the result_status.
    Args:
        result: An object representing the query result. It is expected to have a `collect()` method that returns
                a list of tuples, where the first tuple contains the result_count and result_status as its first and second elements.
    Returns:
        tuple: A tuple containing two integers:
            - success_count (int): The number of successful results if status is 'Pass', otherwise 0.
            - failure_count (int): The number of failed results if status is 'Fail', otherwise 0.
    """
    if result is not None and result.collect():
        result_count = result.collect()[0][0]
        result_status = result.collect()[0][1]

    if result_status == 'Pass':
        success_count= result_count
        failure_count= 0
    elif result_status == 'Fail':
        success_count= 0
        failure_count= result_count
    return result_status, success_count, failure_count