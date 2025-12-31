import uuid
from datetime import datetime
from typing import Text, List

from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame

from we.pipeline.core.util.configuration_util import to_database_name
from we.pipeline.validation.task.validation_context import ValidationContext
from we.pipeline.validation.task.smoke_validation_context import SmokeValidationContext
from we.pipeline.core.validation.load_view_into_table import load_view_into_table
from we.pipeline.core.validation.read_data import read_data
from we.pipeline.core.validation.generate_test_result import SmokeTestResultGeneration
from pyspark.sql.utils import AnalysisException
from we.pipeline.core.validation.jira_integration_lib import TestCreationAndExecution


class Validation:
    def validate(self):
        """Validate some test results.
        :return: True if all expectations are met. False if some expectations
        are not met.
        """
        return True


def generate_md5_concatenate_columns(
        key_column: Text, view_or_table: Text
):
    try:
        all_columns = spark.sql(f"select * from {view_or_table}").columns

        key_columns = key_column.split(",")

        all_columns_string = "\n||'-'||".join([f"nvl(cast({x} as string), '')" for x in all_columns])

        key_columns_string = "\n||'-'||".join([f"nvl(cast({x} as string), '')" for x in key_columns])

        query = f"""
            select {key_columns_string} as key_columns,
            {all_columns_string} as concat_columns,
            md5( {all_columns_string}) as md5_hash_concat_column
            from {view_or_table}
            """

        df = spark.sql(query)

        return df

    except Exception as e:
        print(f"An unexpected error occurred while generating MD5 concatenation: {e}")
        return None


class ColumnValuesValidation(Validation):
    def __init__(
            self,
            spark: SparkSession,
            use_case_id: Text,
            validation_context: SmokeValidationContext,
            source_database: Text,
            target_database: Text,
            source_dataframe: DataFrame,
            target_dataframe: DataFrame,
            s3_folder: Text,
            catalog: str
    ):
        self.spark = spark
        self.use_case_id = use_case_id
        self.validation_context = validation_context
        self.source_database = source_database
        self.target_database = target_database
        self.source_dataframe = source_dataframe
        self.target_dataframe = target_dataframe
        self.s3_folder = s3_folder
        self.catalog = catalog

    def validate_c(self):

        ctx = self.validation_context
        catalog = self.catalog
        source_database = self.source_database
        target_database = self.target_database
        source_dataframe = self.source_dataframe
        target_dataframe = self.target_dataframe

        current_timestamp_val = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        job_log_key = str(uuid.uuid4())

        try:
            if source_dataframe is None or target_dataframe is None:
                print("source or target is none")
                return False
            else:
                source_dataframe.createOrReplaceTempView("source_data_view")
                target_dataframe.createOrReplaceTempView("target_data_view")
                # Fill empty value with 'null'
                source_dataframe = source_dataframe.fillna('null').replace('', 'null')

                source_dataframe = generate_md5_concatenate_columns(key_column=ctx.key_column, view_or_table="source_data_view")
                source_dataframe = source_dataframe.withColumnRenamed("key_columns", "source_key_columns").withColumnRenamed("concat_columns", "source_concat_columns").withColumnRenamed("md5_hash_concat_column", "source_md5_hash_concat_column")

                target_dataframe = generate_md5_concatenate_columns(key_column=ctx.key_column, view_or_table="target_data_view")
                target_dataframe = target_dataframe.withColumnRenamed("key_columns", "target_key_columns").withColumnRenamed("concat_columns", "target_concat_columns").withColumnRenamed("md5_hash_concat_column", "target_md5_hash_concat_column")

                source_count = source_dataframe.count()
                target_count = target_dataframe.count()

                data_failure = (
                    source_dataframe.join(target_dataframe,
                                          source_dataframe.source_key_columns == target_dataframe.target_key_columns,
                                          how="outer")
                    .where(
                        (
                                    source_dataframe.source_md5_hash_concat_column.isNull() | target_dataframe.target_md5_hash_concat_column.isNull()) | (
                                source_dataframe.source_md5_hash_concat_column != target_dataframe.target_md5_hash_concat_column))
                    .select(source_dataframe.source_key_columns, source_dataframe.source_md5_hash_concat_column,
                            target_dataframe.target_key_columns, target_dataframe.target_md5_hash_concat_column)
                )

                failure_count = data_failure.count()
                success_count = max((max(source_count, target_count) - failure_count), 0)

                if success_count > 0 and failure_count == 0:
                    validation_status = 'Success: All the records are Matching'
                elif success_count == 0 and failure_count > 0:
                    validation_status = 'Failure: All the records are NOT Matching'
                elif success_count > 0 and failure_count > 0:
                    validation_status = 'Failure: Few records are matching and few records are NOT matching'
                elif success_count == 0 and failure_count == 0:
                    validation_status = 'Success: There are no records to match'
                else:
                    validation_status = 'Undefined failure'

                # Read existing test report to fetch its columns and schemas
                test_report_df = spark.sql(f"SELECT * FROM {catalog}.{ctx.space}_validation_database.pipeline_validation_report LIMIT 1")
                test_result_gen = SmokeTestResultGeneration(test_report_df)

                test_result_gen.new_test_result(
                    batch_id=self.use_case_id, 
                    pipeline_type=ctx.job_type,
                    object_type=ctx.object_type,
                    validation_type="column_val_validation",
                    zone=ctx.zone,
                    test_type="regression",
                    check_type=None,
                    assert_type=None, 
                    source_location=self.s3_folder,
                    source_database=source_database,
                    source_tablename=ctx.source_tablename,
                    target_database=target_database,
                    target_tablename=ctx.target_tablename,
                    target_column=ctx.key_column,
                    source_row_count=source_count,
                    target_row_count=target_count,
                    success_count=success_count,
                    failure_count=failure_count,
                    validation_status=validation_status,
                    record_created_timestamp=datetime.strptime(current_timestamp_val, "%Y-%m-%d %H:%M:%S"),
                    record_job_log_key=job_log_key      
                    )
                data_validate_report = test_result_gen.to_dataframe()

                db_name = f"{catalog}.{ctx.space}_validation_database"
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                validation_table = f"{catalog}.{ctx.space}_validation_database.pipeline_validation_report"

                data_validate_report.createOrReplaceTempView("data_validate_report")
                load_view_into_table(table=validation_table, view="data_validate_report", mode="append")

                (
                    test_plan_key,
                    test_plan_summary,
                    test_case_key,
                    test_case_summary,
                    test_execution_key,
                    test_execution_summary,
                ) = TestCreationAndExecution(
                    project='WEQA',
                    environment="poc_dev" if ctx.environment == "dev" else ctx.environment,
                    space=ctx.space,
                    pipeline=ctx.pipeline,
                    validation_type='data_check',
                    table_name=ctx.target_tablename,
                    success_count=str(success_count),
                    failure_count=str(failure_count),
                    test_execution_status="PASSED" if "success" in validation_status.lower() else "FAILED"
                ).e2e_test_execution()

                # print(f"Test case executed successfully:- Test Plan: {test_plan_key}, Test Case: {test_case_key}, Test Execution: {test_execution_key}")

                # True if all matched
                return (
                        0 < max(source_count, target_count)
                        and 0 == failure_count
                )

        except Exception as e:
            if isinstance(e, AttributeError):
                error_msg = f"Attribute error: {e}"
                failure_status_msg = "Failed: Due to AttributeError Exception"
            elif isinstance(e, ValueError):
                error_msg = f"Value error: {e}"
                failure_status_msg = "Failed: Due to ValueError Exception"
            elif isinstance(e, AnalysisException):
                error_msg = f"SQL Analysis error: {e}"
                failure_status_msg = "Failed: Due to AnalysisException"
            else:
                error_msg = f"An unexpected error occurred: {e}"
                failure_status_msg = "Failed: Generic exception"

            print(error_msg)
 
            # Read existing test report to fetch its columns and schemas
            test_report_df = spark.sql(f"SELECT * FROM {catalog}.{ctx.space}_validation_database.pipeline_validation_report LIMIT 1")
            test_result_gen = SmokeTestResultGeneration(test_report_df)

            test_result_gen.new_test_result(
                    batch_id=self.use_case_id, 
                    pipeline_type=ctx.job_type,
                    object_type=ctx.object_type,
                    validation_type="column_val_validation",
                    zone=ctx.zone,
                    test_type="regression",
                    check_type=None,
                    assert_type=None, 
                    source_location=self.s3_folder,
                    source_database=source_database,
                    source_tablename=ctx.source_tablename,
                    target_database=target_database,
                    target_tablename=ctx.target_tablename,
                    target_column=ctx.key_column,
                    source_row_count=0,
                    target_row_count=0,
                    success_count=0,
                    failure_count=0,
                    validation_status=failure_status_msg,
                    record_created_timestamp=datetime.strptime(current_timestamp_val, "%Y-%m-%d %H:%M:%S"),
                    record_job_log_key=job_log_key      
                    )
            data_validate_report = test_result_gen.to_dataframe()
            data_validate_report.createOrReplaceTempView("data_validate_report")
            db_name = f"{catalog}.{ctx.space}_validation_database"
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            validation_table = f"{catalog}.{ctx.space}_validation_database.pipeline_validation_report"

            load_view_into_table(table=validation_table, view="data_validate_report", mode="append")

            (
                test_plan_key,
                test_plan_summary,
                test_case_key,
                test_case_summary,
                test_execution_key,
                test_execution_summary,
            ) = TestCreationAndExecution(
                project='WEQA',
                environment="poc_dev" if ctx.environment == "dev" else ctx.environment,
                space=ctx.space,
                pipeline=ctx.pipeline,
                validation_type='data_check',
                table_name=ctx.target_tablename,
                success_count=str(0),
                failure_count=str(0),
                test_execution_status="FAILED"
            ).e2e_test_execution()

            # print(f"Test case executed successfully:- Test Plan: {test_plan_key}, Test Case: {test_case_key}, Test Execution: {test_execution_key}")

            return False
