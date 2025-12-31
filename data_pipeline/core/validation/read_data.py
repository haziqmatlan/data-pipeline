from pyspark.shell import spark

from typing import Text, Union
from we.pipeline.core.util.configuration_util import to_database_name, to_s3_location
from we.pipeline.validation.task.validation_context import ValidationContext
from we.pipeline.validation.task.smoke_validation_context import SmokeValidationContext
from we.pipeline.core.validation.pg_val_lib import read_pg_data_from_table, read_pg_data_from_query
from pyspark.sql import functions as function
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


def read_data(validation_context: SmokeValidationContext, s3_folder, batch_id=None, schema=None, catalog=None):

    ctx = validation_context

    default_excluded_columns = [
        "_rescued_data",
        "record_created_filename",
        "record_created_timestamp",
        "record_job_log_key"
    ]
    # Reading Source location or source table
    try:
        if ctx.s3_location:
            source_database = ""
            # To handle onetime job
            if ctx.job_type == "onetime":
                source_dataframe = spark.read.parquet(s3_folder)
            elif ctx.job_type == "rep" or ctx.job_type == "cip":
                source_dataframe = spark.read.csv(s3_folder, header=False, sep="|", schema=schema)
            elif ctx.job_type == "cdp":
                source_dataframe = spark.read.csv(s3_folder, header=False, sep=",", schema=schema)
                # source_dataframe = spark.read.csv(s3_folder).rdd.zipWithIndex().filter(lambda x: x[1] > 1).map(lambda x: x[0]).toDF(schema=schema)
            elif ctx.job_type == 'rip':
                source_dataframe = spark.read.csv(s3_folder, header=False, sep="|", schema=schema)
            elif ctx.job_type == 'fdp':
                source_dataframe = spark.read.csv(s3_folder, header=False, sep=",", schema=schema)
            elif ctx.job_type == 'pop' and ctx.data_group == 'hvr':
                source_dataframe = spark.read.format("xml").option("rowTag", "COMPANY").schema(schema).load(s3_folder)
            elif ctx.job_type == 'pop' and ctx.data_group == 'dnb':
                source_dataframe = spark.read.csv(s3_folder, header=False, sep="|", schema=schema)
            else:
                source_dataframe = spark.read.csv(s3_folder)
        else:
            source_database = to_database_name(ctx.space, ctx.source_zone, ctx.data_group)
            if ctx.source_zone == "raw" and ctx.job_type != 'cdp':
                if batch_id is None:
                    source_dataframe = spark.sql(f"select distinct * from {catalog}.{source_database}.{ctx.source_tablename}")
                else:
                    source_dataframe = spark.sql(f"select distinct * from {catalog}.{source_database}.{ctx.source_tablename} where batch_id = {batch_id}")
            elif ctx.source_tablename == "we_inactive_profile_master" and ctx.job_type == 'aipp':
                if batch_id is None:
                    source_dataframe = spark.sql(f"select distinct * from {catalog}.{ctx.space}_{ctx.source_zone}_master.{ctx.source_tablename}")
                else:
                    source_dataframe = spark.sql(f"select distinct * from {catalog}.{ctx.space}_{ctx.source_zone}_master.{ctx.source_tablename} where batch_id = {batch_id}")
            else:
                if batch_id is None:
                    source_dataframe = spark.sql(f"select * from {catalog}.{source_database}.{ctx.source_tablename}")
                else:
                    source_dataframe = spark.sql(
                        f"select * from {catalog}.{source_database}.{ctx.source_tablename} where batch_id = {batch_id}")

        # Reading Target table
        target_database = to_database_name(ctx.space, ctx.target_zone, ctx.data_group)
        if batch_id is None:
            if ctx.zone == "pg":
                target_dataframe = read_pg_data_from_table(ctx.target_tablename)
            elif ctx.target_tablename == 'person_address_master':
                target_dataframe = spark.sql(f"select * from {catalog}.{ctx.space}_silver_master.{ctx.target_tablename}")
            elif ctx.data_group == 'npc' and ctx.target_tablename == 'ci_reject_contact_info':
                target_dataframe = spark.sql(f"select * from {catalog}.{ctx.space}_{ctx.zone}_neustar.{ctx.target_tablename}")
            elif ctx.data_group == 'npc' and ctx.target_tablename == 'ri_reject_residential_info':
                target_dataframe = spark.sql(f"select * from {catalog}.{ctx.space}_{ctx.zone}_acx.{ctx.target_tablename}")
            else:
                target_dataframe = spark.sql(f"select * from {catalog}.{target_database}.{ctx.target_tablename}")
        else:
            target_dataframe = spark.sql(f"select * from {catalog}.{target_database}.{ctx.target_tablename} where batch_id = {batch_id}")
        # Excluded the default and table specific excluded columns
        if ctx.excluded_cols is not None:
            self_excluded_columns = ctx.excluded_cols.split(",")
        else:
            self_excluded_columns = []

        excluded = [*self_excluded_columns, *default_excluded_columns]

        source_columns_all = source_dataframe.columns
        target_columns_all = target_dataframe.columns

        if ctx.job_type == 'aipp':
            # Select all columns without any exclusion
            source_dataframe = source_dataframe.select(source_columns_all)
            target_dataframe = target_dataframe.select(target_columns_all)

        else:
            # Exclude columns
            source_columns = [c for c in source_columns_all if c not in excluded]
            target_columns = [c for c in target_columns_all if c not in excluded]

            # Re-generating Source and Target dataframe after removing the exluded columns
            source_dataframe = source_dataframe.select(source_columns)
            target_dataframe = target_dataframe.select(target_columns)

        return source_database, target_database, source_dataframe, target_dataframe
    except FileNotFoundError as fnf_error:
        print(f"File not found: {fnf_error}")
        return None, None, None, None
    except ValueError as ve:
        print(f"Value error occurred: {ve}")
        return None, None, None, None
    except TypeError as te:
        print(f"Type error occurred: {te}")
        return None, None, None, None
    except AttributeError as ae:
        print(f"Attribute error occurred: {ae}")
        return None, None, None, None
    except AnalysisException as ae:
        print(f"SQL query failed: {ae}")
        return None, None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None, None

