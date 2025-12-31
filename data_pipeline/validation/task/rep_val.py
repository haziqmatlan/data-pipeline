import logging
import uuid
from datetime import datetime
from types import SimpleNamespace
from data_pipeline.core.util.sns_util import SnsUtil
from data_pipeline.core.validation.generate_test_result import SmokeTestResultGeneration
from pyspark.shell import spark
from data_pipeline.core.constant import (
    LOG_FORMAT,
    ENV_OPTION,
    SPACE_OPTION,
    ZONE_OPTION,
    OBJECT_TYPE_OPTION,
    JOB_TYPE_OPTION,
    TEST_TYPE_OPTION
)    
from data_pipeline.core.validation.setup_constant import (
    load_config,
    load_smoke_config,
    config_file, 
    yaml_query_load_utils,
    query_result_assignment
)
from data_pipeline.core.validation.constant.smoke_validation_context import SmokeValidationContext


logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


def rep_property_bronze_expected_output_setup(val_context, rank, batch_id=None, catalog=None):
    rep_property_bronze_expected_output = REPPropertyBronzeExpectedOutputSetup(spark=spark, val_context=val_context, catalog=catalog)
    try:
        if rank == 1:
            rep_property_bronze_expected_output.re_raw_property_expected_output_setup()
    except Exception as e:
        logger.error(f"Unexpected error during Propery Bronze expected output setup: {e}")

def smoke_run_validation(use_case_id, config, bucket, catalog, env, space, job_type, object_type, smoke_zone, test_type, check_types, batch_id, flow, rep_schema):
    current_timestamp_val = datetime.utcnow()
    job_log_key = str(uuid.uuid4())

    qa_config_file = config_file(job_type, test_type)
    config_df = load_smoke_config(qa_config_file, test_type)
    get_env_smoke = SmokeValidationContext(config_df)

    pipeline_stopper = False

    # Loop each check_type and rank to properly test all required test scenarios
    for check_type in check_types:
        for rank in get_env_smoke.smoke_ranks(environment=env, space=space, job_type=job_type, object_type=object_type, zone=smoke_zone, test_type=test_type, check_type=check_type):
            target_env = get_env_smoke.get_smoke_config(environment=env, space=space, object_type=object_type, zone=smoke_zone, job_type=job_type, check_type=check_type, rank=rank)
            target_env = SimpleNamespace(**target_env) 

            # To read S3 file - For specific case only
            if check_type == 'software' and smoke_zone == 'raw' and rank == 2:                            
                s3_folder = to_s3_location(bucket, env, space, DELTA, batch_id, target_env.data_group, flow, False)
                source_dataframe = spark.read.csv(s3_folder, header=False, sep="|", schema=rep_schema)
            else:
                source_dataframe = None
            
            # Run the query and return result of count and status of Pass/Fail
            result, query_name = yaml_query_load_utils(target_env, catalog, space, job_type, smoke_zone, test_type, target_env.check_type, source_dataframe)

            # Fetching the result and assign to the proper variables
            result_status, success_count, failure_count = query_result_assignment(result) 

            # Read existing test report to fetch its columns and schemas
            test_report_df = spark.sql(f"SELECT * FROM {catalog}.{space}_validation_database.pipeline_validation_report LIMIT 1")
            test_result_gen = SmokeTestResultGeneration(test_report_df)

            # Generating new test result and append it to the report
            test_result_gen.new_test_result(batch_id=use_case_id, 
                                            pipeline_type=target_env.job_type,
                                            object_type=target_env.object_type,
                                            validation_type=f"{query_name}",
                                            zone=smoke_zone,
                                            test_type=target_env.test_type,
                                            check_type=target_env.check_type,
                                            assert_type=target_env.assert_type,
                                            success_count=success_count,
                                            failure_count=failure_count,
                                            validation_status=result_status,
                                            record_created_timestamp=current_timestamp_val,
                                            record_job_log_key=job_log_key
                                            )
            test_result_report = test_result_gen.to_dataframe()
            test_result_report.createOrReplaceTempView("test_result_report")

            validation_table = f"{catalog}.{space}_validation_database.pipeline_validation_report"
            load_view_into_table(table=validation_table, view="test_result_report", mode="append")

            # Tracking hard assertion failure
            if target_env.assert_type == 'hard_stop' and result_status == 'Fail':
                print(f"\nTracking {target_env.assert_type} assertion of {query_name} is {result_status}...")
                pipeline_stopper = True

    # Final check to stop the pipeline if hard assertion failed
    if pipeline_stopper == True:
        # Push result to sns-topic 
        SnsUtil.push_val_email_report(
            sns_arn=config.get('legacy_sns_arn'),
            region=config.get('legacy_region'),
            job_type=job_type,
            use_case_id=use_case_id,
            env=env,
            space=space,
            catalog=catalog
        )
        raise SystemExit("Pipeline stopped due to hard assertion failure in Smoke validation.")
    else:
        SnsUtil.push_val_email_report(
            sns_arn=config.get('legacy_sns_arn'),
            region=config.get('legacy_region'),
            job_type=job_type,
            use_case_id=use_case_id,
            env=env,
            space=space,
            catalog=catalog
        )
        print("Smoke validation completed successfully without any hard assertion failure.")


def etl_process(**options):
    # Use case ID & run ID
    date_time_obj = datetime.utcnow()
    use_case_id = date_time_obj.strftime("%Y%m%d%H%M%S")

    # Environment and Space
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)
    smoke_zone = options.get(ZONE_OPTION)
    smoke_object_type = options.get(OBJECT_TYPE_OPTION)
    job_type = options.get(JOB_TYPE_OPTION)
    test_type = options.get(TEST_TYPE_OPTION)

    zones = ['raw', 'bronze', 'silver']
    check_types = ['software', 'data_quality']

    qa_config_file = config_file(job_type)
    config_df = load_config(qa_config_file)
    get_env = SmokeValidationContext(config_df)

    print(f"get_env: {get_env}")

    try:
        # Smoke Test run 
        if test_type == 'smoke': 
            print("Running smoke tests...")
            smoke_run_validation(use_case_id, config, bucket, catalog, env, space, job_type, smoke_object_type, smoke_zone, test_type, check_types, batch_id, flow, property_schema) 

        # Regression Test run 
        elif test_type == 'regression':   
            print("Running regression tests...")
    except Exception as e:
        logger.error(f"Unexpected error during Bronze expected output setup: {e}")
