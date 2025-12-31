import boto3
import json
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SnsUtil:

    @staticmethod
    def push_val_email_report(sns_arn: str, region: str, job_type, use_case_id,env, space, df=None, status=None, catalog=None):
        spark = SparkSession.builder.appName('email').getOrCreate()
        if env == 'dev':
            env = 'poc_dev'
        
        if df is None:
            query = f"""           
            SELECT batch_id,
                pipeline_type,
                object_type,
                validation_type,
                zone,
                test_type,
                check_type,
                target_tablename,
                success_count,
                failure_count,
                CASE 
                    WHEN upper(substr(validation_status, 1, 1)) = 'S' OR upper(substr(validation_status, 1, 1)) = 'P' THEN 'PASS'
                    ELSE 'FAIL' 
                END AS validation_status
            FROM {catalog}.{space}_validation_database.pipeline_validation_report
            WHERE pipeline_type = '{job_type}' 
            AND batch_id = '{use_case_id}'
            """

            rows = spark.sql(query).orderBy(F.coalesce('target_tablename', F.lit(''))).collect()

            if rows:
                # Plain text table format
                table_text = (
                    f"[{env.capitalize()} {space.capitalize()}] QA Validation Task Execution in {rows[0].pipeline_type.upper()}: {rows[0].zone.capitalize()} Zone {rows[0].test_type.capitalize()} Test Completed\n\n"
                    "Batch ID       | Pipeline | Object  | Target Table      |  Check Type   | Success Count  | Failure Count  | Status |  Validation Type\n"
                    "-----------------------------------------------------------------------------------------------------------------------------------\n"
                )
                # Populate table rows
                for row in rows:
                    table_text += f"{row.batch_id:<14} | {row.pipeline_type:<7} | {row.object_type:<7} | {row.target_tablename or '' :<18} | {row.check_type or '' :<15} | {row.success_count:<10} | {row.failure_count or '':<10} | {row.validation_status:<7} | {row.validation_type:<18} \n"
            else:
                print("No data found for the specified query.")                    

        else:
            rows = df.orderBy('scenario').collect()
            # Plain text table format
            table_text = (
                f"QA DDP Count Validation Execution Completed ({status})\n\n"
                "Batch ID     | Result Type | Object Type         | Data Group   | Scenario                              | Result\n"
                "--------------------------------------------------------------------------------------------------------------------\n"
            )
            # Populate table rows
            for row in rows:
                # Extract only the last part of object_type
                short_object_type = row.object_type.split('.')[-1] if row.object_type else 'null'

                table_text += f"{row.batch_id:<12} | {row.result_type:<11} | {short_object_type:<25} | {str(row.data_group or ''):<13} | {row.scenario:<36} | {row.result:<6} \n"
        my_config = Config(region_name=region)

        # Create an AWS SNS client
        sns_client = boto3.client('sns', region_name=region, config=my_config)
        # subject_text = f"{env.capitalize()} {space.capitalize()} - QA Validation Task Execution in {rows[0].pipeline_type.upper()}: {rows[0].zone.capitalize()} Zone {rows[0].test_type.capitalize()} Test Summary."
        try:
            # Publish the message to the SNS topic
            response = sns_client.publish(
                TopicArn=sns_arn,
                Message=table_text,
                Subject="QA Validation Task Execution Summary"
            )
            print("Message sent! Message ID:", response['MessageId'])
        except Exception as e:
            print(f"Failed to send message: {str(e)}")