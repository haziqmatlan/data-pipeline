# %pip install phonenumbers

import phonenumbers
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()


def etl_process(**options):
    """ Transforming Delta Data
    
    Data cleanup:
        - Filter out profiles if both first and middle name is null (Null removal)
        - Remove special characters
    Standardization: name, phone number
    """

    # To standardize phone number based on US format
    def us_format_phone(phone):
        try:
            num = phonenumbers.parse(phone, "US")
            if phonenumbers.is_valid_number(num):
                return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
            else:
                return None
        except:
            print("Phone number is invalid")
            return None

    ci_raw_table = 'data_lake_dev.feature_raw_data.ci_raw'
    
    ci_raw = spark.read.table(f"{ci_raw_table}")
    print(f"Read data from {ci_raw_table}")

    # To filter out the rows with NULL values in first_name and last_name
    filter_null_df = ci_raw.filter( ~F.expr("first_name IS NULL AND last_name IS NULL") )

    # To remove special charater in the name
    spec_char_rmv_df = filter_null_df.withColumn(
        "first_name", F.regexp_replace("first_name", r"(?i)[^a-z0-9_-]", "")
    ).withColumn(
        "middle_name", F.regexp_replace("middle_name", r"(?i)[^a-z0-9_-]", "")
    ).withColumn(
        "last_name", F.regexp_replace("last_name", r"(?i)[^a-z0-9_-]", "")
    )

    # # To standardize phone number
    # format_us_phone_udf = F.udf(us_format_phone, StringType())
    # std_phone_df = spec_char_rmv_df.withColumn(
    #     "std_realtor_phone", format_us_phone_udf(F.col("realtor_phone"))
    # )

    # To standardize name -------
    name_df = spec_char_rmv_df.withColumn(
        "std_full_name",
        F.concat_ws(" ", "first_name", "middle_name", "last_name")
    )
    std_name = name_df.withColumn(
        "std_full_name", F.lower( F.col("std_full_name") )
    )

    # Load transformed data to bronze table
    ci_bronze_loc = "data_lake_dev.feature_bronze_data.ci_transformed_bronze"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ci_bronze_loc} USING DELTA")
    std_name.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(ci_bronze_loc)

    print(f"Successfully load transformed data into {ci_bronze_loc}")