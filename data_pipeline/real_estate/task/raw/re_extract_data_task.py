from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from data_pipeline.core.util import get_dbutils

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()


def etl_process(**options):
    """
    Extract delta data from parquet file and populate into re_raw table:
        - Read latest batch and populate to re_raw + new column: batch_id
    """
    
    dbutils = get_dbutils(spark)

    # Fetch all the files in the folder
    based_path = "/Volumes/data_lake_dev/feature_raw_data/real_estate_parquet/"
    files = dbutils.fs.ls(based_path)

    # Get the latest file
    date = [ int(file.name.rstrip("/")) for file in files]
    latest_date = max(date)
    
    print(f"Loading latest data: {latest_date}")

    # Read the latest file and add batch_id
    df = spark.read.parquet(based_path + str(latest_date))
    re_df = df.withColumn("batch_id", F.lit(latest_date))

    re_raw_loc = "data_lake_dev.feature_raw_data.re_raw"
    # Table existence check and append the table
    spark.sql(f"CREATE TABLE IF NOT EXISTS {re_raw_loc} USING DELTA")
    re_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(re_raw_loc)

    print(f"Successfully loaded data into {re_raw_loc}")