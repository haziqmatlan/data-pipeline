from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
import base64
import psycopg2
import pandas as pd
from datetime import datetime
from we.pipeline.core.util import get_dbutils, get_local_dbutils, WE_PIPELINE_SCOPE
dbutils = get_dbutils(spark) or get_local_dbutils()
secrets = dbutils.secrets

_PG_JDBC_DRIVER = str(secrets.get(WE_PIPELINE_SCOPE, "PG_JDBC_DRIVER"))
_PG_PASSWORD = str(secrets.get(WE_PIPELINE_SCOPE, "PG_PASSWORD"))
_PG_USERNAME = str(secrets.get(WE_PIPELINE_SCOPE, "PG_USERNAME"))


def read_pg_data_from_query(query):
    url = _PG_JDBC_DRIVER
    properties = {
        "user": _PG_USERNAME,
        "password": _PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    query = f"({query}) as temp_table"
    df = spark.read.jdbc(url=url, table=query, properties=properties)

    return df