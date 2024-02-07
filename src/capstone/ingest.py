import json
import logging
from typing import Dict

import boto3
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

load_dotenv()
S3_DATA = os.environ["S3_DATA"]
SECRET_ARN = os.environ["SECRET_ARN"]
SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_SOURCE_NAME = os.environ["SNOWFLAKE_SOURCE_NAME"]


def get_snowflake_credentials():
    sms = boto3.client("secretsmanager", region_name="eu-west-1")
    secret = sms.get_secret_value(SecretId=SECRET_ARN)

    return json.loads(secret["SecretString"])


def get_spark_session(name: str | None = None) -> SparkSession:
    return (
        SparkSession.builder
        # .config(
        #     "spark.jars.packages",
        #     ",".join(
        #         [
        #             "org.apache.hadoop:hadoop-aws:3.3.6",
        #             "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
        #             "net.snowflake:snowflake-jdbc:3.14.1",
        #         ]
        #     ),
        # )
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .appName(name)
        .getOrCreate()
    )


def snowflake_config() -> Dict[str, str]:
    credentials = get_snowflake_credentials()
    return {
        "sfURL": credentials["URL"],
        "sfUser": credentials["USER_NAME"],
        "sfPassword": credentials["PASSWORD"],
        "sfDatabase": credentials["DATABASE"],
        "sfWarehouse": credentials["WAREHOUSE"],
        "sfRole": credentials["ROLE"],
        "sfSchema": SNOWFLAKE_SCHEMA,
    }


if __name__ == "__main__":
    spark = get_spark_session("ingest")
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Reading data from S3...")
    df = spark.read.json(S3_DATA)

    clean = df.select(
        [
            sf.col(colname)
            if df.schema[colname].dataType.typeName() != "struct"
            else sf.col(f"{colname}.*")
            for colname in df.columns
        ]
    )

    logger.info("Writing data to Snowflake...")

    clean.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_config()).option(
        "dbtable", "open_aq"
    ).mode("overwrite").save()

    logger.info("Done!")
