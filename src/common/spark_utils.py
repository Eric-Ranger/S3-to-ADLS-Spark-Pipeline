import os
from pyspark.sql import SparkSession

def create_spark(app_name: str = "S3→ADLS Pipeline"):
    """
    Create a Spark session with Hadoop AWS for S3A.
    In Synapse, many connectors are already available.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Add any package refs below if running plain Spark:
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.azure:azure-storage:8.6.6")
    )
    return builder.getOrCreate()


def configure_s3_access(spark, access_key: str, secret_key: str, endpoint: str = None):
    """
    Configure S3A credentials in Spark. If using AWS default provider chain on a cluster,
    this may not be necessary.
    """
    hconf = spark._jsc.hadoopConfiguration()
    if endpoint:
        hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")