import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime
import boto3
import json
# Set the job name explicitly
job_name = 's3-to-rds'
# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args['JOB_NAME'] = job_name  # Set the job name explicitly
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Current timestamp for processed columns
now = datetime.now()
processed_year = now.year
processed_month = now.month
processed_date = now.strftime("%Y-%m-%d")
processed_time = now.strftime("%H:%M:%S")
# S3 paths
transaction_path = "s3://project-processed-06/transaction_sample/processed_time_year=2025/processed_time_month=11/processed_time_day=19/"
user_path = "s3://project-processed-06/user_sample/processed_time_year=2025/processed_time_month=11/processed_time_day=19/"
clickstream_path = "s3://project-processed-06/clickstream/processed_time_year=2025/processed_time_month=11/processed_time_day=19/"
# Read Parquet files
transaction_df = spark.read.parquet(transaction_path)
user_df = spark.read.parquet(user_path)
clickstream_df = spark.read.parquet(clickstream_path)
# Add processed columns
def add_processed_columns(df):
    return (df.withColumn("processed_year", F.lit(processed_year))
            .withColumn("processed_month", F.lit(processed_month))
            .withColumn("processed_date", F.lit(processed_date))
            .withColumn("processed_time", F.lit(processed_time)))
transaction_df = add_processed_columns(transaction_df)
user_df = add_processed_columns(user_df)
clickstream_df = add_processed_columns(clickstream_df)
# Fetch RDS credentials from Secrets Manager
def get_rds_credentials(secret_name, region_name="us-east-1"):
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        # Fetch the secret value
        secret_value = client.get_secret_value(SecretId=secret_name)
        # Parse the secret value
        if 'SecretString' in secret_value:
            secret = json.loads(secret_value['SecretString'])
        else:
            secret = json.loads(secret_value['SecretBinary'].decode('utf-8'))
        return secret
    except Exception as e:
        print(f"Error fetching RDS credentials: {e}")
        raise e
# Secret name from Secrets Manager
secret_name = "s3-to-rds-job-secret"  # Updated to your secret name
# Fetch the credentials
secret = get_rds_credentials(secret_name)
# RDS connection details from the secret
rds_url = secret["rds_url"]
rds_user = secret["username"]
rds_password = secret["password"]
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# Write to RDS tables
transaction_df.write \
    .format("jdbc") \
    .option("url", rds_url) \
    .option("dbtable", "transaction_table") \
    .option("user", rds_user) \
    .option("password", rds_password) \
    .option("driver", driver) \
    .mode("overwrite") \
    .save()
user_df.write \
    .format("jdbc") \
    .option("url", rds_url) \
    .option("dbtable", "user_table") \
    .option("user", rds_user) \
    .option("password", rds_password) \
    .option("driver", driver) \
    .mode("overwrite") \
    .save()
clickstream_df.write \
    .format("jdbc") \
    .option("url", rds_url) \
    .option("dbtable", "clickstream_table") \
    .option("user", rds_user) \
    .option("password", rds_password) \
    .option("driver", driver) \
    .mode("overwrite") \
    .save()
job.commit()