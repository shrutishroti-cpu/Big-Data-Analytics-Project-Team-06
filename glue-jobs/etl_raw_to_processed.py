from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit
from datetime import datetime
# ---------------------------
# CONFIGURATION
# ---------------------------

JOB_NAME = "unified-etl-job"  # Glue Job Name
RAW_BUCKET = "project-raw-06"  # S3 bucket for raw data
PROCESSED_BUCKET = "project-processed-06"  # S3 bucket for processed data
GLUE_DATABASE = "bigdata_project"  # Glue database name
# ---------------------------
# INITIALIZE GLUE CONTEXT
# ---------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

now = datetime.utcnow()
year, month, day = now.year, now.month, now.day
# ---------------------------
# SCHEMAS FOR INPUT FILES
# ---------------------------

users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("signup_channel", StringType(), True)
])
transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("item_category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("created_at", StringType(), True)
])
clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("action", StringType(), True),
    StructField("ts", StringType(), True)
])
# ---------------------------
# HELPER FUNCTIONS
# ---------------------------
def read_csv(path, schema):
    # Reads CSV file from S3 with header and schema
    return spark.read.format("csv").option("header", "true").schema(schema).load(path)
def read_json(path, schema):
    # Reads JSON file from S3 with schema
    return spark.read.format("json").schema(schema).load(path)
def add_partition(df):
    # Adds partition columns for year/month/day
    return df.withColumn("processed_time_year", lit(year)) \
             .withColumn("processed_time_month", lit(month)) \
             .withColumn("processed_time_day", lit(day))
def write_parquet(df, path):
    # Writes valid data in Parquet format partitioned by date
    df.write.mode("overwrite").partitionBy("processed_time_year","processed_time_month","processed_time_day").parquet(path)
def write_errors(df, dataset_name):
    # Writes invalid data to error folder with headers
    error_path = f"s3://{PROCESSED_BUCKET}/error/{dataset_name}/processed_time_year={year}/processed_time_month={month}/processed_time_day={day}/"
    df.write.mode("overwrite").option("header", "true").csv(error_path)
# ---------------------------
# READ RAW DATA FROM S3
# ---------------------------

users_df = read_csv(f"s3://{RAW_BUCKET}/usercsv/users_sample.csv", users_schema)
transactions_df = read_csv(f"s3://{RAW_BUCKET}/transactions-sample/transactions.csv", transactions_schema)
clickstream_df = read_json(f"s3://{RAW_BUCKET}/2025/11/18/*/", clickstream_schema)
# ---------------------------
# DATA QUALITY VALIDATION
# ---------------------------

users_valid = users_df.filter("user_id IS NOT NULL AND age > 0 AND length(signup_date)=10")
users_invalid = users_df.subtract(users_valid)
transactions_valid = transactions_df.filter("price > 0 AND quantity > 0 AND abs(total - (price * quantity)) <= (0.05 * total)")
transactions_invalid = transactions_df.subtract(transactions_valid)
clickstream_valid = clickstream_df.filter("event_id IS NOT NULL AND user_id IS NOT NULL AND length(ts) >= 20")
clickstream_invalid = clickstream_df.subtract(clickstream_valid)
# ---------------------------
# LOG VALID/INVALID COUNTS
# ---------------------------
users_valid_count = users_valid.count()
users_invalid_count = users_invalid.count()
transactions_valid_count = transactions_valid.count()
transactions_invalid_count = transactions_invalid.count()
clickstream_valid_count = clickstream_valid.count()
clickstream_invalid_count = clickstream_invalid.count()
print(f"Users: Valid={users_valid_count}, Invalid={users_invalid_count}")
print(f"Transactions: Valid={transactions_valid_count}, Invalid={transactions_invalid_count}")
print(f"Clickstream: Valid={clickstream_valid_count}, Invalid={clickstream_invalid_count}")
# ---------------------------
# ADD PARTITION COLUMNS TO VALID DATA
# ---------------------------
users_partitioned = add_partition(users_valid)
transactions_partitioned = add_partition(transactions_valid)
clickstream_partitioned = add_partition(clickstream_valid)
# ---------------------------
# WRITE VALID DATA TO PROCESSED BUCKET
# ---------------------------
write_parquet(users_partitioned, f"s3://{PROCESSED_BUCKET}/users_sample/")
write_parquet(transactions_partitioned, f"s3://{PROCESSED_BUCKET}/transactions_sample/")
write_parquet(clickstream_partitioned, f"s3://{PROCESSED_BUCKET}/clickstream/")
# ---------------------------
# WRITE INVALID DATA TO ERROR FOLDER
# ---------------------------
write_errors(users_invalid, "users_sample")
write_errors(transactions_invalid, "transactions_sample")
write_errors(clickstream_invalid, "clickstream")
# ---------------------------
# WRITE SUMMARY FILE TO S3
# ---------------------------
summary_data = [
    ("users_sample", users_valid_count, users_invalid_count),
    ("transactions_sample", transactions_valid_count, transactions_invalid_count),
    ("clickstream", clickstream_valid_count, clickstream_invalid_count)
]
summary_df = spark.createDataFrame(summary_data, ["dataset", "valid_count", "invalid_count"])
summary_path = f"s3://{PROCESSED_BUCKET}/summary/processed_time_year={year}/processed_time_month={month}/processed_time_day={day}/"
summary_df.write.mode("overwrite").option("header", "true").csv(summary_path)
# ---------------------------
# CREATE GLUE DATABASE (OPTIONAL)
# ---------------------------
try:
    glueContext.create_database(name=GLUE_DATABASE)
except Exception as e:
    print(f"Database may already exist: {e}")
# ---------------------------
# COMMIT JOB
# ---------------------------
job.commit()




















