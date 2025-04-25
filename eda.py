import pandas as pd
import boto3
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize S3 client
s3_client = boto3.client('s3')

# Specify bucket and file
bucket_name = 'jaytestbucket12'
file_name = '2019.csv'

# Pandas implementation
print("Pandas Implementation:")
response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
df_pandas = pd.read_csv(response['Body'])
print("Pandas DataFrame head:")
print(df_pandas.head())

# PySpark implementation
print("\nPySpark Implementation:")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3DataProcessing") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key", boto3.Session().get_credentials().access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", boto3.Session().get_credentials().secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()


# Read CSV from S3
s3_path = f"s3a://{bucket_name}/{file_name}"
df_spark = spark.read.csv(s3_path, header=True, inferSchema=True)

# Show basic information
print("Spark DataFrame schema:")
df_spark.printSchema()
print("\nSpark DataFrame first 5 rows:")
df_spark.show(5)

# Basic analysis example
print("\nBasic Analysis:")
# Count total rows
print(f"Total number of rows: {df_spark.count()}")

# Show column names
print("\nColumn names:")
print(df_spark.columns)

# Stop Spark session
spark.stop()
