import pandas as pd
import boto3
from io import StringIO

# Initialize S3 client
s3_client = boto3.client('s3')

# Specify bucket and file
bucket_name = 'jaytestbucket12'
file_name = '2019.csv'

# Get the object from S3
response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

# Read the CSV data
df = pd.read_csv(response['Body'])

# Display the first few rows
print(df.head())
