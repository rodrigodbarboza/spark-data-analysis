import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Analysis") \
    .getOrCreate()

# Load data
data_path = 'data/sample_data.csv'
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Show the data
df.show()

# Basic analysis
# 1. Count of records
record_count = df.count()
print(f"Total records: {record_count}")

# 2. Average salary
average_salary = df.groupBy().avg("salary").first()[0]
print(f"Average salary: {average_salary}")

# 3. Count by age
age_count = df.groupBy("age").count().show()

# Stop the Spark session
spark.stop()
