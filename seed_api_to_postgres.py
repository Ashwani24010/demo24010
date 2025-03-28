import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("API to Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

# API URL (returns a list of JSON objects)
api_url = "https://jsonplaceholder.typicode.com/posts"
try:
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Unexpected data format: Expected a list of JSON objects.")
    print("API data fetched successfully.")
except (requests.RequestException, ValueError) as e:
    print(f"Error fetching or parsing API data: {e}")
    exit()

# Define schema for DataFrame to ensure data consistency
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

# Convert JSON data to DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show()

# Database connection details
db_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
db_properties = {
    "user": "mt24010",
    "password": "mt24010@m04y24",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL
try:
    df.write \
      .format("jdbc") \
      .option("url", db_url) \
      .option("dbtable", "posts1504") \
      .option("user", db_properties["user"]) \
      .option("password", db_properties["password"]) \
      .mode("append") \
      .save()
    print("Data saved to PostgreSQL successfully.")
except Exception as e:
    print(f"Error while writing to PostgreSQL: {e}")
