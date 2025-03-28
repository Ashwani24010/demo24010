import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("API to Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

# API URL (returns a list of JSON objects)
api_url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
    print("API data fetched successfully")
else:
    print(f"Failed to fetch data: {response.status_code}")
    exit()

# Convert JSON data to DataFrame
if isinstance(data, list):
    df = spark.createDataFrame(data)
    df.show()
else:
    print("Data is not in list format")
    exit()

# Database connection details
db_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
db_properties = {
    "user": "mt24010",
    "password": "mt24010@m04y24",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL
df.write \
  .format("jdbc") \
  .option("url", db_url) \
  .option("dbtable", "posts1504") \
  .option("user", db_properties["user"]) \
  .option("password", db_properties["password"]) \
  .mode("append") \
  .save()

print("Data saved to PostgreSQL successfully.")
