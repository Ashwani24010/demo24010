import requests
from pyspark.sql import SparkSession
import json
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("API to Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

api_url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
    print("API data fetched successfully")
else:
    print(f"Failed to fetch data: {response.status_code}")
    exit()

rdd = spark.sparkContext.parallelize(data)
df = spark.read.json(rdd)
df.show()

db_url = "jdbc:postgresql://localhost:5432/mydb"
db_properties = {
    "user": "mt24010",
    "password": "mt24010@m04y24",
    "driver": "org.postgresql.Driver"
}

df.write \
  .format("jdbc") \
  .option("url", db_url) \
  .option("dbtable", "api_data") \
  .option("user", db_properties["user"]) \
  .option("password", db_properties["password"]) \
  .mode("append") \
  .save()

print("Data saved to PostgreSQL successfully.")
