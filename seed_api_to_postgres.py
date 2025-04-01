import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.jars", "/path/to/postgresql-42.7.1.jar")  # Ensure the correct path to your JDBC driver
    .getOrCreate()

# Access SparkContext and configuration
sc = spark.sparkContext
conf = sc.getConf()

# Fetching all the configuration properties from Spark
username = conf.get("postgres-user-name", "Not Set")
password = conf.get("postgres-user-pass", "Not Set")
db_url = conf.get("db_url", "Not Set")

# Fetch data from API
url = "https://picsum.photos/v2/list"
response = requests.get(url)
res_json = response.json()

# Ensure the response is in the expected format (list of dictionaries)
if not isinstance(res_json, list):
    print("Error: Expected a list of JSON objects from the API")
    exit()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("width", IntegerType(), True),
    StructField("height", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("download_url", StringType(), True)
])

# Create a DataFrame from the JSON data using the schema
df = spark.createDataFrame(res_json, schema=schema)

# Database connection properties
db_props = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Print the DataFrame (Optional)
df.show()

# Write DataFrame to PostgreSQL
df.write.jdbc(
    url=db_url,  # Ensure this URL is in the format jdbc:postgresql://<host>:<port>/<database>
    table="images",  # Ensure this table exists in your PostgreSQL database
    mode="overwrite",  # Use "append" to add data without overwriting
    properties=db_props
)

print("Data successfully written to PostgreSQL.")
