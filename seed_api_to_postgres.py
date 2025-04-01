import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *


spark = SparkSession.builder\
    .appName("MySparkApp")\
    .config("spark.jars", "/path/to/postgresql-42.7.1.jar")\
    .getOrCreate()

#fetching all the conf
username = spark.conf.get("postgres-user-name","Not Set")
password = spark.conf.get("postgres-user-pass","Not Set")
db_url = spark.conf.get("db_url","Not Set")

url = "https://jsonplaceholder.typicode.com/posts" 
response = requests.get(url)
res_json = response.json()

json_schema = StructType([
    StructField("userId",IntegerType(),True),
    StructField("id",IntegerType(),True),
    StructField("title",StringType(),True),
    StructField("body",StringType(),True)
])

df = spark.createDataFrame(data=res_json,schema=json_schema)

db_props = {
    "user":username,
    "password":password,
    "driver":"org.postgresql.Driver"
}


print(res_json)

df.write.jdbc(
    url=db_url,
    table="posts1504",
    mode="overwrite",
    properties=db_props
)

