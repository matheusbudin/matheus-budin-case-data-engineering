# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# COMMAND ----------

# Fetch data from the API
api_url = "https://api.openbrewerydb.org/v1/breweries"
response = requests.get(api_url)

# Check for successful response
if response.status_code == 200:
    breweries_data = response.json()
else:
    print("Failed to fetch data:", response.status_code)
    breweries_data = []

# Define schema explicitly
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True)
])

# Preprocess JSON to ensure consistency
processed_data = [
    {
        "id": item.get("id"),
        "name": item.get("name"),
        "brewery_type": item.get("brewery_type"),
        "street": item.get("street"),
        "city": item.get("city"),
        "state": item.get("state"),
        "postal_code": item.get("postal_code"),
        "country": item.get("country"),
        "longitude": float(item["longitude"]) if item.get("longitude") else None,
        "latitude": float(item["latitude"]) if item.get("latitude") else None,
        "phone": item.get("phone"),
        "website_url": item.get("website_url")
    }
    for item in breweries_data
]

# Convert preprocessed data to PySpark DataFrame
df_brewery_api_fetch = spark.createDataFrame(processed_data, schema)

# Show the schema
df_brewery_api_fetch.printSchema()

# Display the data
display(df_brewery_api_fetch)

# COMMAND ----------

# Save the DataFrame to the specified mount point as a Parquet file
df_brewery_api_fetch.write.format("delta").mode("overwrite").partitionBy("state").save("dbfs:/mnt/files/bronze/")

# COMMAND ----------

# MAGIC %md
# MAGIC **apply Z order optimization only for tables - from silver layer and beyond**
