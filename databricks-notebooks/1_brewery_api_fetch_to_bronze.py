# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# COMMAND ----------

import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp

# Initialize variables
# 'total_pages' means = set the ammount of list chuncks to be fetched
api_url = "https://api.openbrewerydb.org/v1/breweries"
per_page = 50
total_pages = 10 
all_breweries_data = []

# Fetch data from the API in chunks of 50 items
for page in range(1, total_pages + 1):
    params = {'per_page': per_page, 'page': page}
    response = requests.get(api_url, params=params)
    
    # Check for successful response
    if response.status_code == 200:
        breweries_data = response.json()
        all_breweries_data.extend(breweries_data)
    else:
        print(f"Failed to fetch data from page {page}:", response.status_code)
        break

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
    for item in all_breweries_data
]

# Convert preprocessed data to PySpark DataFrame
df_brewery_api_fetch = spark.createDataFrame(processed_data, schema)

# Add "ts_load_timestamp" column
df_brewery_api_fetch = df_brewery_api_fetch.withColumn("ts_load_timestamp", current_timestamp())

# Count total number of records before removing duplicates
total_records = df_brewery_api_fetch.count()

# Identify duplicates based on 'id'
df_duplicates = df_brewery_api_fetch.groupBy("id").count().filter("count > 1")
duplicates_count = df_duplicates.count()

# Show duplicates found
print(f"Number of duplicate IDs found: {duplicates_count}")
df_duplicates.show(truncate=False)

# Remove duplicates
df_brewery_api_fetch = df_brewery_api_fetch.dropDuplicates(["id"])

# Count total number of records after removing duplicates
final_records = df_brewery_api_fetch.count()
dropped_records = total_records - final_records

# Report the number of records dropped
print(f"Total records before removing duplicates: {total_records}")
print(f"Total records after removing duplicates: {final_records}")
print(f"Number of records dropped: {dropped_records}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the result in azure data lake gen 2 partitioned by State
# MAGIC - deduplication will be done in the silver layer

# COMMAND ----------

display(df_brewery_api_fetch)

# COMMAND ----------

# Save the DataFrame to the specified mount point as a Parquet file
df_brewery_api_fetch.write.format("delta").mode("overwrite").partitionBy("state").save("dbfs:/mnt/files/bronze/")

# COMMAND ----------

# MAGIC %md
# MAGIC **apply Z order optimization only for tables - from silver layer and beyond**
