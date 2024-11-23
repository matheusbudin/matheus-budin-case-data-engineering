# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook Overview:
# MAGIC - In this databricks notebook i am creating a ETL process layer that consists in reading data from the Bronze Folder, that has the data ingested from the API, i will perform some transformations, do some quality checks like look for duplicates and drop them, and finally save all of it into a silver table

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the databases/schemas (each tool calls it in a different way)**

# COMMAND ----------

spark.sql(""" CREATE DATABASE IF NOT EXISTS silver""")
spark.sql(""" CREATE DATABASE IF NOT EXISTS gold""")

# COMMAND ----------

# display(
#     spark.sql("SHOW DATABASES")
# )

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import hashlib
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,read the data ingested in bronze layer
# Read JSON files from the Bronze layer
df_bronze = spark.read.format("delta").load("/mnt/files/bronze/")

# COMMAND ----------

# DBTITLE 1,Define the schema

# Define the expected schema
expected_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("created_at", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Data Cleaning + data transformation
# Cast longitude and latitude to DoubleType
df_clean = df_bronze.withColumn("longitude", col("longitude").cast(DoubleType())) \
                    .withColumn("latitude", col("latitude").cast(DoubleType()))

# drops the rows with missing id
df_clean = df_clean.na.drop(subset=["id"])

# Standardize text columns to lower case
text_columns = ["name", "brewery_type", "city", "state", "country"]
for col_name in text_columns:
    df_clean = df_clean.withColumn(col_name, lower(col(col_name)))

# masking sensitive data (like phone, user cpf, user uuid and so on)
def hash_sensitive_data(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, sha2(col(column_name), 256))

df_clean = hash_sensitive_data(df_clean, "phone")
display(df_clean)

# COMMAND ----------

# DBTITLE 1,Data quality checks
#drops the duplicated rows
df_clean = df_clean.dropDuplicates(["id"])

#drops the rows that has the postal code out of the 'xxxxx-xxxx' format - testing for USA as an example
usa_postal_code_pattern = "^[0-9]{5}(-[0-9]{4})?$|^[0-9]{5}-?$"  # USA format: xxxxx or xxxxx-xxxx

# Filter rows with invalid postal codes for the United States
df_invalid_postal = df_clean.filter(
    (col("country") == "united states") & ~col("postal_code").rlike(usa_postal_code_pattern)
)

# Check for duplicates based on 'id'
df_duplicates = df_clean.groupBy("id").count().filter("count > 1")
if df_duplicates.count() > 0:
    print(f"Warning: i found: {df_duplicates.count()} Duplicates rows found based on 'id'. ")    

#drops the duplicated rows
df_clean = df_clean.dropDuplicates(["id"])

# Validate 'postal_code' format
if df_invalid_postal.count() > 0:
    print("Warning: Invalid postal codes found.")

#filter and test for both phone and website rows NOT NULL
df_clean = df_clean.filter(~(col("phone").isNull() & col("website_url").isNull()))

df_no_contact_information = df_clean.filter((col("phone").isNull() & col("website_url").isNull()))
                                            
if df_no_contact_information.count() > 0:
    print(f"We have {df_no_contact_information.count()} registries that does not have a contact information")


# Log data quality issues
data_quality_issues = df_duplicates.count() + df_invalid_postal.count() + df_no_contact_information.count()
if data_quality_issues > 0:
    raise Exception("We should not carry data with these quality checks not approved")
else:
    fl_quality = 1 #setting a quality check flag in case we need in the future
    print("Our data is good to procced :)")



# COMMAND ----------

# MAGIC %md
# MAGIC **saving the cleaned data**
# MAGIC - if the table does not exists, perform the overwrite and writes a delta table
# MAGIC - otherwise perform the merge statement, making sure that the 'id's will not repeat.

# COMMAND ----------

# MAGIC %md
# MAGIC ** saving as a delta file **

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/files

# COMMAND ----------

# DBTITLE 1,save it partitioned by ts_load from bronze ingestion

# Define the Delta table path
delta_table_path = "dbfs:/mnt/files/silver/"

# Check if the Delta table already exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    # Load the existing Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Perform the merge (upsert)
    delta_table.alias("target").merge(
        df_clean.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()
else:
    # If the Delta table does not exist, write it as a new Delta table
    df_clean.write.partitionBy("ts_load_timestamp") \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)


# COMMAND ----------

# MAGIC %md
# MAGIC **Overrall review for this notebook**:
# MAGIC - this notebook performs some data cleaning such as: droping -> dupicated id's, postal codes not accordinly to a pattern (tested for USA postal codes, can be replicated to other countries), also droped rows that does not have contact information (both phone AND website_url).
# MAGIC - performed a sensitive data masking for phone as an example, could have been for user name, last name, date of birth and so on.
# MAGIC - this logic also protects the data load if the quality checks fail rasing an error that will be caught in datafactory monitoring
# MAGIC - finally we partitioned the data by the load timestamp which is the usual approach in production, but since we only have this column after setting it up on the ingestion to bronze layer, we had to perform a similar approach when saving the data from the API.
