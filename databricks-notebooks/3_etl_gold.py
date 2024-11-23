# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC - Gold layer usually has aggregations that can introduce or represents some kpi's that will help business units to make precise decisions.
# MAGIC - this layer is very important, once it can impact directly into the company revenue.

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

schema = 'gold'
table = 'brewery_aggregated'
table_full_name = f'{schema}.{table}'
data_lake_target = 'https://storagecasebudin.blob.core.windows.net/api-pipeline/gold/'

# COMMAND ----------

# DBTITLE 1,read the data from silver layer
df_silver = spark.read.format("delta").load("/mnt/silver/")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# DBTITLE 1,perfom aggregations
# Aggregate the number of breweries per type and state
df_gold = df_silver.groupBy("name", "brewery_type", "state", "country", "city") \
                   .agg(count("id").alias("brewery_count")) \
                   .withColumn("ts_loadtimestamp_gold", current_timestamp())


# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Save the aggregated table

# COMMAND ----------

from delta.tables import DeltaTable

# Define the path and table details
data_lake_target = "dbfs:/mnt/files/gold/"
schema = "gold"
table = "brewery_aggregated"
table_full_name = f"{schema}.{table}"

# Check if the table exists
if DeltaTable.isDeltaTable(spark, data_lake_target):
    # Load the existing Delta table
    delta_table = DeltaTable.forPath(spark, data_lake_target)

    # Perform a merge (upsert) operation
    delta_table.alias("target").merge(
        df_gold.alias("source"),
        "target.id = source.id"  # Merge condition based on 'id'
    ).whenMatchedUpdateAll(  # Update all columns if the id matches
    ).whenNotMatchedInsertAll(  # Insert new rows if the id does not match
    ).execute()
else:
    # If the table does not exist, write it as a new Delta table
    df_gold.write \
        .format('delta') \
        .mode('overwrite') \
        .option('path', data_lake_target) \
        .option('overwriteSchema', True) \
        .save()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/files/gold/coalesce"

# Reduzindo para uma única partição antes de salvar
df_gold.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

