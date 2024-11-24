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

# DBTITLE 1,perfom aggregations
# Aggregate the number of breweries per type and state
df_gold = df_silver.groupBy("name", "brewery_type", "state", "country", "city") \
                   .agg(count("id").alias("brewery_count")) \
                   .withColumn("ts_loadtimestamp_gold", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Save the aggregated table

# COMMAND ----------

df_gold.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy("ts_loadtimestamp_gold") \
    .option('path', data_lake_target) \
    .option('overwriteSchema', True) \
    .save()

# COMMAND ----------

spark.sql(f'OPTIMIZE budinworkspac.gold.gold_brewery_aggregated')
