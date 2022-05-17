# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the pit_stops multi-line JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read file into a DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.json("/mnt/dataformula1lake/raw/pit_stops.json", schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output into a parquet file

# COMMAND ----------

pit_stops_final_df.write.parquet("/mnt/dataformula1lake/processed/pit_stops", mode="overwrite")