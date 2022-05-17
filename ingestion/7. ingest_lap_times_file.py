# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read csv files into a DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.csv("/mnt/dataformula1lake/raw/lap_times", schema=lap_times_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output into a parquet file

# COMMAND ----------

lap_times_final_df.write.parquet("/mnt/dataformula1lake/processed/lap_times", mode="overwrite")
