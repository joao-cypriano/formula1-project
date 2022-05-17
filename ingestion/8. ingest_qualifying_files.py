# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON files into a DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/qualifying",
                                schema=qualifying_schema,
                                multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Switch from camelCase to snake_case
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyingId", "qualifying_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output into a parquet file

# COMMAND ----------

qualifying_final_df.write.parquet(f"{processed_folder_path}/qualifying", mode="overwrite")
