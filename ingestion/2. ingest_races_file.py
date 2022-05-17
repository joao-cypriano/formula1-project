# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read csv and create DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/races.csv",
                         header=True,
                         schema=races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"),
                                   col("year"),
                                   col("round"),
                                   col("circuitId"),
                                   col("name"),
                                   col("date"),
                                   col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Create race_timestamp column

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, concat

# COMMAND ----------

races_timestamp_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Add ingested time to the dataframe

# COMMAND ----------

races_final_df = add_ingestion_date(races_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.partitionBy("race_year").parquet(f"{processed_folder_path}/races", mode="overwrite")
