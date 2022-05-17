# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the constructors JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file into a DataFrame

# COMMAND ----------

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url String"

# COMMAND ----------

constructor_df = spark.read.json("/mnt/dataformula1lake/raw/constructors.json",
                                schema=constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data into parquet

# COMMAND ----------

constructor_final_df.write.parquet("/mnt/dataformula1lake/processed/constructors", mode="overwrite")
