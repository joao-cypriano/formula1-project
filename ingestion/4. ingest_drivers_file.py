# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the drivers JSON file 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read data into a DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.json("/mnt/dataformula1lake/raw/drivers.json",
                       schema=drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Save data as parquet type

# COMMAND ----------

drivers_final_df.write.parquet("/mnt/dataformula1lake/processed/drivers", mode="overwrite")
