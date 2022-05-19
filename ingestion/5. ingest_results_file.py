# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the Results JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read data into a DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/results.json",
                            schema=results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add, rename and drop columns
# MAGIC 1. Add ingestion_date
# MAGIC 2. Rename resultId to result_id
# MAGIC 3. Rename raceId to race_id
# MAGIC 4. Rename driverId to driver_id
# MAGIC 5. Rename constructorId to constructor_id
# MAGIC 6. Rename positionText to position_text
# MAGIC 7. Rename positionOrder to position_order
# MAGIC 8. Rename fastestLap to fastest_lap
# MAGIC 9. Rename fastestLapTime to fastest_lap_time
# MAGIC 10. Rename fastestLapSpeed to fastest_lap_speed
# MAGIC 11. Drop statusId

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.drop(col("statusId")) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write new file partitioned by race_id

# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")
