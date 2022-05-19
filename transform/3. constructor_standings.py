# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read info into a DataFrame

# COMMAND ----------

driver_standings_df = spark.read.parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Create the DataFrame with the necessary information

# COMMAND ----------

from pyspark.sql.functions import sum

constructor_standings_df = driver_standings_df \
.groupBy("race_year", "team") \
.agg(sum("total_points").alias("team_points"), sum("wins").alias("team_wins"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Create the rank column

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("team_points"), desc("team_wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data into a parquet file

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/constructor_standings")
