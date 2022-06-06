# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    """Adds a ingestion_date column to a DataFrame with the current timestamp.
    
    Args:
        input_df: DataFrame that you wish to add the ingestion_date column.
    
    Returns:
        DataFrame equal to the input_df with the ingestion_date column added.
        """
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    """Re-arranges the DataFrame so that the column that the data is to be partitioned by is positioned as the last one.
    
    Args:
        input_df: Base DataFrame that needs the columns to be re-arranged.
        partition_column: Column that the data will be partitioned by.
        
    Returns:
        DataFrame with the columns re-arranged.
        """
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    """Uses the overwrite mode to write data as parquet files and DataBase tables.
    
    Args:
        input_df: DataFrame that is to be written as parquet files and DB Table.
        db_name: String containing the name of the DataBase that the data will be written into.
        table_name: String containing the name of the Table that the data will be written into.
        partition_column: Column that will be the one to partition the data by.
        
    Returns:
        None.
        """
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    """Creates a list with the distinct values in a column in a DataFrame.
    
    Args:
        input_df: DataFrame that is to be searched.
        column_name: String containing the name of the column you want to get the distinct values.
        
    Returns:
        column_value_list: list containing all distinct values in the column.
        """
    df_row_list = input_df.select(column_name) \
    .distinct() \
    .collect()
    
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    """Writes a DataFrame's data into the delta lake, using incremental load.
    
    Args:
        input_df: DataFrame with the data to be written into the delta lake.
        db_name: String containing the name of the DataBase that the data will be written into.
        table_name: String containing the name of the Table that the data will be written into.
        folder_path: Path to the folder that the delta file will be written into.
        merge_condition: String containing the merge conditions.
        partition_column: Column that the data will be partitioned by.
        
    Returns:
        None
        """
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
