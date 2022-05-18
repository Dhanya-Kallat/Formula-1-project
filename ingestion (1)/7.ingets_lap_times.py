# Databricks notebook source
# MAGIC %md 
# MAGIC # Step1- ingest mutiple CSV files (folder)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema = StructType(fields= [StructField('raceId',IntegerType(),False),
                            StructField('driverId',IntegerType(),True),
                            StructField('lap',IntegerType(),True),
                            StructField('position',IntegerType(),True),
                            StructField('time',StringType(),True),
                            StructField('milliseconds',IntegerType(),True)
                                    ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv("/FileStore/tables/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step2 - select  only required columns and renaming columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df= lap_times_df.withColumnRenamed('driverId','driver_Id')\
                           .withColumnRenamed('raceId','race_Id')\
                           .withColumn('ingestion_date',current_timestamp())
                           

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3- write data in Parquet file 

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/FileStore/tables/lap_times_outpt")
