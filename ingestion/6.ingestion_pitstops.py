# Databricks notebook source
# MAGIC %md
# MAGIC #Step1 Read JSOn file using spark data frame

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_Schema= StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("stop",StringType(),True),
                                  StructField("lap",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("duration",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True)
                                  ])

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

pit_stops_df= spark.read.schema(pit_Schema).option('multiLine',True).json('/FileStore/tables/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df= pit_stops_df.withColumnRenamed('driverId','driver_Id')\
                           .withColumnRenamed('raceId','race_Id')\
                           .withColumn('ingestion_date',current_timestamp())
                           

# COMMAND ----------

# MAGIC %md
# MAGIC #Step4 Write the date to parquet file 

# COMMAND ----------

final_df.write.mode("overwrite"). parquet("/FileStore/tables/pit_stops_outpt")
