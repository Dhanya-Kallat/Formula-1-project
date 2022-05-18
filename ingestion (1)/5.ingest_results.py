# Databricks notebook source
# MAGIC %md
# MAGIC #Step1 Read JSOn file using spark data frame

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

# COMMAND ----------

results_Schema= StructType(fields=[StructField("resultId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionText",StringType(),True),
                                  StructField("positionOrder",IntegerType(),True),
                                  StructField("points",FloatType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",FloatType(),True),
                                  StructField("statusId",StringType(),True)
  
])

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

results_df= spark.read.schema(results_Schema).json('/FileStore/tables/results-1.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_rename_df= results_df.withColumnRenamed('resultId','result_Id')\
                           .withColumnRenamed('raceId','race_Id')\
                           .withColumnRenamed('driverId','driver_Id')\
                           .withColumnRenamed('constructorId','constructor_Id')\
                           .withColumnRenamed('positionText','position_Text')\
                           .withColumnRenamed('positionOrder','position_Order')\
                           .withColumnRenamed('fastestLap','fastest_Lap')\
                           .withColumnRenamed('fastestLapTime','fastest_Lap_Time')\
                           .withColumnRenamed('fastestLapSpeed','fastest_Lap_Speed')\
                           .withColumn('ingestion_date',current_timestamp())
                           

# COMMAND ----------

# MAGIC %md 
# MAGIC #step3 Drop unwanted columns

# COMMAND ----------

results_final_df= results_rename_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step4 Write the date to parquet file 

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id'). parquet("/FileStore/tables/results_outpt")

# COMMAND ----------


