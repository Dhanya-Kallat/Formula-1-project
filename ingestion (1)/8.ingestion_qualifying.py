# Databricks notebook source
# MAGIC %md
# MAGIC #Step1 Read multiple JSOn file in a folder using spark data frame

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualifying_Schema= StructType(fields=[StructField("constructorId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("q1",StringType(),True),
                                  StructField("q2",StringType(),True),
                                  StructField("q3",StringType(),True),
                                  StructField("qualifyId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True)
                                  ])

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

qualifying_df= spark.read.schema(qualifying_Schema).option('multiLine',True).json('/FileStore/tables/qualifying_split*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df= qualifying_df.withColumnRenamed('qualifyId','qualify_Id')\
                       .withColumnRenamed('driverId','driver_Id')\
                       .withColumnRenamed('raceId','race_Id')\
                       .withColumnRenamed('constructorId','constructor_Id')\
                       .withColumn('ingestion_date',current_timestamp())
                           

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step4 Write the date to parquet file 

# COMMAND ----------

final_df.write.mode("overwrite"). parquet("/FileStore/tables/qualifying_outpt")
