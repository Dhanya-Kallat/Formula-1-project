# Databricks notebook source
# MAGIC %md
# MAGIC #Step1 Read JSOn file using spark data frame

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------



# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
  
])

# COMMAND ----------

driver_Schema= StructType(fields=[StructField("driverId",IntegerType(),False),
                                  StructField("driverRef",StringType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("code",StringType(),True),
                                  StructField("name",name_schema),
                                  StructField("dob",DateType(),True),
                                  StructField("nationality",StringType(),True),
                                  StructField("url",StringType(),True)
  
])

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

driver_df= spark.read.schema(driver_Schema).json('/FileStore/tables/drivers_1.json')

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Step2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,concat

# COMMAND ----------

driver_rename_df= driver_df.withColumnRenamed('driverId','driver_Id')\
                           .withColumnRenamed('driverRef','driver_Ref')\
                           .withColumn('ingestion_date',current_timestamp())\
                           .withColumn('name',concat(col("name.forename"),lit(' '),col('name.surname')))

# COMMAND ----------

display(driver_rename_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #step3 Drop unwanted columns

# COMMAND ----------

driver_final_df= driver_rename_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step4 Write the date to parquet file 

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet("/FileStore/tables/driver_outpt")

# COMMAND ----------


