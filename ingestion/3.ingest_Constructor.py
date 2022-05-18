# Databricks notebook source
# MAGIC %md
# MAGIC #Step1 Read JSOn file using spark data frame

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df= spark.read.schema(constructor_schema).json(f"{folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step2 - Drop unwanted columns

# COMMAND ----------

constructor_drop_df= constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #Step3 Add ingestiondate and rename column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

Construct_Final_df= constructor_drop_df.withColumn("ingestion_date",current_timestamp()) \
                                        .withColumnRenamed("constructorId",'constructor_Id')\
                                        .withColumnRenamed("constructorRef",'constructor_Ref')\
                                       .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step4  write in to parquet file 

# COMMAND ----------

Construct_Final_df.write.mode("overwrite").parquet(f"{folder_path}/construct_outpt")

# COMMAND ----------


