# Databricks notebook source
# MAGIC %md 
# MAGIC #Step1 -Read race file 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

race_schema= StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitId",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",DateType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)
                                
  
])

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

race_df= spark.read\
        .option('header', True) \
        .schema(race_schema)\
        .csv(f"{folder_path}/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step2 - Adding ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

race_time_df= race_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                .withColumn("data_source",lit(v_data_source))
                              

# COMMAND ----------

race_timestamp_df = add_ingestion_date(race_time_df)

# COMMAND ----------

display(race_timestamp_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3 select the required columns and renaming column

# COMMAND ----------

race_select_df= race_timestamp_df.select(col('raceid').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitid').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))


# COMMAND ----------

display(race_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4 write the output in parquet file format 

# COMMAND ----------

race_select_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{folder_path}/race_outpt")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/race_outpt

# COMMAND ----------

display(spark.read.parquet('/FileStore/tables/race_outpt/race_year=2020/'))

# COMMAND ----------


