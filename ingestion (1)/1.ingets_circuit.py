# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step1- ingest circuit.csv file

# COMMAND ----------


# df_circuit = spark.read.csv("/FileStore/tables/circuits-1.csv", header="true", inferSchema="true")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

cicuits_schema = StructType(fields= [StructField('circuitId',IntegerType(),False),
                            StructField('circuitRef',StringType(),True),
                            StructField('name',StringType(),True),
                            StructField('location',StringType(),True),
                            StructField('country',StringType(),True),
                            StructField('lat',DoubleType(),True),
                            StructField('lng',DoubleType(),True),
                            StructField('alt',IntegerType(),True),
                            StructField('url',StringType(),True)])

# COMMAND ----------

df_circuit = spark.read.schema(cicuits_schema).csv(f"{folder_path}/circuits-1.csv", header="true")

# COMMAND ----------

display(df_circuit )
df_circuit .printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step2 - select  only required columns and renaming columns

# COMMAND ----------

# MAGIC %md
# MAGIC #Different way of representing selction criteria
# MAGIC #method-1
# MAGIC circuits_selected_df = df_circuit.select(df_circuit.circuitid,df_circuit.circuitRef,df_circuit.name,df_circuit.location,df_circuit.country,df_circuit.lat,df_circuit.lng,df_circuit.alt)
# MAGIC #method-2
# MAGIC circuits_selected_df = df_circuit.select(df_circuit['circuitid'],df_circuit['circuitRef'],df_circuit['name'],df_circuit['location'],df_circui['country'],df_circuit['lat'],df_circuit['lng'],df_circuit['alt'])
# MAGIC #method-3
# MAGIC from pyspark.sql.functions import col
# MAGIC circuits_selected_df = df_circuit.select(col('circuitid'),col('circuitRef'],col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))
# MAGIC #method-4
# MAGIC circuits_selected_df = df_circuit.select(['circuitid','circuitRef','name','location','country','lat','lng','alt'])

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = df_circuit.select(col('circuitid'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

display(circuits_selected_df )

# COMMAND ----------

from pyspark.sql.functions import  lit
circuit_renamed= circuits_selected_df.withColumnRenamed("circuitid","circuit_id") \
                                    .withColumnRenamed("circuitRef","circuit_Ref") \
                                    .withColumnRenamed("lat","latitude") \
                                    .withColumnRenamed("lng","longitude") \
                                    .withColumnRenamed("alt","altitude") \
                                    .withColumn("Environment",lit("Production"))\
                                    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuit_renamed)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step3 - Adding  a new coumn of current timestamp and add new column with value "production" 

# COMMAND ----------


circuits_final_df= add_ingestion_date(circuit_renamed)
                                  

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 4- write data in Parquet file 

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{folder_path}/circuit_outpt")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.notebook.exit("success")
