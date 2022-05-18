# Databricks notebook source
v_result =dbutils.notebook.run("1.ingets_circuit",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

dbutils.notebook.run("2.ingest_race",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("3.ingest_Constructor",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("4.ingest_driver",0)

# COMMAND ----------

dbutils.notebook.run("5.ingest_results",0)

# COMMAND ----------

dbutils.notebook.run("6.ingestion_pitstops",0)

# COMMAND ----------

dbutils.notebook.run("7.ingets_lap_times",0)

# COMMAND ----------

dbutils.notebook.run("8.ingestion_qualifying",0)
