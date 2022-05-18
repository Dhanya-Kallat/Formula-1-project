# Databricks notebook source
configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
 }
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
   source = "adl://lab1adlgen2.azuredatalakestore.net/raw",
   mount_point = "/mnt/raw",
   extra_configs = configs)

# COMMAND ----------

configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
 }
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
   source = "adl://lab1adlgen2.azuredatalakestore.net/processed",
   mount_point = "/mnt/processed",
   extra_configs = configs)

# COMMAND ----------


