# Databricks notebook source
# Mount Azure storage account

azureAccountClientId = "07db2ddb-5f3c-42d5-bb07-f01aa1484296"
azureAccountClientSecrete = "boapdGwjw_JCPd59MIxBwpgn3zfN-_eo_2"
azureAccountDirectoryId = "https://login.microsoftonline.com/c107dcff-2947-405e-9057-6603ed562b17/oauth2/token"
azureStorageFilesystem = "abfss://taxioutput@databrickslearningsa.dfs.core.windows.net/"


# add service Principal Credentials Information in configs

configs = {"fs.azure.account.auth.type":"OAuth",
           "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": azureAccountClientId,
           "fs.azure.account.oauth2.client.secret": azureAccountClientSecrete,
           "fs.azure.account.oauth2.client.endpoint": azureAccountDirectoryId
          }



# COMMAND ----------

# Mount data Lake Gen2 account

dbutils.fs.mount( source = "abfss://taxioutput@databrickslearningsa.dfs.core.windows.net/",
                  mount_point = "/mnt/taxioutput",
                  extra_configs = configs)

# COMMAND ----------

display(
          dbutils.fs.ls("/mnt/taxioutput")

)

# COMMAND ----------

