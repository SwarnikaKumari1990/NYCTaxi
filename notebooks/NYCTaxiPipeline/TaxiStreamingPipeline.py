# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC Class.forName("org.apache.spark.sql.eventhubs.EventHubsSource")

# COMMAND ----------

# Namespace Connection String

namespaceConnectionString = "Endpoint=sb://taxisourceeventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Ty4+BRcOkXdIP2mNpnyiVWHG3oFTcotS7UNe1XemRAQ="

# EventHub name

eventHubName = "taxisourceeventhub"

#Event Hub Connection string and Configuration

eventHubConnectionString = namespaceConnectionString+";EntityPath="+eventHubName
eventHubConfiguration = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString)
}

# COMMAND ----------

# cretae a straming DataFram

inputDF = (
            spark
                  .readStream
                  .format("eventhubs")
                  .options(**eventHubConfiguration)
                  .load()
            
          )

# COMMAND ----------

# Add The sink

streamingMemoryQuery = (
                          inputDF
                                 .writeStream
                                 .queryName("MemoryQuery")
                                 .format("memory")
                                 .trigger(processingTime = '10 seconds')
                                 .start()
                        )

# COMMAND ----------

display(
        inputDF,
        streamName = "DisplayMemoryQuery",
        processingTime = '10 seconds'
      )

# COMMAND ----------

from pyspark.sql.functions import *

rawDF = (
          inputDF
                .withColumn(
                              "rawdata",
                              col("body").cast("String")
                            )
                .select("rawdata")
        )

display(
          rawDF,
          streamName = "DisplayMemoryQuery",
          processingTime = '10 seconds'
       )

# COMMAND ----------

from pyspark.sql.types import *
schema =(
          StructType()
                .add("Id","integer")
                .add("VendorId","integer")
                .add("PickupTime","timestamp")
                .add("Cablicense","string")
                .add("DriverLicense","string")
                .add("PickupLocationsId","integer")
                .add("PassendgerCount","integer")
                .add("RateCodeId","integer")
        )

rawDF = (
          inputDF
                .withColumn(
                              "rawdata",
                              col("body").cast("String")
                            )
                .select("rawdata")
                .select(
                          from_json(
                                      col("rawdata"),
                                      schema
                                    )
                          .alias("taxidata")
                        )
                .select(
                          "taxidata.Id",
                          "taxidata.VendorId",
                          "taxidata.PickupTime",
                          "taxidata.Cablicense",
                          "taxidata.DriverLicense",
                          "taxidata.PickupLocationsId",
                          "taxidata.PassendgerCount",
                          "taxidata.RateCodeId"
                        )  
          
        )

display(
          rawDF,
          streamName = "DisplayMemoryQuery",
          processingTime = '10 seconds'
       )

# COMMAND ----------

# transformed Datafrom

transformedDF = (
                  rawDF
                      .withColumn("TripType",
                                         when(
                                               col("RateCodeId") == "6",
                                                   "SharedTrip"
                                              )
                                          .otherwise("Solotripe")
                                 )
                        .drop("RateCodeId")
                        .where("PassendgerCount > 0")
                 )


# COMMAND ----------

#from  pyspark.sql.avro import *
##from pyspark.sql.types import *
# cretae a straming DataFram

#inputDF1 = (
#           spark
#                  .readStream
#                  .format("eventhubs")
#                  .options(**eventHubConfiguration)
#                  .load()
#                  .select(
#                            from_avro($"key", SchemaBuilder.builder().StringType()).as("key"),
#                            from_arvo($"value", SchemaBuilder.builder().intType()).as()
#                          )
#            
#          )

 rawStreamingFileQuery = (
                          rawDF
                              .writeStream
                              .queryName("RawTaxiQuery")
                              .format("csv")
                              .option("path","/mnt/taxioutput/Raw/")
                              .option("checkpointLocation","/mnt/taxioutput/checkpointRaw")
                              .trigger(processingTime = '10 seconds')
                              .start()
                        )

# COMMAND ----------

