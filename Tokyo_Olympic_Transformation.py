# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": "b3004842-8fc1-46e9-bd88-855e2af19279",
            "fs.azure.account.oauth2.client.secret": 'FAK8Q~ciKaljrMMKENiHY6Q.gLRUEF2gV-6kicL1',
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b115bf2b-4a29-45b8-b4c5-4f0d2a24a424/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyolympicdata@tokyolympicdatapy.dfs.core.windows.net", # container@storageaccount
mount_point = "/mnt/tokyolympicdata",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyolympicdata" 

# COMMAND ----------

spark

# COMMAND ----------

athletes= spark.read.format("csv").option("header", "true").load("/mnt/tokyolympicdata/raw-data/Athlete.csv")
coaches= spark.read.format("csv").option("header", "true").load("/mnt/tokyolympicdata/raw-data/Coaches.csv")
entriesGender= spark.read.format("csv").option("header", "true").load("/mnt/tokyolympicdata/raw-data/EntriesGender.csv")
medals= spark.read.format("csv").option("header", "true").option("InferSchema", "True").load("/mnt/tokyolympicdata/raw-data/Medals.csv")
teams= spark.read.format("csv").option("header", "true").option("InferSchema", "True").load("/mnt/tokyolympicdata/raw-data/Teams.csv")

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

athletes.repartition(3).write.mode("overwrite").option("Header", "True").csv("/mnt/tokyolympicdata/transformed-data/athletes.csv")

# COMMAND ----------

entriesGender.show()

# COMMAND ----------

entriesGender.printSchema()

# COMMAND ----------

entriesGender= entriesGender.withColumn("Female",col("Female").cast(IntegerType())) \
    .withColumn("Male", col ("Male").cast(IntegerType())) \
    .withColumn("Total", col ("Total").cast(IntegerType()))

# COMMAND ----------

entriesGender.write.mode("overwrite").option("Header", "True").csv("/mnt/tokyolympicdata/transformed-data/EntriesGender.csv")

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

coaches.write.mode("overwrite").option("Header", "True").csv("/mnt/tokyolympicdata/transformed-data/Coaches.csv")

# COMMAND ----------

medals.show()

# COMMAND ----------

medals= medals.withColumn("Rank", col("Rank").cast(IntegerType()))\
    .withColumn("Gold", col("Gold").cast(IntegerType())) \
    .withColumn("Silver", col("Silver").cast(IntegerType())) \
    .withColumn("Bronze", col("Bronze").cast(IntegerType())) \
    .withColumn("Total", col("Total").cast(IntegerType())) \
    .withColumn("Rank by Total", col("Rank by Total").cast(IntegerType()))

# COMMAND ----------

medals.write.mode("overwrite").option("Header", "True").csv("/mnt/tokyolympicdata/transformed-data/Medals.csv")

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

teams.write.mode("overwrite").option("Header", "True").csv("/mnt/tokyolympicdata/transformed-data/Teams.csv")

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries= medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold").show()

# COMMAND ----------

# Calculate the average number of entries by gender for discipline
average_entries_by_gender = entriesGender.withColumn(
    'Avg_Female', entriesGender['Female'] / entriesGender['Total']
).withColumn(
    'Avg_Male', entriesGender['Male'] / ( entriesGender['Total'])
)
average_entries_by_gender.show()
