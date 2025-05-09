# Databricks notebook source
# MAGIC %fs 
# MAGIC
# MAGIC ls /databricks-datasets/nyctaxi/tripdata/yellow/

# COMMAND ----------

nytaxi_df = spark.read.format("csv") \
                .option("header","true") \
                .option("inferschema","true") \
                .option("codec","gzip") \
                .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

nytaxi_df.count()

# COMMAND ----------

nytaxi_df.show()

# COMMAND ----------

nytaxi_df.write.format("delta").mode("append").saveAsTable("nytaxi_yellow_trips")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled","false")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with vendors As (
# MAGIC   select distinct vendor_name
# MAGIC   from nytaxi_yellow_trips)
# MAGIC select count(*) AS distinct_vendor_count
# MAGIC from vendors

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with vendors As (
# MAGIC   select distinct vendor_name
# MAGIC   from nytaxi_yellow_trips)
# MAGIC select count(*) AS distinct_vendor_count
# MAGIC from vendors

# COMMAND ----------

distinct_vendor_count = nytaxi_df.select("vendor_name").distinct().count()

# COMMAND ----------

distinct_vendor_count

# COMMAND ----------

# MAGIC %sql
# MAGIC select vendor_name,count(*) as count 
# MAGIC from nytaxi_yellow_trips
# MAGIC where Payment_Type = "Credit" and fare_Amt > 10
# MAGIC group by vendor_name