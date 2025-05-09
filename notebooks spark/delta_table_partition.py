# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC ls /databricks-datasets/nyctaxi/tripdata/yellow/

# COMMAND ----------

nytaxi_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("codec", "gzip") \
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

nytaxi_df.count()

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

nytaxi_df.write.format("delta").mode("append").saveAsTable("nytaxi_yellow_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_name, count(*) as count
# MAGIC FROM nytaxi_yellow_2
# MAGIC WHERE Payment_Type = "Credit" 
# MAGIC GROUP BY vendor_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL nytaxi_yellow_2

# COMMAND ----------

nytaxi_df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("codec", "gzip").load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-02.csv.gz")
nytaxi_df2.write.format("delta").mode("append").saveAsTable("nytaxi_yellow_2")

# COMMAND ----------

nytaxi_df3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("codec", "gzip").load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-03.csv.gz")
nytaxi_df3.write.format("delta").mode("append").saveAsTable("nytaxi_yellow_2")


# COMMAND ----------

nytaxi_df5 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("codec", "gzip").load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-05.csv.gz")
nytaxi_df5.write.format("delta").mode("append").saveAsTable("nytaxi_yellow_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY nytaxi_yellow_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL nytaxi_yellow_2

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /user/hive/warehouse/nytaxi_yellow_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_name, count(*) as count
# MAGIC FROM nytaxi_yellow_2
# MAGIC WHERE Payment_Type = "Credit" 
# MAGIC GROUP BY vendor_name

# COMMAND ----------

yellow_df = spark.sql("select * from nytaxi_yellow_2")

# COMMAND ----------

yellow_df.write.format("delta").partitionBy("Payment_Type").mode("overwrite").saveAsTable("nytaxi_yellow_2_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL nytaxi_yellow_2_part

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/nytaxi_yellow_2_part

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/nytaxi_yellow_2_part/Payment_Type=CREDIT/

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_name, count(*) as count
# MAGIC FROM nytaxi_yellow_2_part
# MAGIC WHERE Payment_Type = "Credit" 
# MAGIC GROUP BY vendor_name

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE nytaxi_yellow_2 ZORDER BY Payment_Type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_name, count(*) as count
# MAGIC FROM nytaxi_yellow_2_part
# MAGIC WHERE Payment_Type = "Credit" 
# MAGIC GROUP BY vendor_name