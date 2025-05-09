// Databricks notebook source

val diamondsDf = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")


// COMMAND ----------

diamondsDf.show()

// COMMAND ----------

diamondsDf.count()

// COMMAND ----------

val filteredDf = diamondsDf.filter(diamondsDf("cut") === "Ideal" && diamondsDf("price") > 1000)

// COMMAND ----------

filteredDf.count()

// COMMAND ----------

// MAGIC %python
// MAGIC diamonds_df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
// MAGIC
// MAGIC filtered_df = diamonds_df.filter((diamonds_df["cut"] == "Ideal") & (diamonds_df["price"] > 1000))
// MAGIC

// COMMAND ----------

filtered_df.show()

// COMMAND ----------

from pyspark.sql.functions import avg


grouped_df = filtered_df.groupBy("clarity").agg(avg("price"))

// COMMAND ----------

import org.apache.spark.sql.functions.avg


val grouped_df = filteredDf.groupBy("clarity").agg(avg("price"))

// COMMAND ----------

grouped_df.show()

// COMMAND ----------

val orderedDf = grouped_df.orderBy(grouped_df("avg(price)").desc)

// COMMAND ----------

orderedDf.show()

// COMMAND ----------

val orderedDfNew = orderedDf.withColumnRenamed("avg(price)","avg_price")

// COMMAND ----------

orderedDfNew.show()