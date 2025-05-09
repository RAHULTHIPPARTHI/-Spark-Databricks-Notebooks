# Databricks notebook source
diamonds_df = spark.read.format("csv").option("header","true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

# COMMAND ----------

diamonds_df.write.format("delta").mode("overwrite").saveAsTable("diamond")

# COMMAND ----------

diamonds_df.count()

# COMMAND ----------

diamonds_df.show()

# COMMAND ----------

spark.sql("select count(*) from diamond").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail diamond

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table diamond

# COMMAND ----------

from pyspark.sql.functions import avg

filtered_df = diamonds_df.filter((diamonds_df["cut"] == "Ideal") & (diamonds_df["price"] > 1000 ))
grouped_df = filtered_df.groupBy("clarity").agg(avg("price"))
ordered_df = grouped_df.orderBy(grouped_df["avg(price)"].desc())

# COMMAND ----------

display(ordered_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select clarity,avg(price) as avg_price
# MAGIC from(select * from diamond where cut = 'Ideal' and price > 1000
# MAGIC )
# MAGIC group by clarity
# MAGIC order by avg_price desc

# COMMAND ----------

