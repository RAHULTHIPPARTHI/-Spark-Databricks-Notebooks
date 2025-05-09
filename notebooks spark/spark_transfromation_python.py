# Databricks notebook source
# load the "diamond" datasets
diamond_df = spark.read.format("csv").option("header","true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

# COMMAND ----------

diamond_df.show()

# COMMAND ----------

display(diamond_df)

# COMMAND ----------

diamond_df.count()

# COMMAND ----------

filtered_df = diamond_df.filter((diamond_df["cut"] == "Ideal") & (diamond_df["price"] > 1000))

# COMMAND ----------

filtered_df.count()

# COMMAND ----------

filtered_df.show()

# COMMAND ----------

from pyspark.sql.functions import avg
grouped_df = filtered_df.groupby("clarity").agg(avg("price"))

# COMMAND ----------

grouped_df.show()

# COMMAND ----------

ordered_df = grouped_df.orderBy(grouped_df["avg(price)"].desc())

# COMMAND ----------

ordered_df.show()

# COMMAND ----------

ordered_df_new = ordered_df.withColumnRenamed("avg(price)", "avg_price")

# COMMAND ----------

ordered_df_new.write.format("delta").mode("overwrite").saveAsTable("clarity_fx_skills")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED clarity_fx_skills;

# COMMAND ----------

df1 = spark.sql("select * from default.clarity_fx_skills")

# COMMAND ----------

df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC  DESCRIBE HISTORY clarity_fx_skills

# COMMAND ----------

ordered_df_new.write.format("delta").mode("append").saveAsTable("clarity_fx_skills")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC  DESCRIBE HISTORY clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC update clarity_fx_skills set clarity = "fxskills" where clarity = 'SI2'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  DESCRIBE HISTORY clarity_fx_skills

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /user/hive/warehouse

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/clarity_fx_skills/
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/clarity_fx_skills/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe History clarity_fx_skills

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from clarity_fx_skills
# MAGIC timestamp as of '2025-05-08T05:40:49.000+00:00'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from clarity_fx_skills@v0
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE clarity_fx_skills to version as of 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe History clarity_fx_skills

# COMMAND ----------

