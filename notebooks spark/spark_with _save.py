# Databricks notebook source
# MAGIC %fs 
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/databricks-datasets/Rdatasets/data-001/csv/datasets/Titanic.csv

# COMMAND ----------

titanicdata = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/datasets/Titanic.csv",header="true", inferSchema="true")

# COMMAND ----------

titanicdata.show()

# COMMAND ----------

survivors = titanicdata.filter(titanicdata["Survived"] == 'No')

# COMMAND ----------

survivors.show()

# COMMAND ----------

survivors.write.format("csv").mode("overwrite").save("/Titanic/data/survivors.csv")

# COMMAND ----------

survivors.write.format("delta").mode("overwrite").saveAsTable("titanic_survivors")