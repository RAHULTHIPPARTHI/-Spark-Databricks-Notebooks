# Databricks notebook source
spark

# COMMAND ----------

spark.createDataFrame([{"Google":"colab","spark":"scala"} ,{"Google":"dataproc","spark":"python"}]).show()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/credit-card-fraud

# COMMAND ----------

credit_fraud = spark.read.csv("dbfs:/databricks-datasets/credit-card-fraud/odbl-10.txt")

# COMMAND ----------

credit_fraud.show()

# COMMAND ----------

customers_df = spark.sql("select * from default.store_customers")

# COMMAND ----------

customers_df.show()

# COMMAND ----------

customers_df.count()

# COMMAND ----------

display(customers_df)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/
# MAGIC