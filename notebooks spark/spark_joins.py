# Databricks notebook source
customer_mini_df = spark.sql("select * from store_customers_mini")

# COMMAND ----------

customer_mini_df.show()

# COMMAND ----------

transactions_mini_df = spark.sql("select * from store_transactions_mini")

# COMMAND ----------

transactions_mini_df.show()

# COMMAND ----------

customers_df = spark.sql("select * from store_customers")

# COMMAND ----------

customers_df.show()

# COMMAND ----------

customers_df.count()

# COMMAND ----------

transactions_df = spark.sql("select * from store_transactions")

# COMMAND ----------

transactions_df.show()

# COMMAND ----------

transactions_df.count()

# COMMAND ----------

customer_product_df = customers_df.join(transactions_df,customers_df.CustomerID == transactions_df.CustomerID)

# COMMAND ----------

customer_product_df.show()

# COMMAND ----------

customer_product_df.count()

# COMMAND ----------

customer_product_df.groupBy("Country").agg({"Amount" : "sum"}).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerDF = spark.sql("select * from store_customers")
# MAGIC val transactionDF = spark.sql("select * from store_transactions")

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerProductDf = customerDF.join(transactionDF,customerDF("CustomerID") === transactionDF("CustomerID"))

# COMMAND ----------

# MAGIC %scala
# MAGIC customerProductDf.show()
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC  customerProductDf.count()

# COMMAND ----------

# MAGIC %scala

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{sum}
# MAGIC val countryAmountDF = customerProductDf.groupBy("country").agg(sum("Amount"))

# COMMAND ----------

# MAGIC %scala
# MAGIC countryAmountDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val countryAmountDF = spark.sql("select country,sum(Amount) as totalAmount From (select * from store_customers join store_transactions on store_customers.customerID = store_transactions.customerID) group by Country")

# COMMAND ----------

# MAGIC %scala
# MAGIC countryAmountDF.show()

# COMMAND ----------

# MAGIC %scala 
# MAGIC customerDF.createOrReplaceTempView("store_customers")

# COMMAND ----------

transactions_mini_df.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID)

# COMMAND ----------

cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini_left = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID, how ="left")

# COMMAND ----------

cust_prod_mini_left.show()

# COMMAND ----------

cust_prod_mini_right = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID, how ="right")

cust_prod_mini_right.show()

# COMMAND ----------

cust_prod_mini_full = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID, how ="full")

cust_prod_mini_full.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID, how ="left_anti")

cust_prod_mini_left_anti.show()

# COMMAND ----------

cust_prod_mini_left_semi = customer_mini_df.join(transactions_mini_df,customer_mini_df.CustomerID ==transactions_mini_df.CustomerID, how ="left_semi")

cust_prod_mini_left_semi.show()

# COMMAND ----------

