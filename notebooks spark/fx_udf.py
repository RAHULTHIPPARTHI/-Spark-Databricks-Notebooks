# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tripdata/yellow/
# MAGIC

# COMMAND ----------

nyctaxi_df = spark.read.format("csv") \
    .option("header","true") \
    .option("inferschema","true") \
    .option("codec","gzip") \
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

display(nyctaxi_df)

# COMMAND ----------

nyctaxi_df.show()

# COMMAND ----------

def concat_strings(s1,s2):
    return s1 + "-" + s2

# COMMAND ----------

concat_strings("data","engineering")

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

concat_udf = udf(concat_strings,StringType())

# COMMAND ----------

Df1 = nyctaxi_df.withColumn("vendor_payment",concat_udf(nyctaxi_df.vendor_name,nyctaxi_df.Payment_Type))

# COMMAND ----------

Df1.show()

# COMMAND ----------

Df1.select("vendor_payment","Total_Amt").show()

# COMMAND ----------

display(Df1.select("vendor_payment","Total_Amt"))

# COMMAND ----------

from pyspark.sql.functions import concat, col

df2 = nyctaxi_df.withColumn("vendor_payment", concat(col("vendor_name"),col("payment_Type")))
df2.select("vendor_payment","Total_Amt").show()

# COMMAND ----------

from pyspark.sql.functions import concat, col , lit

df2 = nyctaxi_df.withColumn("vendor_payment", concat(col("vendor_name"), lit("-"),col("payment_Type")))
df2.select("vendor_payment","Total_Amt").show()

# COMMAND ----------

df2.withColumn("country",lit("USA")).show()

# COMMAND ----------

display(df2.withColumn("country",lit("USA")))

# COMMAND ----------

df3 = df2.select("vendor_payment","total_Amt").withColumn("country",lit("USA"))
df3.show()

# COMMAND ----------

add = lambda x,y : x + y

# COMMAND ----------

add(3,5)

# COMMAND ----------

find_max = lambda x, y: x if x > y else y

# COMMAND ----------

find_max(4,3)

# COMMAND ----------

from pyspark.sql.types import DoubleType

sumtwo = udf(lambda x, y: x + y,DoubleType())

# COMMAND ----------

df3 = nyctaxi_df.withColumn("fareplusucharge", sumtwo(nyctaxi_df.Fare_Amt,nyctaxi_df.surcharge))

# COMMAND ----------

display(df3)

# COMMAND ----------

def reverse_string(s):
    return s[::-1]

# COMMAND ----------

reverse_string("data engineering")

# COMMAND ----------

reverse_string_udf = udf(reverse_string,StringType())

# COMMAND ----------

df5 = nyctaxi_df.withColumn("reversed_vendor_name",reverse_string_udf("vendor_name")).select("vendor_name","reversed_vendor_name")

# COMMAND ----------

display(df5)

# COMMAND ----------

from pyspark.sql.types import StringType, ArrayType

split_words_udf = udf(lambda text: text.split(),ArrayType(StringType()))

# COMMAND ----------

df = spark.createDataFrame([(1,"data engineering"),(2,"delta lake lakehouse")],["id","text"])
df.show()

# COMMAND ----------

df = df.withColumn("words",split_words_udf(df["text"]))

# COMMAND ----------

display(df)