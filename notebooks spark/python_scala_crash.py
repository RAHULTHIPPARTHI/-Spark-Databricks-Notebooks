# Databricks notebook source

a = 1
b = "abc"

# COMMAND ----------

b

# COMMAND ----------

type(b)

# COMMAND ----------

# MAGIC %scala
# MAGIC val b = 3

# COMMAND ----------

# MAGIC %scala
# MAGIC val c = 3

# COMMAND ----------

var_list = [10,20,30,40]

# COMMAND ----------

var_list[1]

# COMMAND ----------

var_list[-1]

# COMMAND ----------

if 1> 2:
    print("Inside if block")
    print("Also Inside if block")
print("outside if block")

# COMMAND ----------

var_list = [1,2,3,4,5]
for element in var_list:
    print("printing item inside for loop")
    print(element)
    print("now see square")
    print(element**2)

# COMMAND ----------

list(range(10))
for element in range(15):
    print(element)

# COMMAND ----------

for element in range(15):
    print(element)

# COMMAND ----------

def sample_function(x):
    print("inside function")
    print(x)

# COMMAND ----------

sample_function("qwerty")

# COMMAND ----------

def calculate_sum(x,y):
    return x+y

# COMMAND ----------

out = calculate_sum(2,3)
print(out)

# COMMAND ----------

with open("my_file_1.txt","w") as f:
    f.write("sample content 1")

# COMMAND ----------

!ls

# COMMAND ----------

!cat my_file_1.txt

# COMMAND ----------

with open("my_file_1.txt","a") as f:
    f.write("more content ")

# COMMAND ----------

!cat my_file_1.txt

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC var myarray = Array(10,20,30,40)

# COMMAND ----------

# MAGIC %scala
# MAGIC myarray(0)

# COMMAND ----------

# MAGIC %scala
# MAGIC def sampleFunction () : Unit = {
# MAGIC   println("big Data")
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC sampleFunction()

# COMMAND ----------

# MAGIC %scala
# MAGIC def calculatesSum (i: Int, j:Int ) : Int = {
# MAGIC   val k = i + j
# MAGIC   return k
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC calculatesSum(7,1)