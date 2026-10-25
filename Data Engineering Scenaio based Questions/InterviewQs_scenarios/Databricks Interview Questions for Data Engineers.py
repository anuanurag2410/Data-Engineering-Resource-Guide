# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Interview Questions for Data Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge in Delta Tables

# COMMAND ----------

from pyspark.sql import Row


source_data = [
    Row(customer_id=101, name="Anurag", city="Bengaluru"),
    Row(customer_id=103, name="Stuti", city="Pune")
]


# COMMAND ----------

target_data = [
    Row(customer_id=101, name="Anurag", city="Delhi"),
    Row(customer_id=102, name="Ankur", city="Mumbai")
]


# COMMAND ----------

target_df=spark.createDataFrame(target_data)
source_df=spark.createDataFrame(source_data)

# COMMAND ----------

target_df.write.mode("overwrite").saveAsTable("customers")
source_df.createOrReplaceTempView("customer_updates")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from customer_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers as target
# MAGIC USING customer_updates as source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### QUESTION 2 — THOUSANDS OF SMALL FILES

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM --> Remove OlD file 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accidenlly delteted data 

# COMMAND ----------

# MAGIC %md
# MAGIC _Time Travel_

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE Customers TO VERSION AS OF 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Customers

# COMMAND ----------



# COMMAND ----------


