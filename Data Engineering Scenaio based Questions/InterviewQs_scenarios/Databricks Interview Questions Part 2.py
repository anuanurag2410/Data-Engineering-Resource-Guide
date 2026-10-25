# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Databricks Interview Questions Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC **Question1 : Two Source Records match one taget record**
# MAGIC
# MAGIC
# MAGIC - Target :Customers 
# MAGIC - 
# MAGIC - Customer_id, City 
# MAGIC - 101, Delhi
# MAGIC - 102, Bengaluru
# MAGIC - 103, Pune
# MAGIC
# MAGIC
# MAGIC - Source: Updates
# MAGIC - 
# MAGIC - Customer_id, City 
# MAGIC - 101,Mumbai 
# MAGIC - 101,Pune

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers as t 
# MAGIC using updates as s 
# MAGIC on t.customer_id = s.customer_id 
# MAGIC when matched then UPDATE 
# MAGIC SET t.city=s.city
# MAGIC
# MAGIC
# MAGIC when not matched then Insert (customer_id,city)
# MAGIC VALUES(s.customer_id,s.city) 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC This situation will be called as ambiguious Multiple Match Situation

# COMMAND ----------

# MAGIC %md
# MAGIC Question 2: Does drop Duplicates keep the latest records?
# MAGIC
# MAGIC
# MAGIC > - Name ENtry 
# MAGIC > - Anurag 20-AUgust-2026
# MAGIC > - Anurag 22-August-2026
# MAGIC > - Anurag 12-September-2026

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window as w

Window.partitionBy("customer_id").orderBy(F.desc("entry"))


# COMMAND ----------

result=df.dropDuplicates(["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3 : Does Optimize Delete the Old files?

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE datax.dataengineering.bronze_orders;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY datax.dataengineering.bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Question 4: Can you recover if you have ran a pipleine on a table in overwrite mode?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE datax.dataengineering.bronze_orders VERSION AS OF 11 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question 1: Single row insert into bronze_orders
# MAGIC INSERT INTO datax.dataengineering.bronze_orders (order_id, status, amount, order_date, city)
# MAGIC VALUES (1, 'Laptop', 1200.00, '2026-01-15', 'Delhi');
# MAGIC
# MAGIC -- Question 2: Multiple row insert in one statement
# MAGIC INSERT INTO datax.dataengineering.bronze_orders (order_id, status, amount, order_date, city)
# MAGIC VALUES
# MAGIC   (2, 'Mouse', 25.00, '2026-01-16', 'Mumbai'),
# MAGIC   (3, 'Keyboard', 45.00, '2026-01-17', 'Pune'),
# MAGIC   (4, 'Monitor', 300.00, '2026-01-18', 'Bengaluru');
# MAGIC
# MAGIC -- Question 3: Insert with explicit column list (recommended practice)
# MAGIC INSERT INTO datax.dataengineering.bronze_orders (order_id, status, amount, order_date, city)
# MAGIC VALUES (5, 'Headphones', 75.50, '2026-01-19', 'Chennai');
# MAGIC
# MAGIC -- Question 4: Insert using a SELECT from another table
# MAGIC -- Note: datax.dataengineering.staging_orders does not exist; create it before uncommenting
# MAGIC -- INSERT INTO datax.dataengineering.bronze_orders (order_id, status, amount, order_date, city)
# MAGIC -- SELECT order_id, status, amount, order_date, city FROM datax.dataengineering.staging_orders
# MAGIC -- WHERE order_date >= '2026-01-01';
# MAGIC
# MAGIC -- Question 5: Insert with dynamic values using a CTE
# MAGIC WITH new_orders AS (
# MAGIC   SELECT 6 AS order_id, 'Tablet' AS status, 500.00 AS amount, cast(current_date() AS string) AS order_date, 'Hyderabad' AS city
# MAGIC )
# MAGIC INSERT INTO datax.dataengineering.bronze_orders (order_id, status, amount, order_date, city)
# MAGIC SELECT * FROM new_orders;
# MAGIC
# MAGIC -- Question 6: Insert overwrite a partition (replaces only matching partition)
# MAGIC -- Note: bronze_orders is not partitioned by city; requires ALTER TABLE first, and staging_orders does not exist
# MAGIC -- INSERT OVERWRITE TABLE datax.dataengineering.bronze_orders PARTITION (city = 'Delhi')
# MAGIC -- SELECT order_id, status, amount, order_date
# MAGIC -- FROM datax.dataengineering.staging_orders
# MAGIC -- WHERE city = 'Delhi';
