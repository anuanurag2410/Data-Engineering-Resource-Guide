# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CREATING SOURCE DATA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.employees (
# MAGIC     employee_id INTEGER PRIMARY KEY,
# MAGIC     employee_name STRING NOT NULL,
# MAGIC     department STRING NOT NULL,
# MAGIC     salary INTEGER NOT NULL
# MAGIC )
# MAGIC

# COMMAND ----------



# Generate realistic employee records
employees_data = [
    (101, "Anurag",   "Engineering", 95000),
    (102, "Shalini",  "Engineering", 85000),
    (103, "Akash",    "Engineering", 95000),
    (104, "Stuti",    "HR",          70000),
    (105, "Ravi",     "HR",          80000),
    (106, "Vaishnav", "Finance",     90000),
    (107, "Ankur",    "Finance",     90000),
    (108, "Tanmay",   "Finance",     75000)
]


# COMMAND ----------

# Create a PySpark DataFrame from the employees_data list and overwrite the employees table
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

employees_schema = StructType([
    StructField("employee_id",   IntegerType(), nullable=False),
    StructField("employee_name", StringType(),  nullable=False),
    StructField("department",    StringType(),  nullable=False),
    StructField("salary",        IntegerType(), nullable=False),
])

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

# Overwrite the employees table with the DataFrame data
employees_df.write.mode("overwrite").saveAsTable("default.employees")

print("Data inserted into employees table successfully!")
employees_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.employees

# COMMAND ----------

# MAGIC %md
# MAGIC Find all highest-paid employees in every department
# MAGIC You are given an employees table containing employee IDs, names, departments and salaries.
# MAGIC
# MAGIC Write a SQL query to find the highest-paid employees in every department.
# MAGIC
# MAGIC Conditions: If two or more employees share the maximum salary in a department, return all of them. The solution should work for any number of departments.

# COMMAND ----------

# DBTITLE 1,SQL Solution
# MAGIC %sql
# MAGIC
# MAGIC WITH ranked_employees AS
# MAGIC (
# MAGIC     SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as Salary_Rank
# MAGIC     FROM default.employees
# MAGIC
# MAGIC )
# MAGIC
# MAGIC Select 
# MAGIC employee_id,
# MAGIC     employee_name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     Salary_Rank
# MAGIC     FROM ranked_employees
# MAGIC     WHERE Salary_Rank=1 
# MAGIC     ORDER BY department, employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2: Find the latest record for every order
# MAGIC
# MAGIC Find the latest order record from historical updates
# MAGIC You are working with an e-commerce order management system. Each time an order changes, the system generates a new event.
# MAGIC
# MAGIC Write a SQL query to return exactly one latest record per order.
# MAGIC
# MAGIC Conditions: Select the greatest updated_at first. If timestamps tie, select the greatest event_version, then use event_id as the final tie-breaker. In this fictional dataset, higher versions and event IDs represent later events when prior ordering values tie.

# COMMAND ----------

# DBTITLE 1,QUESTION 2 — SOURCE DATA GENERATION
# MAGIC %sql
# MAGIC CREATE TABLE default.order_updates (
# MAGIC     event_id INTEGER PRIMARY KEY,
# MAGIC     order_id STRING NOT NULL,
# MAGIC     customer_name STRING NOT NULL,
# MAGIC     amount INTEGER NOT NULL,
# MAGIC     status STRING NOT NULL,
# MAGIC     updated_at STRING NOT NULL,
# MAGIC     event_version INTEGER NOT NULL
# MAGIC );

# COMMAND ----------

orders_data = [
    (1, "O101", "Anurag", 1200, "Pending",
     "2026-10-01 09:00:00", 1),

    (2, "O101", "Anurag", 1200, "Completed",
     "2026-10-01 11:00:00", 2),

    (3, "O102", "Stuti", 800, "Pending",
     "2026-10-01 09:30:00", 1),

    (4, "O102", "Stuti", 900, "Completed",
     "2026-10-01 12:00:00", 2),

    (5, "O102", "Stuti", 950, "Completed",
     "2026-10-01 12:00:00", 3),

    (6, "O103", "Ravi", 1500, "Completed",
     "2026-10-02 10:00:00", 1),

    (7, "O104", "Shalini", 500, "Pending",
     "2026-10-02 08:00:00", 1),

    (8, "O104", "Shalini", 500, "Cancelled",
     "2026-10-02 13:00:00", 2)
]

# COMMAND ----------

# Create a PySpark DataFrame from the orders_data list and overwrite the order_updates table
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

orders_schema = StructType([
    StructField("event_id",      IntegerType(), nullable=False),
    StructField("order_id",      StringType(),  nullable=False),
    StructField("customer_name", StringType(),  nullable=False),
    StructField("amount",        IntegerType(), nullable=False),
    StructField("status",        StringType(),  nullable=False),
    StructField("updated_at",    StringType(),  nullable=False),
    StructField("event_version", IntegerType(), nullable=False),
])

order_updates_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Overwrite the order_updates table with the DataFrame data
order_updates_df.write.mode("overwrite").saveAsTable("default.order_updates")

print("Data inserted into order_updates table successfully!")
order_updates_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from order_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC Select 
# MAGIC order_id,
# MAGIC amount,
# MAGIC event_version,
# MAGIC ROW_NUMBER() OVER(PartITION BY order_id ORDER BY updated_at DESC,event_version DESC, event_id DESC) as row_num
# MAGIC
# MAGIC FROM default.order_updates ORDER BY order_id,row_num;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_orders as 
# MAGIC (Select 
# MAGIC event_id,
# MAGIC         order_id,
# MAGIC         customer_name,
# MAGIC         amount,
# MAGIC         status,
# MAGIC         updated_at,
# MAGIC         event_version,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY order_id
# MAGIC             ORDER BY
# MAGIC                 updated_at DESC,
# MAGIC                 event_version DESC,
# MAGIC                 event_id DESC
# MAGIC         ) AS row_num
# MAGIC     FROM order_updates)
# MAGIC
# MAGIC Select order_id,
# MAGIC customer_name,
# MAGIC amount,
# MAGIC status,
# MAGIC event_version
# MAGIC FROM ranked_orders
# MAGIC WHERE row_num=1
# MAGIC     ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 3: Calculate running total for each customer
# MAGIC **_Calculate daily payments and a running total for each customer_**
# MAGIC
# MAGIC You are given a payments table containing customer names, payment dates and payment amounts.
# MAGIC
# MAGIC Write SQL that returns the total payments made by each customer on each date, along with the cumulative payment amount up to that date.
# MAGIC
# MAGIC Conditions: Multiple payments on the same day must be combined into one daily record. The running total must restart for each customer.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.payments (
# MAGIC     payment_id INTEGER PRIMARY KEY,
# MAGIC     customer_name STRING NOT NULL,
# MAGIC     payment_date STRING NOT NULL,
# MAGIC     amount INTEGER NOT NULL
# MAGIC )

# COMMAND ----------

payments_data = [
    (1, "Anurag", "2026-10-01", 500),
    (2, "Anurag", "2026-10-01", 200),
    (3, "Anurag", "2026-10-03", 300),
    (4, "Anurag", "2026-10-05", 400),
    (5, "Stuti",  "2026-10-01", 250),
    (6, "Stuti",  "2026-10-02", 350),
    (7, "Stuti",  "2026-10-02", 100),
    (8, "Stuti",  "2026-10-04", 200)
]

# Create a PySpark DataFrame from the payments_data list and overwrite the payments table
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

payments_schema = StructType([
    StructField("payment_id",    IntegerType(), nullable=False),
    StructField("customer_name", StringType(),  nullable=False),
    StructField("payment_date",  StringType(),  nullable=False),
    StructField("amount",        IntegerType(), nullable=False),
])

payments_df = spark.createDataFrame(payments_data, schema=payments_schema)

# Overwrite the payments table with the DataFrame data
payments_df.write.mode("overwrite").saveAsTable("default.payments")

print("Data inserted into payments table successfully!")
payments_df.display()



# COMMAND ----------

# DBTITLE 1,Step 1 - DAILY AGGREGATION
# MAGIC %sql
# MAGIC SELECT 
# MAGIC customer_name,
# MAGIC payment_date,
# MAGIC SUM(amount) as daily_amount
# MAGIC FROM default.payments
# MAGIC GROUP BY customer_name,payment_date
# MAGIC ORDER BY customer_name,payment_date;

# COMMAND ----------

# MAGIC %sql 
# MAGIC WITH daily_payments as (
# MAGIC     SELECT 
# MAGIC customer_name,
# MAGIC payment_date,
# MAGIC SUM(amount) as daily_amount
# MAGIC FROM default.payments
# MAGIC GROUP BY customer_name,payment_date
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC customer_name,
# MAGIC payment_date,
# MAGIC daily_amount,
# MAGIC SUM(daily_amount) OVER(PARTITION BY customer_name ORDER BY payment_date 
# MAGIC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Running_Sum_Total
# MAGIC FROM daily_payments
# MAGIC ORDER BY customer_name,payment_date;

# COMMAND ----------


