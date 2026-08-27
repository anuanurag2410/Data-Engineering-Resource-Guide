# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario-Based Data Engineering Interview Questions 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1: Your Pipeline Ran Successfully, But No Data Was Loaded

# COMMAND ----------

# MAGIC %md
# MAGIC Source record count
# MAGIC
# MAGIC - Incremental/watermark condition
# MAGIC - Transformation filters
# MAGIC - Target write
# MAGIC - Pipeline logs
# MAGIC
# MAGIC
# MAGIC The important point is:
# MAGIC
# MAGIC
# MAGIC Pipeline success does not always mean data success.
# MAGIC This is why record-count validation is important.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 2: Your Source Sends the Same File Twice

# COMMAND ----------

# MAGIC %md
# MAGIC - Before processing, check whether that file has already been successfully processed.
# MAGIC - For record-level protection, we can also use business keys and MERGE/upsert logic.
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: 100 Million Records, Only 50,000 Change Daily

# COMMAND ----------

# MAGIC %md
# MAGIC maintain updated_at timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC 2026-08-28 12:30:00 
# MAGIC
# MAGIC implement Incremental Load

# COMMAND ----------

# MAGIC %md
# MAGIC Depending on the source, CDC can also be used.
# MAGIC
# MAGIC
# MAGIC This reduces:
# MAGIC - Processing time
# MAGIC - Compute
# MAGIC - Data movement
# MAGIC - Cost
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4: The Source Schema Suddenly Changes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SChema overwrite or scheam evolution

# COMMAND ----------

# MAGIC %md
# MAGIC - First, I need to understand whether my pipeline supports schema evolution.
# MAGIC - I would not blindly accept every schema change.
# MAGIC - For an expected additive column such as phone_number, the pipeline can be designed to evolve safely.
# MAGIC - But if a critical column disappears or its datatype changes, I may want the pipeline to fail or quarantine the data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5: Source Has 1 Million Rows, Target Has 999,500

# COMMAND ----------

# MAGIC %md
# MAGIC Then investigate those 500 records.
# MAGIC
# MAGIC
# MAGIC Possible causes include:
# MAGIC - Transformation filters
# MAGIC - Duplicate handling
# MAGIC - Null keys
# MAGIC - Failed records
# MAGIC - Incorrect joins
# MAGIC - Incremental logic
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


