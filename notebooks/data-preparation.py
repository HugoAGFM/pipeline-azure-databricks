# Databricks notebook source
from pyspark.sql.functions import first, col, round

# COMMAND ----------

df = spark.read.parquet("dbfs:/FileStore/tables/bronze/*/*/*")

# COMMAND ----------

pivot_df = df.groupBy("date") \
    .pivot("currency") \
    .agg(first("rate")) \
    .orderBy("date", ascending=False)

# COMMAND ----------

result_df = pivot_df
for currency in ["USD", "GBP", "EUR"]:
    result_df = result_df.withColumn(
        currency, round(1 / col(currency), 4)
        )

# COMMAND ----------

result_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save("dbfs:/FileStore/tables/silver/BRL_rates")