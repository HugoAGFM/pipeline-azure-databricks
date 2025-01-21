# Databricks notebook source
dbutils.widgets.text("execution_date", "")
execution_date = dbutils.widgets.get("execution_date")

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame

# COMMAND ----------

def extract(date: str,  base: str, symbols: list[str]) -> dict:
    url = "https://api.apilayer.com/exchangerates_data/{date}"
    params = {
        "base": base,
        "symbols": ",".join(symbols)
    }
    headers= {
        "apikey": "ieRIvNXPS3kuKk8qBxVSBmyRnbAejDBo"
    }
    
    r = requests.get(url.format(date=date), headers=headers, params=params)

    return r.json()


# COMMAND ----------

def transform(exchange_rates: dict) -> DataFrame:
    data = list(exchange_rates["rates"].items())
    schema = ["currency", "rate"]
    date = exchange_rates["date"]
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("date", lit(date).cast(DateType()))

    return df

# COMMAND ----------

def load(df: DataFrame, path: str) -> None:
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("parquet") \
        .save(path)

# COMMAND ----------

def pipeline(date: str) -> None:
    exchange_rates = extract(date, "BRL", ["USD", "GBP", "EUR"])
    df = transform(exchange_rates)

    year, month, day = exchange_rates["date"].split("-")
    path = f"dbfs:/FileStore/tables/bronze/{year}/{month}/{day}"
    load(df, path)

# COMMAND ----------

pipeline(execution_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving as JSON

# COMMAND ----------

# path = f"dbfs:/FileStore/tables/bronze/year={year}/month={month}/day={day}"
# file_name = f"exchange_rates-BRL-{year}-{month}-{day}.json"

# COMMAND ----------

# dbutils.fs.mkdirs(path)

# COMMAND ----------

# local_buffer = f"/tmp/{file_name}"


# with open(local_buffer, "w") as file:
#     json.dump(exchange_rates, file)

# COMMAND ----------

# dbutils.fs.cp(f"file://{local_buffer}", path)

# COMMAND ----------

