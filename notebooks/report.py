# Databricks notebook source
import pyspark.pandas as ps
import os
from slack_sdk import WebClient

# COMMAND ----------

slack_token = "xoxb-8321459392102-8328098787746-VF8SNyx7TAs7HCsEHegDQ6IB"
client = WebClient(token=slack_token)

# COMMAND ----------

file_name = dbutils.fs.ls("dbfs:/FileStore/tables/silver/BRL_rates/")[-1].name 
path = f"/dbfs/FileStore/tables/silver/BRL_rates/{file_name}"

# COMMAND ----------

client.files_upload_v2(
    channel="C089K79EMM3", # Channel ID
    title="Exchange Rates Report",
    file=path,
    file_name="excchange_rates.csv",
    initial_comment="Follows CSV file:"
)

# COMMAND ----------

df = ps.read_csv("dbfs:/FileStore/tables/silver/BRL_rates/")

# COMMAND ----------

if not os.path.isdir("/tmp/images"):
    os.mkdir("/tmp/images")

# COMMAND ----------

currencies = df.columns[1:]
for currency in currencies:
    fig = df.plot.line(x="date", y=currency)
    fig.write_image(f"/tmp/images/{currency}.png")

# COMMAND ----------

images = os.listdir("/tmp/images")
for image in images:
    _ = client.files_upload_v2(
        channel="C089K79EMM3", # Channel ID
        title="Exchange Rates Charts",
        file=f"/tmp/images/{image}"
    )