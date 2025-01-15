# Databricks notebook source
path = "dbfs:/mnt/inbound/imoveis/dados_brutos_imoveis.json"
df = spark.read.json(path)

# COMMAND ----------

df_anuncio = df.select("anuncio")

# COMMAND ----------

df_bronze = df_anuncio.withColumn("id", df_anuncio.anuncio.id)

# COMMAND ----------

bronze_path = "dbfs:/mnt/bronze/imoveis/anuncios"
df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

# COMMAND ----------


