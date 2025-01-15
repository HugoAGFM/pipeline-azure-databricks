# Databricks notebook source
bronze_path = "dbfs:/mnt/bronze/imoveis/anuncios"

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

df_silver = df_bronze.select("anuncio.*", "anuncio.endereco.*") \
    .drop("caracteristicas", "endereco")

# COMMAND ----------

silver_path = "dbfs:/mnt/silver/imoveis/anuncios"

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

df_silver.display()

# COMMAND ----------


