# Databricks notebook source
spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW VOLUMES IN workspace.bronze;

# COMMAND ----------

caminho_volume = "/Volumes/workspace/bronze/raw_files/"

display(dbutils.fs.ls(caminho_volume))

# COMMAND ----------

caminho_arquivo = "/Volumes/workspace/bronze/raw_files/Absenteeism_at_work.csv"

df_raw = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", False)
    .csv(caminho_arquivo)
)

display(df_raw)

# COMMAND ----------

quantidade_linhas = df_raw.count()
quantidade_colunas = len(df_raw.columns)

print(f"Quantidade de linhas: {quantidade_linhas}")
print(f"Quantidade de colunas: {quantidade_colunas}")

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

novos_nomes = [
    "employee_id",
    "reason_for_absence",
    "month_of_absence",
    "day_of_week",
    "season",
    "transportation_expense",
    "distance_from_residence_to_work",
    "service_time",
    "age",
    "workload_average_per_day",
    "hit_target",
    "disciplinary_failure",
    "education",
    "number_of_children",
    "social_drinker",
    "social_smoker",
    "number_of_pets",
    "weight",
    "height",
    "body_mass_index",
    "absenteeism_time_in_hours"
]

if len(df_raw.columns) != len(novos_nomes):
    raise ValueError(
        f"Foram encontradas {len(df_raw.columns)} colunas, "
        f"mas foram definidos {len(novos_nomes)} nomes."
    )

df_bronze = df_raw.toDF(*novos_nomes)

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_bronze
    .withColumn(
        "source_file",
        F.lit("/Volumes/workspace/bronze/raw_files/Absenteeism_at_work.csv")
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
)

df_bronze.select(
    "source_file",
    "ingestion_timestamp"
).show(5, truncate=False)

# COMMAND ----------

print(f"Quantidade de linhas: {df_bronze.count()}")
print(f"Quantidade de colunas: {len(df_bronze.columns)}")

display(df_bronze.limit(10))

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.bronze.absenteeism_raw")
)

print("Tabela workspace.bronze.absenteeism_raw gravada com sucesso.")

# COMMAND ----------

df_bronze_salva = spark.table("workspace.bronze.absenteeism_raw")

print(f"Linhas gravadas: {df_bronze_salva.count()}")
print(f"Colunas gravadas: {len(df_bronze_salva.columns)}")

display(df_bronze_salva.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE workspace.bronze.absenteeism_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT source_file) AS source_files,
# MAGIC     MIN(ingestion_timestamp) AS first_ingestion,
# MAGIC     MAX(ingestion_timestamp) AS last_ingestion
# MAGIC FROM workspace.bronze.absenteeism_raw;