# Databricks notebook source
from pyspark.sql import functions as F

df_bronze = spark.table("workspace.bronze.absenteeism_raw")

print(f"Quantidade de linhas: {df_bronze.count()}")
print(f"Quantidade de colunas: {len(df_bronze.columns)}")

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Qualidade dos dados e camada Silver
# MAGIC
# MAGIC Este notebook executa as seguintes atividades:
# MAGIC
# MAGIC 1. Leitura da tabela Bronze.
# MAGIC 2. Análise de completude.
# MAGIC 3. Verificação de duplicidades.
# MAGIC 4. Conversão dos tipos de dados.
# MAGIC 5. Validação dos domínios.
# MAGIC 6. Identificação de valores extremos.
# MAGIC 7. Persistência da tabela Silver.
# MAGIC
# MAGIC Os registros aparentemente duplicados serão investigados, mas não serão removidos automaticamente, pois cada linha pode representar uma ocorrência distinta de ausência.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "workspace.bronze.absenteeism_raw"
silver_table = "workspace.silver.absenteeism_clean"

df_bronze = spark.table(bronze_table)

print("Quantidade de linhas:", df_bronze.count())
print("Quantidade de colunas:", len(df_bronze.columns))

display(df_bronze.limit(5))

# COMMAND ----------

business_columns = [
    c for c in df_bronze.columns
    if c not in ["source_file", "ingestion_timestamp"]
]

total_rows = df_bronze.count()

completeness_rows = []

for column_name in business_columns:
    missing_condition = (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name).cast("string")) == "")
    )

    missing_count = df_bronze.filter(missing_condition).count()
    complete_count = total_rows - missing_count
    missing_percentage = round((missing_count / total_rows) * 100, 2)

    completeness_rows.append(
        (
            column_name,
            int(missing_count),
            int(complete_count),
            float(missing_percentage)
        )
    )

df_completeness = spark.createDataFrame(
    completeness_rows,
    [
        "column_name",
        "missing_count",
        "complete_count",
        "missing_percentage"
    ]
)

display(df_completeness.orderBy(F.desc("missing_percentage")))

# COMMAND ----------

df_completeness.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dq_completeness_bronze")

print("Relatório de completude salvo.")

# COMMAND ----------

duplicate_groups = (
    df_bronze
    .groupBy(*business_columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_group_count = duplicate_groups.count()

duplicate_extra_rows = (
    duplicate_groups
    .select(
        F.coalesce(
            F.sum(F.col("count") - F.lit(1)),
            F.lit(0)
        ).alias("duplicate_extra_rows")
    )
    .first()["duplicate_extra_rows"]
)

print("Grupos com registros repetidos:", duplicate_group_count)
print("Linhas adicionais nesses grupos:", duplicate_extra_rows)

display(duplicate_groups.orderBy(F.desc("count")))

# COMMAND ----------

df_duplicates_summary = spark.createDataFrame(
    [
        (
            int(total_rows),
            int(duplicate_group_count),
            int(duplicate_extra_rows),
            "Registros preservados: a base não possui chave única da ocorrência."
        )
    ],
    [
        "total_rows",
        "duplicate_groups",
        "duplicate_extra_rows",
        "treatment"
    ]
)

display(df_duplicates_summary)

df_duplicates_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dq_duplicates_bronze")

# COMMAND ----------

integer_columns = [
    "employee_id",
    "reason_for_absence",
    "month_of_absence",
    "day_of_week",
    "season",
    "transportation_expense",
    "distance_from_residence_to_work",
    "service_time",
    "age",
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

df_silver = df_bronze

# Remove espaços desnecessários.
for column_name in business_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

# Converte as colunas inteiras.
for column_name in integer_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.col(column_name).cast("integer")
    )

# Converte a carga média de trabalho para número decimal.
df_silver = df_silver.withColumn(
    "workload_average_per_day",
    F.regexp_replace(
        F.col("workload_average_per_day"),
        ",",
        "."
    ).cast("double")
)

df_silver.printSchema()

# COMMAND ----------

typed_columns = integer_columns + ["workload_average_per_day"]

conversion_rows = []

for column_name in typed_columns:
    null_after_cast = df_silver.filter(
        F.col(column_name).isNull()
    ).count()

    conversion_rows.append(
        (column_name, int(null_after_cast))
    )

df_conversion_check = spark.createDataFrame(
    conversion_rows,
    ["column_name", "nulls_after_conversion"]
)

display(
    df_conversion_check.orderBy(
        F.desc("nulls_after_conversion")
    )
)

# COMMAND ----------

quality_rules = [
    (
        "reason_for_absence_between_0_and_28",
        ~F.col("reason_for_absence").between(0, 28)
    ),
    (
        "month_of_absence_between_0_and_12",
        ~F.col("month_of_absence").between(0, 12)
    ),
    (
        "day_of_week_between_2_and_6",
        ~F.col("day_of_week").between(2, 6)
    ),
    (
        "season_between_1_and_4",
        ~F.col("season").between(1, 4)
    ),
    (
        "education_between_1_and_4",
        ~F.col("education").between(1, 4)
    ),
    (
        "disciplinary_failure_binary",
        ~F.col("disciplinary_failure").isin(0, 1)
    ),
    (
        "social_drinker_binary",
        ~F.col("social_drinker").isin(0, 1)
    ),
    (
        "social_smoker_binary",
        ~F.col("social_smoker").isin(0, 1)
    ),
    (
        "age_positive",
        F.col("age") <= 0
    ),
    (
        "weight_positive",
        F.col("weight") <= 0
    ),
    (
        "height_positive",
        F.col("height") <= 0
    ),
    (
        "body_mass_index_positive",
        F.col("body_mass_index") <= 0
    ),
    (
        "absenteeism_hours_non_negative",
        F.col("absenteeism_time_in_hours") < 0
    )
]

domain_rows = []

for rule_name, invalid_condition in quality_rules:
    invalid_count = df_silver.filter(invalid_condition).count()

    domain_rows.append(
        (
            rule_name,
            int(invalid_count),
            "OK" if invalid_count == 0 else "REVIEW"
        )
    )

df_domain_quality = spark.createDataFrame(
    domain_rows,
    ["quality_rule", "invalid_count", "status"]
)

display(df_domain_quality)

# COMMAND ----------

df_domain_quality.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dq_domain_rules_bronze")

print("Validações de domínio salvas.")

# COMMAND ----------

continuous_columns = [
    "transportation_expense",
    "distance_from_residence_to_work",
    "service_time",
    "age",
    "workload_average_per_day",
    "hit_target",
    "number_of_children",
    "number_of_pets",
    "weight",
    "height",
    "body_mass_index",
    "absenteeism_time_in_hours"
]

outlier_rows = []

for column_name in continuous_columns:
    q1, q3 = df_silver.approxQuantile(
        column_name,
        [0.25, 0.75],
        0.01
    )

    iqr = q3 - q1
    lower_limit = q1 - (1.5 * iqr)
    upper_limit = q3 + (1.5 * iqr)

    outlier_count = df_silver.filter(
        (F.col(column_name) < lower_limit)
        | (F.col(column_name) > upper_limit)
    ).count()

    outlier_rows.append(
        (
            column_name,
            float(q1),
            float(q3),
            float(lower_limit),
            float(upper_limit),
            int(outlier_count)
        )
    )

df_outliers = spark.createDataFrame(
    outlier_rows,
    [
        "column_name",
        "q1",
        "q3",
        "lower_limit",
        "upper_limit",
        "possible_outlier_count"
    ]
)

display(
    df_outliers.orderBy(
        F.desc("possible_outlier_count")
    )
)

# COMMAND ----------

df_outliers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.dq_outliers_bronze")

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

print("Tabela Silver gravada:", silver_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT employee_id) AS distinct_employees,
# MAGIC     MIN(absenteeism_time_in_hours) AS minimum_absence_hours,
# MAGIC     MAX(absenteeism_time_in_hours) AS maximum_absence_hours
# MAGIC FROM workspace.silver.absenteeism_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE workspace.silver.absenteeism_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.dq_completeness_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.dq_duplicates_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.dq_domain_rules_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.dq_outliers_bronze;