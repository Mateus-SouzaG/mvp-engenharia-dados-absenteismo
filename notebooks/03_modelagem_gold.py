# Databricks notebook source
# MAGIC %md
# MAGIC # Modelagem da camada Gold
# MAGIC
# MAGIC Este notebook cria um modelo dimensional simplificado para análise do absenteísmo.
# MAGIC
# MAGIC Tabelas criadas:
# MAGIC
# MAGIC - `dim_employee`: características do trabalhador.
# MAGIC - `dim_reason`: classificação dos motivos de ausência.
# MAGIC - `dim_period`: atributos temporais disponíveis.
# MAGIC - `fact_absence`: ocorrências e horas de ausência.
# MAGIC - `mart_reason_summary`: indicadores por motivo.
# MAGIC - `mart_temporal_summary`: indicadores temporais.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_reason
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (0,  'Não informado', 'Sem classificação'),
# MAGIC     (1,  'Doenças infecciosas e parasitárias', 'CID'),
# MAGIC     (2,  'Neoplasias', 'CID'),
# MAGIC     (3,  'Doenças do sangue e transtornos imunitários', 'CID'),
# MAGIC     (4,  'Doenças endócrinas nutricionais e metabólicas', 'CID'),
# MAGIC     (5,  'Transtornos mentais e comportamentais', 'CID'),
# MAGIC     (6,  'Doenças do sistema nervoso', 'CID'),
# MAGIC     (7,  'Doenças dos olhos e anexos', 'CID'),
# MAGIC     (8,  'Doenças do ouvido', 'CID'),
# MAGIC     (9,  'Doenças do aparelho circulatório', 'CID'),
# MAGIC     (10, 'Doenças do aparelho respiratório', 'CID'),
# MAGIC     (11, 'Doenças do aparelho digestivo', 'CID'),
# MAGIC     (12, 'Doenças da pele e tecido subcutâneo', 'CID'),
# MAGIC     (13, 'Doenças osteomusculares e do tecido conjuntivo', 'CID'),
# MAGIC     (14, 'Doenças do aparelho geniturinário', 'CID'),
# MAGIC     (15, 'Gravidez parto e puerpério', 'CID'),
# MAGIC     (16, 'Afecções originadas no período perinatal', 'CID'),
# MAGIC     (17, 'Malformações congênitas', 'CID'),
# MAGIC     (18, 'Sintomas e achados não classificados', 'CID'),
# MAGIC     (19, 'Lesões, intoxicações e outras consequências de causas externas', 'CID'),
# MAGIC     (20, 'Causas externas de morbidade e mortalidade', 'CID'),
# MAGIC     (21, 'Fatores que influenciam o estado de saúde', 'CID'),
# MAGIC     (22, 'Acompanhamento de paciente', 'Sem CID'),
# MAGIC     (23, 'Consulta médica', 'Sem CID'),
# MAGIC     (24, 'Doação de sangue', 'Sem CID'),
# MAGIC     (25, 'Exame laboratorial', 'Sem CID'),
# MAGIC     (26, 'Ausência injustificada', 'Sem CID'),
# MAGIC     (27, 'Fisioterapia', 'Sem CID'),
# MAGIC     (28, 'Consulta odontológica', 'Sem CID')
# MAGIC AS reason(reason_id, reason_description, reason_group);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total_reasons
# MAGIC FROM workspace.gold.dim_reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_period
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     (month_of_absence * 100) + (day_of_week * 10) + season AS period_key,
# MAGIC     month_of_absence,
# MAGIC
# MAGIC     CASE month_of_absence
# MAGIC         WHEN 0 THEN 'Não informado'
# MAGIC         WHEN 1 THEN 'Janeiro'
# MAGIC         WHEN 2 THEN 'Fevereiro'
# MAGIC         WHEN 3 THEN 'Março'
# MAGIC         WHEN 4 THEN 'Abril'
# MAGIC         WHEN 5 THEN 'Maio'
# MAGIC         WHEN 6 THEN 'Junho'
# MAGIC         WHEN 7 THEN 'Julho'
# MAGIC         WHEN 8 THEN 'Agosto'
# MAGIC         WHEN 9 THEN 'Setembro'
# MAGIC         WHEN 10 THEN 'Outubro'
# MAGIC         WHEN 11 THEN 'Novembro'
# MAGIC         WHEN 12 THEN 'Dezembro'
# MAGIC     END AS month_name,
# MAGIC
# MAGIC     day_of_week,
# MAGIC
# MAGIC     CASE day_of_week
# MAGIC         WHEN 2 THEN 'Segunda-feira'
# MAGIC         WHEN 3 THEN 'Terça-feira'
# MAGIC         WHEN 4 THEN 'Quarta-feira'
# MAGIC         WHEN 5 THEN 'Quinta-feira'
# MAGIC         WHEN 6 THEN 'Sexta-feira'
# MAGIC     END AS day_name,
# MAGIC
# MAGIC     season,
# MAGIC
# MAGIC     CASE season
# MAGIC         WHEN 1 THEN 'Verão'
# MAGIC         WHEN 2 THEN 'Outono'
# MAGIC         WHEN 3 THEN 'Inverno'
# MAGIC         WHEN 4 THEN 'Primavera'
# MAGIC     END AS season_name
# MAGIC
# MAGIC FROM workspace.silver.absenteeism_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_employee
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH profile_counts AS (
# MAGIC     SELECT
# MAGIC         employee_id,
# MAGIC         transportation_expense,
# MAGIC         distance_from_residence_to_work,
# MAGIC         service_time,
# MAGIC         age,
# MAGIC         education,
# MAGIC         number_of_children,
# MAGIC         social_drinker,
# MAGIC         social_smoker,
# MAGIC         number_of_pets,
# MAGIC         weight,
# MAGIC         height,
# MAGIC         body_mass_index,
# MAGIC         COUNT(*) AS observed_profile_frequency
# MAGIC     FROM workspace.silver.absenteeism_clean
# MAGIC     GROUP BY
# MAGIC         employee_id,
# MAGIC         transportation_expense,
# MAGIC         distance_from_residence_to_work,
# MAGIC         service_time,
# MAGIC         age,
# MAGIC         education,
# MAGIC         number_of_children,
# MAGIC         social_drinker,
# MAGIC         social_smoker,
# MAGIC         number_of_pets,
# MAGIC         weight,
# MAGIC         height,
# MAGIC         body_mass_index
# MAGIC ),
# MAGIC ranked_profiles AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY employee_id
# MAGIC             ORDER BY observed_profile_frequency DESC
# MAGIC         ) AS profile_rank
# MAGIC     FROM profile_counts
# MAGIC )
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     transportation_expense,
# MAGIC     distance_from_residence_to_work,
# MAGIC     service_time,
# MAGIC     age,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN age < 30 THEN 'Até 29 anos'
# MAGIC         WHEN age < 40 THEN '30 a 39 anos'
# MAGIC         WHEN age < 50 THEN '40 a 49 anos'
# MAGIC         ELSE '50 anos ou mais'
# MAGIC     END AS age_group,
# MAGIC
# MAGIC     education,
# MAGIC
# MAGIC     CASE education
# MAGIC         WHEN 1 THEN 'Ensino médio'
# MAGIC         WHEN 2 THEN 'Graduação'
# MAGIC         WHEN 3 THEN 'Pós-graduação'
# MAGIC         WHEN 4 THEN 'Mestrado ou doutorado'
# MAGIC     END AS education_level,
# MAGIC
# MAGIC     number_of_children,
# MAGIC     social_drinker,
# MAGIC     social_smoker,
# MAGIC     number_of_pets,
# MAGIC     weight,
# MAGIC     height,
# MAGIC     body_mass_index,
# MAGIC     observed_profile_frequency
# MAGIC
# MAGIC FROM ranked_profiles
# MAGIC WHERE profile_rank = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.fact_absence
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC             employee_id,
# MAGIC             month_of_absence,
# MAGIC             day_of_week,
# MAGIC             reason_for_absence,
# MAGIC             absenteeism_time_in_hours,
# MAGIC             ingestion_timestamp
# MAGIC     ) AS absence_event_id,
# MAGIC
# MAGIC     employee_id,
# MAGIC     reason_for_absence AS reason_id,
# MAGIC     (month_of_absence * 100) + (day_of_week * 10) + season AS period_key,
# MAGIC     workload_average_per_day,
# MAGIC     hit_target,
# MAGIC     disciplinary_failure,
# MAGIC     absenteeism_time_in_hours,
# MAGIC     source_file,
# MAGIC     ingestion_timestamp
# MAGIC
# MAGIC FROM workspace.silver.absenteeism_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*)
# MAGIC      FROM workspace.silver.absenteeism_clean) AS rows_silver,
# MAGIC
# MAGIC     (SELECT COUNT(*)
# MAGIC      FROM workspace.gold.fact_absence) AS rows_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.mart_reason_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     f.reason_id,
# MAGIC     r.reason_description,
# MAGIC     r.reason_group,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(f.absenteeism_time_in_hours) AS total_absence_hours,
# MAGIC     ROUND(AVG(f.absenteeism_time_in_hours), 2) AS average_absence_hours
# MAGIC FROM workspace.gold.fact_absence f
# MAGIC LEFT JOIN workspace.gold.dim_reason r
# MAGIC     ON f.reason_id = r.reason_id
# MAGIC GROUP BY
# MAGIC     f.reason_id,
# MAGIC     r.reason_description,
# MAGIC     r.reason_group;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS reason_rows,
# MAGIC     SUM(event_count) AS total_events,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN reason_description IS NULL THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS missing_descriptions
# MAGIC FROM workspace.gold.mart_reason_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.mart_temporal_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     p.month_of_absence,
# MAGIC     p.month_name,
# MAGIC     p.season,
# MAGIC     p.season_name,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(f.absenteeism_time_in_hours) AS total_absence_hours,
# MAGIC     ROUND(
# MAGIC         AVG(f.absenteeism_time_in_hours),
# MAGIC         2
# MAGIC     ) AS average_absence_hours
# MAGIC
# MAGIC FROM workspace.gold.fact_absence f
# MAGIC
# MAGIC LEFT JOIN workspace.gold.dim_period p
# MAGIC     ON f.period_key = p.period_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     p.month_of_absence,
# MAGIC     p.month_name,
# MAGIC     p.season,
# MAGIC     p.season_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS temporal_rows,
# MAGIC     SUM(event_count) AS total_events,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN month_name IS NULL
# MAGIC               OR season_name IS NULL
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS missing_descriptions
# MAGIC FROM workspace.gold.mart_temporal_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'dim_employee' AS table_name, COUNT(*) AS row_count
# MAGIC FROM workspace.gold.dim_employee
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_reason', COUNT(*)
# MAGIC FROM workspace.gold.dim_reason
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_period', COUNT(*)
# MAGIC FROM workspace.gold.dim_period
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_absence', COUNT(*)
# MAGIC FROM workspace.gold.fact_absence
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'mart_reason_summary', COUNT(*)
# MAGIC FROM workspace.gold.mart_reason_summary
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'mart_temporal_summary', COUNT(*)
# MAGIC FROM workspace.gold.mart_temporal_summary;

# COMMAND ----------

table_comments = {
    "workspace.bronze.absenteeism_raw":
        "Dados ingeridos do arquivo Absenteeism_at_work.csv. Os valores de negócio foram preservados e foram adicionados metadados de origem e ingestão.",

    "workspace.silver.absenteeism_clean":
        "Dados de absenteísmo padronizados, tipados e validados para utilização analítica.",

    "workspace.silver.dq_completeness_bronze":
        "Resultado da verificação de valores nulos e vazios nas colunas da camada Bronze.",

    "workspace.silver.dq_duplicates_bronze":
        "Resultado da investigação de registros aparentemente duplicados na camada Bronze.",

    "workspace.silver.dq_domain_rules_bronze":
        "Resultado das regras de validação dos domínios dos dados.",

    "workspace.silver.dq_outliers_bronze":
        "Resultado da identificação de possíveis valores extremos pelo intervalo interquartil.",

    "workspace.gold.dim_employee":
        "Dimensão com o perfil mais frequentemente observado para cada empregado.",

    "workspace.gold.dim_reason":
        "Dimensão com os códigos, descrições e grupos dos motivos de ausência.",

    "workspace.gold.dim_period":
        "Dimensão temporal construída com mês, dia da semana e estação disponíveis na fonte.",

    "workspace.gold.fact_absence":
        "Tabela fato com uma linha para cada registro de ausência existente na base.",

    "workspace.gold.mart_reason_summary":
        "Resumo analítico de ocorrências e horas de ausência por motivo.",

    "workspace.gold.mart_temporal_summary":
        "Resumo analítico de ocorrências e horas de ausência por mês e estação."
}

column_comments = {
    "absence_event_id":
        "Identificador técnico atribuído ao registro da ocorrência.",

    "employee_id":
        "Identificador anonimizado do empregado.",

    "reason_for_absence":
        "Código original do motivo da ausência.",

    "reason_id":
        "Código do motivo da ausência relacionado à dimensão de motivos.",

    "reason_description":
        "Descrição do motivo da ausência.",

    "reason_group":
        "Classificação do motivo em CID, sem CID ou sem classificação.",

    "period_key":
        "Chave técnica formada pela combinação de mês, dia da semana e estação.",

    "month_of_absence":
        "Número do mês da ausência. O valor zero indica mês não informado.",

    "month_name":
        "Nome do mês da ausência.",

    "day_of_week":
        "Código do dia da semana, entre 2 e 6.",

    "day_name":
        "Nome do dia da semana.",

    "season":
        "Código da estação do ano, entre 1 e 4.",

    "season_name":
        "Nome da estação do ano.",

    "transportation_expense":
        "Despesa de transporte associada ao empregado.",

    "distance_from_residence_to_work":
        "Distância em quilômetros entre a residência e o local de trabalho.",

    "service_time":
        "Tempo de serviço do empregado.",

    "age":
        "Idade do empregado em anos.",

    "age_group":
        "Faixa etária criada para utilização nas análises.",

    "workload_average_per_day":
        "Carga média diária de trabalho.",

    "hit_target":
        "Indicador de atingimento da meta.",

    "disciplinary_failure":
        "Indicador de ocorrência disciplinar: 1 para sim e 0 para não.",

    "education":
        "Código do nível educacional.",

    "education_level":
        "Descrição do nível educacional.",

    "number_of_children":
        "Quantidade de filhos.",

    "social_drinker":
        "Indicador de consumo social de bebida: 1 para sim e 0 para não.",

    "social_smoker":
        "Indicador de tabagismo social: 1 para sim e 0 para não.",

    "number_of_pets":
        "Quantidade de animais de estimação.",

    "weight":
        "Peso informado para o empregado.",

    "height":
        "Altura informada para o empregado.",

    "body_mass_index":
        "Índice de massa corporal.",

    "absenteeism_time_in_hours":
        "Quantidade de horas de ausência.",

    "source_file":
        "Nome do arquivo utilizado na ingestão.",

    "ingestion_timestamp":
        "Data e hora de ingestão do registro.",

    "observed_profile_frequency":
        "Quantidade de vezes em que o perfil do empregado foi observado.",

    "event_count":
        "Quantidade de registros de ausência.",

    "total_absence_hours":
        "Soma das horas de ausência.",

    "average_absence_hours":
        "Média de horas de ausência por registro.",

    "column_name":
        "Nome da coluna avaliada.",

    "missing_count":
        "Quantidade de valores nulos ou vazios.",

    "complete_count":
        "Quantidade de valores preenchidos.",

    "missing_percentage":
        "Percentual de valores nulos ou vazios.",

    "total_rows":
        "Quantidade total de registros avaliados.",

    "duplicate_groups":
        "Quantidade de grupos com registros aparentemente repetidos.",

    "duplicate_extra_rows":
        "Quantidade de linhas adicionais existentes nos grupos repetidos.",

    "treatment":
        "Decisão adotada para o tratamento das duplicidades.",

    "quality_rule":
        "Nome da regra de qualidade executada.",

    "invalid_count":
        "Quantidade de registros que não atenderam à regra.",

    "status":
        "Resultado da validação da regra.",

    "nulls_after_conversion":
        "Quantidade de valores nulos encontrados depois da conversão de tipo.",

    "q1":
        "Primeiro quartil da distribuição.",

    "q3":
        "Terceiro quartil da distribuição.",

    "lower_limit":
        "Limite inferior utilizado na identificação de possíveis outliers.",

    "upper_limit":
        "Limite superior utilizado na identificação de possíveis outliers.",

    "possible_outlier_count":
        "Quantidade de possíveis valores extremos identificados."
}

for table_name, table_description in table_comments.items():

    if not spark.catalog.tableExists(table_name):
        print(f"ATENÇÃO: tabela não encontrada: {table_name}")
        continue

    safe_table_description = table_description.replace("'", "''")

    spark.sql(
        f"COMMENT ON TABLE {table_name} "
        f"IS '{safe_table_description}'"
    )

    for column_name in spark.table(table_name).columns:
        description = column_comments.get(
            column_name,
            f"Campo {column_name} pertencente à tabela {table_name}."
        )

        safe_column_description = description.replace("'", "''")

        spark.sql(
            f"ALTER TABLE {table_name} "
            f"ALTER COLUMN `{column_name}` "
            f"COMMENT '{safe_column_description}'"
        )

    print(f"Documentação concluída: {table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED workspace.gold.fact_absence;