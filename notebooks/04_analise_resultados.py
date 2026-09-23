# Databricks notebook source
# MAGIC %md
# MAGIC # Análise dos resultados
# MAGIC
# MAGIC ## Problema de negócio
# MAGIC
# MAGIC O absenteísmo pode comprometer a produtividade e a organização do trabalho. Este MVP busca organizar e disponibilizar dados confiáveis para analisar como as horas de ausência estão distribuídas entre diferentes motivos, períodos e características dos empregados.
# MAGIC
# MAGIC ## Perguntas de negócio
# MAGIC
# MAGIC 1. Quais motivos concentram a maior quantidade de horas de ausência?
# MAGIC 2. Como as horas de ausência variam entre os meses?
# MAGIC 3. Como o absenteísmo varia entre as faixas etárias?
# MAGIC 4. Como o absenteísmo varia conforme a distância entre residência e trabalho?
# MAGIC
# MAGIC As análises realizadas são descritivas. Os resultados mostram associações existentes na base, mas não demonstram relações de causa e efeito.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS absence_records,
# MAGIC     COUNT(DISTINCT employee_id) AS distinct_employees,
# MAGIC     SUM(absenteeism_time_in_hours) AS total_absence_hours,
# MAGIC     ROUND(
# MAGIC         AVG(absenteeism_time_in_hours),
# MAGIC         2
# MAGIC     ) AS average_hours_per_record,
# MAGIC     PERCENTILE_APPROX(
# MAGIC         absenteeism_time_in_hours,
# MAGIC         0.5
# MAGIC     ) AS median_hours_per_record
# MAGIC
# MAGIC FROM workspace.gold.fact_absence;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     reason_id,
# MAGIC     reason_description,
# MAGIC     reason_group,
# MAGIC     event_count,
# MAGIC     total_absence_hours,
# MAGIC     average_absence_hours
# MAGIC
# MAGIC FROM workspace.gold.mart_reason_summary
# MAGIC
# MAGIC WHERE reason_id <> 0
# MAGIC
# MAGIC ORDER BY total_absence_hours DESC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretação dos motivos das ausências
# MAGIC
# MAGIC A análise demonstra que as doenças osteomusculares e do tecido conjuntivo concentraram a maior quantidade de horas de ausência, com 842 horas registradas. Em seguida, aparecem as lesões, intoxicações e outras consequências de causas externas, com 729 horas, e as consultas médicas, com 424 horas.
# MAGIC
# MAGIC Os dois principais grupos apresentam uma diferença de 113 horas, enquanto o segundo grupo supera as consultas médicas em 305 horas. Isso demonstra uma concentração relevante das horas de ausência em motivos relacionados a condições osteomusculares e a lesões ou outras consequências de causas externas.
# MAGIC
# MAGIC A análise é descritiva e não permite afirmar que esses motivos tenham sido causados pelas características pessoais ou profissionais disponíveis na base.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     month_of_absence,
# MAGIC     CONCAT(
# MAGIC         LPAD(CAST(month_of_absence AS STRING), 2, '0'),
# MAGIC         ' - ',
# MAGIC         month_name
# MAGIC     ) AS month_label,
# MAGIC     SUM(event_count) AS event_count,
# MAGIC     SUM(total_absence_hours) AS total_absence_hours,
# MAGIC     ROUND(
# MAGIC         SUM(total_absence_hours) / SUM(event_count),
# MAGIC         2
# MAGIC     ) AS average_hours_per_record
# MAGIC FROM workspace.gold.mart_temporal_summary
# MAGIC WHERE month_of_absence <> 0
# MAGIC GROUP BY
# MAGIC     month_of_absence,
# MAGIC     month_name
# MAGIC ORDER BY
# MAGIC     month_of_absence;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretação da distribuição mensal
# MAGIC
# MAGIC O mês com maior quantidade de horas de ausência foi de março, com 765 horas. O menor resultado foi observado em janeiro, com 222 horas.
# MAGIC
# MAGIC Os resultados demonstram que as horas de ausência não se distribuem uniformemente entre os meses. Entretanto, a análise é descritiva e a base não permite concluir que o mês, isoladamente, seja responsável pelo aumento ou pela redução das ausências.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE e.age_group
# MAGIC         WHEN 'Até 29 anos' THEN '1 - Até 29 anos'
# MAGIC         WHEN '30 a 39 anos' THEN '2 - 30 a 39 anos'
# MAGIC         WHEN '40 a 49 anos' THEN '3 - 40 a 49 anos'
# MAGIC         ELSE '4 - 50 anos ou mais'
# MAGIC     END AS age_group,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(f.absenteeism_time_in_hours) AS total_absence_hours,
# MAGIC     ROUND(AVG(f.absenteeism_time_in_hours), 2) AS average_hours_per_record
# MAGIC FROM workspace.gold.fact_absence AS f
# MAGIC INNER JOIN workspace.gold.dim_employee AS e
# MAGIC     ON f.employee_id = e.employee_id
# MAGIC GROUP BY
# MAGIC     e.age_group
# MAGIC ORDER BY
# MAGIC     age_group;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretação por faixa etária
# MAGIC
# MAGIC A faixa etária com a maior média de horas de ausência por registro foi a de pessoas com 50 anos ou mais, com média de 12,8 horas. A menor média foi observada na faixa de pessoas com até 29 anos, com 5,45 horas.
# MAGIC
# MAGIC Essa comparação descreve diferenças existentes na base, mas não demonstra que a idade seja a causa das ausências.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN e.distance_from_residence_to_work <= 10
# MAGIC             THEN '1 - Até 10 km'
# MAGIC         WHEN e.distance_from_residence_to_work <= 20
# MAGIC             THEN '2 - 11 a 20 km'
# MAGIC         WHEN e.distance_from_residence_to_work <= 30
# MAGIC             THEN '3 - 21 a 30 km'
# MAGIC         ELSE '4 - Acima de 30 km'
# MAGIC     END AS distance_group,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(f.absenteeism_time_in_hours) AS total_absence_hours,
# MAGIC     ROUND(AVG(f.absenteeism_time_in_hours), 2) AS average_hours_per_record
# MAGIC FROM workspace.gold.fact_absence AS f
# MAGIC INNER JOIN workspace.gold.dim_employee AS e
# MAGIC     ON f.employee_id = e.employee_id
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN e.distance_from_residence_to_work <= 10
# MAGIC             THEN '1 - Até 10 km'
# MAGIC         WHEN e.distance_from_residence_to_work <= 20
# MAGIC             THEN '2 - 11 a 20 km'
# MAGIC         WHEN e.distance_from_residence_to_work <= 30
# MAGIC             THEN '3 - 21 a 30 km'
# MAGIC         ELSE '4 - Acima de 30 km'
# MAGIC     END
# MAGIC ORDER BY
# MAGIC     distance_group;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretação por distância
# MAGIC
# MAGIC A faixa de distância com a maior média de horas de ausência por registro foi a de 11 a 20 km entre a residência e o local de trabalho, com 10,04 horas. A menor média foi observada na faixa de 21 a 30 km, com 5,69 horas.
# MAGIC
# MAGIC O resultado mostra uma associação descritiva entre distância e horas de ausência nesta base, mas não permite concluir que a distância seja a causa do absenteísmo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusões gerais
# MAGIC
# MAGIC A base analisada contém 740 registros referentes a 36 empregados, totalizando 5.124 horas de ausência. A média foi de 6,92 horas por registro, enquanto a mediana foi de 3 horas. A diferença entre essas duas medidas indica que registros com durações mais elevadas influenciam a média geral.
# MAGIC
# MAGIC Em relação aos motivos, as doenças osteomusculares e do tecido conjuntivo concentraram o maior total de horas de ausência, com 842 horas. Em seguida, aparecem as lesões, intoxicações e outras consequências de causas externas, com 729 horas, e as consultas médicas, com 424 horas. Esses resultados indicam maior concentração das horas de ausência nos dois primeiros grupos.
# MAGIC
# MAGIC Na distribuição mensal, março apresentou o maior total de horas de ausência, com 765 horas, enquanto janeiro apresentou o menor, com 222 horas. Essa variação demonstra que as ausências não se distribuíram uniformemente ao longo dos meses analisados.
# MAGIC
# MAGIC Na comparação por idade, os empregados com 50 anos ou mais apresentaram a maior média de horas de ausência por registro, com 12,8 horas. A menor média foi observada entre os empregados com até 29 anos, com 5,45 horas.
# MAGIC
# MAGIC Na análise da distância entre residência e trabalho, a faixa de 11 a 20 km apresentou a maior média, com 10,04 horas por registro. A menor média foi encontrada na faixa de 21 a 30 km, com 5,69 horas.
# MAGIC
# MAGIC Os resultados permitem identificar grupos, períodos e motivos que podem receber maior atenção em análises posteriores. Entretanto, todas as análises realizadas são descritivas. As diferenças encontradas não comprovam que o mês, a idade ou a distância sejam causas do absenteísmo.
# MAGIC
# MAGIC Também é necessário considerar que as faixas etárias e de distância podem conter quantidades diferentes de registros. Além disso, a base representa uma única organização e um período específico, o que limita a generalização dos resultados para outras empresas ou populações.
# MAGIC
# MAGIC Como aplicação prática, os dados podem apoiar o monitoramento agregado do absenteísmo, o planejamento de ações de saúde ocupacional e a investigação de períodos com maior concentração de horas de ausência. Os resultados não devem ser utilizados isoladamente para avaliar empregados ou tomar decisões individuais.