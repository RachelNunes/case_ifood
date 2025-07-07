# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 iFood - Case Técnico de Cupons
# MAGIC
# MAGIC Este notebook tem como objetivo iniciar o processamento dos dados para análise do teste A/B de cupons realizado pelo iFood.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tabelas utilizadas nestes notebook
# MAGIC
# MAGIC case_ifood.silver.orders
# MAGIC
# MAGIC case_ifood.gold.orders
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_timestamp, expr, explode_outer, round, format_string, countDistinct, sum,count, avg, explode, when, date_format, min, row_number, desc, dayofweek
from pyspark.sql import Window, functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análises Primárias
# MAGIC

# COMMAND ----------

query = """
SELECT
  cpf,
  p.customer_id,
  p.customer_name,
  ab.is_target,
  c.created_at AS customer_created_at,
  c.active,
  delivery_address_city,
  delivery_address_country,
  delivery_address_district,
  delivery_address_external_id,
  delivery_address_latitude,
  delivery_address_longitude,
  delivery_address_state,
  delivery_address_zip_code,
  items,
  merchant_id,
  merchant_latitude,
  merchant_longitude,
  order_created_at,
  order_id,
  order_scheduled,
  order_total_amount,
  origin_platform,
  order_scheduled_date
FROM case_ifood.silver.orders p
LEFT JOIN case_ifood.silver.consumer c ON p.customer_id = c.customer_id
LEFT JOIN case_ifood.silver.ab_test_ref ab ON p.customer_id = ab.customer_id
LEFT JOIN case_ifood.silver.restaurant r ON p.merchant_id = r.id
"""

df_order_test_ab = spark.sql(query)
df_order_test_ab.display() 


# COMMAND ----------

# Calculando total de pedidos por grupo
window_total = Window.partitionBy()
df_group = (
    df_order_test_ab.groupBy("is_target")
      .agg(countDistinct("order_id").alias("quantidade"))
      .withColumn("total_geral", sum("quantidade").over(window_total))
      .withColumn("porcentagem", round(100.0 * col("quantidade") / col("total_geral"), 2))
      .select(
          col("is_target").alias("grupo"),
          col("quantidade"),
          col("porcentagem")
      )
)

df_group.display()

# COMMAND ----------

df_receita_groupo = (
    df_order_test_ab.groupBy("is_target")
      .agg(round(sum("order_total_amount"), 2).alias("valor_total_pedidos"))
      .withColumn("total_geral", sum("valor_total_pedidos").over(window_total))
      .withColumn("porcentagem", round(100.0 * col("valor_total_pedidos") / col("total_geral"), 2))
      .select(
          col("is_target").alias("grupo"),
          col("valor_total_pedidos"),
          col("porcentagem")
      )
)

df_receita_groupo.display()

# COMMAND ----------

# calculando quantidade de pedidos, receita e ticket médio por grupo
df_result_ab = (
    df_order_test_ab.groupBy("is_target")
      .agg(
          countDistinct("order_id").alias("total_pedidos"),
          round(sum("order_total_amount"), 2).alias("valor_total"),
          round(avg("order_total_amount"), 2).alias("ticket_medio")
      )
      .select(
          col("is_target").alias("grupo"),
          col("total_pedidos"),
          col("valor_total"),
          col("ticket_medio")
      )
)

df_result_ab.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Durante as análises primárias foram identificados alguns pontos:
# MAGIC - As tabelas order e ab_test_ref possuem uma volumetria de customer_id maior que da tabela consumer - 310 registros a mais;
# MAGIC - Pedidos registrados com customer_id nulos - registros que por sua vez, serão desconsiderados da Análise, por representar um valor representa apenas 0,23% dos pedidos e 0,43% da receita, sendo estatisticamente irrelevante para a análise. Além de nãoo serem rastreáveis de identificáveis.
# MAGIC - O terceiro ponto foi a identificação de order_ids duplicados, porém com cpfs e datas de criação do pedido distintas (ex: um mesmo order Id com data de criação em 2018 e 2019). E por entender que um id é uma chave primária identificadora, o primeiro reflexo foi de deduplicar a tabela e utilizar o registro mais recente. Porem ai fazer isso percebi que a base perderia os registros de 2018. Então resolvi desfazer a deduplicação e seguir com a analises partindo da premissa de que se trata de um case, alguns registros podem ter sido duplicados propositalmente com datas diferentes para se ter um histórico.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Análise de Impacto do Teste A/B
# MAGIC

# COMMAND ----------

df_orders_gold = spark.table("case_ifood.gold.orders")

# COMMAND ----------

#Indicadores para Análise do Teste A/B
df_filtered = df_orders_gold.filter(F.col("customer_id").isNotNull())
kpis = df_filtered.groupBy("is_target").agg(
    F.countDistinct("customer_id").alias("total_clientes"),
    F.countDistinct("order_id").alias("total_pedidos"),
    F.round(F.sum("order_total_amount"), 2).alias("valor_total"),
    F.round(F.avg("order_total_amount"), 2).alias("ticket_medio"),
    F.round(F.sum("order_total_amount") / F.countDistinct("customer_id"), 2).alias("receita_por_cliente")
)

kpis.orderBy("is_target").display(truncate=False)

# COMMAND ----------

#Quantidade de clientes por grupo

df_orders_gold.groupBy("is_target") \
    .agg(countDistinct("customer_id").alias("total_clientes")) \
    .display()

# COMMAND ----------

# calculando quantidade de pedidos, receita e ticket médio por grupo
window_spec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing) 

df_result_test_ab = (
    df_orders_gold.groupBy("is_target")
      .agg(
          countDistinct("order_id").alias("total_pedidos"),
          countDistinct("customer_id").alias("total_clientes"),
          round(sum("order_total_amount"), 2).alias("valor_total"),
          round(avg("order_total_amount"), 2).alias("ticket_medio")
      )
)
df_result_test_ab = (
    df_result_test_ab
    .withColumn("percentual_pedidos", round(col("total_pedidos") / sum("total_pedidos").over(window_spec) * 100, 2))
    .withColumn("percentual_valor_total", round(col("valor_total") / sum("valor_total").over(window_spec) * 100, 2))
    .select(
        col("is_target").alias("grupo"),
        "total_clientes",
        "total_pedidos",
        "valor_total",
        "ticket_medio",
        "percentual_pedidos",
        "percentual_valor_total"
    )
)

display(df_result_test_ab)

# COMMAND ----------

# Média de Pedidos por Cliente/grupo

df_pedidos_por_cliente = (
    df_orders_gold.groupBy("is_target")
    .agg(
        countDistinct("order_id").alias("total_pedidos"),
        countDistinct("customer_id").alias("total_clientes")
    )
    .withColumn("pedidos_por_cliente", round(col("total_pedidos") / col("total_clientes"), 2))
)

df_pedidos_por_cliente.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculo de Viabilidade Financeira

# COMMAND ----------

# Calculando total de desconto aplicado na base, através do campo discount.value no array items
df_desconto_total = df_orders_gold.withColumn(
    "desconto_total_pedido",
    F.expr("""
        aggregate(
            items,
            0D,
            (acc, x) -> acc + double(x.discount.value)
        )
    """)
)

# Soma total de desconto aplicado na base
desconto_total = df_desconto_total.agg(
    F.sum("desconto_total_pedido").alias("total_desconto")
).first()["total_desconto"]

print(f"Total de desconto aplicado na base: R${desconto_total:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC - Cupom de R$10 por pedido;
# MAGIC - Foi considerado que todos os pedidos do grupo teste(target) usaram o cupom;
# MAGIC - Cálculo do Custo da Campanha (Como só o grupo Target recebeu cupons, o custo total será proporcional ao número de pedidos feitos por esse grupo);
# MAGIC - Cálculo da Receita Incremental (cálculo de quanto a campanha trouxe a mais em receita por cliente);
# MAGIC - Comparação Custo x Receita Incremental
# MAGIC

# COMMAND ----------

## Cupom de R$10 fixos

df_metrics = (
    df_orders_gold.groupBy("is_target")
    .agg(
        F.countDistinct("order_id").alias("total_pedidos"),
        F.countDistinct("customer_id").alias("total_clientes"),
        F.sum("order_total_amount").alias("valor_total")
    )
    .withColumn("ticket_medio", F.col("valor_total") / F.col("total_pedidos"))
    .withColumn("receita_media_por_cliente", F.col("valor_total") / F.col("total_clientes"))
)

# Separando os valores por grupo
control = df_metrics.filter(F.col("is_target") == "control").first()
target = df_metrics.filter(F.col("is_target") == "target").first()

receita_incremental_total = target["valor_total"] - control["valor_total"]

receita_media_control = control["receita_media_por_cliente"]
receita_media_target = target["receita_media_por_cliente"]
receita_incremental_por_cliente = receita_media_target - receita_media_control

receita_incremental_estim_clientes = receita_incremental_por_cliente * target["total_clientes"]


# Custo da campanha com cupom fixo de R$10 por pedido
custo_campanha_10reais = 10.0 * target["total_pedidos"]

# Saldos líquidos
saldo_10reais_total = receita_incremental_total - custo_campanha_10reais
saldo_10reais_por_cliente = receita_incremental_estim_clientes - custo_campanha_10reais

print("==== Métricas Gerais ====")
print(f"Total de clientes (Target): {target['total_clientes']}")
print(f"Total de clientes (Control): {control['total_clientes']}")
print(f"Total de pedidos (Target): {target['total_pedidos']}")
print(f"Total de pedidos (Control): {control['total_pedidos']}")
print(f"Receita total (Target): R${target['valor_total']:.2f}")
print(f"Receita total (Control): R${control['valor_total']:.2f}")
print(f"Ticket médio (Target): R${target['ticket_medio']:.2f}")
print(f"Ticket médio (Control): R${control['ticket_medio']:.2f}")
print(f"Receita média por cliente (Target): R${receita_media_target:.2f}")
print(f"Receita média por cliente (Control): R${receita_media_control:.2f}")

print("\n==== Receita Incremental ====")
print(f"Receita incremental total: R${receita_incremental_total:.2f}")
print(f"Receita incremental por cliente: R${receita_incremental_por_cliente:.2f}")
print(f"Receita incremental estimada por cliente: R${receita_incremental_estim_clientes:.2f}")

print("\n==== Custo e Saldo da Campanha ====")
print(f"Custo com cupom fixo de R$10: R${custo_campanha_10reais:.2f}")
print(f"Saldo líquido com cupom fixo R$10 (base total): R${saldo_10reais_total:.2f}")
print(f"Saldo líquido com cupom fixo R$10 (base receita por cliente): R${saldo_10reais_por_cliente:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Segmentação
# MAGIC

# COMMAND ----------

#Clusterização por perfil

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# Agrupamento dos dados por cliente e definição de métricas/indicadores
df_clientes = spark.table("case_ifood.gold.orders").groupBy("customer_id").agg(
    F.countDistinct("order_id").alias("qtd_pedidos"),
    F.sum("order_total_amount").alias("receita_total"),
    F.avg("order_total_amount").alias("ticket_medio"),
    F.max("order_created_at").alias("ultima_compra"),
    F.min("order_created_at").alias("primeira_compra")

#variáveis de comportamento temporal
).withColumn(
    "tempo_ativo_dias", F.datediff("ultima_compra", "primeira_compra") #quanto tempo se passou entre o primeiro e o último pedido
).withColumn(
    "frequencia_media", F.when(F.col("qtd_pedidos") > 1,
    F.col("tempo_ativo_dias") / (F.col("qtd_pedidos") - 1)).otherwise(None)
) #em média, quantos dias se passam entre pedidos

# Converção para Pandas
pdf_clientes = df_clientes.select(
    "customer_id", "qtd_pedidos", "receita_total", "ticket_medio", "frequencia_media"
).dropna().toPandas()

# Normalização das features
features = ["qtd_pedidos", "receita_total", "ticket_medio", "frequencia_media"]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(pdf_clientes[features])

# Aplica KMeans e clusterização (4 clusters)
kmeans = KMeans(n_clusters=4, random_state=42)
pdf_clientes["cluster"] = kmeans.fit_predict(X_scaled)

# Exibe clusters
pdf_clientes.head()

# COMMAND ----------

# análise das caracterísiticas médias de cada cluster
pdf_clientes.groupby("cluster")[features].mean().round(2)

# COMMAND ----------

pdf_clientes["cluster"].value_counts().sort_index()

# COMMAND ----------

# Nomeando os clusters de acordo com as variáveis comportamentais
nomes_clusters = {
    0: "Inativos / Ocasionais",
    1: "Fiel / Recorrente",
    2: "Ativo moderado / Regular",
    3: "Mega gasto"
}

pdf_clientes["perfil_cluster"] = pdf_clientes["cluster"].map(nomes_clusters)

# COMMAND ----------

pdf_clientes.groupby("perfil_cluster")[features].mean().round(2)

# COMMAND ----------

# Contagem absoluta
qtd_por_perfil = pdf_clientes["perfil_cluster"].value_counts()

# Com porcentagem
qtd_por_perfil_pct = pdf_clientes["perfil_cluster"].value_counts(normalize=True) * 100

# Juntando tudo em um DataFrame
df_qtd_perfis = pd.DataFrame({
    "quantidade_clientes": qtd_por_perfil,
    "percentual_clientes": qtd_por_perfil_pct.round(2)
}).reset_index().rename(columns={"index": "perfil_cluster"})

print(df_qtd_perfis)

# COMMAND ----------

pdf_clientes["perfil_cluster"].value_counts(normalize=True).plot(
    kind="barh", figsize=(8, 4), title="Distribuição de Clientes por Perfil (%)", xlabel="Proporção"
)

# COMMAND ----------

pdf_clientes.groupby("perfil_cluster")[["receita_total", "ticket_medio", "qtd_pedidos", "frequencia_media"]].mean().round(2)

# COMMAND ----------

df_campanha = spark.table("case_ifood.gold.orders") \
    .select("customer_id", "is_target") \
    .dropDuplicates(["customer_id"]) \
    .toPandas()

# Junta com os clusters
df_analise = pdf_clientes.merge(df_campanha, on="customer_id", how="left")

# Checa valores únicos em is_target
print(df_analise["is_target"].value_counts())


# COMMAND ----------

agrupado = df_analise.groupby(["perfil_cluster", "is_target"]).agg(
    clientes=("customer_id", "nunique"),
    receita_total=("receita_total", "sum"),
    receita_media=("receita_total", "mean"),
    ticket_medio=("ticket_medio", "mean"),
    qtd_pedidos_media=("qtd_pedidos", "mean"),
).reset_index()

colunas_para_arredondar = ["receita_total", "receita_media", "ticket_medio", "qtd_pedidos_media"]
agrupado[colunas_para_arredondar] = agrupado[colunas_para_arredondar].round(2)

display(agrupado)

# COMMAND ----------

# MAGIC %md
# MAGIC Foi identificado um perfil com comportamento anormal, que por vez será excluído das análises principais. Mas em um cenário real, o recomendável é fazer uma análise individual, se legítimo, oferecer atendimento dedicado.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Outras segmentações

# COMMAND ----------

# MAGIC %md
# MAGIC Indicadores da análise

# COMMAND ----------

# Dataframe definindo os indicadores para analisar cada segmento
df_metricas_analise = df_orders_gold.groupBy("customer_id").agg(
    countDistinct("order_id").alias("qtd_pedidos"),
    sum("order_total_amount").alias("receita_total"),
    avg(col("order_scheduled").cast("double")).alias("pct_pedidos_agendados"),
    F.first("is_target").alias("grupo_ab")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Segmentação por horário do pedido
# MAGIC
# MAGIC

# COMMAND ----------

# segmentação de clientes por horário de pedido
df = df_orders_gold.select(
    "customer_id", "order_id", "order_created_at", "order_total_amount", "order_scheduled", "is_target"
).dropna(subset=["customer_id", "order_id", "order_created_at"])
df = df.withColumn("hora_pedido", F.hour("order_created_at"))

# classificação de horários
df = df.withColumn(
    "segmento_horario",
    when((col("hora_pedido") >= 0) & (col("hora_pedido") < 6), "Madrugador")
    .when((col("hora_pedido") >= 6) & (col("hora_pedido") < 12), "Matinal")
    .when((col("hora_pedido") >= 12) & (col("hora_pedido") < 14), "Almoço")
    .when((col("hora_pedido") >= 14) & (col("hora_pedido") < 18), "Tarde")
    .otherwise("Noturno")
)


# COMMAND ----------

# quantos pedidos o cliente fez em cada horário e qual horário predominante por cliente
df_horario = df.groupBy("customer_id", "segmento_horario").agg(count("*").alias("qtd_pedidos"))
window_horario = Window.partitionBy("customer_id").orderBy(col("qtd_pedidos").desc())
df_horario_dominante = df_horario.withColumn("rank", row_number().over(window_horario)) \
.filter(col("rank") == 1) \
.drop("rank", "qtd_pedidos")


# COMMAND ----------

# Resultado da segmentação por horário do pedido

df_final = df_metricas_analise.join(df_horario_dominante, "customer_id", "left")

df_resultado = df_final.groupBy("segmento_horario", "grupo_ab").agg(
    countDistinct("customer_id").alias("total_clientes"),
    sum("qtd_pedidos").alias("total_pedidos"),
    sum("receita_total").alias("receita_total"),
    (sum("receita_total") / countDistinct("customer_id")).alias("receita_por_cliente"),
    (sum("receita_total") / sum("qtd_pedidos")).alias("ticket_medio"),
    avg("pct_pedidos_agendados").alias("pct_agendados_medio")
)

df_resultado = df_resultado.select(
    "segmento_horario", "grupo_ab",
    "total_clientes", "total_pedidos",
    round("receita_total", 2).alias("receita_total"),
    round("receita_por_cliente", 2).alias("receita_por_cliente"),
    round("ticket_medio", 2).alias("ticket_medio"),
    round("pct_agendados_medio", 2).alias("pct_agendados_medio")
).orderBy("segmento_horario", "grupo_ab")

df_resultado.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Segmentação por dia da semana/pedido
# MAGIC

# COMMAND ----------

df = df_orders_gold.select(
    "customer_id", "order_id", "order_total_amount", "order_scheduled", "order_created_at", "is_target"
).dropna(subset=["customer_id", "order_id", "order_created_at"])
df = df.withColumn("dia_semana", dayofweek("order_created_at")) #extrair dias da semana
df = df.withColumn("hora_pedido", F.hour("order_created_at")) #extrair horário

# classificando os dias da semana em fim de semana e dias de semana
# de sexta-feira a partir das 18h à domingo - segmentado como fim de semana
# de segunda à sexta até às 17h - segmentado como dias de semana
df = df.withColumn(
    "tipo_dia",
    when((col("dia_semana").isin(1, 7)) | ((col("dia_semana") == 6) & (col("hora_pedido") >= 18)), "Fim de semana")
    .otherwise("Dias de semana")
)

# COMMAND ----------

# Contagem da qtd de pedidos por tipo de dia 
df_tipo_dia = df.groupBy("customer_id", "tipo_dia").agg(count("order_id").alias("qtd"))
window_dia = Window.partitionBy("customer_id").orderBy(col("qtd").desc())

# definição do dia predominante
df_segmento_dia = df_tipo_dia.withColumn("rank", row_number().over(window_dia)) \
.filter(col("rank") == 1) \
.drop("rank", "qtd") \
.withColumnRenamed("tipo_dia", "segmento_dia_preferido")

# COMMAND ----------

# Resultado da segmentação por dia do pedido
df_final = df_metricas_analise.join(df_segmento_dia, on="customer_id", how="left")

df_resultado_dia = df_final.groupBy("segmento_dia_preferido", "grupo_ab").agg(
    countDistinct("customer_id").alias("total_clientes"),
    sum("qtd_pedidos").alias("total_pedidos"),
    sum("receita_total").alias("receita_total"),
    (sum("receita_total") / countDistinct("customer_id")).alias("receita_por_cliente"),
    (sum("receita_total") / sum("qtd_pedidos")).alias("ticket_medio"),
    avg("pct_pedidos_agendados").alias("pct_agendados_medio")
)

df_resultado_dia = df_resultado_dia.select(
    "segmento_dia_preferido", "grupo_ab",
    "total_clientes", "total_pedidos",
    round("receita_total", 2).alias("receita_total"),
    round("receita_por_cliente", 2).alias("receita_por_cliente"),
    round("ticket_medio", 2).alias("ticket_medio"),
    round("pct_agendados_medio", 2).alias("pct_agendados_medio")
).orderBy("segmento_dia_preferido", "grupo_ab")

display(df_resultado_dia)

# COMMAND ----------

# MAGIC %md
# MAGIC Segmentação por tipo de restaurante - a partir da média de preço do restaurante

# COMMAND ----------

df_price_range = df_orders_gold.select("customer_id", "merchant_id", "merchant_price_range") \
                               .dropna(subset=["merchant_price_range"]).dropDuplicates()

# 2. Calcular média da faixa de preço por cliente
df_avg_price_range = df_price_range.groupBy("customer_id").agg(
    avg("merchant_price_range").alias("media_price_range")
)

# 3. Classificar os clientes em segmentos
df_segmento_preco = df_avg_price_range.withColumn(
    "segmento_faixa_preco_restaurante",
    when(col("media_price_range") <= 1.5, "Econômico")
    .when(col("media_price_range") <= 2.5, "Intermediário")
    .otherwise("Premium")
)


# COMMAND ----------

df_final_preco_rest = df_metricas_analise.join(df_segmento_preco, on="customer_id", how="left")

df_resultado_range_rest = df_final_preco_rest.groupBy("segmento_faixa_preco_restaurante", "grupo_ab").agg(
    countDistinct("customer_id").alias("total_clientes"),
    sum("qtd_pedidos").alias("total_pedidos"),
    sum("receita_total").alias("receita_total"),
    (sum("receita_total") / countDistinct("customer_id")).alias("receita_por_cliente"),
    (sum("receita_total") / sum("qtd_pedidos")).alias("ticket_medio"),
    avg("pct_pedidos_agendados").alias("pct_agendados_medio")
)

df_resultado_range_rest = df_resultado_range_rest.select(
    "segmento_faixa_preco_restaurante", "grupo_ab",
    "total_clientes", "total_pedidos",
    round("receita_total", 2).alias("receita_total"),
    round("receita_por_cliente", 2).alias("receita_por_cliente"),
    round("ticket_medio", 2).alias("ticket_medio"),
    round("pct_agendados_medio", 2).alias("pct_agendados_medio")
).orderBy("segmento_faixa_preco_restaurante", "grupo_ab")

display(df_resultado_range_rest)

# COMMAND ----------

