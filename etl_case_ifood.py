# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 iFood - Case Técnico de Cupons
# MAGIC
# MAGIC Este notebook tem como objetivo de ler e tratar as bases para análise do teste A/B de cupons realizado pelo iFood.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descrição do Método Utilizado no Databricks
# MAGIC
# MAGIC Para a ingestão e estruturação dos dados no ambiente Databricks, foi adotado o processo de upload direto de arquivos no formato CSV, que foram posteriormente registrados como tabelas dentro de um catálogo unificado (case_ifood) da plataforma.
# MAGIC
# MAGIC A estrutura adotada contemplou arquitetura em camadas, baseada na arquitetura de medalha:
# MAGIC
# MAGIC - Bronze: upload e armazenamento do arquivo order.json.
# MAGIC
# MAGIC - Silver: armazenamento dos dados de consumer, ab_test_ref e restaurant conforme recebidos, com garantia de integridade, padronização de nomenclaturas e conformidade estrutural. Essa camada serve como base confiável para o consumo e transformação dos dados.
# MAGIC
# MAGIC - Gold: voltada à geração de insights e indicadores, a partir da aplicação de regras de negócio, agregações e cálculos sobre os dados da camada Silver, com foco em consumo analítico.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura das Tabelas

# COMMAND ----------

df_consumer = spark.table("case_ifood.silver.consumer")

# COMMAND ----------

df_test_ab = spark.table("case_ifood.silver.ab_test_ref")

# COMMAND ----------

df_restaurant = spark.read.table("case_ifood.silver.restaurant") 

# COMMAND ----------

# Leitura do arquivo json
df_orders = spark.read \
    .json("dbfs:/Volumes/case_ifood/bronze/orders/order.json.gz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento para corrigir o Schema da base de pedidos

# COMMAND ----------

# Definição do schema na coluna items



items_schema = ArrayType(
    StructType([
        StructField("name", StringType(), True),
        StructField("addition", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("discount", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("quantity", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("unitPrice", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("externalId", StringType(), True),
        StructField("totalValue", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("customerNote", StringType(), True),
        StructField("integrationId", StringType(), True),
        StructField("totalAddition", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("totalDiscount", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("garnishItems", ArrayType(
            StructType([
                StructField("name", StringType(), True),
                StructField("addition", StructType([
                    StructField("value", StringType(), True),
                    StructField("currency", StringType(), True)
                ]), True),
                StructField("discount", StructType([
                    StructField("value", StringType(), True),
                    StructField("currency", StringType(), True)
                ]), True),
                StructField("quantity", StringType(), True),
                StructField("sequence", StringType(), True),
                StructField("unitPrice", StructType([
                    StructField("value", StringType(), True),
                    StructField("currency", StringType(), True)
                ]), True),
                StructField("categoryId", StringType(), True),
                StructField("externalId", StringType(), True),
                StructField("totalValue", StructType([
                    StructField("value", StringType(), True),
                    StructField("currency", StringType(), True)
                ]), True),
                StructField("categoryName", StringType(), True),
                StructField("integrationId", StringType(), True)
            ])
        ), True)
    ])
)

# COMMAND ----------

df_orders_with_items_schema = df_orders \
    .withColumn("items", 
        from_json(col("items"), items_schema)
    )

# COMMAND ----------

df_orders_with_schema = df_orders_with_items_schema \
    .withColumn("delivery_address_latitude", round(col("delivery_address_latitude").cast("double"), 2)) \
    .withColumn("delivery_address_longitude", round(col("delivery_address_longitude").cast("double"), 2)) \
    .withColumn("merchant_latitude", round(col("merchant_latitude").cast("double"), 2)) \
    .withColumn("merchant_longitude", round(col("merchant_longitude").cast("double"), 2)) \
    .withColumn("order_created_at", to_timestamp(col("order_created_at"))) \
    .withColumn("order_scheduled_date", to_timestamp(col("order_scheduled_date")))


# COMMAND ----------

# Dedeuplicação da tabela de pedidos
#window_spec = Window.partitionBy("order_id").orderBy(desc("order_created_at"))

#df_deduplicado = (
    #.withColumn("row_num", row_number().over(window_spec))  # numera os registros por order_id, mantendo o mais recente como 1
    #.filter(col("row_num") == 1)  # mantém apenas o mais recente
    #.drop("row_num") )

# COMMAND ----------

# MAGIC %md
# MAGIC Escrita da tabela de Orders (Pedidos) na Silver

# COMMAND ----------


df_orders_with_schema.write.mode("overwrite").saveAsTable("case_ifood.silver.orders") # tabela de pedidos agrupados

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Criação da tabela final - usada para as análises

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
  r.created_at as merchant_created_at,
  r.enabled as merchant_enabled,
  r.price_range AS merchant_price_range,
  r.average_ticket AS merchant_average_ticket,
  r.delivery_time AS merchant_delivery_time,
  r.minimum_order_value AS merchant_minimum_order_value,
  r.merchant_city,
  r.merchant_state,
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
INNER JOIN case_ifood.silver.ab_test_ref ab ON p.customer_id = ab.customer_id
LEFT JOIN case_ifood.silver.restaurant r ON p.merchant_id = r.id
WHERE p.customer_id IS NOT NULL
"""

df_order_without_null = spark.sql(query)
df_order_without_null.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escrita da tabela final(ABT) de Orders na Gold 

# COMMAND ----------

### Tabela final de pedidos para análises 
df_order_without_null.write.mode("overwrite").saveAsTable("case_ifood.gold.orders")

# COMMAND ----------

    