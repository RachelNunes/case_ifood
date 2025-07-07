#  Campanha de Cupons — Case iFood

Esta análise busca avaliar os efeitos de uma campanha promocional aplicada ao grupo **Target**, em comparação ao grupo **Control**, com o intuito de identificar se houve impacto relevante em métricas como **volume de pedidos**, **receita total** e **comportamento de gasto**.

---

## 📊 Visão Geral

O projeto foi conduzido em um ambiente analítico baseado no **Databricks Community**, utilizando as linguagens **Python** e **Spark**, com foco em:

- Análise de impacto de um **teste A/B** promocional
- Estimativa de **viabilidade financeira** da campanha
- clusterização para criar **Segmentações de clientes** para entender padrões comportamentais

---

## 🏗️ Arquitetura e Metodologia

A estrutura de dados segue a arquitetura em camadas (**medalha**), com organização por catálogos e schemas:

### 🔵 Bronze  
- Upload manual do arquivo `order.json` como volume bruto, pelos dados estarem mais brutos e precisarem de tratamento no schema.

### 🟠 Silver  
- Upload manual dos arquivos CVS `consumer`, `ab_test_ref` e `restaurant` como tabelas 


### 🟡 Gold  
- Geração de **insights e indicadores analíticos** por meio de:
  - Agregações
  - Regras de negócio
  - Cálculos para análises estratégicas

---

## 📓 Notebooks

Foram desenvolvidos dois notebooks principais:

### 1. **ETL & Tratamento**
- Leitura e estruturação da tabela `orders`
- Integração com outras tabelas da camada Silver
- Preparação da base final para análise

### 2. **Análise de Dados**
- Avaliação do impacto do teste A/B
- Análise da viabilidade financeira da campanha
- Criação de **segmentações estratégicas** para análise de comportamento

---

## 🛠 Tecnologias Utilizadas

- 🐍 Python  
- ⚡ PySpark  
- 📊 Databricks Community Edition  
- 💾 Delta Lake (implícito via arquitetura de camadas)  

---

## ▶️ Como Executar o Projeto

1. Faça login no [Databricks Community](https://community.cloud.databricks.com/)
2. Crie um catálogo com nome "case_ifood".
3. Crie os três database bronze, silver e gold.
4. Faça o upload dos arquivos CSV como tabela na silver e JSON como volume na bronze, conforme a estrutura original.
5. Crie um novo Workspace e importe os notebooks (`etl_case_ifood` e `analytics_ifood_case`)
6. Execute o notebook de ETL para preparar os dados
7. Execute o notebook de Análise para gerar os insights

---



## 👤 Autora

**Rachel Moura**  
