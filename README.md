# <img src="https://www.vectorlogo.zone/logos/google_bigquery/google_bigquery-icon.svg" height="40" align="top"> Otimização de Consultas Avançadas no Google BigQuery

![BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=for-the-badge&logo=google-bigquery&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-00758F?style=for-the-badge&logo=mysql&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Data_Engineering-orange?style=for-the-badge&logo=databricks&logoColor=white)

Este repositório apresenta uma série de técnicas práticas para a otimização de consultas SQL no **Google BigQuery**, focando na redução de custos (**Bytes Billed**) e na melhoria do tempo de execução (**Slot Time**).

As otimizações foram aplicadas sobre uma base de dados de varejo, demonstrando como sair de padrões ineficientes para arquiteturas de alta performance na nuvem.

---

## 🏗️ Contexto dos Dados
Os dados utilizados neste projeto são provenientes do ecossistema de varejo já explorado no meu projeto anterior: 
👉 **[Pipeline de Dados Varejo - BigQuery & Python](https://github.com/andrelcolombo/Pipeline_de_Dados_Varejo_BigQuery_Python)**.

A estrutura principal consiste em um esquema estrela (*Star Schema*):
* **Fato:** `fato_venda` (Registros de transações e valores)
* **Dimensões:** `dim_cliente` e `dim_data`

---

## 🛠️ Técnicas de Otimização Implementadas

### 1. Window Functions vs. Subconsultas Correlacionadas
Subconsultas correlacionadas forçam o BigQuery a re-escanear a tabela para cada linha.
* **Solução:** Implementação de `SUM() OVER()` para cálculos acumulados em uma única passagem pelos dados (*Single Pass*).

### 2. Eliminação de Produtos Cartesianos (`CROSS JOIN`)
Identificação de `JOINS` mal planejados que esgotam a memória dos slots e inflam os resultados.
* **Solução:** Substituição por `INNER JOIN` utilizando chaves de relacionamento (Hash Join).

### 3. Refatoração de CTEs e Redução de Overhead
Remoção de funções de rankeamento (`ROW_NUMBER`) e agrupamentos desnecessários que adicionavam complexidade sem valor analítico.

### 4. Particionamento de Tabelas
Consultas em tabelas não particionadas resultam em *Full Table Scans*.
* **Solução:** Criação de tabelas particionadas por `DATE_TRUNC(Data_Completa, YEAR)`.
* **Benefício:** Ativação do *Partition Pruning*, reduzindo drasticamente os custos de I/O.

### 5. Materialized Views (MViews)
Para agregações recorrentes em tabelas gigantescas.
* **Solução:** Criação de Views Materializadas que armazenam fisicamente os resultados e se auto-atualizam de forma incremental.

---

## 📈 Resultados Esperados

| Técnica | Impacto em Performance | Impacto em Custo |
| :--- | :--- | :--- |
| **Window Functions** | ⚡ Alta | 💰 Médio |
| **Inner Join vs Cross** | ⚡ Muito Alta | 💰 Alta |
| **Particionamento** | ⚡ Média | 💰 Muito Alta |
| **Materialized Views** | ⚡ Extrema | 💰 Alta (Recorrente) |

---

## 📂 Como utilizar o script
O arquivo `Script.sql` está organizado em blocos didáticos, contendo a versão **[RUIM]** e a versão **[OTIMIZADO]** para cada cenário mencionado acima. Basta executá-los no console do BigQuery para comparar os planos de execução.

---

## 👨‍💻 Autor
**André Luiz Colombo** 

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/andr%C3%A9-luiz-colombo-729755111/)
[![Portfolio](https://img.shields.io/badge/Portfolio-Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://andre-colombo-portfolio.streamlit.app)
