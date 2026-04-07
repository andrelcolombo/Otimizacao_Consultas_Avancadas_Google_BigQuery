/*******************************************************************************
 * OTIMIZAÇÃO DE CONSULTAS AVANÇADAS NO GOOGLE BIGQUERY
 * * Este Script demonstra padrões de "Consulta Ruim" vs "Consulta Otimizada",
 * focando em redução de custos (Bytes Billed) e tempo de execução (Slot Time).
 *******************************************************************************/

-- =============================================================================
-- 1. FUNÇÕES DE JANELA (WINDOW FUNCTIONS) VS. SUBCONSULTAS
-- Objetivo: Evitar o processamento de subconsultas correlacionadas.
-- =============================================================================

-- [RUIM]: Subconsulta correlacionada. 
-- Força o BigQuery a re-escanear a tabela para cada linha retornada.
-- Extremamente caro em datasets de larga escala.
SELECT
  dd.Data_Completa,
  SUM(fv.Valor) AS Valor_Diario,
  (SELECT SUM(fv2.Valor)
   FROM `projeto-loja.Loja.fato_venda` fv2
   JOIN `projeto-loja.Loja.dim_data` dd2 ON fv2.Data_ID = dd2.Data_ID
   WHERE dd2.Data_Completa <= dd.Data_Completa) AS Valor_Acumulado
FROM `projeto-loja.Loja.fato_venda` fv
JOIN `projeto-loja.Loja.dim_data` dd ON fv.Data_ID = dd.Data_ID
GROUP BY dd.Data_Completa
ORDER BY dd.Data_Completa;

-- [OTIMIZADO]: Uso de Window Functions (OVER).
-- O motor calcula o acumulado em uma única passagem pelos dados (Single Pass),
-- distribuindo o processamento de forma paralela e eficiente.
SELECT
  dd.Data_Completa,
  SUM(fv.Valor) AS Valor_Diario,
  SUM(SUM(fv.Valor)) OVER (ORDER BY dd.Data_Completa) AS Valor_Acumulado
FROM `projeto-loja.Loja.fato_venda` fv
JOIN `projeto-loja.Loja.dim_data` dd ON fv.Data_ID = dd.Data_ID
GROUP BY dd.Data_Completa
ORDER BY dd.Data_Completa;


-- =============================================================================
-- 2. ESTRATÉGIAS DE JUNÇÃO (JOINS)
-- Objetivo: Evitar o produto cartesiano que esgota a memória dos slots.
-- =============================================================================

-- [RUIM]: CROSS JOIN acidental ou mal planejado.
-- Gera uma combinação de todas as linhas de A com todas de B.
-- Em tabelas grandes, isso causa o erro "Resources Exceeded".
SELECT
  dc.Nome AS Nome_Cliente,
  SUM(fv.Valor) AS Total_Vendas
FROM `projeto-loja.Loja.dim_cliente` dc
CROSS JOIN `projeto-loja.Loja.fato_venda` fv
GROUP BY dc.Nome;

-- [OTIMIZADO]: INNER JOIN com chaves de relacionamento.
-- O BigQuery utiliza algoritmos de Hash Join para associar registros de forma 
-- performática, processando apenas os pares de dados que fazem sentido.
SELECT
  dc.Nome AS Nome_Cliente,
  SUM(fv.Valor) AS Total_Vendas
FROM `projeto-loja.Loja.fato_venda` fv
INNER JOIN `projeto-loja.Loja.dim_cliente` dc ON fv.Cliente_ID = dc.Cliente_ID
GROUP BY dc.Nome;


-- =============================================================================
-- 3. SIMPLIFICAÇÃO DE CTEs (Common Table Expressions)
-- Objetivo: Eliminar overhead de processamento desnecessário.
-- =============================================================================

-- [RUIM]: Overhead de Window Functions e Agrupamentos Inúteis.
-- O uso de ROW_NUMBER() sem finalidade de filtro e o agrupamento por 
-- colunas de alta cardinalidade impedem a otimização do plano de execução.
WITH vendas_por_ano AS (
  SELECT
    sub.Ano,
    SUM(sub.Valor) AS Total_Vendas_Ano
  FROM (
    SELECT
      dd.Ano,
      fv.Valor,
      ROW_NUMBER() OVER (PARTITION BY dd.Ano ORDER BY fv.Valor DESC) AS RowNum
    FROM `projeto-loja.Loja.fato_venda` fv
    JOIN `projeto-loja.Loja.dim_data` dd ON fv.Data_ID = dd.Data_ID
  ) AS sub
  GROUP BY sub.Ano, sub.RowNum
  HAVING SUM(sub.Valor) > 0
)
SELECT * FROM vendas_por_ano ORDER BY Ano;

-- [OTIMIZADO]: Lógica enxuta.
-- Removendo funções de rankeamento e subconsultas desnecessárias, 
-- permitindo que o BigQuery aplique agregações diretas e mais rápidas.
WITH vendas_por_ano AS (
  SELECT
    dd.Ano,
    SUM(fv.Valor) AS Total_Vendas_Ano
  FROM `projeto-loja.Loja.fato_venda` fv
  JOIN `projeto-loja.Loja.dim_data` dd ON fv.Data_ID = dd.Data_ID
  GROUP BY dd.Ano
)
SELECT * FROM vendas_por_ano ORDER BY Ano;


-- =============================================================================
-- 4. PARTICIONAMENTO DE TABELAS
-- Objetivo: Reduzir a leitura de dados (I/O) e o custo financeiro da consulta.
-- =============================================================================

-- [CONTEXTO]: Consultas em tabelas não particionadas realizam "Full Table Scan",
-- lendo todas as colunas e linhas mesmo que o filtro peça apenas um ano.

-- PASSO 1: Criação da tabela particionada (Melhor prática de design)
CREATE OR REPLACE TABLE `projeto-loja.Loja.fato_venda_particionada`
PARTITION BY DATE_TRUNC(Data_Completa, YEAR) -- Define a poda de dados por Ano
AS
SELECT
  fv.*,
  dd.Data_Completa
FROM `projeto-loja.Loja.fato_venda` fv
JOIN `projeto-loja.Loja.dim_data` dd ON fv.Data_ID = dd.Data_ID;

-- [OTIMIZADO]: Consulta com Poda de Partição (Partition Pruning).
-- O BigQuery lerá apenas o "pedaço" da tabela referente ao ano de 2025.
SELECT
  EXTRACT(YEAR FROM Data_Completa) AS Ano,
  SUM(Valor) AS Total_Vendas
FROM `projeto-loja.Loja.fato_venda_particionada`
WHERE Data_Completa BETWEEN '2025-01-01' AND '2025-12-31' -- Filtro na coluna de partição
GROUP BY 1;


-- =============================================================================
-- 5. MATERIALIZED VIEWS (VIEWS MATERIALIZADAS)
-- Objetivo: Cachear agregações para consultas recorrentes e dashboards.
-- =============================================================================

-- [PROBLEMA]: Tabelas de fatos gigantes que são agregadas da mesma forma 
-- centenas de vezes ao dia, gerando custos repetitivos.

-- [SOLUÇÃO]: Criar uma Materialized View.
-- Diferente de uma View comum, a MV armazena fisicamente o resultado agregado
-- e se auto-atualiza de forma incremental.
CREATE MATERIALIZED VIEW `projeto-loja.Loja.mv_total_vendas_produto` AS
SELECT
  Produto_ID,
  SUM(Quantidade) AS Total_Quantidade,
  SUM(Valor) AS Total_Valor
FROM `projeto-loja.Loja.fato_venda`
GROUP BY Produto_ID;

-- Consulta à MV: Alta performance e custo quase zero de processamento.
SELECT * FROM `projeto-loja.Loja.mv_total_vendas_produto`
WHERE Total_Valor > 1000;

-- FIM 