--queries
--How many unique nodes are there on the Data Bank system?

WITH combinations AS (
SELECT DISTINCT
  region_id,
  node_id
FROM data_bank.customer_nodes
)
SELECT COUNT(*) FROM combinations;

--How many unique nodes are there on the Data Bank system?
SELECT 
  customer_nodes.region_id, 
  regions.region_name, 
  COUNT(DISTINCT node_id) AS unique_nodes
FROM data_bank.customer_nodes
INNER JOIN data_bank.regions
ON customer_nodes.region_id = regions.region_id
GROUP BY 1, 2; 

--How many customers are allocated to each region?
SELECT 
  customer_nodes.region_id, 
  regions.region_name, 
  COUNT(DISTINCT customer_id) AS unique_nodes
FROM data_bank.customer_nodes
INNER JOIN data_bank.regions
ON customer_nodes.region_id = regions.region_id
GROUP BY 1, 2; 

--How many days on average are customers reallocated to a different node?

DROP TABLE IF EXISTS ranked_customer_nodes; 
CREATE TEMP TABLE ranked_customer_nodes AS 
SELECT 
  customer_id,  
  node_id, 
  start_date, 
  end_date, 
  DATE_PART('day', AGE(end_date, start_date))::INTEGER AS duration, 
  ROW_NUMBER() OVER(
    PARTITION BY customer_id
    ORDER BY start_date
  ) AS _row_number 
FROM data_bank.customer_nodes; 

WITH RECURSIVE output_table AS(
  SELECT 
    customer_id, 
    node_id, 
    duration, 
    _row_number, 
    1 AS rn_id
  FROM ranked_customer_nodes 
  WHERE _row_number = 1 
  
  UNION ALL 
  
  SELECT 
    t1.customer_id, 
    t2.node_id, 
    t2.duration, 
    t2._row_number, 
    CASE
      WHEN t1.node_id != t2.node_id THEN t1.rn_id + 1
      ELSE t1.rn_id 
    END AS rn_id
  FROM output_table AS t1
  INNER JOIN ranked_customer_nodes AS t2
    ON t1._row_number + 1 = t2._row_number
    AND t1.customer_id = t2.customer_id 
    AND t2._row_number > 1
),
cte_nodes_duration AS (
  SELECT
    customer_id, 
    rn_id, 
    SUM(duration) AS node_duration
  FROM output_table
  GROUP BY 
    customer_id, 
    rn_id
)
SELECT 
  ROUND(AVG(node_duration)) AS avg_node_duration
FROM cte_nodes_duration;

--What is the median, 80th and 95th percentile for this same reallocation days metric for each region? 

SELECT
  t2.region_name,
  PERCENTILE_CONT(0.50) WITHIN GROUP(ORDER BY node_duration) AS median,
  PERCENTILE_CONT(0.80) WITHIN GROUP(ORDER BY node_duration) AS perc_80,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY node_duration)) AS perc_95
FROM nodes_duration AS t1
INNER JOIN data_bank.regions AS t2
  ON t1.region_id = t2.region_id
GROUP BY t2.region_name
ORDER BY 1;