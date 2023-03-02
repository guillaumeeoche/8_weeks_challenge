
/*

YOU HAVE TO CREATE THE DATABASE BEFORE
ALL THE SQL QUERY IS IN init.sql 

*/ 

--create views to not overlap data sources

DROP SCHEMA IF EXISTS v_foodie_fi CASCADE; 
CREATE SCHEMA v_foodie_fi; 

DROP VIEW IF EXISTS v_foodie_fi.plans; 
CREATE VIEW v_foodie_fi.plans AS
SELECT * FROM foodie_fi.plans; 

DROP VIEW IF EXISTS v_foodie_fi.subscriptions; 
CREATE VIEW v_foodie_fi.subscriptions AS
SELECT * FROM foodie_fi.subscriptions; 

/* CUSTOMER JOURNEY */

SELECT 
  customer_id, 
  plans.plan_id, 
  start_date, 
  plans.plan_name, 
  RANK() OVER(
    PARTITION BY customer_id
    ORDER BY start_date
  )
FROM v_foodie_fi.subscriptions
INNER JOIN v_foodie_fi.plans 
  ON subscriptions.plan_id = plans.plan_id
WHERE customer_id in (1, 2, 11, 13, 15, 16, 18, 19);

/* EDA */

--how many customers has Foodie-Fi ever had?

SELECT 
  COUNT(DISTINCT customer_id) AS customers_number
FROM v_foodie_fi.subscriptions;


--what is the monthly distribution of trial plan start_date values for our dataset - use the start of the month as the group by value

WITH cte_month_distribution AS (
  SELECT 
    *, 
    DATE_TRUNC('month', start_date) AS day_of_month
  FROM v_foodie_fi.subscriptions
  WHERE plan_id = 0
)
SELECT 
  DATE_PART('month', day_of_month) AS month_number, 
  COUNT(DISTINCT customer_id) AS trials_number
FROM cte_month_distribution
GROUP BY 1
ORDER BY 1;