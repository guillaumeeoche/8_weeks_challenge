--data cleaning steps 

DROP TABLE IF EXISTS data_mart.clean_weekly_sales;
CREATE TABLE data_mart.clean_weekly_sales 
(
  week_date DATE, 
  week_number INTEGER,
  month_number INTEGER,
  calendar_year INTEGER,
  region STRING, 
  plateform STRING, 
  segment STRING, 
  age_band STRING,
  demographic STRING,
  customer_type STRING, 
  transactions INTEGER, 
  sales INTEGER, 
  avg_transaction FLOAT64
)
AS (
  SELECT 
    week_date, 
    EXTRACT(WEEK(MONDAY) FROM week_date) AS week_number,
    EXTRACT(MONTH FROM week_date) AS month_number,
    EXTRACT(YEAR FROM week_date) AS calendar_year,
    region, 
    platform, 
    segment,
    CASE 
      WHEN segment LIKE '%1%' THEN "Young Adults"
      WHEN segment LIKE '%2%' THEN "Middle Aged"
      WHEN segment LIKE '%3%'OR segment LIKE '%4%' THEN "Retirees"
      ELSE "unknown"
    END AS age_band,
    CASE 
      WHEN segment LIKE '%C%' THEN "Couples"
      WHEN segment LIKE '%F%' THEN "Families"
      ELSE "unknown"
    END AS demographic,
    customer_type, 
    transactions, 
    sales, 
    ROUND(sales/transactions, 2)
  FROM `ferrous-syntax-352217.data_mart.weekly_sales`
);

--What day of the week is used for each `week_date` value?

SELECT 
  FORMAT_DATE('%A', week_date) AS week_day,
  COUNT(*)
FROM data_mart.clean_weekly_sales
GROUP BY 1; 

--What range of week numbers are missing from the dataset?

SELECT 
  week_number 
FROM data_mart.clean_weekly_sales
GROUP BY 1
ORDER BY 1; 

WITH all_week_numbers AS (
 SELECT 
  *
 FROM UNNEST(GENERATE_ARRAY(1, 52)) AS week_number 
)
SELECT
  week_number 
FROM all_week_numbers AS t1
WHERE NOT EXISTS (
  SELECT 1
  FROM data_mart.clean_weekly_sales AS t2
  WHERE t1.week_number = t2.week_number
)

--How many total transactions were there for each year in the dataset?

SELECT 
  calendar_year, 
  SUM(transactions) AS total_transactions 
FROM data_mart.clean_weekly_sales 
GROUP BY calendar_year
ORDER BY 2 DESC; 

--What is the total sales for each region for each month?

SELECT 
  region, 
  month_number, 
  SUM(sales) AS total_sales
FROM `data_mart.clean_weekly_sales`
GROUP BY 
  region, 
  month_number
ORDER BY
  region, 
  month_number;

--What is the total count of transactions for each platform

SELECT 
  plateform, 
  SUM(transactions) AS transactions_number
FROM data_mart.clean_weekly_sales 
GROUP BY 
  plateform; 

--What is the percentage of sales for Retail vs Shopify for each month?

WITH cte_total_sales AS (
  SELECT
    calendar_year,
    month_number, 
    plateform, 
    SUM(sales) AS monthly_sales 
FROM data_mart.clean_weekly_sales 
GROUP BY 
  calendar_year, 
  month_number, 
  plateform 
)
SELECT 
  calendar_year, 
  month_number, 
  ROUND(
    100 * MAX(CASE WHEN plateform = "Retail" THEN monthly_sales ELSE NULL END) / 
    SUM(monthly_sales), 
    2
   ) AS retail_sales, 
  ROUND(
    100 * MAX(CASE WHEN plateform = "Shopify" THEN monthly_sales ELSE NULL END) / 
    SUM(monthly_sales),
    2
  ) AS shopify_sales
FROM cte_total_sales 
GROUP BY 
  calendar_year, 
  month_number
ORDER BY 
  calendar_year, 
  month_number;

--What is the percentage of sales by demographic for each year in the dataset?

WITH cte_total_sales AS (
  SELECT
    calendar_year,
    demographic, 
    SUM(sales) AS yearly_sales
FROM data_mart.clean_weekly_sales 
GROUP BY 
  calendar_year, 
  demographic
)
SELECT 
  calendar_year, 
  ROUND(
    100 * MAX(CASE WHEN demographic = "Couples" THEN yearly_sales ELSE NULL END) / 
    SUM(yearly_sales), 
    2
   ) AS couples_sales, 
  ROUND(
    100 * MAX(CASE WHEN demographic = "Families" THEN yearly_sales ELSE NULL END) / 
    SUM(yearly_sales),
    2
  ) AS families_sales, 
  ROUND(
    100 * MAX(CASE WHEN demographic = "unknown" THEN yearly_sales ELSE NULL END) / 
    SUM(yearly_sales),
    2
  ) AS unknown_sales
FROM cte_total_sales 
GROUP BY 
  calendar_year
ORDER BY 
  calendar_year; 

--Which age_band and demographic values contribute the most to Retail sales?

WITH cte_total_sales AS (
  SELECT
    plateform,
    demographic, 
    SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales 
GROUP BY 
  plateform,
  demographic
)
SELECT 
  plateform,
  ROUND(
    100 * MAX(CASE WHEN demographic = "Couples" THEN total_sales ELSE NULL END) / 
    SUM(total_sales), 
    2
   ) AS couples_sales, 
  ROUND(
    100 * MAX(CASE WHEN demographic = "Families" THEN total_sales ELSE NULL END) / 
    SUM(total_sales),
    2
  ) AS families_sales, 
  ROUND(
    100 * MAX(CASE WHEN demographic = "unknown" THEN total_sales ELSE NULL END) / 
    SUM(total_sales),
    2
  ) AS unknown_sales
FROM cte_total_sales 
WHERE plateform = "Retail"
GROUP BY 
  plateform;

WITH cte_total_sales AS (
  SELECT
    plateform,
    age_band, 
    SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales 
GROUP BY 
  plateform,
  age_band
)
SELECT 
  plateform,
  ROUND(
    100 * MAX(CASE WHEN age_band = "Young Adults" THEN total_sales ELSE NULL END) / 
    SUM(total_sales), 
    2
   ) AS young_adults_sales, 
  ROUND(
    100 * MAX(CASE WHEN age_band = "Middle Aged" THEN total_sales ELSE NULL END) / 
    SUM(total_sales),
    2
  ) AS middle_aged_sales, 
  ROUND(
    100 * MAX(CASE WHEN age_band = "Retirees" THEN total_sales ELSE NULL END) / 
    SUM(total_sales),
    2
  ) AS retirees_sales,
  ROUND(
    100 * MAX(CASE WHEN age_band = "unknown" THEN total_sales ELSE NULL END) / 
    SUM(total_sales),
    2
  ) AS unknown_sales
FROM cte_total_sales 
WHERE plateform = "Retail"
GROUP BY 
  plateform;

--Can we use the `avg_transaction` column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?

WITH cte_avg_transac_by_year_plateform AS (
  SELECT 
    calendar_year,
    plateform, 
    SUM(transactions) AS total_transactions, 
    SUM(sales) AS total_sales 
  FROM data_mart.clean_weekly_sales
  GROUP BY 
    calendar_year, 
    plateform
) 
SELECT 
  calendar_year, 
  plateform, 
  ROUND(total_sales/total_transactions) AS avg_transaction 
FROM cte_avg_transac_by_year_plateform
ORDER BY calendar_year;

/*
Before & After Analysis 

*This technique is usually used when we inspect an important event and want to inspect the impact before and after a certain point in time.*

**Event date : `2020-06-15`**

Before further analysis, we have to create two new tables : one before the event and one after the event. 
*/

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_before_event; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_before_event AS (
  SELECT 
    week_date, 
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      ORDER BY week_date DESC
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_date < "2020-06-15"
  GROUP BY week_date
);

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_after_event; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_after_event AS (
  SELECT 
    week_date, 
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      ORDER BY week_date 
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_date >= "2020-06-15"
  GROUP BY week_date
)

WITH cte_total_sales_4_weeks AS (
  SELECT 
    "before" AS event_state,
    SUM(total_sales) AS total_sales_4_weeks, 
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_before_event
  WHERE _row_number <= 4
  UNION ALL 
  SELECT 
    "after" AS event_state, 
    SUM(total_sales) AS total_sales_4_weeks,  
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_after_event
  WHERE _row_number <= 4
), 
cte_ranked_period AS (
  SELECT 
    event_state, 
    total_sales_4_weeks, 
    avg_transaction, 
    ROW_NUMBER() OVER(
      ORDER BY event_state DESC
    ) AS _row_state_order
  FROM cte_total_sales_4_weeks
), 
cte_diff_bw_after_before AS (
  SELECT 
    event_state, 
    total_sales_4_weeks, 
    avg_transaction, 
    LAG(total_sales_4_weeks) OVER(
      ORDER BY _row_state_order
    ) AS previous_total_sales, 
    LAG(avg_transaction) OVER(
      ORDER BY _row_state_order
    ) AS previous_avg_transaction
  FROM cte_ranked_period 
)
SELECT 
  total_sales_4_weeks - previous_total_sales AS sales_diff, 
  ROUND(
      100 * ((CAST(total_sales_4_weeks AS NUMERIC) / previous_total_sales) - 1),
      2
    ) AS sales_change
FROM cte_diff_bw_after_before
WHERE event_state = "after";


WITH cte_total_sales_12_weeks AS (
  SELECT 
    "before" AS event_state,
    SUM(total_sales) AS total_sales_12_weeks, 
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_before_event
  WHERE _row_number <= 12
  UNION ALL 
  SELECT 
    "after" AS event_state, 
    SUM(total_sales) AS total_sales_12_weeks,  
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_after_event
  WHERE _row_number <= 12
)

--How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_before_event; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_before_event AS (
  SELECT 
    calendar_year,
    week_number, 
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      PARTITION BY calendar_year
      ORDER BY week_number DESC
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_number < 24
  GROUP BY 
    calendar_year, 
    week_number
);

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_after_event; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_after_event AS (
  SELECT 
    calendar_year, 
    week_number,
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      PARTITION BY calendar_year
      ORDER BY week_number 
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_number >= 24
  GROUP BY 
    calendar_year,
    week_number
);

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_before_event_2018; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_before_event_2018 AS (
  SELECT 
    calendar_year,
    week_number, 
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      PARTITION BY calendar_year
      ORDER BY week_number DESC
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_number < 25
  GROUP BY 
    calendar_year, 
    week_number
);

DROP TABLE IF EXISTS `ferrous-syntax-352217`.data_mart.weekly_sales_after_event_2018; 
CREATE TABLE `ferrous-syntax-352217`.data_mart.weekly_sales_after_event_2018 AS (
  SELECT 
    calendar_year, 
    week_number,
    SUM(sales) AS total_sales,
    SUM(transactions) AS total_transactions,
    SUM(sales)/SUM(transactions) AS avg_transaction, 
    ROW_NUMBER() OVER( 
      PARTITION BY calendar_year
      ORDER BY week_number 
    ) AS _row_number
  FROM data_mart.clean_weekly_sales 
  WHERE week_number >= 25
  GROUP BY 
    calendar_year,
    week_number
);
```
After creating the tables, we can use them to calculate the difference between 4 weeks before/after the event by year. 

```sql
CREATE TEMP TABLE weekly_sales_4weeks_2019_2020 AS (
WITH cte_total_sales_4_weeks AS (
  SELECT 
    calendar_year,
    "1.before" AS event_state,
    SUM(total_sales) AS total_sales_4_weeks, 
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_before_event
  WHERE _row_number <= 4
  GROUP BY calendar_year
  UNION ALL 
  SELECT 
    calendar_year,
    "2.after" AS event_state, 
    SUM(total_sales) AS total_sales_4_weeks,  
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_after_event
  WHERE _row_number <= 4
  GROUP BY calendar_year
),
cte_diff_bw_after_before AS (
  SELECT 
    calendar_year,
    event_state, 
    total_sales_4_weeks, 
    avg_transaction, 
    LAG(total_sales_4_weeks) OVER(
      PARTITION BY calendar_year
      ORDER BY event_state 
    ) AS previous_total_sales, 
    LAG(avg_transaction) OVER(
      PARTITION BY calendar_year
      ORDER BY event_state 
    ) AS previous_avg_transaction
  FROM cte_total_sales_4_weeks
)
SELECT 
  calendar_year,
  total_sales_4_weeks - previous_total_sales AS sales_diff, 
  ROUND(
      100 * ((CAST(total_sales_4_weeks AS NUMERIC) / previous_total_sales) - 1),
      2
    ) AS sales_change
FROM cte_diff_bw_after_before 
WHERE event_state = "2.after"
AND calendar_year IN (2019, 2020)
ORDER BY calendar_year
); 

CREATE TEMP TABLE weekly_sales_4weeks_2018 AS (
WITH cte_total_sales_4_weeks AS (
  SELECT 
    calendar_year,
    "1.before" AS event_state,
    SUM(total_sales) AS total_sales_4_weeks, 
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_before_event_2018
  WHERE _row_number <= 4
  GROUP BY calendar_year
  UNION ALL 
  SELECT 
    calendar_year,
    "2.after" AS event_state, 
    SUM(total_sales) AS total_sales_4_weeks,  
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_after_event_2018
  WHERE _row_number <= 4
  GROUP BY calendar_year
),
cte_diff_bw_after_before AS (
  SELECT 
    calendar_year,
    event_state, 
    total_sales_4_weeks, 
    avg_transaction, 
    LAG(total_sales_4_weeks) OVER(
      PARTITION BY calendar_year
      ORDER BY event_state 
    ) AS previous_total_sales, 
    LAG(avg_transaction) OVER(
      PARTITION BY calendar_year
      ORDER BY event_state 
    ) AS previous_avg_transaction
  FROM cte_total_sales_4_weeks
)
SELECT 
  calendar_year,
  total_sales_4_weeks - previous_total_sales AS sales_diff, 
  ROUND(
      100 * ((CAST(total_sales_4_weeks AS NUMERIC) / previous_total_sales) - 1),
      2
    ) AS sales_change
FROM cte_diff_bw_after_before 
WHERE event_state = "2.after"
AND calendar_year = 2018
); 

SELECT * FROM weekly_sales_4weeks_2018
UNION ALL
SELECT * FROM weekly_sales_4weeks_2019_2020
ORDER BY calendar_year; 

--12 weeks

 SELECT 
    calendar_year,
    "1.before" AS event_state,
    SUM(total_sales) AS total_sales_12_weeks, 
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_before_event_2018
  WHERE _row_number <= 12
  GROUP BY calendar_year
  UNION ALL 
  SELECT 
    calendar_year,
    "2.after" AS event_state, 
    SUM(total_sales) AS total_sales_12_weeks,  
    SUM(total_sales)/SUM(total_transactions) AS avg_transaction
  FROM data_mart.weekly_sales_after_event_2018
  WHERE _row_number <= 12
  GROUP BY calendar_year

  --bonus question

--region
CREATE TEMP TABLE weekly_sales_12weeks_2020 AS (
WITH cte_total_sales_12_weeks AS (
  SELECT 
    region,
    "1.before" AS event_state,
    SUM(total_sales) AS total_sales_12_weeks, 
  FROM data_mart.weekly_sales_before_event
  WHERE _row_number <= 12
  GROUP BY region
  UNION ALL 
  SELECT 
    region,
    "2.after" AS event_state, 
    SUM(total_sales) AS total_sales_12_weeks,  
  FROM data_mart.weekly_sales_after_event
  WHERE _row_number <= 12
  GROUP BY region
),
cte_diff_bw_after_before AS (
  SELECT 
    region,
    event_state, 
    total_sales_12_weeks,
    LAG(total_sales_12_weeks) OVER(
      PARTITION BY region
      ORDER BY event_state 
    ) AS previous_total_sales, 
  FROM cte_total_sales_12_weeks
)
SELECT 
  region,
  total_sales_12_weeks - previous_total_sales AS sales_diff, 
  ROUND(
      100 * ((CAST(total_sales_12_weeks AS NUMERIC) / previous_total_sales) - 1),
      2
    ) AS sales_change
FROM cte_diff_bw_after_before 
WHERE event_state = "2.after"
); 


SELECT * 
FROM weekly_sales_12weeks_2020
ORDER BY sales_change; 

--plateform

CREATE TEMP TABLE weekly_sales_12weeks_2020 AS (
WITH cte_total_sales_12_weeks AS (
  SELECT 
    plateform,
    "1.before" AS event_state,
    SUM(total_sales) AS total_sales_12_weeks, 
  FROM data_mart.weekly_sales_before_event
  WHERE _row_number <= 12
  GROUP BY plateform
  UNION ALL 
  SELECT 
    plateform,
    "2.after" AS event_state, 
    SUM(total_sales) AS total_sales_12_weeks,  
  FROM data_mart.weekly_sales_after_event
  WHERE _row_number <= 12
  GROUP BY plateform
),
cte_diff_bw_after_before AS (
  SELECT 
    plateform,
    event_state, 
    total_sales_12_weeks,
    LAG(total_sales_12_weeks) OVER(
      PARTITION BY plateform
      ORDER BY event_state 
    ) AS previous_total_sales, 
  FROM cte_total_sales_12_weeks
)
SELECT 
  plateform,
  total_sales_12_weeks - previous_total_sales AS sales_diff, 
  ROUND(
      100 * ((CAST(total_sales_12_weeks AS NUMERIC) / previous_total_sales) - 1),
      2
    ) AS sales_change
FROM cte_diff_bw_after_before 
WHERE event_state = "2.after"
);